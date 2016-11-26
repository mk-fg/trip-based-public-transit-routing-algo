#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict, OrderedDict
from pathlib import Path
import os, sys, re, csv, math, time, datetime

import tb_routing as tb

try: import pytz
except ImportError: pytz = None


@tb.u.attr_struct(vals_to_attrs=True)
class Conf:

	# Filtering for parser will only produce timetable data (trips/footpaths)
	#  for specific days, with ones after parse_start_date having 24h*N time offsets.
	# Trips starting on before parse_start_date (and up to parse_days_pre) will also
	#  be processed, so that e.g. journeys starting at midnight on that day can use them.
	parse_start_date = None # datetime.date object or YYYYMMDD string
	parse_days = 2 # should be >= 1
	parse_days_pre = 1 # also >= 1

	# gtfs_timezone is only used if parse_start_date is set.
	# It is important to account for stuff like daylight saving time, leap seconds, etc
	# To understand why, answer a question:
	#  how many seconds are between 0:00 and 6:00? (not always 6*3600)
	gtfs_timezone = 'Europe/London' # pytz zone name or datetime.timezone

	group_stops_into_stations = False # use "parent_station" to group all stops into one under its id

	# Options for footpath-generation - not used if transfers.txt is non-empty
	dt_ch = 2*60 # fixed time-delta overhead for changing trips (i.e. p->p footpaths)
	footpath_dt_base = 2*60 # footpath_dt = dt_base + km / speed_kmh
	footpath_speed_kmh = 5 / 3600
	footpath_dt_max = 7*60 # all footpaths longer than that are discarded as invalid

log = tb.u.get_logger('gtfs-cli')


@tb.u.attr_struct
class GTFSTimeOffset:
	keys = 'd h m s'

	@classmethod
	def parse(cls, ts_str):
		if ':' not in ts_str: return
		ts_list = list(int(v.strip()) for v in ts_str.split(':'))
		if len(ts_list) == 2: ts_list.append(0)
		days, hours = divmod(ts_list[0], 24)
		return cls(days, hours, ts_list[1], ts_list[2])

	@property
	def flat(self):
		return (self.d * 24 + self.h) * 3600 + self.m * 60 + self.s

	def apply_to_datetime(self, dt):
		d, h, m, s = u.attr.astuple(self)
		if d > 0:
			# Daylight savings jump must only be accounted for on the first day
			#  of the offset, and adding timedelta with >1 *days* will get that wrong.
			# Assuming that won't be an issue with adding delta with >24 *hours* though.
			# XXX: test assumption above
			dt, d = dt + timedelta(days=1), d - 1
		dt = dt.replace(hours=h, minutes=m, seconds=s)
		if d > 0: dt += timedelta(hours=d * 24)
		return dt

class CalendarException(enum.Enum): added, removed = '1', '2'


def calculate_dts(dt_start, dt, offset_arr, offset_dep):
	if dt is None:
		# Either both dt_start and dt are None or neither,
		#  otherwise dts values won't make sense according to one of them.
		assert dt_start is None
		return offset_arr.flat, offset_dep.flat
	if not offset_arr:
		if not trip: # first stop of the trip - arrival ~ departure
			if offset_dep: offset_arr = offset_dep
			else: raise ValueError('Missing arrival/departure times for trip stop: {}'.format(ts))
		else: offset_arr = trip[-1].offset_dep # "scheduled based on the nearest preceding timed stop"
	if offset_arr_prev is not None:
		if offset_arr < offset_arr_prev: offset_arr.days += 1 # assuming bogus 24:00 -> 00:00 wrapping
	offset_arr_prev = offset_arr
	if not offset_dep: offset_dep = offset_arr
	assert offset_arr and offset_dep
	dt_arr, dt_dep = (o.apply_to_datetime(dt) for o in [offset_arr, offset_dep])
	dts_arr, dts_dep = ((dt - dt_start).total_seconds() for dt in [dt_arr, dt_dep])
	return dts_arr, dts_dep

def footpath_dt(stop_a, stop_b, dt_base, speed_kmh, math=math):
	'''Calculate footpath time-delta (dt) between two stops,
		based on their lon/lat distance (using Haversine Formula) and walking-speed constant.'''
	# Alternative: use UTM coordinates and KDTree (e.g. scipy) or spatial dbs
	lon1, lat1, lon2, lat2 = (
		math.radians(float(v)) for v in
		[stop_a.lon, stop_a.lat, stop_b.lon, stop_b.lat] )
	km = 6367 * 2 * math.asin(math.sqrt(
		math.sin((lat2 - lat1)/2)**2 +
		math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))
	return dt_base + km / speed_kmh

def iter_gtfs_tuples(gtfs_dir, filename, empty_if_missing=False):
	log.debug('Processing gtfs file: {}', filename)
	if filename.endswith('.txt'): filename = filename[:-4]
	tuple_t = ''.join(' '.join(filename.rstrip('s').split('_')).title().split())
	p = gtfs_dir / '{}.txt'.format(filename)
	if empty_if_missing and not os.access(str(p), os.R_OK): return
	with p.open(encoding='utf-8-sig') as src:
		src_csv = csv.reader(src)
		fields = list(v.strip() for v in next(src_csv))
		tuple_t = namedtuple(tuple_t, fields)
		for line in src_csv:
			try: yield tuple_t(*line)
			except TypeError:
				log.debug('Skipping bogus CSV line (file: {}): {!r}', p, line)

def parse_gtfs_timetable(gtfs_dir, conf):
	'Parse Timetable from GTFS data directory.'
	# XXX: split this into a separate submodule
	# Stops/footpaths that don't belong to trips are discarded here
	types = tb.t.public

	stop_dict, stop_sets = dict(), dict() # {id: stop}, {id: station_stops}
	for t in iter_gtfs_tuples(gtfs_dir, 'stops'):
		stop = types.Stop(t.stop_id, t.stop_name, float(t.stop_lon), float(t.stop_lat))
		stop_set_id = t.parent_station or t.stop_id
		stop_dict[t.stop_id] = stop_set_id, stop
		if not t.parent_station: stop_sets[t.stop_id] = {stop}
		else:
			stop_sets[t.stop_id] = stop_sets.setdefault(stop_set_id, set())
			stop_sets[stop_set_id].add(stop)
	if conf.group_stops_into_stations:
		for stop_id in stop_dict: # resolve all stops to stations
			stop_dict[stop_id] = stop_dict[stop_dict[stop_id][0]]
	stop_dict, stop_sets = (
		dict((k, stop) for k, (k_set, stop) in stop_dict.items()),
		dict((k, stop_sets[k_set]) for k, (k_set, stop) in stop_dict.items()) )

	dt_start = service_days = None
	date_map = date_min_str = date_max_str = None
	if conf.parse_start_date:
		assert conf.parse_days >= 1 and conf.parse_days_pre >= 1
		assert pytz, 'pytz is required when processing calendar.txt'
		weekday_cols = 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'
		date_min, gtfs_date_fmt = conf.parse_start_date, '%Y%m%d'
		if isinstance(conf.parse_start_date, str):
			date_min = datetime.date.strptime(date_min, gtfs_date_fmt)
		date_min -= datetime.timedelta(days=conf.parse_days_pre)
		date_map = list( (date_min + datetime.timedelta(n))
			for n in range(conf.parse_days + conf.parse_days_pre) )
		date_min_str, date_max_str = (d.strftime(gtfs_date_fmt) for d in [date_min, date_map[-1]])
		date_map = OrderedDict((d.strftime(gtfs_date_fmt), d) for d in date_map)

		dt_start = conf.gtfs_timezone
		if isinstance(dt_start, str): dt_start = pytz.timezone(dt_start)
		dt_start = datetime.datetime(date_min.year, date_min.month, date_min.day, tzinfo=dt_start)

		service_exceptions = defaultdict(ft.partial(defaultdict, set))
		for s in iter_gtfs_tuples(gtfs_dir, 'calendar_dates'):
			service_exceptions[s.service_id][CalendarException(s.exception_type)].add(s.date)

		service_days = dict() # {service_id (int): datetimes (seq)}
		for s in iter_gtfs_tuples(gtfs_dir, 'calendar'):
			if not (s.start_date >= date_max_str and s.end_date <= date_min_str): continue
			days = service_days.setdefault(s.service_id, dict())

			parse_days = dict((date_str, (False, date)) for date_str, date in date_map.items())
			for t, date_str in service_exceptions[s.service_id]:
				if not (date_min_str <= date_str <= date_max_str): continue
				if t == CalendarException.added:
					parse_days[date_str] = True, datetime.date.strptime(date_str, gtfs_date_fmt)
				elif t == CalendarException.removed: parse_days.pop(date_str, None)
				else: raise ValueError(t)

			for date_str, (exc, date) in sorted(parse_days.items()):
				if not exc:
					if date_str < s.start_date: continue
					elif date_str > s.end_date: break
				weekday_value = getattr(s, weekday_cols[date.weekday()])
				if not (weekday_value and int(weekday_value)): continue
				days[date_str] = datetime.datetime(date.year, date.month, date.day, tzinfo=dt_start)

		if not service_offsets:
			log.debug('No services were found to be operational on specified days')

	trip_stops = defaultdict(list)
	for t in iter_gtfs_tuples(gtfs_dir, 'stop_times'): trip_stops[t.trip_id].append(t)

	trips, stops = types.Trips(), types.Stops()
	for t in iter_gtfs_tuples(gtfs_dir, 'trips'):
		if service_days is not None:
			days = service_days.get(t.service_id)
			if not days: continue
		else: days = [None]
		for dt in days:
			trip, dts_arr_prev = types.Trip(), None
			for stopidx, ts in enumerate(
					sorted(trip_stops[t.trip_id], key=lambda t: int(t.stop_sequence)) ):
				dts_arr, dts_dep = calculate_dts( dt_start,
					*map(GTFSTimeOffset.parse, [ts.arrival_time, ts.departure_time]) )
				stop = stops.add(stop_dict[ts.stop_id])
				trip.add(types.TripStop(trip, stopidx, stop, dts_arr, dts_dep))
				if trip: trips.add(trip)

	footpaths, fp_samestop_count, fp_synth = types.Footpaths(), 0, False
	get_stop_set = lambda stop_id: list(filter(stops.get, stop_sets.get(stop_id, list())))
	for src_type in 'transfers', 'links':
		for t in iter_gtfs_tuples(gtfs_dir, name, empty_if_missing=True):
			# XXX: make footpaths properly dts-dependent
			# Current hack is to simply ok any footpath that falls into date range
			if date_map and src_type == 'links'\
				and not (s.start_date >= date_max_str and s.end_date <= date_min_str): continue
			stops_from, stops_to = map(get_stop_set, [t.from_stop_id, t.to_stop_id])
			if not (stops_from and stops_to): continue
			dt = tb.u.get_any(t._asdict(), 'min_transfer_time', 'link_secs')
			if dt is None:
				log.debug('Missing transfer time value in CSV tuple: {}', t)
				continue
			for stop_from, stop_to in it.product(stops_from, stops_to):
				if stop_from == stop_to: fp_samestop_count += 1
				footpaths.add(stop_from, stop_to, int(dt))

	if not len(footpaths):
		log.debug('No transfers/links data found, generating synthetic footpaths from lon/lat')
		fp_synth, fp_dt = True, ft.partial( footpath_dt,
			dt_base=conf.footpath_dt_base, speed_kmh=conf.footpath_speed_kmh )
		for stop_a, stop_b in it.permutations(list(stops), 2):
			footpaths.add(stop_a, stop_b, fp_dt(stop_a, stop_b))
		footpaths.discard_longer(conf.footpath_dt_max)
	if fp_samestop_count < len(stops) / 2:
		if not fp_synth:
			log.debug(
				'Generating missing same-stop footpaths (dt_ch={}),'
					' because source data seem to have very few of them - {} for {} stops',
				conf.dt_ch, fp_samestop_count, len(stops) )
		for stop in stops:
			try: footpaths.between(stop, stop)
			except KeyError:
				footpaths.add(stop, stop, conf.dt_ch)
				fp_samestop_count += 1

	return types.Timetable(dt_start, stops, footpaths, trips)


def calc_timer(func, *args, log=tb.u.get_logger('timer'), **kws):
	func_id = '.'.join([func.__module__.strip('__'), func.__name__])
	log.debug('[{}] Starting...', func_id)
	td = time.monotonic()
	data = func(*args, **kws)
	td = time.monotonic() - td
	log.debug('[{}] Finished in: {:.1f}s', func_id, td)
	return data

def init_gtfs_router( path, cache_path=None,
		conf=None, conf_engine=None, path_timetable=False, timer_func=None ):
	if not conf: conf = Conf()
	timetable_func = parse_gtfs_timetable\
		if not timer_func else ft.partial(timer_func, parse_gtfs_timetable)
	router_factory = ft.partial(
		tb.engine.TBRoutingEngine, conf=conf_engine, timer_func=timer_func )
	graph = tb.u.pickle_load(cache_path) if cache_path else None
	if not graph:
		path = Path(path)
		if not path_timetable: timetable = timetable_func(path, conf)
		else: timetable = tb.u.pickle_load(path, fail=True)
		log.debug(
			'Parsed timetable: stops={:,}, footpaths={:,}'
				' (mean_dt={:,.1f}s, same-stop={:,}), trips={:,} (mean_stops={:,.1f})',
			len(timetable.stops), len(timetable.footpaths),
			timetable.footpaths.stat_mean_dt(),
			timetable.footpaths.stat_same_stop_count(),
			len(timetable.trips), timetable.trips.stat_mean_stops() )
		router = router_factory(timetable)
		if cache_path: tb.u.pickle_dump(router.graph, cache_path)
	else:
		timetable = graph.timetable
		router = router_factory(cached_graph=graph)
	return timetable, router


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of trip-based graph-db and algorithms.')
	parser.add_argument('gtfs_dir', help='Path to gtfs data directory to build graph from.')

	group = parser.add_argument_group('Graph options')
	group.add_argument('-c', '--cache', metavar='path',
		help='Pickle cache-file to load (if exists)'
			' or save (if missing) resulting graph data from/to.')
	group.add_argument('-s', '--stops-to-stations', action='store_true',
		help='Convert/translate GTFS "stop" ids to "parent_station" ids,'
				' i.e. group all stops on the station into a single one.'
			' Can produce smaller graphs that would be easier to query.')

	group = parser.add_argument_group('Misc/debug options')
	group.add_argument('-t', '--timetable', action='store_true',
		help='Treat "gtfs_dir" argument as a pickled TImetable object.')
	group.add_argument('--dot-for-lines', metavar='path',
		help='Dump Stop/Line graph (in graphviz dot format) to a specified file and exit.')
	group.add_argument('--dot-opts', metavar='yaml-data',
		help='Options for graphviz graph/nodes/edges to use with all'
			' --dot-for-* commands, as a YAML mappings. Example: {graph: {rankdir: LR}}')
	group.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')

	cmds = parser.add_subparsers(title='Commands', dest='call')


	cmd = cmds.add_parser('query-earliest-arrival',
		help='Run earliest arrival query, output resulting journey set.')
	cmd.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	cmd.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	cmd.add_argument('day_time', nargs='?', default='00:00',
		help='Day time to start journey at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')


	cmd = cmds.add_parser('query-profile',
		help='Run profile query, output resulting journey set.')

	group = cmd.add_argument_group('Query parameters')
	group.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	group.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	group.add_argument('day_time_earliest', nargs='?', default='00:00',
		help='Earliest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')
	group.add_argument('day_time_latest', nargs='?', default='24:00',
		help='Latest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')

	group = cmd.add_argument_group('Limits')
	group.add_argument('-m', '--max-transfers',
		type=int, metavar='n', default=15,
		help='Max number of transfers (i.e. interchanges)'
			' between journey trips allowed in the results. Default: %(default)s')


	cmd = cmds.add_parser('query-transfer-patterns',
		help='Build/load Transfer-Patterns trie and run queries on it.')

	group = cmd.add_argument_group('Query parameters')
	group.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	group.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	group.add_argument('day_time_earliest', nargs='?', default='00:00',
		help='Earliest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')
	group.add_argument('day_time_latest', nargs='?', default='24:00',
		help='Latest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')

	group = cmd.add_argument_group('Limits')
	group.add_argument('-m', '--max-transfers',
		type=int, metavar='n', default=15,
		help='Max number of transfers (i.e. interchanges)'
			' between journey trips allowed in the results. Default: %(default)s')

	group = cmd.add_argument_group('Graph options')
	group.add_argument('--tree-cache', metavar='path',
		help='Pickle cache-file to load (if exists)'
			' or save (if missing) resulting Transfer-Patterns'
			' prefix-tree from/to (see arXiv:1607.01299v2 paper).')

	group = cmd.add_argument_group('Misc/debug options')
	group.add_argument('--dot-for-tp-subtree', metavar='path',
		help='Dump TB-TP subtree graph for specified'
			' stop_from (in graphviz dot format) to a file and exit.')
	group.add_argument('--dot-for-tp-query-tree', metavar='path',
		help='Dump TB-TP query tree graph for specified'
			' stop_from/stop_to pair (in graphviz dot format) to a file and exit.')


	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	tb.u.logging.basicConfig(
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		level=tb.u.logging.DEBUG if opts.debug else tb.u.logging.WARNING )

	conf = Conf()
	if opts.stops_to_stations: conf.group_stops_into_stations = True
	conf_engine = tb.engine.EngineConf(
		log_progress_for={'lines', 'pre-initial-set', 'pre-reduction', 'transfer-patterns'} )
	timetable, router = init_gtfs_router( opts.gtfs_dir,
		opts.cache, conf_engine=conf_engine,
		path_timetable=opts.timetable, timer_func=calc_timer )

	dot_opts = dict()
	if opts.dot_opts:
		import yaml
		dot_opts = yaml.safe_load(opts.dot_opts)
	if opts.dot_for_lines:
		with tb.u.safe_replacement(opts.dot_for_lines) as dst:
			tb.vis.dot_for_lines(router.graph.lines, dst, dot_opts=dot_opts)
		return

	if opts.call == 'query-earliest-arrival':
		dts_start = tb.u.dts_parse(opts.day_time)
		a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]
		journeys = router.query_earliest_arrival(a, b, dts_start)
		journeys.pretty_print()

	elif opts.call == 'query-profile':
		dts_edt, dts_ldt = tb.u.dts_parse(opts.day_time_earliest), tb.u.dts_parse(opts.day_time_latest)
		a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]
		journeys = router.query_profile(a, b, dts_edt, dts_ldt, max_transfers=opts.max_transfers)
		journeys.pretty_print()

	elif opts.call == 'query-transfer-patterns':
		dts_edt, dts_ldt = tb.u.dts_parse(opts.day_time_earliest), tb.u.dts_parse(opts.day_time_latest)
		a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]

		cache_path = opts.tree_cache
		tp_tree = tb.u.pickle_load(cache_path) if cache_path else None
		tp_router = router.build_tp_engine(tp_tree, max_transfers=opts.max_transfers)
		if not tp_tree and cache_path: tb.u.pickle_dump(tp_router.tree, cache_path)

		if opts.dot_for_tp_subtree:
			with tb.u.safe_replacement(opts.dot_for_tp_subtree) as dst:
				tb.vis.dot_for_tp_subtree(tp_router.tree[a], dst, dst_to_src=True, dot_opts=dot_opts)
			return

		query_tree = tp_router.build_query_tree(a, b)
		if opts.dot_for_tp_query_tree:
			with tb.u.safe_replacement(opts.dot_for_tp_query_tree) as dst:
				tb.vis.dot_for_tp_subtree(query_tree, dst, dot_opts=dot_opts)
			return

		journeys = tp_router.query_profile(a, b, dts_edt, dts_ldt, query_tree)
		journeys.pretty_print()

	else: parser.error('Action not implemented: {}'.format(opts.call))

if __name__ == '__main__': sys.exit(main())
