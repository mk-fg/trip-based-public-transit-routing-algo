#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict
from pathlib import Path
import os, sys, re, csv, math, time

import tb_routing as tb


@tb.u.attr_struct(vals_to_attrs=True)
class Conf:
	dt_ch = 2*60 # fixed time-delta overhead for changing trips (i.e. p->p footpaths)
	stop_linger_time_default = 5*60 # used if departure-time is missing
	footpath_dt_base = 2*60 # footpath_dt = dt_base + km / speed_kmh
	footpath_speed_kmh = 5 / 3600
	footpath_dt_max = 7*60 # all footpaths longer than that are discarded as invalid

log = tb.u.get_logger('gtfs-cli')


def iter_gtfs_tuples(gtfs_dir, filename):
	if filename.endswith('.txt'): filename = filename[:-4]
	tuple_t = ''.join(' '.join(filename.rstrip('s').split('_')).title().split())
	with (gtfs_dir / '{}.txt'.format(filename)).open(encoding='utf-8-sig') as src:
		src_csv = csv.reader(src)
		tuple_t = namedtuple(tuple_t, list(v.strip() for v in next(src_csv)))
		for line in src_csv: yield tuple_t(*line)

def parse_gtfs_dts(ts_str):
	if ':' not in ts_str: return
	return sum((mul * int(v)) for mul, v in zip([3600, 60, 1], ts_str.split(':')))

def footpath_dt(stop_a, stop_b, dt_ch, dt_base, speed_kmh, math=math):
	'''Calculate footpath time-delta (dt) between two stops,
		based on their lon/lat distance (using Haversine Formula) and walking-speed constant.'''
	# Alternative: use UTM coordinates and KDTree (e.g. scipy) or spatial dbs
	if stop_a is stop_b: return dt_ch
	lon1, lat1, lon2, lat2 = (
		math.radians(float(v)) for v in
		[stop_a.lon, stop_a.lat, stop_b.lon, stop_b.lat] )
	km = 6367 * 2 * math.asin(math.sqrt(
		math.sin((lat2 - lat1)/2)**2 +
		math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))
	return dt_base + km / speed_kmh

def parse_gtfs_timetable(gtfs_dir, conf):
	'Parse Timetable from GTFS data directory.'
	types = tb.t.public

	stops_dict = dict() # only subset that is part of trips will be used
	for t in iter_gtfs_tuples(gtfs_dir, 'stops'):
		stops_dict[t.stop_id] = types.Stop(
			t.stop_id, t.stop_name, float(t.stop_lon), float(t.stop_lat) )

	trip_stops = defaultdict(list) # only subset listed in trips.txt will be used
	for t in iter_gtfs_tuples(gtfs_dir, 'stop_times'): trip_stops[t.trip_id].append(t)

	trips, stops = types.Trips(), types.Stops()
	for t in iter_gtfs_tuples(gtfs_dir, 'trips'):
		trip = types.Trip()
		for stopidx, ts in enumerate(
				sorted(trip_stops[t.trip_id], key=lambda t: int(t.stop_sequence)) ):
			dts_arr, dts_dep = map(parse_gtfs_dts, [ts.arrival_time, ts.departure_time])
			if not dts_arr:
				if not trip: # first stop of the trip - arrival ~ departure
					if dts_dep: dts_arr = dts_dep - conf.stop_linger_time_default
					else: continue
				else: dts_arr = trip[-1].dts_dep # "scheduled based on the nearest preceding timed stop"
			if not dts_dep: dts_dep = dts_arr + conf.stop_linger_time_default
			stop = stops.add(stops_dict[ts.stop_id])
			trip.add(types.TripStop(trip, stopidx, stop, dts_arr, dts_dep))
		if trip: trips.add(trip)

	footpaths, fp_samestop_count = types.Footpaths(), 0
	fp_dt = ft.partial( footpath_dt, dt_ch=conf.dt_ch,
		dt_base=conf.footpath_dt_base, speed_kmh=conf.footpath_speed_kmh )
	for stop_a, stop_b in it.combinations_with_replacement(list(stops), 2):
		footpaths.add(stop_a, stop_b, fp_dt(stop_a, stop_b))
		if stop_a is stop_b: fp_samestop_count += 1
	footpaths.discard_longer(conf.footpath_dt_max)

	log.debug(
		'Parsed timetable: stops={:,}, footpaths={:,}'
			' (mean_dt={:,.1f}s, same-stop={:,}), trips={:,} (mean_stops={:,.1f})',
		len(stops),
		len(footpaths), footpaths.stat_mean_dt(), fp_samestop_count,
		len(trips), trips.stat_mean_stops() )
	return types.Timetable(conf.dt_ch, stops, footpaths, trips)


def dts_parse(dts_str):
	if ':' not in dts_str: return float(dts_str)
	dts_vals = dts_str.split(':')
	if len(dts_vals) == 2: dts_vals.append('00')
	return sum(int(n)*k for k, n in zip([3600, 60, 1], dts_vals))

def calc_timer(func, *args, log=tb.u.get_logger('timer'), **kws):
	func_id = '.'.join([func.__module__.strip('__'), func.__name__])
	log.debug('[{}] Starting...', func_id)
	td = time.monotonic()
	data = func(*args, **kws)
	td = time.monotonic() - td
	log.debug('[{}] Finished in: {:.1f}s', func_id, td)
	return data

def init_gtfs_router(gtfs_dir, cache_path=None, conf=None, timer_func=None):
	if not conf: conf = Conf()
	timetable_func = parse_gtfs_timetable\
		if not timer_func else ft.partial(timer_func, parse_gtfs_timetable)
	router_factory = ft.partial(tb.engine.TBRoutingEngine, timer_func=timer_func)
	graph = tb.u.pickle_load(cache_path) if cache_path else None
	if not graph:
		timetable = timetable_func(Path(gtfs_dir), conf)
		router = router_factory(timetable)
		if cache_path: tb.u.pickle_dump(router.graph, cache_path)
	else:
		timetable = graph.timetable
		router = router_factory(cached_graph=graph)
	return timetable, router

def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of graph-db and algos on top of that.')
	parser.add_argument('gtfs_dir', help='Path to gtfs data directory to build graph from.')

	parser.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	parser.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	parser.add_argument('day_time', nargs='?', default='00:00',
		help='Day time to start journey at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')

	parser.add_argument('-c', '--cache', metavar='path',
		help='Pickle cache-file to load (if exists)'
			' or save (if missing) resulting graph data from/to.')

	parser.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	tb.u.logging.basicConfig(
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		level=tb.u.logging.DEBUG if opts.debug else tb.u.logging.WARNING )

	dts_start = dts_parse(opts.day_time)
	timetable, router = init_gtfs_router(opts.gtfs_dir, opts.cache, timer_func=calc_timer)

	a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]
	journeys = router.query_earliest_arrival(a, b, dts_start)
	journeys.pretty_print()

if __name__ == '__main__': sys.exit(main())
