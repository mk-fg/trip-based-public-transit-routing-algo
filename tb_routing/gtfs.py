import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict, OrderedDict
import os, sys, re, csv, math, datetime, enum

try: import pytz
except ImportError: pytz = None

from . import utils as u, types as t


log = u.get_logger('gtfs')


@u.attr_struct(vals_to_attrs=True)
class GTFSConf:

	# Filtering for parser will only produce timetable data (trips/footpaths)
	#  for specific days, with ones after parse_start_date having 24h*N time offsets.
	# Trips starting on before parse_start_date (and up to parse_days_pre) will also
	#  be processed, so that e.g. journeys starting at midnight on that day can use them.
	parse_start_date = None # datetime.datetime object or YYYYMMDD string
	parse_days = 2 # should be >= 1 and up to max days limit for journey (probably 1-2)
	parse_days_pre = 1 # also >= 1 would make sense

	# gtfs_timezone is only used if parse_start_date is set.
	# It is important to account for stuff like daylight saving time, leap seconds, etc
	# To understand why, answer a question:
	#  how many seconds are between 0:00 and 6:00? (not always 6*3600)
	gtfs_timezone = 'Europe/London' # pytz zone name or datetime.timezone

	group_stops_into_stations = False # use "parent_station" to group all stops into one under its id

	# Options for footpath-generation - not used if transfers.txt is non-empty
	delta_ch = 2*60 # fixed time-delta overhead for changing trips (i.e. p->p footpaths)
	footpath_delta_base = 2*60 # footpath_delta = delta_base + km / speed_kmh
	footpath_speed_kmh = 5 / 3600
	footpath_delta_max = 7*60 # all footpaths longer than that are discarded as invalid
	footpath_gen_thresholds = 0, 0.5


weekday_columns = [ 'monday', 'tuesday',
	'wednesday', 'thursday', 'friday', 'saturday', 'sunday' ]

class CalendarException(enum.Enum): added, removed = '1', '2'

def dt_adjust(dt, d=0, h=0, m=0, s=0, subtract=False):
	'''Apply timedelta objects in a sensible manner,
			where adding N days only adjusts date, never time.
		Note that in general: "dt - delta != dt + (-delta)",
			hence `subtract` and negative values are only allowed in `d`.'''
	if isinstance(d, datetime.timedelta):
		d, h, m, s= d.days, d.hours, d.minutes, d.seconds
		assert not any([d.microseconds, d.milliseconds, d.weeks])
	if h == m == s == 0: # adding days should only adjust date, not time
		if d == 0: return dt
		if d < 0:
			assert not subtract, [d, subtract]
			d, subtract = -d, True
		dt = (dt + datetime.timedelta(d)) if not subtract else (dt - datetime.timedelta(d))
		return dt.tzinfo.localize(dt.replace(tzinfo=None))
	else:
		assert not d, 'Adjusting both date by days= and time - probably a bug'
		assert h >= 0 and m >= 0 and s >= 0
		delta = datetime.timedelta(hours=h, minutes=m, seconds=s)
		return dt.tzinfo.normalize((dt + delta) if not subtract else (dt - delta))

@u.attr_struct
class GTFSTimeOffset:
	keys = 'd h m s'

	# In GTFS stop_times.txt "00:20" can actually mean 01:20 in localtime
	#  or 23:20 of the previous day, when DST-related time jump happens.
	#
	# Quote:
	#  The time is measured from "noon minus 12h"
	#   (effectively midnight, except for days on which daylight
	#   savings time changes occur) at the beginning of the service date.
	# https://developers.google.com/transit/gtfs/reference/stop_times-file

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
		'''Returns datetime with this offset applied to date specified in `dt`.
			Any time set there will be disregarded.'''
		d, h, m, s = u.attr.astuple(self)
		dt = dt_adjust(dt, d=d).replace(hour=12, minute=0, second=0) # noon of specified day
		dt = dt_adjust(dt, h=12, subtract=True) # "noon minus 12h"
		return dt_adjust(dt, h=h, m=m, s=s) # "noon minus 12h" + time offset

ServiceCalendarEntry = namedtuple('SCE', 'date_start date_end weekdays')


def iter_gtfs_tuples(gtfs_dir, filename, empty_if_missing=False, yield_fields=False):
	log.debug('Processing gtfs file: {}', filename)
	if filename.endswith('.txt'): filename = filename[:-4]
	tuple_t = ''.join(' '.join(filename.rstrip('s').split('_')).title().split())
	p = gtfs_dir / '{}.txt'.format(filename)
	if empty_if_missing and not os.access(str(p), os.R_OK):
		if yield_fields: yield list()
		return
	with p.open(encoding='utf-8-sig') as src:
		src_csv = csv.reader(src)
		fields = list(v.strip() for v in next(src_csv))
		tuple_t = namedtuple(tuple_t, fields)
		if yield_fields: yield fields
		for line in src_csv:
			try: yield tuple_t(*line)
			except TypeError:
				log.debug('Skipping bogus CSV line (file: {}): {!r}', p, line)

def get_timespan_info( svc_calendar, svc_exceptions,
		parse_start_date, parse_days, parse_days_pre,
		gtfs_timezone, gtfs_date_fmt='%Y%m%d' ):
	'Return TimespanInfo with map of services to days when they are operating within it.'
	if isinstance(gtfs_timezone, str):
		assert pytz, ['pytz is required for processing timezone spec', gtfs_timezone]
		gtfs_timezone = pytz.timezone(gtfs_timezone)
	dt_cls, tz = datetime.datetime, gtfs_timezone

	dt_start = parse_start_date
	if isinstance(parse_start_date, str):
		dt_start = tz.localize(dt_cls.strptime(dt_start, gtfs_date_fmt))
	dt_min = dt_adjust(dt_start, d=parse_days_pre, subtract=True)
	date_map = list( dt_adjust(dt_min, d=n)
		for n in range(parse_days + parse_days_pre + 1) )
	date_min_str, date_max_str = (d.strftime(gtfs_date_fmt) for d in [dt_min, date_map[-1]])
	date_map = OrderedDict((d.strftime(gtfs_date_fmt), d) for d in date_map)

	svc_days = dict() # {service_id: {date_str: datetime}}
	for svc_id, sce in svc_calendar.items():
		if sce.date_start > date_max_str or sce.date_end < date_min_str: continue
		days = svc_days.setdefault(svc_id, dict())

		# Apply any service-specific exceptions
		parse_days = dict((date_str, (False, date)) for date_str, date in date_map.items())
		for exc, date_str in svc_exceptions[svc_id]:
			if not (date_min_str <= date_str <= date_max_str): continue
			if exc == CalendarException.added:
				parse_days[date_str] = True, tz.localize(date.strptime(date_str, gtfs_date_fmt))
			elif exc == CalendarException.removed: parse_days.pop(date_str, None)
			else: raise ValueError(t)

		# Add datetime to svc_days for each date that service is operating on
		for date_str, (exc, dt) in sorted(parse_days.items()):
			if not exc:
				if date_str < sce.date_start: continue
				elif date_str > sce.date_end: break
				if not sce.weekdays[dt.weekday()]: continue
			days[date_str] = dt

	# Add days for exception-only services, not mentioned in svc_calendar at all
	for svc_id, excs in svc_exceptions.items():
		if svc_id in svc_calendar: continue
		for date_str in excs[CalendarException.added]:
			if not (date_min_str <= date_str <= date_max_str): continue
			dt = tz.localize(dt_cls.strptime(date_str, gtfs_date_fmt))
			svc_days.setdefault(svc_id, dict())[date_str] = dt

	if not svc_days:
		log.info('No services were found to be operational on specified days')

	return t.public.TimespanInfo(
		dt_start, dt_min, svc_days, date_map, date_min_str, date_max_str )

def offset_to_dts(dt_min, dt, offset):
	if dt is None: return offset.flat
	return (offset.apply_to_datetime(dt) - dt_min).total_seconds()

def calculate_trip_dts(dt_min, dt, offset_arr, offset_dep):
	'''Calculate relative timestamps ("dts" floats of seconds) for
			arrival/departure GTFSTimeOffsets on a specific day (`dt` datetime).
		"relative" to `dt_min` datetime - start of the parsed interval.
		If both dt_min and dt are passed as None, offsets are simply taken from 0.'''
	if dt is None:
		# Either both dt_min and dt are None or neither,
		#  otherwise dts values won't make sense according to one of them.
		assert dt_min is None
		return offset_arr.flat, offset_dep.flat
	if not offset_arr:
		if not trip: # first stop of the trip - arrival ~ departure
			if offset_dep: offset_arr = offset_dep
			else: raise ValueError('Missing arrival/departure times for trip stop: {}'.format(ts))
		else: offset_arr = trip[-1].offset_dep # "scheduled based on the nearest preceding timed stop"
	if not offset_dep: offset_dep = offset_arr
	assert offset_arr and offset_dep
	return tuple(offset_to_dts(dt_min, dt, o) for o in [offset_arr, offset_dep])

def footpath_dt(stop_a, stop_b, delta_base, speed_kmh, math=math):
	'''Calculate footpath time-delta (dt) between two stops,
		based on their lon/lat distance (using Haversine Formula) and walking-speed constant.'''
	# Alternative: use UTM coordinates and KDTree (e.g. scipy) or spatial dbs
	lon1, lat1, lon2, lat2 = (
		math.radians(float(v)) for v in
		[stop_a.lon, stop_a.lat, stop_b.lon, stop_b.lat] )
	km = 6367 * 2 * math.asin(math.sqrt(
		math.sin((lat2 - lat1)/2)**2 +
		math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))
	return delta_base + km / speed_kmh


def parse_timetable(gtfs_dir, conf):
	'Parse Timetable from GTFS data directory.'
	# Stops/footpaths that don't belong to trips are discarded here

	### Calculate processing timespan / calendar and map of services operating there
	if conf.parse_start_date:
		svc_calendar = dict()
		for s in iter_gtfs_tuples(gtfs_dir, 'calendar', empty_if_missing=True):
			weekdays = list(bool(int(getattr(s, k))) for k in weekday_columns)
			svc_calendar[s.service_id] = ServiceCalendarEntry(s.start_date, s.end_date, weekdays)

		svc_exceptions = defaultdict(ft.partial(defaultdict, set))
		for s in iter_gtfs_tuples(gtfs_dir, 'calendar_dates', empty_if_missing=True):
			svc_exceptions[s.service_id][CalendarException(s.exception_type)].add(s.date)

		timespan_info = get_timespan_info( svc_calendar, svc_exceptions,
			conf.parse_start_date, conf.parse_days, conf.parse_days_pre, conf.gtfs_timezone )
	else: timespan_info = t.public.TimespanInfo()

	### Stops (incl. grouping by station)
	stop_dict, stop_sets = dict(), dict() # {id: stop}, {id: station_stops}
	for s in iter_gtfs_tuples(gtfs_dir, 'stops'):
		stop = t.public.Stop(s.stop_id, s.stop_name, float(s.stop_lon), float(s.stop_lat))
		stop_set_id = s.parent_station or s.stop_id
		stop_dict[s.stop_id] = stop_set_id, stop
		if not s.parent_station: stop_sets[s.stop_id] = {stop}
		else:
			stop_sets[s.stop_id] = stop_sets.setdefault(stop_set_id, set())
			stop_sets[stop_set_id].add(stop)
	if conf.group_stops_into_stations:
		for stop_id in stop_dict: # resolve all stops to stations
			stop_dict[stop_id] = stop_dict[stop_dict[stop_id][0]]
	stop_dict, stop_sets = (
		dict((k, stop) for k, (k_set, stop) in stop_dict.items()),
		dict((k, stop_sets[k_set]) for k, (k_set, stop) in stop_dict.items()) )

	### Trips
	trip_stops = defaultdict(list)
	for s in iter_gtfs_tuples(gtfs_dir, 'stop_times'): trip_stops[s.trip_id].append(s)

	trips, stops = t.public.Trips(), t.public.Stops()
	for s in iter_gtfs_tuples(gtfs_dir, 'trips'):
		if timespan_info.dt_start:
			days = timespan_info.service_days.get(s.service_id)
			if not days: continue
		else: days = {None: None}
		for dt in days.values():
			trip, offset_arr_prev = t.public.Trip(), None
			for stopidx, ts in enumerate(
					sorted(trip_stops[s.trip_id], key=lambda s: int(s.stop_sequence)) ):
				offset_arr, offset_dep = map(GTFSTimeOffset.parse, [ts.arrival_time, ts.departure_time])
				if offset_arr_prev is not None:
					if offset_arr < offset_arr_prev: offset_arr.d += 1 # assuming bogus 24:00 -> 00:00
				offset_arr_prev = offset_arr
				dts_arr, dts_dep = calculate_trip_dts(timespan_info.dt_min, dt, offset_arr, offset_dep)
				stop = stops.add(stop_dict[ts.stop_id])
				trip.add(t.public.TripStop(trip, stopidx, stop, dts_arr, dts_dep))
			if trip: trips.add(trip)

	### Footpaths
	footpaths, fp_samestop_count, fp_synth = t.public.Footpaths(), 0, False
	with footpaths.populate() as fp_add:
		get_stop_set = lambda stop_id: list(filter(stops.get, stop_sets.get(stop_id, list())))

		transfers = iter_gtfs_tuples(gtfs_dir, 'transfers', empty_if_missing=True, yield_fields=True)
		transfers_fields = next(transfers)
		if 'min_transfer_time' not in transfers_fields:
			if transfers_fields:
				# Can maybe be used as a hint about which transfers to generate/skip
				log.info('Skipping transfers.txt file as it has no "min_transfer_time" field')
		else:
			for s in transfers:
				stops_from, stops_to = map(get_stop_set, [s.from_stop_id, s.to_stop_id])
				if not (stops_from and stops_to): continue
				delta = int(s.min_transfer_time)
				for stop_from, stop_to in it.product(stops_from, stops_to):
					if stop_from == stop_to: fp_samestop_count += 1
					fp_add(stop_from, stop_to, delta)

		# links.txt is specific to gbrail.info and has
		#  transfers that are only valid for specific date/time intervals
		for s in iter_gtfs_tuples(gtfs_dir, 'links', empty_if_missing=True):
			stops_from, stops_to = map(get_stop_set, [s.from_stop_id, s.to_stop_id])
			if not (stops_from and stops_to): continue
			delta = int(s.link_secs)
			if not timespan_info.dt_start: # not using calendar info
				fp_add(stop_from, stop_to, delta)
				continue
			if ( s.start_date > timespan_info.date_max_str
				or s.end_date < timespan_info.date_min_str ): continue
			for dt in timespan_info.date_map.values():
				if not bool(int(getattr(s, weekday_columns[dt.weekday()]))): continue
				dts_min, dts_max = (
					offset_to_dts(timespan_info.dt_min, dt, GTFSTimeOffset.parse(v))
					for v in [s.start_time, s.end_time] )
				for stop_from, stop_to in it.product(stops_from, stops_to):
					if stop_from == stop_to: fp_samestop_count += 1
					fp_add(stop_from, stop_to, delta, dts_min, dts_max)

		if len(stops):
			fp_min, fp_min_samestop = conf.footpath_gen_thresholds
			if len(footpaths) / len(stops) <= fp_min:
				log.debug('No transfers/links data found, generating synthetic footpaths from lon/lat')
				fp_synth, fp_delta_func = True, ft.partial( footpath_dt,
					delta_base=conf.footpath_delta_base, speed_kmh=conf.footpath_speed_kmh )
				for stop_a, stop_b in it.permutations(list(stops), 2):
					delta = fp_delta_func(stop_a, stop_b)
					if delta <= conf.footpath_delta_max: fp_add(stop_a, stop_b, delta)
			if fp_samestop_count / len(stops) <= fp_min_samestop:
				if not fp_synth:
					log.debug(
						'Generating missing same-stop footpaths (delta_ch={}),'
							' because source data seem to have very few of them - {} for {} stops',
						conf.delta_ch, fp_samestop_count, len(stops) )
				for stop in stops:
					if footpaths.connected(stop, stop): continue
					fp_add(stop, stop, conf.delta_ch)
					fp_samestop_count += 1

	return t.public.Timetable(stops, footpaths, trips, timespan_info)
