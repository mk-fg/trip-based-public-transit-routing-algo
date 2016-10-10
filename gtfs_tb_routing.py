#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict
from pathlib import Path
from math import radians, cos, sin, asin, sqrt
import os, sys, logging, csv, math


class LogMessage:
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger._log(level, LogMessage(msg, args, kws), (), log_kws)

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))



class conf:

	dt_ch = 5*60 # fixed time-delta overhead for changing trips

	stop_linger_time_default = 5*60 # used if departure-time is missing
	footpath_dt_base = dt_ch # footpath_dt = dt_base + km / speed_kmh
	footpath_speed_kmh = 5 / 3600


Timetable = namedtuple('Timetable', 'stops footpaths trips')
Stop = namedtuple('Stop', 'id name lon lat')
TripStop = namedtuple('TripStop', 'stop dts_arr dts_dep')

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

def footpath_dt(stop_a, stop_b, math=math):
	# Calculates footpath time-delta (dt) between two stops
	# Alternative: use UTM coordinates and KDTree (e.g. scipy) or spatial dbs
	lon1, lat1, lon2, lat2 = ( math.radians(float(v)) for v in
		[stop_a.lon, stop_a.lat, stop_b.lon, stop_b.lat] )
	km = 6367 * 2 * math.asin(math.sqrt(
		math.sin((lat2 - lat1)/2)**2 +
		math.cos(lat1) * cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))
	return conf.footpath_dt_base + km / conf.footpath_speed_kmh

def parse_gtfs_timetable(gtfs_dir):
	# "We consider public transit networks defined by an aperiodic
	#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."

	stops = dict(
		(t.stop_id, Stop(t.stop_id, t.stop_name, t.stop_lon, t.stop_lat))
		for t in iter_gtfs_tuples(gtfs_dir, 'stops') )

	footpaths = dict()
	for a, b in it.combinations(list(stops.values()), 2):
		footpaths[frozenset([a.id, b.id])] = footpath_dt(a, b)

	trips, trip_stops = dict(), defaultdict(list)
	for t in iter_gtfs_tuples(gtfs_dir, 'stop_times'): trip_stops[t.trip_id].append(t)
	for t in iter_gtfs_tuples(gtfs_dir, 'trips'):
		trip = list()
		for ts in sorted(trip_stops[t.trip_id], key=lambda t: int(t.stop_sequence)):
			dts_arr, dts_dep = map(parse_gtfs_dts, [ts.arrival_time, ts.departure_time])
			if not dts_arr:
				if not trip: # first stop of the trip - arrival ~ departure
					if dts_dep: dts_arr = dts_dep - conf.stop_linger_time_default
					else: continue
				else: dts_arr = trip[-1].dts_dep # "scheduled based on the nearest preceding timed stop"
			if not dts_dep: dts_dep = dts_arr + conf.stop_linger_time_default
		if trip: trips[t.trip_id] = trip

	return Timetable(stops, footpaths, trips)


def timetable_lines(tt):
	# Lines - trips with identical stop sequences, ordered from earliest-to-latest by arrival time
	# If one trip overtakes another (making ordering impossible) - split into diff line

	line_trips = defaultdict(list)
	line_stops = lambda trip: list(map(op.attrgetter('stop'), trip.stops))
	for trip in tt.trips: line_trips[line_stops(a)].append(trip)

	lines = list()
	for trips in line_trips.values():
		lines_subgroup = list()
		for a in trips:
			for trips in lines_subgroup:
				for b in trips:
					overtake_check = set( # True: a ≺ b, False: b ≺ a, None: a == b
						(None if sa.dts_arr == sb.dts_arr else sa.dts_arr <= sb.dts_arr)
						for sa, sb in zip(a.stops, b.stops) )
					if True in overtake_check and False in overtake_check: break # diff line
				else:
					trips.append(a)
					break
			else: lines_subgroup.append([a]) # failed to find line to group trip into
		for line in lines_subgroup:
			line.sort(key=lambda trip: sum(map(op.attrgetter('dts_arr'), trip.stops)))
		lines.extend(lines_subgroup)

	return lines


def precalc_transfer_set(tt, lines):
	raise NotImplementedError
	for trip in tt.trips:
		for p in trip[1:]:
			for q in tt.stops.values():
				try: dt_fp = tt.footpaths[frozenset([p, q])]
				except KeyError: continue # unreachable on foot
				# dt_fp



def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of graph-db and algos on top of that.')
	parser.add_argument('gtfs_dir', help='Path to gtfs data directory to build graph from.')
	parser.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	logging.basicConfig(level=logging.DEBUG if opts.debug else logging.WARNING)
	log = get_logger('main')

	tt = parse_gtfs_timetable(Path(opts.gtfs_dir))
	lines = timetable_lines(tt)
	transfers = precalc_transfer_set(tt, lines)


if __name__ == '__main__': sys.exit(main())
