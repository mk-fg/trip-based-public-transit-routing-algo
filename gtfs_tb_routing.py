#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict
from pathlib import Path
import os, sys, logging, csv, math
import pickle, base64, hashlib

import tb_routing as tb


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


class Conf:

	dt_ch = 5*60 # fixed time-delta overhead for changing trips

	stop_linger_time_default = 5*60 # used if departure-time is missing
	footpath_dt_base = dt_ch # footpath_dt = dt_base + km / speed_kmh
	footpath_speed_kmh = 5 / 3600

conf = Conf() # XXX: placeholder


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
	'''Calculate footpath time-delta (dt) between two stops,
		based on their lon/lat distance (using Haversine Formula) and walking-speed constant.'''
	# Alternative: use UTM coordinates and KDTree (e.g. scipy) or spatial dbs
	lon1, lat1, lon2, lat2 = ( math.radians(float(v)) for v in
		[stop_a.lon, stop_a.lat, stop_b.lon, stop_b.lat] )
	km = 6367 * 2 * math.asin(math.sqrt(
		math.sin((lat2 - lat1)/2)**2 +
		math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))
	return conf.footpath_dt_base + km / conf.footpath_speed_kmh

def parse_gtfs_timetable(gtfs_dir):
	'Parse Timetable from GTFS data directory.'
	stops = dict(
		(t.stop_id, tb.Stop(t.stop_id, t.stop_name, t.stop_lon, t.stop_lat))
		for t in iter_gtfs_tuples(gtfs_dir, 'stops') )
	footpaths = dict(
		(tb.stop_pair_key(a, b), footpath_dt(a, b))
		for a, b in it.combinations(list(stops.values()), 2) )

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

	return tb.Timetable(stops, footpaths, trips)


class CalculationCache:
	'''Wrapper to cache calculation steps to disk.
		Used purely for easier/faster testing of the steps that follow.'''

	version = 1

	@staticmethod
	def seed_hash(val, n=6):
		return base64.urlsafe_b64encode(
			hashlib.sha256(repr(val).encode()).digest() ).decode()[:n]

	def __init__(self, cache_dir, seed):
		self.cache_dir, self.seed = cache_dir, self.seed_hash(seed)

	def run(self, func, *args, **kws):
		if self.cache_dir:
			func_id = func.__module__.strip('__'), func.__name__
			cache_file = ( self.cache_dir / ('.'.join([
				'v{:02d}'.format(self.version), self.seed, *func_id ]) + '.json'))
			if cache_file.exists():
				try:
					with cache_file.open('rb') as src: return pickle.load(src)
				except Exception as err:
					log.exception('Failed to process cache-file for func {}, skipping it:'
						' {} - [{}] {}', '.'.join(func_id), cache_file.name, err.__class__.__name__, err)
		data = func(*args, **kws)
		if self.cache_dir:
			with cache_file.open('wb') as dst: pickle.dump(data, dst)
		return data


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of graph-db and algos on top of that.')
	parser.add_argument('gtfs_dir', help='Path to gtfs data directory to build graph from.')
	parser.add_argument('-c', '--cache-dir', metavar='path',
		help='Cache each step of calculation where that is supported to files in specified dir.')
	parser.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')
	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	logging.basicConfig(level=logging.DEBUG if opts.debug else logging.WARNING)
	log = get_logger('main')

	c = CalculationCache(Path(opts.cache_dir), [opts.gtfs_dir])

	tt = c.run(parse_gtfs_timetable, Path(opts.gtfs_dir))
	lines = c.run(tb.timetable_lines, tt)
	router = tb.TBRoutingEngine(tt, lines)

if __name__ == '__main__': sys.exit(main())
