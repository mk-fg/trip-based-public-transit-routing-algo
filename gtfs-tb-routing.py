#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict
from pathlib import Path
import os, sys, re, csv, math

import tb_routing as tb


class Conf:

	path_dep_tree = re.sub(r'.*/([^/]+?)(\.py)?$', r'\1.cache-dep-tree', __file__)

	dt_ch = 5*60 # fixed time-delta overhead for changing trips

	stop_linger_time_default = 5*60 # used if departure-time is missing
	footpath_dt_base = 1*60 # footpath_dt = dt_base + km / speed_kmh
	footpath_speed_kmh = 5 / 3600
	footpath_dt_max = 20*60 # all footpaths longer than that are discarded as invalid

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
	lon1, lat1, lon2, lat2 = (
		math.radians(float(v)) for v in
		[stop_a.lon, stop_a.lat, stop_b.lon, stop_b.lat] )
	km = 6367 * 2 * math.asin(math.sqrt(
		math.sin((lat2 - lat1)/2)**2 +
		math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))
	return conf.footpath_dt_base + km / conf.footpath_speed_kmh

def parse_gtfs_timetable(gtfs_dir):
	'Parse Timetable from GTFS data directory.'
	types = tb.t.input

	stops = types.Stops()
	for t in iter_gtfs_tuples(gtfs_dir, 'stops'):
		stops.add(types.Stop(t.stop_id, t.stop_name, t.stop_lon, t.stop_lat))

	footpaths = types.Footpaths()
	for stop_a, stop_b in it.combinations(list(stops), 2):
		footpaths.add(stop_a, stop_b, footpath_dt(stop_a, stop_b))

	trip_stops = defaultdict(list)
	for t in iter_gtfs_tuples(gtfs_dir, 'stop_times'): trip_stops[t.trip_id].append(t)

	trips = types.Trips()
	for t in iter_gtfs_tuples(gtfs_dir, 'trips'):
		trip = types.Trip()
		for ts in sorted(trip_stops[t.trip_id], key=lambda t: int(t.stop_sequence)):
			dts_arr, dts_dep = map(parse_gtfs_dts, [ts.arrival_time, ts.departure_time])
			if not dts_arr:
				if not trip: # first stop of the trip - arrival ~ departure
					if dts_dep: dts_arr = dts_dep - conf.stop_linger_time_default
					else: continue
				else: dts_arr = trip[-1].dts_dep # "scheduled based on the nearest preceding timed stop"
			if not dts_dep: dts_dep = dts_arr + conf.stop_linger_time_default
			trip.append(
				types.TripStop(stop=stops[ts.stop_id], dts_arr=dts_arr, dts_dep=dts_dep) )
		if trip: trips.add(trip)

	log.debug(
		'Parsed timetable: stops={} footpaths={} trips={} trip_stops={}',
		len(stops), len(footpaths), len(trips), len(trip_stops) )
	return types.Timetable(stops, footpaths, trips)


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of graph-db and algos on top of that.')
	parser.add_argument('gtfs_dir', help='Path to gtfs data directory to build graph from.')
	parser.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')

	group = parser.add_argument_group('Caching options')
	group.add_argument('-c', '--cache-dir', metavar='path',
		help='Cache each step of calculation (where supported) to files in specified dir.')
	group.add_argument('-s', '--cache-skip', metavar='pattern',
		help='Module/function name(s) (space-separated) to'
				' auto-invalidate any existing cached data for.'
			' Matched against "<module-name>.<func-name>" as a simple substring.')
	group.add_argument('-t', '--cache-dep-tree',
		metavar='path', default=conf.path_dep_tree,
		help='Cache dependency tree - i.e. which cached'
				' calculation depends on which, in asciitree.LeftAligned format.'
			' Default is to look it up in following file (if exists): %(default)s')
	group.add_argument('-l', '--cache-lazy', action='store_true',
		help='Skip loading cached data for dependencies of stuff that is also cached.')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	tb.u.logging.basicConfig(
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		level=tb.u.logging.DEBUG if opts.debug else tb.u.logging.WARNING )
	log = tb.u.get_logger('main')

	cache = tb.cache.CalculationCache(
		opts.cache_dir and Path(opts.cache_dir), [opts.gtfs_dir],
		invalidate=opts.cache_skip and opts.cache_skip.split(),
		dep_tree_file=Path(opts.cache_dep_tree), lazy=opts.cache_lazy )

	timetable = cache.run(parse_gtfs_timetable, Path(opts.gtfs_dir))
	router = tb.engine.TBRoutingEngine(conf, timetable, cache=cache)

if __name__ == '__main__': sys.exit(main())
