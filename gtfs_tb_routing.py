#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict, OrderedDict
from pathlib import Path
import os, sys, csv, math, bisect
import re, pickle, base64, hashlib, time

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
	stops = dict(
		(t.stop_id, tb.Stop(t.stop_id, t.stop_name, t.stop_lon, t.stop_lat))
		for t in iter_gtfs_tuples(gtfs_dir, 'stops') )

	footpaths = sorted(
		(footpath_dt(a, b), tb.stop_pair_key(a, b))
		for a, b in it.combinations(list(stops.values()), 2) )
	n = bisect.bisect_left(footpaths, (conf.footpath_dt_max, ''))
	footpaths = dict((k,v) for v,k in footpaths[:n])

	trips, trip_stops = list(), defaultdict(list)
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
			trip.append(tb.TripStop(stops[ts.stop_id], dts_arr, dts_dep))
		if trip: trips.append(trip)

	return tb.Timetable(stops, footpaths, trips)


class CalculationCache:
	'''Wrapper to cache calculation steps to disk.
		Used purely for easier/faster testing of the steps that follow.'''

	version = 1

	@staticmethod
	def seed_hash(val, n=6):
		return base64.urlsafe_b64encode(
			hashlib.sha256(repr(val).encode()).digest() ).decode()[:n]

	@staticmethod
	def parse_asciitree(src_file):
		tree, node_pos = OrderedDict(), dict()
		for line in src_file:
			if line.lstrip().startswith('#') or not line.strip(): continue
			tokens = iter(re.finditer(r'(\|)|(\+-+)|(\S.*$)', line))
			node_parsed = False
			for m in tokens:
				assert not node_parsed, line # only one node per line
				pos, (t_next, t_leaf, val) = m.start(), m.groups()
				if t_next:
					assert pos in node_pos, [line, pos, node_pos]
					continue
				elif t_leaf:
					m = next(tokens)
					val, pos_sub = m.group(3), m.start()
				elif val:# root
					assert not tree, line
					pos_sub = 1
				parent = node_pos[pos] if pos else tree
				node = parent[val] = dict()
				node_parsed, node_pos[pos_sub] = True, node
		return tree

	def __init__(self, cache_dir, seed, skip=None, dep_tree_file=None):
		self.cache_dir, self.seed = cache_dir, self.seed_hash(seed)
		self.skip, self.invalidated = skip or list(), set()
		if dep_tree_file and dep_tree_file.exists():
			with dep_tree_file.open() as src: self.dep_tree = self.parse_asciitree(src)
		else: self.dep_tree = dict()
		self.log = tb.get_logger('main.cache')

	def cache_valid_check(self, func_id, cache_file):
		if func_id in self.invalidated: return False
		if any((pat in func_id) for pat in self.skip): return False
		return self._cache_dep_tree_check(func_id, self.dep_tree or dict())

	def _cache_dep_tree_check(self, func_id, tree, chk_str=None):
		for pat, tree in tree.items():
			if pat in func_id:
				return self._cache_dep_tree_check(
					func_id, tree, '\0'.join(self.invalidated) )
			elif chk_str and pat in chk_str: return False
			self._cache_dep_tree_check(func_id, tree, chk_str=chk_str)
		return True

	def run(self, func, *args, **kws):
		func_id = '.'.join([func.__module__.strip('__'), func.__name__])

		if self.cache_dir:
			cache_file = (self.cache_dir / ('.'.join([
				'v{:02d}'.format(self.version), self.seed, func_id ]) + '.pickle'))
			if cache_file.exists():
				try:
					if not self.cache_valid_check(func_id, cache_file): raise AssertionError
					with cache_file.open('rb') as src: data = pickle.load(src)
				except AssertionError as err:
					self.log.debug('[{}] Invalidated cache: {}', func_id, cache_file.name)
				except Exception as err:
					self.log.exception( '[{}] Failed to process cache-file,'
						' skipping it: {} - [{}] {}', func_id, cache_file.name, err.__class__.__name__, err )
				else:
					self.log.debug('[{}] Returning cached result', func_id)
					return data
		self.invalidated.add(func_id)

		self.log.debug('[{}] Starting...', func_id)
		func_td = time.monotonic()
		data = func(*args, **kws)
		func_td = time.monotonic() - func_td
		self.log.debug('[{}] Finished in: {:.1f}s', func_id, func_td)

		if self.cache_dir:
			with cache_file.open('wb') as dst: pickle.dump(data, dst)
		return data


def main(args=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of graph-db and algos on top of that.')
	parser.add_argument('gtfs_dir', help='Path to gtfs data directory to build graph from.')
	parser.add_argument('-d', '--debug', action='store_true', help='Verbose operation mode.')

	group = parser.add_argument_group('Caching options')
	group.add_argument('-c', '--cache-dir', metavar='path',
		help='Cache each step of calculation where that is supported to files in specified dir.')
	group.add_argument('-s', '--cache-skip', metavar='pattern',
		help='Module/function name(s) (space-separated) to'
				' auto-invalidate any existing cached data for.'
			' Matched against "<module-name>.<func-name>" as a simple substring.')
	group.add_argument('-t', '--cache-dep-tree',
		metavar='path', default=conf.path_dep_tree,
		help='Cache dependency tree - i.e. which cached'
				' calculation depends on which, in asciitree.LeftAligned format.'
			' Default is to look it up in following file (if exists): %(default)s')

	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	global log
	tb.logging.basicConfig(
		level=tb.logging.DEBUG if opts.debug else tb.logging.WARNING )
	log = tb.get_logger('main')

	cache = CalculationCache(
		opts.cache_dir and Path(opts.cache_dir), [opts.gtfs_dir],
		skip=opts.cache_skip and opts.cache_skip.split(),
		dep_tree_file=Path(opts.cache_dep_tree) )

	timetable = cache.run(parse_gtfs_timetable, Path(opts.gtfs_dir))
	router = tb.TBRoutingEngine(timetable, cache=cache)

if __name__ == '__main__': sys.exit(main())
