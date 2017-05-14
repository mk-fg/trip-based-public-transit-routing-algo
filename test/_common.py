import itertools as it, operator as op, functools as ft
from collections import ChainMap, Mapping, OrderedDict, defaultdict
from pathlib import Path
from pprint import pprint
import os, sys, unittest, types, datetime, re, math
import tempfile, warnings, shutil, zipfile

import yaml # PyYAML module is required for tests

path_project = Path(__file__).parent.parent
sys.path.insert(1, str(path_project))
import tb_routing as tb

verbose = os.environ.get('TB_DEBUG')
if verbose:
	tb.u.logging.basicConfig(
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S', level=tb.u.logging.DEBUG )



class dmap(ChainMap):

	maps = None

	def __init__(self, *maps, **map0):
		maps = list((v if not isinstance( v,
			(types.GeneratorType, list, tuple) ) else OrderedDict(v)) for v in maps)
		if map0 or not maps: maps = [map0] + maps
		super(dmap, self).__init__(*maps)

	def __repr__(self):
		return '<{} {:x} {}>'.format(
			self.__class__.__name__, id(self), repr(self._asdict()) )

	def _asdict(self):
		items = dict()
		for k, v in self.items():
			if isinstance(v, self.__class__): v = v._asdict()
			items[k] = v
		return items

	def _set_attr(self, k, v):
		self.__dict__[k] = v

	def __iter__(self):
		key_set = dict.fromkeys(set().union(*self.maps), True)
		return filter(lambda k: key_set.pop(k, False), it.chain.from_iterable(self.maps))

	def __getitem__(self, k):
		k_maps = list()
		for m in self.maps:
			if k in m:
				if isinstance(m[k], Mapping): k_maps.append(m[k])
				elif not (m[k] is None and k_maps): return m[k]
		if not k_maps: raise KeyError(k)
		return self.__class__(*k_maps)

	def __getattr__(self, k):
		try: return self[k]
		except KeyError: raise AttributeError(k)

	def __setattr__(self, k, v):
		for m in map(op.attrgetter('__dict__'), [self] + self.__class__.mro()):
			if k in m:
				self._set_attr(k, v)
				break
		else: self[k] = v

	def __delitem__(self, k):
		for m in self.maps:
			if k in m: del m[k]


def yaml_load(stream, dict_cls=OrderedDict, loader_cls=yaml.SafeLoader):
	if not hasattr(yaml_load, '_cls'):
		class CustomLoader(loader_cls): pass
		def construct_mapping(loader, node):
			loader.flatten_mapping(node)
			return dict_cls(loader.construct_pairs(node))
		CustomLoader.add_constructor(
			yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping )
		# Do not auto-resolve dates/timestamps, as PyYAML does that badly
		res_map = CustomLoader.yaml_implicit_resolvers = CustomLoader.yaml_implicit_resolvers.copy()
		res_int = list('-+0123456789')
		for c in res_int: del res_map[c]
		CustomLoader.add_implicit_resolver(
			'tag:yaml.org,2002:int',
			re.compile(r'''^(?:[-+]?0b[0-1_]+
				|[-+]?0[0-7_]+
				|[-+]?(?:0|[1-9][0-9_]*)
				|[-+]?0x[0-9a-fA-F_]+)$''', re.X), res_int )
		yaml_load._cls = CustomLoader
	return yaml.load(stream, yaml_load._cls)

def load_test_data(path_dir, path_stem, name):
	'Load test data from specified YAML file and return as dmap object.'
	with (path_dir / '{}.test.{}.yaml'.format(path_stem, name)).open() as src:
		return dmap(yaml_load(src))


def struct_from_val(val, cls, as_tuple=False):
	if isinstance(val, (tuple, list)): val = cls(*val)
	elif isinstance(val, (dmap, dict, OrderedDict)): val = cls(**val)
	else: raise ValueError(val)
	return val if not as_tuple else tb.u.attr.astuple(val)

@tb.u.attr_struct
class JourneyStats: keys = 'start end'

@tb.u.attr_struct
class JourneySeg: keys = 'type src dst'

@tb.u.attr_struct
class TestGoal:
	src = tb.u.attr_init()
	dst = tb.u.attr_init()
	dts_start = tb.u.attr_init()
	dts_latest = tb.u.attr_init(None)



class GTFSTestFixture:

	def __init__(self, path_gtfs_zip, path_file):
		self.path_gtfs_zip = Path(path_gtfs_zip)
		self.path_file = Path(path_file)
		self.path_test = self.path_file.parent
		self.path_project = self.path_test.parent
		self.path_tmp_base = '{}.test.{}'.format(
			self.path_project.parent.resolve().name, self.path_file.stem )
		self._path_cache_state = defaultdict(lambda: ...)

	def load_test_data(self, name):
		return load_test_data(self.path_test, self.path_file.stem, name)


	_path_unzip = None
	@property
	def path_unzip(self):
		if self._path_unzip: return self._path_unzip

		paths_unzip = [ self.path_test / '{}.data.unzip'.format(self.path_file.stem),
			Path(tempfile.gettempdir()) / '{}.data.unzip'.format(self.path_tmp_base) ]
		for p in paths_unzip:
			if not p.exists():
				try: p.mkdir(parents=True)
				except OSError: continue
			path_unzip = p
			break
		else:
			raise OSError( 'Failed to find/create path to unzip data to.'
				' Paths checked: {}'.format(' '.join(repr(str(p)) for p in paths_unzip)) )

		path_done = path_unzip / '.unzip-done.check'
		mtime_src = self.path_gtfs_zip.stat().st_mtime
		mtime_done = path_done.stat().st_mtime if path_done.exists() else 0
		if mtime_done < mtime_src:
			shutil.rmtree(str(path_unzip))
			path_unzip.mkdir(parents=True)
			mtime_done = None

		if not mtime_done:
			with zipfile.ZipFile(str(self.path_gtfs_zip)) as src: src.extractall(str(path_unzip))
			path_done.touch()

		self._path_unzip = path_unzip
		return self._path_unzip


	def _paths_src_mtimes(self):
		paths_src = [Path(tb.__file__).parent, path_project]
		for root, dirs, files in it.chain.from_iterable(os.walk(str(p)) for p in paths_src):
			p = Path(root)
			for name in files: yield (p / name).stat().st_mtime

	def _path_cache(self, ext):
		path = self._path_cache_state[ext]
		if path is not ...: return state
		path = self._path_cache_state[ext] = None

		paths_cache = [ self.path_test / '{}.cache.{}'.format(self.path_file.stem, ext),
			Path(tempfile.gettempdir()) / '{}.cache.{}'.format(self.path_tmp_base, ext) ]

		for p in paths_cache:
			if not p.exists():
				try:
					p.touch()
					p.unlink()
				except OSError: continue
			path = self._path_cache_state[ext] = p
			break
		else:
			warnings.warn('Failed to find writable cache-path, disabling cache')
			warnings.warn(
				'Cache paths checked: {}'.format(' '.join(repr(str(p)) for p in paths_cache)) )

		mtime_src = max(self._paths_src_mtimes())
		mtime_cache = 0 if not path.exists() else path.stat().st_mtime
		if mtime_cache and mtime_src > mtime_cache:
			warnings.warn( 'Existing timetable/transfer cache'
				' file is older than code, but using it anyway: {}'.format(path) )
		return path

	@property
	def path_cache(self): return self._path_cache('graph.bin')

	@property
	def path_timetable(self): return self._path_cache('tt.pickle')



class GraphAssertions:

	dts_slack = 3 * 60

	def __init__(self, graph=None): self.graph = graph


	def debug_trip_transfers(self, stop1, stop2, stop3, max_km=0.2, max_td=3600, graph=None):
		'''Show info on possible T[stop-1] -> T[stop-2] -> U[stop-2] -> U[stop-3]
			transfers between trips (both passing stop-2), going only by timetable data.'''
		graph = graph or self.graph
		stop1, stop2, stop3 = (graph.timetable.stops[s] for s in [stop1, stop2, stop3])

		for (n1_min, line1), (n2_max, line2) in it.product(
				graph.lines.lines_with_stop(stop1), graph.lines.lines_with_stop(stop3) ):

			for ts1 in line1[0]:
				if ts1.stop == stop2: break
			else: continue
			for ts2 in line2[0]:
				if ts2.stop == stop2: break
			else: continue
			n1_max, n2_min = ts1.stopidx, ts2.stopidx

			for ts1, ts2 in it.product(line1[0][n1_min:n1_max+1], line2[0][n2_min:n2_max+1]):
				n1, n2 = ts1.stopidx, ts2.stopidx

				if ts1.stop == ts2.stop: km = 0
				else:
					lon1, lat1, lon2, lat2 = (
						math.radians(float(v)) for v in
						[ts1.stop.lon, ts1.stop.lat, ts2.stop.lon, ts2.stop.lat] )
					km = 6367 * 2 * math.asin(math.sqrt(
						math.sin((lat2 - lat1)/2)**2 +
						math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1)/2)**2 ))

				if km <= max_km:
					fp_delta = graph.timetable.footpaths.time_delta(ts1.stop, ts2.stop)
					if fp_delta is None: fp_delta = -1
					print(
						'X-{}: lon={:.4f} lat={:.4f}\n  walk:'
							' {:,.1f}m, dt={:,.0f}s\n  Y-{}: {:.4f} {:.4f}'.format(
						n1, ts1.stop.lon, ts1.stop.lat,
							km * 1000, fp_delta, n2, ts2.stop.lon, ts2.stop.lat ))
					for trip1, trip2 in it.product(line1, line2):
						ts1, ts2 = trip1[n1], trip2[n2]
						td = ts2.dts_dep - ts1.dts_arr
						if 0 <= td <= max_td:
							print('  X-arr[{}]: {} -> Y-dep[{}]: {} (delta: {:,.1f}s)'.format(
								trip1.id, tb.u.dts_format(ts1.dts_arr),
								trip2.id, tb.u.dts_format(ts2.dts_dep), td ))
					print()


	def assert_journey_components(self, test, graph=None, verbose=verbose):
		'''Check that lines, trips, footpaths
			and transfers for all test journeys can be found individually.'''
		graph = graph or self.graph
		goal = struct_from_val(test.goal, TestGoal)
		goal_src, goal_dst = op.itemgetter(goal.src, goal.dst)(graph.timetable.stops)
		assert goal_src and goal_dst

		def raise_error(tpl, *args, **kws):
			jn_seg = kws.get('err_seg', seg_name)
			jn_seg = ':{}'.format(jn_seg) if jn_seg else ''
			raise AssertionError('[{}{}] {}'.format(jn_name, jn_seg, tpl).format(*args, **kws))

		for jn_name, jn_info in (test.journey_set or dict()).items():
			jn_stats = struct_from_val(jn_info.stats, JourneyStats)
			jn_start, jn_end = map(graph.timetable.dts_parse, [jn_stats.start, jn_stats.end])
			ts_first, ts_last, ts_transfer = set(), set(), set()

			# Check segments
			for seg_name, seg in jn_info.segments.items():
				seg = struct_from_val(seg, JourneySeg)
				a, b = op.itemgetter(seg.src, seg.dst)(graph.timetable.stops)
				ts_transfer_chk, ts_transfer_found, line_found = list(ts_transfer), False, False
				ts_transfer.clear()

				if seg.type == 'trip':
					for n, line in graph.lines.lines_with_stop(a):
						for m, stop in enumerate(line.stops[n:], n):
							if stop is b: break
						else: continue
						for trip in line:
							for ts in ts_transfer_chk:
								if not (ts.trip.id == trip.id and ts.stop is a):
									for transfer in graph.transfers.from_trip_stop(ts):
										if transfer.ts_to.stop is trip[n].stop: break
									else: continue
								ts_transfer_found = True
								ts_transfer_chk.clear()
								break
							if a is goal_src: ts_first.add(trip[n])
							if b is goal_dst: ts_last.add(trip[m])
							ts_transfer.add(trip[m])
						line_found = True
					if not line_found: raise_error('No Lines/Trips found for trip-segment')

				elif seg.type == 'fp':
					if not graph.timetable.footpaths.connected(a, b):
						raise_error('No footpath-transfer found between src/dst: {} -> {}', a, b)
					for ts in ts_transfer_chk:
						if ts.stop is not a: continue
						ts_transfer_found = True
						ts_transfer_chk.clear()
						break
					for m, line in graph.lines.lines_with_stop(b):
						for trip in line:
							# if b is goal_dst: ts_last.add(trip[m])
							ts_transfer.add(trip[m])
							line_found = True
					if not line_found and b is not goal_dst:
						raise_error('No Lines/Trips found for footpath-segment dst')

				else: raise NotImplementedError

				if not ts_transfer_found and a is not goal_src:
					raise_error( 'No transfers found from'
						' previous segment (checked: {})', len(ts_transfer_chk) )
				if not ts_transfer and b is not goal_dst:
					raise_error('No transfers found from segment (type={}) end ({!r})', seg.type, seg.dst)

			# Check start/end times
			seg_name = None
			for k, ts_set, chk in [('dts_dep', ts_first, jn_start), ('dts_arr', ts_last, jn_end)]:
				dt_min = min(abs(chk - getattr(ts, k)) for ts in ts_set) if ts_set else 0
				if dt_min > self.dts_slack:
					if verbose:
						print('[{}] All TripStops for {} goal-point:'.format(jn_name, k))
						for ts in ts_set:
							print( '  TripStop(trip_id={}, stopidx={}, stop_id={}, {}={})'\
								.format(ts.trip.id, ts.stopidx, ts.stop.id, k, tb.u.dts_format(getattr(ts, k))) )
						print('[{}] Checking {} against: {}'.format(jn_name, k, tb.u.dts_format(chk)))
					raise_error( 'No trip-stops close to {} goal-point'
						' in time (within {:,}s), min diff: {:,}s', k, self.dts_slack, dt_min )


	def assert_journey_results(self, test, journeys, graph=None, verbose=verbose):
		'Assert that all journeys described by test-data (from YAML) match journeys (JourneySet).'
		graph = graph or self.graph
		if verbose:
			print('\n' + ' -'*5, 'Journeys found:')
			journeys.pretty_print()

		jn_matched = set()
		for jn_name, jn_info in (test.journey_set or dict()).items():
			jn_info_match = False
			for journey in journeys:
				if id(journey) in jn_matched: continue
				if verbose: print('\n[{}] check vs journey:'.format(jn_name), journey)
				jn_stats = struct_from_val(jn_info.stats, JourneyStats)
				dts_dep_test, dts_arr_test = map(graph.timetable.dts_parse, [jn_stats.start, jn_stats.end])
				dts_dep_jn, dts_arr_jn = journey.dts_dep, journey.dts_arr

				time_check = max(
					abs(dts_dep_test - dts_dep_jn),
					abs(dts_arr_test - dts_arr_jn) ) <= self.dts_slack
				if verbose:
					print(' ', 'time check - {}: {} == {} and {} == {}'.format(
						['fail', 'pass'][time_check],
						*map(tb.u.dts_format, [dts_dep_test, dts_dep_jn, dts_arr_test, dts_arr_jn]) ))
				if not time_check: continue

				for seg_jn, seg_test in it.zip_longest(journey, jn_info.segments.items()):
					seg_test_name, seg_test = seg_test
					if not (seg_jn and seg_test): break
					seg_test = struct_from_val(seg_test, JourneySeg)
					a_test, b_test = op.itemgetter(seg_test.src, seg_test.dst)(graph.timetable.stops)
					type_test = seg_test.type
					if isinstance(seg_jn, tb.t.public.JourneyTrip):
						type_jn, a_jn, b_jn = 'trip', seg_jn.ts_from.stop, seg_jn.ts_to.stop
					elif isinstance(seg_jn, tb.t.public.JourneyFp):
						type_jn, a_jn, b_jn = 'fp', seg_jn.stop_from, seg_jn.stop_to
					else: raise ValueError(seg_jn)
					if verbose:
						print(' ', seg_test_name, type_test == type_jn, a_test is a_jn, b_test is b_jn)
					if not (type_test == type_jn and a_test is a_jn and b_test is b_jn): break
				else:
					jn_info_match = True
					jn_matched.add(id(journey))
					break

			if not jn_info_match:
				raise AssertionError('No journeys to match test-data for: {}'.format(jn_name))
			if verbose: print('[{}] match found'.format(jn_name))

		for journey in journeys:
			if id(journey) not in jn_matched:
				raise AssertionError('Unmatched journey found: {}'.format(journey))
