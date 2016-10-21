import itertools as it, operator as op, functools as ft
from collections import ChainMap, Mapping, OrderedDict
from pathlib import Path
from pprint import pprint
import os, sys, unittest, types, datetime, re
import runpy, tempfile, warnings, shutil, zipfile

import yaml # PyYAML module is required for tests


path_file = Path(__file__)
path_test = Path(__file__).parent
path_project = path_test.parent

sys.path.insert(1, str(path_project))
import tb_routing
gtfs_cli = type( 'FakeModule', (object,),
	runpy.run_path(str(path_project / 'gtfs-tb-routing.py')) )


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

class FixedOffsetTZ(datetime.tzinfo):
	_offset = _name = None
	@classmethod
	def from_offset(cls, name=None, delta=None, hh=None, mm=None):
		self = cls()
		if delta is None: delta = datetime.timedelta(hours=hh or 0, minutes=mm or 0)
		self._name, self._offset = name, delta
		return self
	def utcoffset(self, dt): return self._offset
	def tzname(self, dt): return self._name
	def dst(self, dt, ZERO=datetime.timedelta(0)): return ZERO
	def __repr__(self): return '<FixedOffset {!r}>'.format(self._name)

TZ_UTC = FixedOffsetTZ.from_offset('UTC')

def parse_iso8601( spec, tz_default=TZ_UTC,
		_re=re.compile(
			r'(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})'
			r'(?::(?P<s>\d{2}(\.\d+)?))?\s*(?P<tz>Z|[-+]\d{2}:\d{2})?' ) ):
	m = _re.search(spec)
	if not m: raise ValueError(m)
	if m.group('tz'):
		tz = m.group('tz')
		if tz == 'Z': tz = TZ_UTC
		else:
			k = {'+':1,'-':-1}[tz[0]]
			hh, mm = ((int(n) * k) for n in tz[1:].split(':', 1))
			tz = FixedOffsetTZ.from_offset(hh=hh, mm=mm)
	else: tz = tz_default
	ts_list = list(map(int, m.groups()[:5]))
	ts_list.append(
		0 if not m.group('s') else int(m.group('s').split('.', 1)[0]) )
	ts = datetime.datetime.strptime(
		'{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}'.format(*ts_list),
		'%Y-%m-%d %H:%M:%S' )
	assert tz
	ts = ts.replace(tzinfo=tz)
	return ts

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


class GTFS_Shizuoka_20161013(unittest.TestCase):

	path_gtfs_zip = path_test / (path_file.stem + '.data.2016-10-13.zip')
	path_cache = path_unzip = None
	timetable = router = None

	@classmethod
	def tmp_path_base(cls):
		return '{}.test.{}'.format(path_project.parent.resolve().name, path_file.stem)

	@classmethod
	def setUpClass(cls):
		if not cls.path_cache:
			paths_src = [Path(tb_routing.__file__).parent, Path(gtfs_cli.__file__)]
			paths_cache = [ path_test / '{}.cache.pickle'.format(path_file.stem),
				Path(tempfile.gettempdir()) / '{}.cache.pickle'.format(cls.tmp_path_base()) ]

			for p in paths_cache:
				if not p.exists():
					try:
						p.touch()
						p.unlink()
					except OSError: continue
				cls.path_cache = p
				break
			else:
				warnings.warn('Failed to find writable cache-path, disabling cache')
				warnings.warn(
					'Cache paths checked: {}'.format(' '.join(repr(str(p)) for p in paths_cache)) )

			def paths_src_mtimes():
				for root, dirs, files in it.chain.from_iterable(os.walk(str(p)) for p in paths_src):
					p = Path(root)
					for name in files: yield (p / name).stat().st_mtime
			mtime_src = max(paths_src_mtimes())
			mtime_cache = 0 if not cls.path_cache.exists() else cls.path_cache.stat().st_mtime
			if mtime_src > mtime_cache:
				warnings.warn( 'Existing timetable/transfer cache'
					' file is older than code, but using it anyway: {}'.format(cls.path_cache) )

		if not cls.path_unzip:
			paths_unzip = [ path_test / '{}.data.unzip'.format(path_file.stem),
				Path(tempfile.gettempdir()) / '{}.data.unzip'.format(cls.tmp_path_base()) ]
			for p in paths_unzip:
				if not p.exists():
					try: p.mkdir(parents=True)
					except OSError: continue
				cls.path_unzip = p
				break
			else:
				raise OSError( 'Failed to find/create path to unzip data to.'
					' Paths checked: {}'.format(' '.join(repr(str(p)) for p in paths_unzip)) )

			path_done = cls.path_unzip / '.unzip-done.check'
			mtime_src = cls.path_gtfs_zip.stat().st_mtime
			mtime_done = path_done.stat().st_mtime if path_done.exists() else 0
			if mtime_done < mtime_src:
				shutil.rmtree(str(cls.path_unzip))
				cls.path_unzip.mkdir(parents=True)
				mtime_done = None

			if not mtime_done:
				with zipfile.ZipFile(str(cls.path_gtfs_zip)) as src: src.extractall(str(cls.path_unzip))
				path_done.touch()

		cls.timetable, cls.router = gtfs_cli.init_gtfs_router(
			cls.path_unzip, cls.path_cache, timer_func=gtfs_cli.calc_timer )

	@classmethod
	def tearDownClass(cls): pass


	dts_slack = 10 * 60

	@classmethod
	def dts_parse(cls, dts_str):
		if ':' not in dts_str: return float(dts_str)
		dts_vals = dts_str.split(':')
		if len(dts_vals) == 2: dts_vals.append('00')
		assert len(dts_vals) == 3, dts_vals
		return sum(int(n)*k for k, n in zip([3600, 60, 1], dts_vals))

	@classmethod
	def dts_format(cls, dts):
		dts = int(dts)
		return datetime.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1)

	@classmethod
	def load_test_data(cls, name):
		'Load test data from specified YAML file and return as dmap object.'
		with (path_test / '{}.test.{}.yaml'.format(path_file.stem, name)).open() as src:
			return dmap(yaml_load(src))

	def assert_journey_components(self, test):
		'''Check that lines, trips, footpaths
			and transfers for all test journeys can be found individually.'''
		goal_src, goal_dst = op.itemgetter(test.goal.src, test.goal.dst)(self.timetable.stops)
		self.assertTrue(goal_src and goal_dst)

		def raise_error(tpl, *args, **kws):
			raise AssertionError('[{}:{}] {}'.format(jn_name, seg_name, tpl).format(*args, **kws))

		g = self.router.graph
		for jn_name, jn_info in (test.journey_set or dict()).items():
			jn_start, jn_end = map(self.dts_parse, [jn_info.stats.start, jn_info.stats.end])
			ts_first, ts_last, ts_transfer = set(), set(), set()

			for seg_name, seg in jn_info.segments.items():
				a, b = op.itemgetter(seg.src, seg.dst)(self.timetable.stops)
				ts_transfer_chk, ts_transfer_found, line_found = list(ts_transfer), False, False
				ts_transfer.clear()

				if seg.type == 'trip':
					for n, line in g.lines.lines_with_stop(a):
						for m, stop in enumerate(line.stops[n:], n):
							if stop is b: break
						else: continue
						for trip in line:
							for ts in ts_transfer_chk:
								for k, (t1, n1, t2, n2) in g.transfers.from_trip_stop(ts.trip, ts.stopidx):
									if t2[n2].stop is trip[n].stop: break
								else: continue
								ts_transfer_found = True
								ts_transfer_chk.clear()
								break
							if a is goal_src: ts_first.update(trip)
							if b is goal_dst: ts_last.update(trip)
							ts_transfer.add(trip[m])
						line_found = True
					if not line_found: raise_error('No Lines/Trips found for trip-segment')

				elif seg.type == 'fp': raise NotImplementedError
				else: raise NotImplementedError

				if not ts_transfer_found and a is not goal_src:
					raise_error('No transfers found from previous segment')
				if not ts_transfer and b is not goal_dst:
					raise_error('No transfers found from segment (type={}) end ({!r})', seg.type, seg.dst)

			self.assertLess(min(abs(jn_start - ts.dts_dep) for ts in ts_first), self.dts_slack)
			self.assertLess(min(abs(jn_end - ts.dts_arr) for ts in ts_last), self.dts_slack)

	def assert_journey_results(self, test, journeys, verbose=False):
		t, jn_matched = tb_routing.types.public, set()
		for jn_name, jn_info in (test.journey_set or dict()).items():
			for journey in journeys:
				if verbose: print('\n--- journey:', journey)
				dts_dep_test, dts_arr_test = map(
					self.dts_parse, [jn_info.stats.start, jn_info.stats.end] )
				dts_dep_jn, dts_arr_jn = journey.dts_dep, journey.dts_arr
				if verbose:
					print(' ', 'time: {} == {} and {} == {}'.format(*map(
						self.dts_format, [dts_dep_test, dts_dep_jn, dts_arr_test, dts_arr_jn] )))
				if max(
					abs(dts_dep_test - dts_dep_jn),
					abs(dts_arr_test - dts_arr_jn) ) > self.dts_slack: break
				for seg_jn, seg_test in it.zip_longest(journey, jn_info.segments.items()):
					seg_test_name, seg_test = seg_test
					if not (seg_jn and seg_test): break
					a_test, b_test = op.itemgetter(seg_test.src, seg_test.dst)(self.timetable.stops)
					type_test = seg_test.type
					if isinstance(seg_jn, t.JourneyTrip):
						type_jn, a_jn, b_jn = 'trip', seg_jn.ts_from.stop, seg_jn.ts_to.stop
					elif isinstance(seg_jn, t.JourneyFp):
						type_jn, a_jn, b_jn = 'fp', seg_jn.stop_from, seg_jn.stop_to
					else: raise ValueError(seg_jn)
					if verbose:
						print(' ', seg_test_name, type_test == type_jn, a_test is a_jn, b_test is b_jn)
					if not (type_test == type_jn and a_test is a_jn and b_test is b_jn): break
					jn_matched.add(id(journey))
				else: break
			else: raise AssertionError('No journeys to match test-data for: {}'.format(jn_name))
			if verbose: print()
		for journey in journeys:
			if id(journey) not in jn_matched:
				raise AssertionError('Unmatched journey found: {}'.format(journey))


	def test_journeys_J22209723_J2220952426(self):
		test = self.load_test_data('J22209723-J2220952426')
		self.assert_journey_components(test)

		dts_start = sum(n*k for k, n in zip( [3600, 60, 1],
			op.attrgetter('hour', 'minute', 'second')(parse_iso8601(test.goal.start_time)) ))
		src, dst = op.itemgetter(test.goal.src, test.goal.dst)(self.timetable.stops)
		journeys = self.router.query_earliest_arrival(src, dst, dts_start)
		self.assert_journey_results(test, journeys)
