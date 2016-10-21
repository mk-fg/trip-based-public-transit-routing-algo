import itertools as it, operator as op, functools as ft
from collections import ChainMap, Mapping, OrderedDict
from pathlib import Path
from pprint import pprint
import os, sys, unittest, types, datetime
import runpy, tempfile, warnings, shutil, zipfile

import yaml # PyYAML module is required for tests



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
	class CustomLoader(loader_cls): pass
	def construct_mapping(loader, node):
		loader.flatten_mapping(node)
		return dict_cls(loader.construct_pairs(node))
	CustomLoader.add_constructor(
		yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping )
	return yaml.load(stream, CustomLoader)



path_file = Path(__file__)
path_test = Path(__file__).parent
path_project = path_test.parent

sys.path.insert(1, str(path_project))
import tb_routing
gtfs_cli = type( 'FakeModule', (object,),
	runpy.run_path(str(path_project / 'gtfs-tb-routing.py')) )



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
	def load_test_data(cls, name):
		'Load test data from specified YAML file and return as dmap object.'
		with (path_test / '{}.test.{}.yaml'.format(path_file.stem, name)).open() as src:
			return dmap(yaml_load(src))

	def check_journey_components(self, test):
		'''Check that lines, trips, footpaths
			and transfers for all test journeys can be found individually.'''
		goal_start_ts = test.goal.start_time
		goal_start_dts = sum(n*k for k, n in zip( [3600, 60, 1],
			op.attrgetter('hour', 'minute', 'second')(goal_start_ts) ))
		goal_src, goal_dst = op.itemgetter(test.goal.src, test.goal.dst)(self.timetable.stops)
		self.assertTrue(goal_src and goal_dst)

		def raise_error(tpl, *args, **kws):
			raise AssertionError('[{}:{}] {}'.format(jn_name, seg_name, tpl).format(*args, **kws))

		g = self.router.graph
		for jn_name, jn_info in test.journey_set.items():
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


	def test_journeys_J22209723_J2220952426(self):
		test = self.load_test_data('J22209723-J2220952426')
		self.check_journey_components(test)
