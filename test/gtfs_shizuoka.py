import itertools as it, operator as op, functools as ft
from pathlib import Path
from pprint import pprint
import os, sys, unittest
import runpy, tempfile, warnings, shutil, zipfile


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


	def test_journeys_J22209723_to_J2220952426(self):
		a, b = self.timetable.stops['J22209723_0'], self.timetable.stops['J2220952426_0']
		journeys = self.router.query_earliest_arrival(a, b, 0)

		pprint(['Journeys:', journeys])

		self.assertEqual(len(journeys), 3)
