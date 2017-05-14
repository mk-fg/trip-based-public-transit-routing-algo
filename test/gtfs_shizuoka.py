import itertools as it, operator as op, functools as ft
from pathlib import Path
from pprint import pprint
import os, sys, unittest

from . import _common as c


class GTFS_Shizuoka_20161013(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		path_file = Path(__file__)
		path_gtfs_zip = path_file.parent / (path_file.stem + '.data.2016-10-13.zip')
		cls.fx = c.GTFSTestFixture(path_gtfs_zip, path_file)

		tt_path, tt_path_dump = cls.fx.path_timetable, None
		if not tt_path.exists(): tt_path, tt_path_dump = cls.fx.path_unzip, tt_path

		cls.timetable, cls.router = c.tb.init_gtfs_router(
			tt_path, cls.fx.path_cache, tt_path_dump, timer_func=c.tb.calc_timer )
		cls.checks = c.GraphAssertions(cls.router.graph)

	@classmethod
	def tearDownClass(cls): pass

	def _test_journeys_base(self, data_name):
		test = self.fx.load_test_data(data_name)
		self.checks.assert_journey_components(test)

		goal = c.struct_from_val(test.goal, c.TestGoal)
		goal.dts_start = self.timetable.dts_parse(goal.dts_start)
		goal.src, goal.dst = op.itemgetter(goal.src, goal.dst)(self.timetable.stops)

		journeys = self.router.query_earliest_arrival(goal.src, goal.dst, goal.dts_start)
		self.checks.assert_journey_results(test, journeys)


	def test_journeys_J22209723_J2220952426(self):
		self._test_journeys_base('J22209723-J2220952426')

	def test_journeys_J22209843_J222093345(self):
		self._test_journeys_base('J22209843-J222093345')

	def test_journeys_J22209730_J22209790(self):
		self._test_journeys_base('J22209730-J22209790')


def load_tests(loader, tests, pattern):
	# XXX: because unittest in pypy3/3.3 doesn't have subTest ctx yet
	return unittest.makeSuite(GTFS_Shizuoka_20161013)
