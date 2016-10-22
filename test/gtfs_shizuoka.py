import itertools as it, operator as op, functools as ft
from pathlib import Path
from pprint import pprint
import os, sys, unittest

from . import common


class GTFS_Shizuoka_20161013(unittest.TestCase):

	@classmethod
	def setUpClass(cls):
		path_file = Path(__file__)
		path_gtfs_zip = path_file.parent / (path_file.stem + '.data.2016-10-13.zip')
		cls.fx = common.GTFSTestFixture(path_gtfs_zip, path_file)
		cls.timetable, cls.router = common.gtfs_cli.init_gtfs_router(
			cls.fx.path_unzip, cls.fx.path_cache, timer_func=common.gtfs_cli.calc_timer )
		cls.checks = common.GraphAssertions(cls.router.graph)

	@classmethod
	def tearDownClass(cls): pass


	def test_journeys_J22209723_J2220952426(self):
		test = self.fx.load_test_data('J22209723-J2220952426')
		self.checks.assert_journey_components(test)

		dts_start = sum(n*k for k, n in zip( [3600, 60, 1],
			op.attrgetter('hour', 'minute', 'second')(common.parse_iso8601(test.goal.start_time)) ))
		src, dst = op.itemgetter(test.goal.src, test.goal.dst)(self.timetable.stops)
		journeys = self.router.query_earliest_arrival(src, dst, dts_start)
		self.checks.assert_journey_results(test, journeys)
