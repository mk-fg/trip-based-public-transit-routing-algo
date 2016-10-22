import itertools as it, operator as op, functools as ft
from pathlib import Path
import unittest

from . import _common as c


class SimpleTestCase(unittest.TestCase):

	def __init__(self, test_name, timetable, router, checks, test_data):
		self.timetable, self.router = timetable, router
		self.checks, self.test_data = checks, test_data
		setattr(self, test_name, self.run_test)
		super(SimpleTestCase, self).__init__(test_name)

	def run_test(self):
		self.checks.assert_journey_components(self.test_data)
		goal = c.struct_from_val(self.test_data.goal, c.TestGoal)
		goal.dts_start = c.dts_parse(goal.dts_start)
		goal.src, goal.dst = op.itemgetter(goal.src, goal.dst)(self.timetable.stops)
		journeys = self.router.query_earliest_arrival(goal.src, goal.dst, goal.dts_start)
		self.checks.assert_journey_results(self.test_data, journeys)


@c.tb.u.attr_struct
class TestTripStop: keys = 'stop_id dts_arr dts_dep'

class SimpleGraphTests(unittest.TestSuite):

	dt_ch = 2*60 # fixed time-delta overhead for changing trips (i.e. p->p footpaths)

	def __init__(self):
		path_file = Path(__file__)
		tests, test_data = list(), c.load_test_data(
			path_file.parent, path_file.stem, 'journey-planner-csa' )
		tb, types = c.tb, c.tb.t.public

		for test_name, test in test_data.items():
			trips, stops, footpaths = types.Trips(), types.Stops(), types.Footpaths()
			for trip_id, trip_data in test.timetable.items():
				trip = types.Trip()
				for stopidx, ts in enumerate(trip_data):
					stop_id, dts_arr, dts_dep = c.struct_from_val(ts, TestTripStop, as_tuple=True)
					if not dts_arr or dts_arr == 'x': dts_arr = dts_dep
					if not dts_dep or dts_dep == 'x': dts_dep = dts_arr
					dts_arr, dts_dep = map(c.dts_parse, [dts_arr, dts_dep])
					stop = stops.add(types.Stop(stop_id, stop_id, 0, 0))
					trip.add(types.TripStop(trip, stopidx, stop, dts_arr, dts_dep))
				trips.add(trip)
			for stop in stops: footpaths.add(stop, stop, self.dt_ch)
			timetable = types.Timetable(stops, footpaths, trips)
			router = tb.engine.TBRoutingEngine(timetable, timer_func=c.gtfs_cli.calc_timer)
			checks = c.GraphAssertions(router.graph)
			tests.append(SimpleTestCase(test_name, timetable, router, checks, test))

		super(SimpleGraphTests, self).__init__(tests)


def load_tests(loader, tests, pattern):
	# XXX: because unittest in pypy3/3.3 doesn't have subTest ctx yet
	return SimpleGraphTests()
