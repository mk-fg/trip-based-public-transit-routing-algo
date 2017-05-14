import itertools as it, operator as op, functools as ft
from pathlib import Path
import unittest

from . import _common as c


@c.tb.u.attr_struct
class TestTripStop: keys = 'stop_id dts_arr dts_dep'
@c.tb.u.attr_struct
class TestFootpath: keys = 'src dst dt'

class SimpleTestCase(unittest.TestCase):

	dt_ch = 2*60 # fixed time-delta overhead for changing trips (i.e. p->p footpaths)

	def __init__(self, test_name, test_data):
		self.test_name, self.test_data = test_name, test_data
		setattr(self, test_name, self.run_test)
		super(SimpleTestCase, self).__init__(test_name)

	def init_router(self):
		types = c.tb.t.public
		trips, stops, footpaths = types.Trips(), types.Stops(), types.Footpaths()

		tt = self.test_data.timetable or dict()
		if not set(tt.keys()).difference(['trips', 'footpaths']):
			tt_trips, tt_footpaths = (tt.get(k, list()) for k in ['trips', 'footpaths'])
		else: tt_trips, tt_footpaths = self.test_data.timetable, list()

		for trip_id, trip_data in tt_trips.items():
			trip = types.Trip()
			for stopidx, ts in enumerate(trip_data):
				stop_id, dts_arr, dts_dep = c.struct_from_val(ts, TestTripStop, as_tuple=True)
				if not dts_arr or dts_arr == 'x': dts_arr = dts_dep
				if not dts_dep or dts_dep == 'x': dts_dep = dts_arr
				dts_arr, dts_dep = map(c.tb.u.dts_parse, [dts_arr, dts_dep])
				stop = stops.add(types.Stop(stop_id, stop_id, 0, 0))
				trip.add(types.TripStop(trip, stopidx, stop, dts_arr, dts_dep))
			trips.add(trip)

		with footpaths.populate() as fp_add:
			for spec in tt_footpaths:
				src_id, dst_id, delta = c.struct_from_val(spec, TestFootpath, as_tuple=True)
				src, dst = (stops.add(types.Stop(s, s, 0, 0)) for s in [src_id, dst_id])
				fp_add(src, dst, delta * 60)
			for stop in stops: fp_add(stop, stop, self.dt_ch)

		timetable = types.Timetable(stops, footpaths, trips)
		router = c.tb.engine.TBRoutingEngine(timetable, timer_func=c.tb.calc_timer)
		checks = c.GraphAssertions(router.graph)
		return timetable, router, checks

	def run_test(self):
		timetable, router, checks = self.init_router()
		checks.assert_journey_components(self.test_data)
		goal = c.struct_from_val(self.test_data.goal, c.TestGoal)
		goal.dts_start = timetable.dts_parse(goal.dts_start)
		goal.src, goal.dst = op.itemgetter(goal.src, goal.dst)(timetable.stops)
		if not goal.dts_latest:
			journeys = router.query_earliest_arrival(goal.src, goal.dst, goal.dts_start)
		else:
			goal.dts_latest = timetable.dts_parse(goal.dts_latest)
			journeys = router.query_profile(goal.src, goal.dst, goal.dts_start, goal.dts_latest)
		checks.assert_journey_results(self.test_data, journeys)


class SimpleGraphTests(unittest.TestSuite):

	def __init__(self):
		path_file = Path(__file__)
		tests, tests_data = list(), c.load_test_data(
			path_file.parent, path_file.stem, 'journey-planner-csa' )
		for test_name, test_data in tests_data.items():
			tests.append(SimpleTestCase(test_name, test_data))
		super(SimpleGraphTests, self).__init__(tests)


def load_tests(loader, tests, pattern):
	# XXX: because unittest in pypy3/3.3 doesn't have subTest ctx yet
	return SimpleGraphTests()
