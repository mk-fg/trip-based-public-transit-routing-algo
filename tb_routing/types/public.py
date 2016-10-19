### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."

import itertools as it, operator as op, functools as ft
from collections import UserList
import bisect, enum

from .. import utils as u


class SolutionStatus(enum.Enum):
	'Used as a result for solution (e.g. Trip) comparisons.'
	dominated = False
	non_dominated = True
	equal = None
	undecidable = ...


@u.attr_struct(hash=False)
class Stop:
	keys = 'id name lon lat'
	def __hash__(self): return hash(self.id)

class Stops:
	def __init__(self): self.set_idx = dict()
	def add(self, stop): self.set_idx[stop.id] = stop
	def __getitem__(self, stop_id): return self.set_idx[stop_id]
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.values())


class Footpaths:

	def __init__(self): self.set_idx = dict()

	def _stop_pair_key(self, stop_a, stop_b):
		# XXX: non-directional
		return '\0'.join(sorted([stop_a.id, stop_b.id]))

	def add(self, stop_a, stop_b, dt):
		self.set_idx[self._stop_pair_key(stop_a, stop_b)] = dt

	def discard_longer(self, dt_max):
		items = list(sorted((v,k) for k,v in self.set_idx.items()))
		n = bisect.bisect_left(items, (dt_max, ''))
		for v,k in items[n:]: del self.set_idx[k]

	def stat_mean_dt(self):
		return sum(self.set_idx.values()) / len(self.set_idx)

	def __getitem__(self, stop_tuple):
		return self.set_idx[self._stop_pair_key(*stop_tuple)]

	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.items())


trip_stop_daytime = lambda dts: dts % (24 * 3600)

@u.attr_struct(hash=False, repr=False)
class TripStop:
	trip = u.attr_init()
	stopidx = u.attr_init()
	stop = u.attr_init()
	dts_arr = u.attr_init(convert=trip_stop_daytime)
	dts_dep = u.attr_init(convert=trip_stop_daytime)

	def __hash__(self): return hash((self.trip, self.stopidx))
	def __repr__(self): # mostly to avoid recursion
		return ( 'TripStop('
			'trip_id={0.trip.id}, stopidx={0.stopidx}, stop={0.stop},'
			' dts_arr={0.dts_arr}, dts_dep={0.dts_dep})' ).format(self)

@u.attr_struct(hash=False, cmp=False)
class Trip:
	stops = u.attr_init(list)
	id = u.attr_init(lambda seq=iter(range(2**40)): next(seq))
	def __hash__(self): return hash(self.id)

	def add(self, stop): self.stops.append(stop)

	def compare(self, trip):
		'Return SolutionStatus for this trip as compared to other trip.'
		check = set(
			(None if sa.dts_arr == sb.dts_arr else sa.dts_arr < sb.dts_arr)
			for sa, sb in zip(self, trip) ).difference([None])
		if len(check) == 1: return SolutionStatus(check.pop())
		if not check: return SolutionStatus.equal
		return SolutionStatus.undecidable

	def __getitem__(self, n): return self.stops[n]
	def __len__(self): return len(self.stops)
	def __iter__(self): return iter(self.stops)

class Trips(UserList):
	def add(self, trip): self.append(trip)
	def stat_mean_stops(self): return sum(len(t) for t in self) / len(self)


@u.attr_struct
class Timetable: keys = 'stops footpaths trips'
