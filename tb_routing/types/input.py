import itertools as it, operator as op, functools as ft
from collections import UserList
import bisect

from .. import utils as u


### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."


@u.attr_struct(slots=True)
class Stop: keys = 'id name lon lat'

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

	def __getitem__(self, stop_tuple):
		return self.set_idx[self._stop_pair_key(*stop_tuple)]

	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.items())


@u.attr_struct(slots=True)
class TripStop: keys = 'stop dts_arr dts_dep'

@u.attr_struct(slots=True)
class Trip:
	stops = u.attr_init(list)
	id = u.attr_init(lambda seq=iter(range(2**40)): next(seq))

	def add(self, stop): self.stops.append(stop)
	def __getitem__(self, n): return self.stops[n]
	def __len__(self): return len(self.stops)
	def __iter__(self): return iter(self.stops)

class Trips(UserList):
	def add(self, trip): self.append(trip)


@u.attr_struct
class Timetable: keys = 'stops footpaths trips'
