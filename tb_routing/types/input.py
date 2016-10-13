import itertools as it, operator as op, functools as ft
from collections import UserList
import bisect

from .. import utils as u


### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."


@u.attr_struct
class Stop: keys = 'id name lon lat'

class Stops:
	def __init__(self): self.set_idx = dict()
	def add(self, stop): self.set_idx[stop.id] = stop
	def __getitem__(self, stop_id): return self.set_idx[stop_id]
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.values())


class Footpaths:

	def __init__(self): self.set_idx = dict()

	def add(self, stop_a, stop_b, dt):
		self.set_idx[self.stop_pair_key(stop_a, stop_b)] = dt

	def stop_pair_key(self, stop_a, stop_b):
		# XXX: non-directional
		return '\0'.join(sorted([stop_a.id, stop_b.id]))

	def discard_longer(self, dt_max):
		items = list(sorted((v,k) for k,v in self.set_idx.items()))
		n = bisect.bisect_left(items, (dt_max, ''))
		for v,k in items[n:]: del self.set_idx[k]

	def __getitem__(self, stop_a, stop_b):
		return self.set_idx[stop_pair_key(stop_a, stop_b)]

	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.items())


@u.attr_struct
class TripStop:
	stop = u.attr_init()
	dts_arr = u.attr_init()
	dts_dep = u.attr_init()
	id = u.attr_init(lambda seq=iter(range(2**40)): next(seq))

class Trip(UserList): pass # list of TripStop objects

class Trips(UserList):
	def add(self, trip): self.append(trip)


@u.attr_struct
class Timetable: keys = 'stops footpaths trips'
