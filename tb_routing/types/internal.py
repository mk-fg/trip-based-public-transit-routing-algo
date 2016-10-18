### TBRoutingEngine internal types

import itertools as it, operator as op, functools as ft
import bisect

from . import public as tp
from .. import utils as u


class Line:
	'''Line - group of trips with identical stop sequences,
			ordered from earliest-to-latest by arrival time on ALL stops.
		If one trip overtakes another (making
			such strict ordering impossible), trips should be split into different lines.'''

	def __init__(self, *trips): self.set_idx = list(trips)

	@property
	def stops(self):
		'Sequence of Stops for all of the Trips on this Line.'
		return list(map(op.attrgetter('stop'), self.set_idx[0].stops))

	def add(self, *trips):
		self.set_idx.extend(trips)
		self.set_idx.sort(key=lambda trip: sum(map(op.attrgetter('dts_arr'), trip)))

	def earliest_trip(self, stopidx, dts=0):
		for trip in self:
			if trip[stopidx].dts_dep >= dts: return trip

	def trips_by_relation(self, trip, *rel_set):
		'''Return trips from line with specified SolutionStatus relation(s) *from* trip.
			E.g. func(t, non_dominated) will return u where t ≺ u.'''
		for line_trip in self:
			rel = trip.compare(line_trip)
			if rel in rel_set: yield line_trip

	def __getitem__(self, k): return self.set_idx[k]
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx)


class Lines:

	def __init__(self):
		self.idx_stop, self.idx_trip = dict(), dict()

	def add(self, *lines):
		for line in lines:
			for stopidx, ts in enumerate(line[0]):
				self.idx_stop.setdefault(ts.stop, list()).append((stopidx, line))
			for trip in line: self.idx_trip[trip] = line

	def lines_with_stop(self, stop):
		'All lines going through stop as (stopidx, line) tuples.'
		return self.idx_stop[stop]

	def line_for_trip(self, trip): return self.idx_trip[trip]

	def __iter__(self): return iter(self.idx_trip.values())


@u.attr_struct
class Transfer:
	keys = 'trip_from stopidx_from trip_to stopidx_to'
	def __iter__(self): return iter(u.attr.astuple(self, recurse=False))

class TransferSet:

	def __init__(self): self.set_idx = dict()

	def _trip_stop_key(self, trip, stopidx):
		return trip.id, trip[stopidx].stop.id

	def add(self, transfer):
		# Second mapping is used purely for more efficient O(1) removals
		k = self._trip_stop_key(transfer.trip_from, transfer.stopidx_from)
		self.set_idx.setdefault(k, dict())[len(self.set_idx[k])] = transfer

	def discard(self, keys):
		for k1, k2 in keys:
			del self.set_idx[k1][k2]
			if not self.set_idx[k1]: del self.set_idx[k1]

	def from_trip_stop(self, trip, stopidx):
		k1 = self._trip_stop_key(trip, stopidx)
		for k2, transfer in self.set_idx.get(k1, dict()).items(): yield (k1, k2), transfer

	def __len__(self): return sum(map(len, self.set_idx.values()))
	def __iter__(self):
		for k1, sub_idx in self.set_idx.items():
			for k2, transfer in sub_idx.items(): yield (k1, k2), transfer


@u.attr_struct
class Graph:
	keys = 'timetable lines transfers'
	def __iter__(self): return iter(u.attr.astuple(self, recurse=False))
