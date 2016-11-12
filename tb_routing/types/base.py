### TBRoutingEngine internal types - base: lines, transfers, graph

import itertools as it, operator as op, functools as ft
import bisect

from .. import utils as u


class Line:
	'''Line - group of trips with identical stop sequences,
			ordered from earliest-to-latest by arrival time on ALL stops.
		If one trip overtakes another (making
			such strict ordering impossible), trips should be split into different lines.'''

	def __init__(self, *trips): self.set_idx = list(trips)
	def __repr__(self): return '<Line {:x}>'.format(self.id)

	@property
	def stops(self):
		'Sequence of Stops for all of the Trips on this Line.'
		return list(map(op.attrgetter('stop'), self.set_idx[0].stops))

	_id_cache = None
	@property
	def id(self):
		if not self._id_cache:
			self._id_cache = hash(tuple(map(op.attrgetter('id'), self.set_idx)))
		return self._id_cache

	def add(self, *trips):
		assert not self._id_cache,\
			'Changing Line after its Trips-derived id was used.'
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
	def __hash__(self): return hash(self.id)
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx)


@u.attr_struct
class LineStop:
	line = u.attr_init()
	stopidx = u.attr_init()
	def __hash__(self): return hash((self.line.id, self.stopidx))


class Lines:

	def __init__(self):
		self.idx_stop, self.idx_trip, self.idx_id = dict(), dict(), dict()

	def add(self, *lines):
		for line in lines:
			for stopidx, ts in enumerate(line[0]):
				self.idx_stop.setdefault(ts.stop, list()).append((stopidx, line))
			for trip in line: self.idx_trip[trip] = line
			self.idx_id[line.id] = line

	def lines_with_stop(self, stop):
		'All lines going through stop as (stopidx, line) tuples.'
		return self.idx_stop.get(stop, list())

	def line_for_trip(self, trip): return self.idx_trip[trip]

	def __getitem__(self, line_id): return self.idx_id[line_id]
	def __iter__(self): return iter(self.idx_trip.values())
	def __len__(self): return len(set(map(id, self.idx_trip.values())))


@u.attr_struct
class Transfer:
	ts_from = u.attr_init()
	ts_to = u.attr_init()
	dt = u.attr_init(0) # used for min-footpath ordering
	id = u.attr_init_id()
	def __hash__(self): return hash(self.id)
	def __iter__(self): return iter(u.attr.astuple(self, recurse=False))

class TransferSet:

	def __init__(self): self.set_idx, self.set_idx_keys = dict(), dict()

	def add(self, transfer):
		# Second mapping is used purely for more efficient O(1) removals
		k1 = transfer.ts_from.trip.id, transfer.ts_from.stopidx
		if k1 not in self.set_idx: self.set_idx[k1] = dict()
		k2 = len(self.set_idx[k1])
		self.set_idx[k1][k2] = transfer
		self.set_idx_keys[transfer.id] = k1, k2

	def from_trip_stop(self, trip_stop):
		k1 = trip_stop.trip.id, trip_stop.stopidx
		return self.set_idx.get(k1, dict()).values()

	def earliest_from_trip_to_line_stop(self, trip, ls, dts_dep):
		raise NotImplementedError # XXX: needed for TBTPRoutingEngine

	def __contains__(self, transfer):
		k1, k2 = self.set_idx_keys[transfer.id]
		return bool(self.set_idx.get(k1, dict()).get(k2))
	def __delitem__(self, transfer):
		k1, k2 = self.set_idx_keys.pop(transfer.id)
		del self.set_idx[k1][k2]
		if not self.set_idx[k1]: del self.set_idx[k1]
	def __len__(self): return len(self.set_idx_keys)
	def __iter__(self):
		for k1, k2 in self.set_idx_keys.values(): yield self.set_idx[k1][k2]


@u.attr_struct
class Graph:
	keys = 'timetable lines transfers'
	def __iter__(self): return iter(u.attr.astuple(self, recurse=False))
