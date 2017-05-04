### TBRoutingEngine internal types - base: lines, transfers, graph

import itertools as it, operator as op, functools as ft
from collections import namedtuple
import struct, array

from .. import utils as u


struct_dump_header_fmt = '>I'

def struct_dumps(chunk_fmt, chunks, chunk_count=None):
	header_t = struct.Struct(struct_dump_header_fmt)
	if chunk_count is None: chunk_count = len(chunks)
	chunk_t = struct.Struct(chunk_fmt)
	buff_len = header_t.size + chunk_t.size * chunk_count
	buff = bytearray(buff_len)
	header_t.pack_into(buff, 0, chunk_count)
	for n, (buff_n, chunk) in enumerate(zip(
			range(header_t.size, buff_len, chunk_t.size), chunks )):
		chunk_t.pack_into(buff, buff_n, *chunk)
	assert n == chunk_count-1 and buff_n == buff_len-chunk_t.size
	return buff

def struct_load_iter(chunk_fmt, stream):
	header_t = struct.Struct(struct_dump_header_fmt)
	chunk_count, = header_t.unpack(stream.read(header_t.size))
	chunk_t = struct.Struct(chunk_fmt)
	chunk_buff_len = chunk_t.size * chunk_count
	chunk_buff = stream.read(chunk_buff_len)
	for n, buff_n in zip(
			range(0, chunk_count), range(0, chunk_buff_len, chunk_t.size) ):
		yield chunk_t.unpack_from(chunk_buff, buff_n)


class Line:
	'''Line - group of trips with identical stop sequences,
			ordered from earliest-to-latest by arrival time on ALL stops.
		If one trip overtakes another (making
			such strict ordering impossible), trips should be split into different lines.'''

	def __init__(self, *trips): self.set_idx = list(trips)
	def __repr__(self):
		return '<Line {}>'.format('{:x}'.format(self.id) if isinstance(self.id, int) else self.id)

	@property
	def stops(self):
		'Sequence of Stops for all of the Trips on this Line.'
		return list(map(op.attrgetter('stop'), self.set_idx[0].stops))

	def hash_trips(self): return hash(tuple(map(op.attrgetter('id'), self.set_idx)))

	_id_cache = None
	@property
	def id(self):
		if not self._id_cache:
			# Purely for ease of introspection/debugging
			line_id_hints = sorted(set(filter( None,
				map(op.attrgetter('line_id_hint'), self.set_idx) )))
			if line_id_hints: self._id_cache = '/'.join(line_id_hints)
			else: self._id_cache = self.hash_trips()
		return self._id_cache
	@id.setter
	def id(self, value): self._id_cache = value

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
	def __hash__(self): return hash(self.id)
	def __eq__(self, line): return u.same_type_and_id(self, line)
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx)


@u.attr_struct
class LineStop:
	line_id = u.attr_init()
	stopidx = u.attr_init()
	def __hash__(self): return hash((self.line_id, self.stopidx))


class Lines:

	def __init__(self):
		self.idx_stop, self.idx_trip, self.idx_id = dict(), dict(), dict()

	def add(self, *lines):
		for line in lines:
			for stopidx, ts in enumerate(line[0]):
				self.idx_stop.setdefault(ts.stop, list()).append((stopidx, line))
			for trip in line: self.idx_trip[trip] = line

			# Resolve any potential line.id conflicts for named lines
			# This should only be used/necessary for "nice" test-graphs
			if line.id in self.idx_id and line is not self.idx_id[line.id]:
				if self.idx_id[line.id]:
					line2 = self.idx_id[line.id]
					line2.id = '{}.{:x}'.format(line2.id, line2.hash_trips())
					self.idx_id[line2.id], self.idx_id[line.id] = line2, None
				line.id = '{}.{:x}'.format(line.id, line.hash_trips())
				assert line.id not in self.idx_id # trip-id-hash collisions
			self.idx_id[line.id] = line

	def lines_with_stop(self, stop):
		'All lines going through stop as (stopidx, line) tuples.'
		return self.idx_stop.get(stop, list())

	def line_for_trip(self, trip): return self.idx_trip[trip]

	_dump_prefix, _dump_t, _dump_sep = '>I', 'I', 2**32-1

	def dump(self, stream):
		dump = array.array(self._dump_t)
		for line in self:
			for trip in line:
				assert trip.id != self._dump_sep, trip
				dump.append(trip.id)
			dump.append(self._dump_sep)
		stream.write(struct.pack(self._dump_prefix, len(dump)))
		dump.tofile(stream)

	@classmethod
	def load(cls, stream, timetable):
		dump, prefix_t = array.array(cls._dump_t), struct.Struct(cls._dump_prefix)
		dump_len, = prefix_t.unpack(stream.read(prefix_t.size))
		with u.supress_warnings():
			# DeprecationWarning about fromfile using fromstring internally
			dump.fromfile(stream, dump_len)
		self, line_trips = cls(), list()
		for trip_id in dump:
			if trip_id == cls._dump_sep:
				self.add(Line(*line_trips))
				line_trips.clear()
				continue
			line_trips.append(timetable.trips[trip_id])
		assert not line_trips
		return self

	def __getitem__(self, line_id): return self.idx_id[line_id]
	def __iter__(self): return iter(filter(None, self.idx_id.values()))
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
		assert transfer.ts_from.dts_arr < transfer.ts_to.dts_dep # sanity check
		k1 = transfer.ts_from.trip.id, transfer.ts_from.stopidx
		if k1 not in self.set_idx: self.set_idx[k1] = dict()
		k2 = len(self.set_idx[k1])
		self.set_idx[k1][k2] = transfer
		self.set_idx_keys[transfer.id] = k1, k2

	def from_trip_stop(self, ts):
		k1 = ts.trip.id, ts.stopidx
		return self.set_idx.get(k1, dict()).values()

	_dump_fmt = '>IBIBfI'

	def dump(self, stream):
		chunk_iter = (
			( transfer.ts_from.trip.id, transfer.ts_from.stopidx,
				transfer.ts_to.trip.id, transfer.ts_to.stopidx, transfer.dt, transfer.id )
			for transfer in self )
		stream.write(struct_dumps(self._dump_fmt, chunk_iter, len(self)))

	@classmethod
	def load(cls, stream, timetable):
		self = cls()
		for transfer_tuple in struct_load_iter(cls._dump_fmt, stream):
			ts_from = timetable.trips[transfer_tuple[0]][transfer_tuple[1]]
			ts_to = timetable.trips[transfer_tuple[2]][transfer_tuple[3]]
			self.add(Transfer(ts_from, ts_to, transfer_tuple[4], transfer_tuple[5]))
		return self

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

	def dump(self, stream):
		self.lines.dump(stream)
		self.transfers.dump(stream)

	@classmethod
	def load(cls, stream, timetable):
		lines = Lines.load(stream, timetable)
		transfers = TransferSet.load(stream, timetable)
		return cls(timetable, lines, transfers)


@u.attr_struct
class QueryResult:
	'''Internal query result, containing only an
			optimal list of Trips to take, later resolved into a Journey.
		Used in ParetoSets to discard some of these results early.'''
	dts_arr = u.attr_init()
	n = u.attr_init()
	jtrips = u.attr_init()
	dts_dep = u.attr_init(0)


StopLabel = namedtuple('StopLabel', 'dts_dep dts_arr ts_list') # dts_dep -> ts_list -> dts_arr
