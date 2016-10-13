import itertools as it, operator as op, functools as ft
import bisect

from .. import utils as u


### TBRoutingEngine internal types

class Line:
	'''Line - group of trips with identical stop sequences,
			ordered from earliest-to-latest by arrival time on ALL stops.
		If one trip overtakes another (making
			such strict ordering impossible), trips should be split into different lines.'''

	def __init__(self, *trips): self.set_idx = list(trips)

	def add(self, *trips):
		self.set_idx.extend(trips)
		self.set_idx.sort(key=lambda t: sum(map(op.attrgetter('dts_arr'), t)))

	def __getitem__(self, k): return self.set_idx[k]
	def __iter__(self): return iter(self.set_idx)


class Lines:

	def __init__(self): self.set_idx = dict()

	def add(self, *lines):
		for line in lines:
			for n, ts in enumerate(line[0]):
				self.set_idx.setdefault(ts.stop.id, list()).append((n, line))

	def __getitem__(self, k): return self.set_idx[k]
	def __iter__(self):
		for n, line in self.set_idx.values(): yield line


@u.attr_struct
class Transfer: keys = 'trip_from stopidx_from trip_to stopidx_to'

class TransferSet:

	def __init__(self): self.set_idx = dict()

	def add(self, transfer):
		k = transfer.trip_from[transfer.stopidx_from].id
		self.set_idx.setdefault(k, dict())[len(self.set_idx[k])] = transfer

	def discard(self, keys):
		for k1, k2 in keys:
			del self.set_idx[k1][k2]
			if not self.set_idx[k1]: del self.set_idx[k1]

	def __len__(self): return sum(map(len, self.set_idx.values()))
	def __iter__(self):
		for k1, sub_idx in self.set_idx.items():
			for k2, transfer in sub_idx.items(): yield (k1, k2), transfer
