### TBRoutingEngine internal types - pareto-set / multicriteria-prio-queue and related stuff
### Mostly used for results and labels in engine algos

import itertools as it, operator as op, functools as ft
import heapq

from .. import utils as u


@u.attr_struct(cmp=False)
@ft.total_ordering
class PrioItem:
	prio = u.attr_init()
	value = u.attr_init()

	def __hash__(self): return hash(self.prio)
	def __eq__(self, item): return self.prio == item.prio
	def __lt__(self, item): return self.prio < item.prio
	def __iter__(self): return iter((self.prio, self.value))

	@classmethod
	def get_factory(cls, attr_args):
		'''Returns factory to create PrioItem by extracting
				specified prio attrs (or extractor func, if callable) from values.
			Intended to work with "*attrs" spec,
				where either single callable/string or individual attrs get passed.'''
		if isinstance(attr_args, str): attr_args = attr_args.split()
		if len(attr_args) == 1:
			if isinstance(attr_args[0], str): attr_args = attr_args[0].split()
			elif callable(attr_args[0]): attr_args = attr_args[0]
		if not callable(attr_args): attr_args = op.attrgetter(*attr_args)
		return lambda v: cls(attr_args(v), v)


class PrioQueue:
	def __init__(self, *prio_attrs):
		self.items, self.item_func = list(), PrioItem.get_factory(prio_attrs)
	def __len__(self): return len(self.items)
	def push(self, value): heapq.heappush(self.items, self.item_func(value))
	def pop(self): return heapq.heappop(self.items).value
	def peek(self): return self.items[0].value


class BiCriteriaParetoSet:

	def __init__(self, *dts_n_attrs):
		self.items, self.item_func = list(), PrioItem.get_factory(dts_n_attrs)

	def add(self, value):
		'''Check if value is pareto-optimal, and if so, add it
			to the set, remove and dominated values and return True.'''
		item = self.item_func(value)
		item_c1, item_c2 = item.prio
		for item_chk in list(self.items):
			c1, c2 = item_chk.prio
			if item_c1 >= c1 and item_c2 >= c2: break # dominated
			if item_c1 <= c1 and item_c2 <= c2: self.items.remove(item_chk) # dominates
		else:
			self.items.append(item) # nondominated
			return True

	def __iter__(self): return iter(map(op.attrgetter('value'), self.items))
