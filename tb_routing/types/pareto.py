### TBRoutingEngine internal types - pareto-optimization stuff
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


class ParetoSet:
	'''ParetoSet with 2 or 3 criterias.
		First two are min-optimal, but the last one is maximized, if used.
		Designed to be used with arrival-time,
			transfer-count, and - for profile queries - departure-time criterias.'''

	def __init__(self, *dts_n_attrs):
		self.items, self.items_exc = list(), list()
		self.item_func = PrioItem.get_factory(dts_n_attrs)

	def get_criterias(self, item):
		c1, c2 = item.prio[:2]
		c3 = item.prio[2] if len(item.prio) > 2 else 0 # always 0 for bi-criteria sets
		return c1, c2, c3

	def add(self, value):
		'''Check if value is pareto-optimal, and if so, add it
			to the set, remove and dominated values and return True.'''
		item = self.item_func(value)
		item_c1, item_c2, item_c3 = self.get_criterias(item)
		for item_chk in list(self.items):
			c1, c2, c3 = self.get_criterias(item_chk)
			if item_c1 >= c1 and item_c2 >= c2 and item_c3 <= c3: break # dominated
			if item_c1 <= c1 and item_c2 <= c2 and item_c3 >= c3: self.items.remove(item_chk) # dominates
		else:
			self.items.append(item) # nondominated
			return True

	def add_exception(self, value):
		'''Add value that should not be compared to anything and will always be in the set.
			Example for such special cases are footpath-only
				journeys in profile queries that have no fixed arrival/departure times,
				hence can't really be compared to other results.'''
		self.items_exc.append(value)

	def __len__(self): return len(self.items) + len(self.items_exc)
	def __iter__(self):
		return iter(it.chain(map(op.attrgetter('value'), self.items), self.items_exc))
	def __repr__(self): return '<ParetoSet {}>'.format(list(self))


# Special-case ParetoSet used for common QueryResult values
QueryResultParetoSet = ft.partial(ParetoSet, 'dts_arr n dts_dep')
