### TBRoutingEngine internal types

import itertools as it, operator as op, functools as ft
from collections import namedtuple, Counter
import bisect

from . import public as tp
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



@u.attr_struct(repr=False)
class TPNodeID:
	prefix = u.attr_init()
	t = u.attr_init()
	k = u.attr_init()
	def __hash__(self): return hash((self.prefix, self.t, self.k))
	def __repr__(self): return '<TPNodeID [{0.t} {0.k}]>'.format(self)

	@classmethod
	def for_k_type(cls, prefix, k):
		t = k.__class__.__name__.lower()
		return cls(prefix, t, k)


@u.attr_struct(repr=False)
class TPNode:
	value = u.attr_init()
	id = u.attr_init()
	edges_to = u.attr_init(set)
	seed = u.attr_init_id()
	def __hash__(self): return hash(self.id)
	def __repr__(self):
		return ( '<TPNode-{0:x} [{1.t} {1.k}]'
			' out-edges={2}>' ).format(self.seed, self.id, len(self.edges_to))


TPTreeStats = namedtuple('TPTreeStats', 'nodes nodes_unique t_src t_dst t_line edges')
class TPTreeLookupError(Exception): pass

class TPTree:

	def __init__(self, tree=None, stats=None, prefix=None):
		self.prefix, self.tree = prefix, u.init_if_none(tree, dict)
		self.stats = u.init_if_none(stats, Counter)

	def stat_counts(self):
		assert not self.prefix, 'Only tracked for the whole tree'
		count_node_t = lambda t,s=self.stats: sum(v for k,v in s.items() if k[0] == t)
		return TPTreeStats(
			sum(self.stats.values()), len(self.stats),
			count_node_t('src'), count_node_t('stop'), count_node_t('linestop'),
			sum(len(node.edges_to)
				for subtree in self.tree.values()
				for node_dict in subtree.values()
				for node in node_dict.values() ) )

	def path_exists(self, node_src, node_dst):
		queue = [node_src]
		while queue:
			queue_prev, queue = queue, list()
			for node in queue_prev:
				if node is node_dst: return True # found path
				queue.extend(self[k] for k in node.edges_to)
		return False

	def node(self, k, value=None, t=None, no_path_to=None):
		'''Returns node with specified k/t or creates new one with value (or k as a fallback value).
			If no_path_to node is passed, returned node will never
				have a path to it, creating another same-k node if necessary.'''
		assert self.prefix, 'Can only add elements to prefixed subtree'
		if not t: node_id = TPNodeID.for_k_type(self.prefix, k)
		else: node_id = TPNodeID(self.prefix, t, k)
		if node_id not in self.tree:
			node = TPNode(value or k, node_id)
			self.tree[node_id] = {node.seed: node}
			self.stats[node_id.t, node_id.k] += 1
		else:
			node_dict = self.tree[node_id]
			if not no_path_to: node = next(iter(node_dict.values()))
			else: # find node with no reverse path or create new one
				for node in node_dict.values():
					if not self.path_exists(node, no_path_to): break
				else:
					node = TPNode(value or k, node_id)
					self.tree[node_id][node.seed] = node
		return node

	def _node_id_for_k(self, k, t=None):
		if isinstance(k, TPNode): k = k.id
		if not isinstance(k, TPNodeID): k = TPNodeID.for_k_type(self.prefix, k)
		return k

	def get_all(self, k, t=None):
		assert self.prefix, 'Only makes sense for subtrees'
		return self.tree[self._node_id_for_k(k, t)].values()

	def __getitem__(self, k):
		'''Returns subtree for prefix of the main tree, or unique node for
				specified node/node-id/k (using both id and seed from node objects!).
			If no unique element can be returned, TPTreeLookupError will be raised.
			get_all() can be used to fetch duplicate nodes for the same k, or with special t.'''
		if not self.prefix: return TPTree(self.tree.setdefault(k, dict()), self.stats, k)
		node_dict = self.tree[self._node_id_for_k(k)]
		if isinstance(k, TPNode): return node_dict[k.seed]
		if len(node_dict) != 1:
			raise TPTreeLookupError('Non-unique node(s) for {}: {}'.format(k, node_dict))
		return next(iter(node_dict.values()))
