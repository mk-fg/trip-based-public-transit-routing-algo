### TBRoutingEngine internal types - transfer patterns: tp-tree and its nodes

import itertools as it, operator as op, functools as ft
from collections import namedtuple, Counter

from .. import utils as u


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


TPTreeCounters = namedtuple('TPTreeCounters', 'total prefix')
TPTreeStats = namedtuple('TPTreeStats', 'nodes nodes_unique t_src t_stop t_line edges')
class TPTreeLookupError(Exception): pass

class TPTree:

	def __init__(self, tree=None, stats=None, prefix=None):
		self.prefix, self.tree = prefix, u.init_if_none(tree, dict)
		self.stats = u.init_if_none(stats, lambda: TPTreeCounters(Counter(), Counter()))

	def stat_counts(self):
		stats = self.stats.total
		count_node_t = lambda t: sum(v for k,v in stats.items() if k[0] == t)
		return TPTreeStats(
			sum(stats.values()), len(stats),
			count_node_t('src'), count_node_t('stop'), count_node_t('linestop'),
			sum(len(node.edges_to)
				for subtree in ([self.tree] if self.prefix else self.tree.values())
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
		no_path_to = None # XXX: it should be ok to bypass this check, but not 100% sure
		if isinstance(k, TPNode): k = k.value
		if not t: node_id = TPNodeID.for_k_type(self.prefix, k)
		else: node_id = TPNodeID(self.prefix, t, k)
		if not value: value = k
		if node_id not in self.tree:
			node = TPNode(value, node_id)
			self.tree[node_id] = {node.seed: node}
			self.stats.total[node_id.t, node_id.k] += 1 # nodes by type/key
			self.stats.prefix[self.prefix] += 1 # nodes for each prefix
		else:
			node_dict = self.tree[node_id]
			if not no_path_to: node = next(iter(node_dict.values()))
			else: # find node with no reverse path or create new one
				for node in node_dict.values():
					if not self.path_exists(node, no_path_to): break
				else:
					node = TPNode(value, node_id)
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
