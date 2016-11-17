# Visualization tools, mostly useful for debugging

import itertools as it, operator as op, functools as ft

from . import types as t


def dot_for_tp_subtree(subtree, dst):
	assert subtree.prefix, 'Only subtrees are proper graphs'
	p = lambda tpl,*a,end='\n',**k: print(tpl.format(*a,**k), file=dst, end=end)
	def node_name(node):
		v = node.value
		if isinstance(v, t.public.Stop): v = v.name
		elif isinstance(v, t.base.LineStop):
			v = '{}:{:x}[{}]'.format(node.seed, v.line.id, v.stopidx)
		else: raise ValueError(type(v), v)
		return v
	stops_src, stops_dst = set(), set()
	p('digraph {{')
	for k, node_src_set in subtree.tree.items():
		for node_seed, node_src in node_src_set.items():
			name_src = node_name(node_src)
			if isinstance(node_src.value, t.public.Stop):
				if node_src.edges_to: stops_src.add(name_src)
				else: stops_dst.add(name_src)
			for node_dst in node_src.edges_to:
				name_dst = node_name(node_dst)
				p('  "{}" -> "{}"', name_src.replace('"', '\\"'), name_dst.replace('"', '\\"'))
	for subset in filter(None, [stops_src, stops_dst]):
		p( 'subgraph {{\n  rank = same;{};\n}}',
			', '.join('"{}"'.format(n.replace('"', '\\"')) for n in sorted(subset)) )
	p('}}')
