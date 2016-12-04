# Visualization tools, mostly useful for debugging

import itertools as it, operator as op, functools as ft
from collections import defaultdict
import contextlib

from . import utils as u, types as t


print_fmt = lambda tpl, *a, file=None, end='\n', **k:\
	print(tpl.format(*a,**k), file=file, end=end)

dot_str = lambda n: '"{}"'.format(n.replace('"', '\\"'))
dot_html = lambda n: '<{}>'.format(n)


@contextlib.contextmanager
def dot_graph(dst, dot_opts, indent=2):
	print_fmt('digraph {{', file=dst)
	if isinstance(indent, int): indent = ' '*indent
	p = lambda tpl, *a, end='\n', **k:\
		print_fmt(indent + tpl, *a, file=dst, end=end, **k)
	p('### Defaults')
	for t, opts in (dot_opts or dict()).items():
		p('{} [ {} ]'.format(t, ', '.join('{}={}'.format(k, v) for k, v in opts.items())))
	yield p
	print_fmt('}}', file=dst)


def dot_for_lines(lines, dst, dot_opts=None):
	stop_names, stop_edges = defaultdict(set), defaultdict(set)
	for line in lines:
		stop_prev = None
		for n, stop in enumerate(line.stops):
			stop_names[stop].add('{}[{}]'.format(line.id, n))
			if stop_prev: stop_edges[stop_prev].add(stop)
			stop_prev = stop

	with dot_graph(dst, dot_opts) as p:

		p('')
		p('### Labels')
		for stop, line_names in stop_names.items():
			label = '<b>{}</b>{}'.format(
				stop.name, '<br/>- '.join([''] + sorted(line_names)) )
			name = stop_names[stop] = 'stop-{}'.format(stop.id)
			p('{} [label={}]'.format(dot_str(name), dot_html(label)))

		p('')
		p('### Edges')
		for stop_src, edges in stop_edges.items():
			name_src = stop_names[stop_src]
			for stop_dst in edges:
				name_dst = stop_names[stop_dst]
				p('{} -> {}', *map(dot_str, [name_src, name_dst]))


def dot_for_tp_subtree(subtree, dst, dst_to_src=False, dot_opts=None):
	assert subtree.prefix, 'Only subtrees are proper graphs'

	def node_name(node, pre=None, pre_type=None):
		v, pre_type = node.value, pre_type or dict()
		if isinstance(v, t.public.Stop):
			v = v.name
			if 'stop' in pre_type: v = '{}:{}'.format(pre_type['stop'], v)
		elif isinstance(v, t.base.LineStop):
			line_id = v.line_id
			if isinstance(v.line_id, int): line_id = '{:x}'.format(line_id)
			v = '{}:{}[{}]'.format(node.seed, line_id, v.stopidx)
			if 'line' in pre_type: v = '{}:{}'.format(pre_type['line'], v)
		else: raise ValueError(type(v), v)
		if pre: v = '{}:{}'.format(pre, v)
		return v

	dot_opts = dot_opts or dict()
	dot_opts.setdefault('graph', dict()).setdefault('rankdir', 'LR')
	src_dst = ('src dst' if not dst_to_src else 'dst src').split()
	with dot_graph(dst, dot_opts) as p:

		stops_src, stops_dst = set(), set()
		for k, node_src_set in subtree.tree.items():
			for node_seed, node_src in node_src_set.items():
				name_src = node_name(node_src, pre_type=dict(stop=src_dst[0]))
				if isinstance(node_src.value, t.public.Stop):
					if node_src.edges_to: stops_src.add(name_src)
					else: stops_dst.add(node_name(node_src, pre=src_dst[1]))
				for node_dst in node_src.edges_to:
					name_dst = node_name(node_dst, pre_type=dict(stop=src_dst[1]))
					p('{} -> {}', *map(dot_str, [name_src, name_dst]))

		for subset in filter(None, [stops_src, stops_dst]):
			p( 'subgraph {{{{ rank=same;{}; }}}}'.format(', '.join(map(dot_str, sorted(subset)))))
