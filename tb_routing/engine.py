import itertools as it, operator as op, functools as ft
from collections import defaultdict, namedtuple

from . import utils as u, types as t


@u.attr_struct(vals_to_attrs=True)
class EngineConf:
	log_progress_for = None # or a set/list of prefixes
	log_progress_steps = 30


def timer(self_or_func, func=None, *args, **kws):
	'Calculation call wrapper for timer/progress logging.'
	if not func: return lambda s,*a,**k: s.timer_wrapper(self_or_func, s, *a, **k)
	return self_or_func.timer_wrapper(func, *args, **kws)


class TBRoutingEngine:

	graph = None

	def __init__(self, timetable=None, conf=None, cached_graph=None, timer_func=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.conf, self.log = conf or EngineConf(), u.get_logger('tb')
		self.timer_wrapper = timer_func if timer_func else lambda f,*a,**k: func(*a,**k)

		graph = cached_graph
		if not graph:
			lines = self.timetable_lines(timetable)
			transfers = self.precalc_transfer_set(timetable, lines)
			graph = t.base.Graph(timetable, lines, transfers)
		self.graph = graph

	@u.coroutine
	def progress_iter(self, prefix, n_max, steps=None, n=0):
		'Progress logging helper coroutine for long calculations.'
		prefix_set = self.conf.log_progress_for
		if not prefix_set or prefix not in prefix_set:
			while True: yield # dry-run
		if not steps: steps = self.conf.log_progress_steps
		steps = min(n_max, steps)
		step_n = steps and n_max / steps
		msg_tpl = '[{{}}] Step {{:>{0}.0f}} / {{:{0}d}}{{}}'.format(len(str(steps)))
		while True:
			dn_msg = yield
			if isinstance(dn_msg, tuple): dn, msg = dn_msg
			elif isinstance(dn_msg, int): dn, msg = dn_msg, None
			else: dn, msg = 1, dn_msg
			n += dn
			if n == dn or n % step_n < 1:
				if msg:
					if not isinstance(msg, str): msg = msg[0].format(*msg[1:])
					msg = ': {}'.format(msg)
				self.log.debug(msg_tpl, prefix, n / step_n, steps, msg or '')


	@timer
	def timetable_lines(self, timetable):
		'Line (pre-)calculation from Timetable data.'

		line_trips = defaultdict(list)
		line_stops = lambda trip: tuple(map(op.attrgetter('stop'), trip))
		for trip in timetable.trips: line_trips[line_stops(trip)].append(trip)

		lines, progress = t.base.Lines(), self.progress_iter('lines', len(line_trips))
		for trips in line_trips.values():
			progress.send(['line-count={:,}', len(lines)])
			lines_for_stopseq = list()

			# Split same-stops trips into non-overtaking groups
			for trip_a in trips:
				for line in lines_for_stopseq:
					for trip_b in line:
						ordering = trip_a.compare(trip_b)
						if ordering is ordering.undecidable: break
					else:
						line.add(trip_a)
						break
					if not trip_a: break
				else: # failed to find line to group trip into
					lines_for_stopseq.append(t.base.Line(trip_a))

			lines.add(*lines_for_stopseq)

		return lines


	@timer
	def precalc_transfer_set(self, timetable, lines):
		# Precalculation steps here are not merged and not parallelized in any way.
		transfers = self._pre_initial_set(timetable, lines) # Algorithm 1
		transfers = self._pre_remove_u_turns(transfers, timetable.footpaths) # Algorithm 2
		transfers = self._pre_reduction(timetable, transfers) # Algorithm 3
		self.log.debug('Precalculated transfer set size: {:,}', len(transfers))
		return transfers

	@timer
	def _pre_initial_set(self, timetable, lines):
		'Algorithm 1: Initial transfer computation.'
		transfers = t.base.TransferSet()

		progress = self.progress_iter('pre-initial-set', len(timetable.trips))
		for n, trip_t in enumerate(timetable.trips):
			progress.send(['transfer-set-size={:,} processed-trips={:,}', len(transfers), n])
			for i, ts_p in enumerate(trip_t):
				if i == 0: continue # "do not add any transfers from the first stop ..."

				for stop_q, dt_fp in timetable.footpaths.to_stops_from(ts_p.stop):
					dts_q = ts_p.dts_arr + dt_fp
					for j, line in lines.lines_with_stop(stop_q):
						if j == len(line[0]) - 1: continue # "do not add any transfers ... to the last stop"
						trip_u = line.earliest_trip(j, dts_q)
						if not trip_u: continue # all trips for L(q) have departed by dts_q
						if not (
							line != lines.line_for_trip(trip_t)
							or trip_u.compare(trip_t) is t.public.SolutionStatus.non_dominated
							or j < i ): continue
						transfers.add(t.base.Transfer(trip_t[i], trip_u[j], dt_fp))

		self.log.debug('Initial transfer set size: {:,}', len(transfers))
		return transfers

	@timer
	def _pre_remove_u_turns(self, transfers, footpaths):
		'Algorithm 2: Remove U-turn transfers.'
		discard_count = 0
		for transfer in list(transfers):
			try:
				ts_t = transfer.ts_from.trip[transfer.ts_from.stopidx-1]
				ts_u = transfer.ts_to.trip[transfer.ts_to.stopidx+1]
			except IndexError: continue # transfers from-start/to-end of t/u trips
			if ts_t.stop == ts_u.stop:
				try: dt_ch = footpaths.time_delta(ts_t.stop, ts_t.stop)
				except KeyError: continue
				if ts_t.dts_arr + dt_ch <= ts_u.dts_dep:
					del transfers[transfer]
					discard_count += 1
		self.log.debug('Discarded U-turns: {:,}', discard_count)
		return transfers

	@timer
	def _pre_reduction(self, timetable, transfers):
		'Algorithm 3: Transfer reduction.'

		def update_min_time(min_time_map, stop, dts):
			if dts < min_time_map.get(stop, u.inf):
				min_time_map[stop] = dts
				return True
			return False

		discard_count, progress = 0, self.progress_iter('pre-reduction', len(timetable.trips))
		for trip_t in timetable.trips:
			min_time_arr, min_time_ch = dict(), dict()
			progress.send(['transfer-set-size={:,} discarded={:,}', len(transfers), discard_count])

			for i in range(len(trip_t)-1, 0, -1): # first stop is skipped here as well
				ts_p = trip_t[i]
				update_min_time(min_time_arr, ts_p.stop, ts_p.dts_arr)
				for stop_q, dt_fp in timetable.footpaths.to_stops_from(ts_p.stop):
					dts_q = ts_p.dts_arr + dt_fp
					update_min_time(min_time_arr, stop_q, dts_q)
					update_min_time(min_time_ch, stop_q, dts_q)

				for transfer in list(transfers.from_trip_stop(ts_p)):
					trip_u, j = transfer.ts_to.trip, transfer.ts_to.stopidx
					keep = False
					for k in range(j+1, len(trip_u)):
						ts_u = trip_u[k]
						keep = keep | update_min_time(min_time_arr, ts_u.stop, ts_u.dts_arr)
						for stop_q, dt_fp in timetable.footpaths.to_stops_from(ts_u.stop):
							dts_q = ts_u.dts_arr + dt_fp
							keep = keep | update_min_time(min_time_arr, stop_q, dts_q)
							keep = keep | update_min_time(min_time_ch, stop_q, dts_q)
					if not keep:
						del transfers[transfer]
						discard_count += 1

		self.log.debug('Discarded no-improvement transfers: {:,}', discard_count)
		return transfers


	def jtrips_to_journeys(self, stop_src, stop_dst, dts_src, results, dts_dep_criteria=False):
		'Convert lists of trips to JourneySet with proper journey descriptions.'
		JourneySoFar = namedtuple('JSF', 'ts_src journey prio') # unfinished journey up to ts_src
		get_dt_fp = ft.partial(self.graph.timetable.footpaths.time_delta, default=u.inf)

		journeys = t.public.JourneySet()
		for jtrips in results:
			queue = [JourneySoFar(
				t.public.TripStop.dummy_for_stop(stop_src),
				t.public.Journey(dts_src), prio=0 )]

			for trip in it.chain(jtrips, [None]): # +1 iteration to add fp to stop_dst
				queue_prev, queue = queue, list()
				for jsf in queue_prev:

					if not trip: # final footpath to stop_dst
						ts_list = jsf.ts_src.trip[jsf.ts_src.stopidx+1:] if jsf.ts_src.trip else [jsf.ts_src]
						for ts in ts_list:
							dt_fp = get_dt_fp(ts.stop, stop_dst)
							if dt_fp is u.inf: continue
							jn = jsf.journey.copy()
							if ts.trip: jn.append_trip(jsf.ts_src, ts)
							jn.append_fp(ts.stop, stop_dst, dt_fp)
							queue.append(JourneySoFar(None, jn, jsf.prio + dt_fp))

					elif not jsf.ts_src.trip: # footpath from stop_src, not a trip
						for ts in trip:
							dt_fp = get_dt_fp(jsf.ts_src.stop, ts.stop)
							if dt_fp is u.inf: continue
							jn = jsf.journey.copy().append_fp(jsf.ts_src.stop, ts.stop, dt_fp)
							queue.append(JourneySoFar(ts, jn, jsf.prio + dt_fp))

					else: # footpath from previous trip - common case
						for ts1, ts2 in it.product(jsf.ts_src.trip[jsf.ts_src.stopidx+1:], trip):
							dt_fp = get_dt_fp(ts1.stop, ts2.stop)
							if dt_fp is u.inf: continue
							if ts1.dts_arr + dt_fp > ts2.dts_dep: continue
							jn = jsf.journey.copy()
							jn.append_trip(jsf.ts_src, ts1)
							jn.append_fp(ts1.stop, ts2.stop, dt_fp)
							queue.append(JourneySoFar(ts2, jn, jsf.prio + dt_fp))

			best_jsf = min(queue, key=op.attrgetter('prio'))
			journeys.add(best_jsf.journey, dts_dep_criteria=dts_dep_criteria)

		return journeys


	@timer
	def query_earliest_arrival(self, stop_src, stop_dst, dts_src):
		'''Algorithm 4: Earliest arrival query.
			Actually a bicriteria query that finds
				min-transfer journeys as well, just called that in the paper.'''
		# XXX: special case of profile-query, should be merged into that
		timetable, lines, transfers = self.graph

		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')
		results = list()
		R, Q = dict(), dict()

		def enqueue(trip, i, n, jtrips, _ss=t.public.SolutionStatus):
			i_max = len(trip) - 1 # for the purposes of "infinity" here
			if i >= R.get(trip, i_max): return
			Q.setdefault(n, list()).append(
				TripSegment(trip, i, R.get(trip, i_max), jtrips) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				R[trip_u] = min(i, R.get(trip_u, i_max))

		lines_to_dst = dict() # {trip: (i, line, dt)}
		for stop_q, dt_fp in timetable.footpaths.from_stops_to(stop_dst):
			if stop_q == stop_dst: dt_fp = 0
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line: lines_to_dst.setdefault(trip, list()).append((i, line, dt_fp))
		for line_infos in lines_to_dst.values(): # so that all "i > b" come up first
			line_infos.sort(reverse=True, key=op.itemgetter(0))

		# Queue initial set of trips (reachable from stop_src) to examine
		for stop_q, dt_fp in timetable.footpaths.to_stops_from(stop_src):
			if stop_q == stop_src: dt_fp = 0
			dts_q, jtrips = dts_src + dt_fp, list()
			if stop_q == stop_dst:
				results.append(jtrips)
				continue # can't be beaten on time or transfers - can only be extended
			for i, line in lines.lines_with_stop(stop_q):
				trip = line.earliest_trip(i, dts_q)
				if trip: enqueue(trip, i, 0, jtrips)

		# Main loop
		t_min, n = u.inf, 0
		while Q:
			for trip, b, e, jtrips in Q.pop(n):

				# Check if trip reaches stop_dst (or its footpath-vicinity) directly
				for i_dst, line, dt_fp in lines_to_dst.get(trip, list()):
					if i_dst <= b: break # can't reach previous stop
					line_dts_dst = trip[i_dst].dts_arr + dt_fp
					if line_dts_dst < t_min:
						t_min = line_dts_dst
						results.append(jtrips + [trip])

				for i in range(b+1, e+1): # b < i <= e
					if trip[i].dts_arr >= t_min: break # after +1 transfer, it's guaranteed to be dominated
					for transfer in transfers.from_trip_stop(trip[i]):
						if transfer.ts_to.dts_arr >= t_min: continue
						enqueue(transfer.ts_to.trip, transfer.ts_to.stopidx, n+1, jtrips + [trip])

			n += 1

		return self.jtrips_to_journeys(stop_src, stop_dst, dts_src, results)


	@timer
	def query_profile(self, stop_src, stop_dst, dts_edt, dts_ldt, max_transfers=15):
		'''Profile query, returning a list of pareto-optimal JourneySet results with Journeys
				from stop_src to stop_dst, with departure at stop_src in a day-time (dts) interval
				from dts_edt (earliest departure time) to dts_ldt (latest).'''
		timetable, lines, transfers = self.graph

		DepartureCriteriaCheck = namedtuple('DCCheck', 'trip stopidx dts_src journey')
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')

		results = list()
		R, Q = dict(), dict()

		def enqueue(trip, i, n, jtrips, _ss=t.public.SolutionStatus):
			i_max = len(trip) - 1 # for the purposes of "infinity" here
			if i >= R.get((n, trip), i_max): return
			Q.setdefault(n, list()).append(
				TripSegment(trip, i, R.get((n, trip), i_max), jtrips) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				i_min = min(i, R.get((n, trip_u), i_max))
				for n in range(n, max_transfers): R[n, trip_u] = i_min

		lines_to_dst = dict() # {trip: (i, line, dt)}
		for stop_q, dt_fp in timetable.footpaths.from_stops_to(stop_dst):
			if stop_q == stop_dst: dt_fp = 0
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line: lines_to_dst.setdefault(trip, list()).append((i, line, dt_fp))
		for line_infos in lines_to_dst.values(): # so that all "i > b" come up first
			line_infos.sort(reverse=True, key=op.itemgetter(0))

		profile_queue = list()
		for stop_q, dt_fp in timetable.footpaths.to_stops_from(stop_src):
			if stop_q == stop_src: dt_fp = 0
			# XXX: special fp-only journeys that work anytime
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line:
					dts_trip = trip[i].dts_dep
					dts_min, dts_max = dts_trip + dt_fp, dts_trip - dt_fp
					if not (dts_min >= dts_edt and dts_max <= dts_ldt): continue
					profile_queue.append(DepartureCriteriaCheck(trip, i, dts_max, list()))
		profile_queue.sort(key=op.attrgetter('dts_src'), reverse=True) # latest-to-earliest

		t_min_idx = dict()
		for dts_src, checks in it.groupby(profile_queue, op.attrgetter('dts_src')):
			n = 0
			for trip, stopidx, dts_src, jtrips in checks: enqueue(trip, stopidx, n, jtrips)

			while Q and n < max_transfers:
				t_min = t_min_idx.get(n, u.inf)
				for trip, b, e, jtrips in Q.pop(n):

					# Check if trip reaches stop_dst (or its footpath-vicinity) directly
					for i_dst, line, dt_fp in lines_to_dst.get(trip, list()):
						if i_dst <= b: break # can't reach previous stop
						line_dts_dst = trip[i_dst].dts_arr + dt_fp
						if line_dts_dst < t_min:
							t_min_idx[n] = line_dts_dst
							results.append(jtrips + [trip])

					# Check if trip can lead to nondominated journeys, and queue trips reachable from it
					for i in range(b+1, e+1): # b < i <= e
						if trip[i].dts_arr >= t_min: break # after +1 transfer, it's guaranteed to be dominated
						for transfer in transfers.from_trip_stop(trip[i]):
							if transfer.ts_to.dts_arr >= t_min: continue
							enqueue(transfer.ts_to.trip, transfer.ts_to.stopidx, n+1, jtrips + [trip])

				n += 1
			Q.clear()

		return self.jtrips_to_journeys(stop_src, stop_dst, dts_edt, results, dts_dep_criteria=True)


	@timer
	def build_tp_tree(self, max_transfers=15):
		'''Run all-to-all profile queries to build Transfer-Patterns
			prefix-tree of stop_src-to-stop_dst Line connections.'''

		# To avoid duplicating paper-1 algos' weird naming/types here:
		#  R -> trip_labels: Mapping[(n, Trip), int]
		#  Q -> queue: Sequence[TripSegment] (no point using Q-mapping here)

		timetable, lines, transfers = self.graph

		DepartureCriteriaCheck = namedtuple('DCCheck', 'trip stopidx dts_src ts_list')
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b ts_list')
		StopLabelSet = ft.partial(t.tp.BiCriteriaParetoSet, lambda v: (v[-1].dts_arr, len(v) - 1))

		tree = t.tp.TPTree() # adj-lists, with nodes being either Stop or Line objects
		stop_labels = dict() # {stop: ts_list (all TripStops on the way from stop_src to stop)}
		trip_tails_checked = dict() # {trip: earliest_checked_stopidx}

		def enqueue(trip, i, ts_list, _ss=t.public.SolutionStatus):
			'Ensures that each TripStop is only ever processed once via trip_tails_checked index.'
			n, i_max = len(ts_list), len(trip) - 1
			if i >= trip_tails_checked.get((n, trip), i_max): return
			queue.append(TripSegment(trip, i, trip_tails_checked.get((n, trip), i_max), ts_list))
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				i_min = min(i, trip_tails_checked.get((n, trip_u), i_max))
				for n in range(n, max_transfers): trip_tails_checked[n, trip_u] = i_min

		progress = self.progress_iter('transfer-patterns', len(timetable.stops))
		for stop_src in timetable.stops:
			progress.send(['tree-nodes={} (unique={})', sum(tree.stats.values()), len(tree.stats)])

			stop_labels.clear()
			trip_tails_checked.clear()

			profile_queue = list()
			for stop_q, dt_fp in timetable.footpaths.to_stops_from(stop_src):
				if stop_q == stop_src: dt_fp = 0
				for i, line in lines.lines_with_stop(stop_q):
					for trip in line:
						profile_queue.append(DepartureCriteriaCheck(trip, i, trip[i].dts_dep - dt_fp, list()))
			profile_queue.sort(key=op.attrgetter('dts_src'), reverse=True) # latest-to-earliest

			for dts_src, checks in it.groupby(profile_queue, op.attrgetter('dts_src')):
				queue = list()
				for trip, stopidx, dts_src, ts_list in checks: enqueue(trip, stopidx, ts_list)

				for n in range(0, max_transfers):
					if not queue: break
					queue_prev, queue = queue, list()
					for trip, b, e, ts_list in queue_prev:
						for i in range(b+1, e+1): # b < i <= e
							ts, ts_list = trip[i], ts_list + [trip[i]]

							# Update labels for all stops reachable from this TripStop
							for stop_q, dt_fp in timetable.footpaths.to_stops_from(ts.stop):
								stop_q_arr = ts.dts_arr + dt_fp
								if stop_q not in stop_labels: stop_labels[stop_q] = StopLabelSet()
								stop_labels[stop_q].add(ts_list)

							for transfer in transfers.from_trip_stop(ts):
								enqueue(transfer.ts_to.trip, transfer.ts_to.stopidx, ts_list)

			subtree = tree[stop_src]
			node_src = subtree.node(stop_src, t='src')
			for stop_dst, sl_set in stop_labels.items():
				node_dst = subtree.node(stop_dst)
				for sl in sl_set:
					node = node_dst
					for ts in reversed(sl):
						node_prev, node = node, subtree.node(
							t.base.LineStop(lines.line_for_trip(ts.trip), ts.stopidx) )
						node_prev.edges_to.add(node)
					node.edges_to.add(node_src)

		self.log.debug(
			'Search-tree stats: nodes={0.nodes:,} (unique={0.nodes_unique:,},'
				' src={0.t_src:,}, dst={0.t_stop:,}, line-stops={0.t_line:,}), edges={0.edges:,}',
			tree.stat_counts() )
		return tree

	def build_tp_engine(self, tp_tree=None, **tree_opts):
		if not tp_tree: tp_tree = self.build_tp_tree(**tree_opts)
		return TBTPRoutingEngine(self.graph, tp_tree, self.conf, timer_func=self.timer_wrapper)



class TBTPRoutingEngine:

	graph = tree = None

	def __init__(self, graph, tp_tree, conf=None, timer_func=None):
		self.conf, self.log = conf or EngineConf(), u.get_logger('tb-tp')
		self.graph, self.tree = graph, tp_tree
		self.timer_wrapper = timer_func if timer_func else lambda f,*a,**k: func(*a,**k)

	@timer
	def build_query_tree(self, stop_src, stop_dst):
		query_tree = t.tp.TPTree(prefix=stop_src)
		subtree = self.tree[stop_src]

		queue = [(subtree[stop_dst], list())]
		while queue:
			queue_prev, queue = queue, list()
			for node, path in queue_prev:
				path += [node]
				for k in node.edges_to:
					node_k = subtree[k]
					if node_k.value != stop_src:
						queue.append((node_k, path))
						continue

					# Add src->...->dst path to query_tree, reusing LineStop nodes
					node = query_tree.node(node_k)
					for node_next in reversed(path): # reverse() because tp_tree has dst->...->src paths
						node_next = query_tree.node(node_next)
						node.edges_to.add(node_next)
						node = node_next

		qt_stats = query_tree.stat_counts()
		self.log.debug(
			'Query-tree stats: nodes={0.nodes:,} (unique={0.nodes_unique:,},'
				' stops={0.t_stop:,}, line-stops={0.t_line:,}), edges={0.edges:,}', qt_stats )
		return query_tree if qt_stats.nodes > 0 else None


	def query_profile(self, stop_src, stop_dst, dts_edt, dts_ldt, max_transfers=15):
		# XXX: algo optimizations from "Multi-criteria
		#  Shortest Paths in Time-Dependent Train Networks" paper
		tt, lines, transfers = self.graph
		query_tree = self.build_query_tree(stop_src, stop_dst)
		if not query_tree: return list()

		# XXX: add path-list attr to resolve it into journey
		NodeLabel = namedtuple('NodeLabel', 'ts n')
		NodeLabelCheck = namedtuple('NodeLabelChk', 'node label')

		# XXX: implementing earliest-arrival first
		# XXX: for profile query, will need dts_dep in labels and +1 loop
		dts_dep_src = 0
		node_labels = defaultdict(ft.partial(t.tp.BiCriteriaParetoSet, 'ts.dts_arr n'))

		prio_queue = t.tp.PrioQueue('label.ts.dts_arr label.n')
		prio_queue.push(NodeLabelCheck( query_tree[stop_src],
			NodeLabel(t.public.TripStop.dummy_for_stop(stop_src, dts_dep=dts_dep_src), 0) ))

		while prio_queue:
			node_src, label_src = prio_queue.pop()

			for node in node_src.edges_to:
				if node.value != stop_dst:
					ls = node.value
					stop = ls.line.stops[ls.stopidx]
				else: ls, stop = None, node.value # ... -> stop_dst

				if not label_src.ts.trip: # stop_src -> {stop_dst or line1}
					dts = label_src.ts.dts_dep + tt.footpaths.time_delta(node_src.value, stop)
					ts = NodeLabel(t.public.TripStop.dummy_for_stop(stop, dts_arr=dts), 0)\
						if not ls else ls.line.earliest_trip(ls.stopidx, dts)[ls.stopidx]
					node_label = NodeLabel(ts, label_src.n)
				elif not ls: # lineN -> stop_dst
					dts = min(
						(ts.dts_arr + tt.footpaths.time_delta(ts.stop, stop, u.inf))
						for ts in label_src.ts.trip[label_src.ts.stopidx+1:] )
					assert dts < u.inf # must be at least one, otherwise tp_tree is wrong
					node_label = NodeLabel(
						t.public.TripStop.dummy_for_stop(stop, dts_arr=dts), label_src.n )
				else: # lineN -> lineN+1
					transfer = min(
						( transfer
							for ts in label_src.ts.trip[label_src.ts.stopidx+1:]
							for transfer in transfers.from_trip_stop(ts)
							if transfer.ts_to.stopidx == ls.stopidx
								and lines.line_for_trip(transfer.ts_to.trip) == ls.line ),
						key=op.attrgetter('ts_to.dts_arr') )
					node_label = NodeLabel(transfer.ts_to, label.n+1)

				if node_labels[node].add(node_label):
					prio_queue.push(NodeLabelCheck(node, node_label))

		return list(node_labels[query_tree[stop_dst]])
