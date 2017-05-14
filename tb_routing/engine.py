import itertools as it, operator as op, functools as ft
from collections import defaultdict, namedtuple, Counter

from . import utils as u, types as t


@u.attr_struct(vals_to_attrs=True)
class EngineConf:
	log_progress_for = None # or a set/list of prefixes
	log_progress_steps = 30


def timer(self_or_func, func=None, *args, **kws):
	'Calculation call wrapper for timer/progress logging.'
	if not func: return lambda s,*a,**k: s.timer_wrapper(self_or_func, s, *a, **k)
	return self_or_func.timer_wrapper(func, *args, **kws)


def jtrips_to_journeys(footpaths, stop_src, stop_dst, dts_src, results):
	'Convert list/set of QueryResults to JourneySet with proper journey descriptions.'
	JourneySoFar = namedtuple('JSF', 'ts_src journey prio') # unfinished journey up to ts_src

	journeys = t.public.JourneySet()
	for result in results:
		jtrips = result.jtrips
		queue = [JourneySoFar(
			t.public.TripStop.dummy_for_stop(stop_src),
			t.public.Journey(dts_src), prio=0 )]

		for trip in it.chain(jtrips, [None]): # +1 iteration to add fp to stop_dst
			queue_prev, queue = queue, list()
			for jsf in queue_prev:

				if not trip: # final footpath to stop_dst
					ts_list = jsf.ts_src.trip[jsf.ts_src.stopidx+1:] if jsf.ts_src.trip else [jsf.ts_src]
					for ts in ts_list:
						fp_delta = 0 if ts.stop == stop_dst else\
							footpaths.time_delta(ts.stop, stop_dst, dts_src=ts.dts_arr)
						if fp_delta is None: continue
						jn = jsf.journey.copy()
						if ts.trip: jn.append_trip(jsf.ts_src, ts)
						jn.append_fp(ts.stop, stop_dst, fp_delta)
						queue.append(JourneySoFar(None, jn, jsf.prio + fp_delta))

				elif not jsf.ts_src.trip: # footpath from stop_src, not a trip
					for ts in trip:
						fp_delta = 0 if jsf.ts_src.stop == ts.stop else\
							footpaths.time_delta(jsf.ts_src.stop, ts.stop, dts_dst=ts.dts_dep)
						if fp_delta is None: continue
						jn = jsf.journey.copy().append_fp(jsf.ts_src.stop, ts.stop, fp_delta)
						queue.append(JourneySoFar(ts, jn, jsf.prio + fp_delta))

				else: # footpath from previous trip - common case
					for ts1, ts2 in it.product(jsf.ts_src.trip[jsf.ts_src.stopidx+1:], trip):
						fp_delta = footpaths.time_delta(
							ts1.stop, ts2.stop, dts_src=ts1.dts_arr, dts_dst=ts2.dts_dep )
						if fp_delta is None: continue
						jn = jsf.journey.copy()
						jn.append_trip(jsf.ts_src, ts1)
						jn.append_fp(ts1.stop, ts2.stop, fp_delta)
						queue.append(JourneySoFar(ts2, jn, jsf.prio + fp_delta))

		if queue:
			best_jsf = min(queue, key=op.attrgetter('prio'))
			journeys.add(best_jsf.journey)

	return journeys


class TimetableError(Exception): pass

class TBRoutingEngine:

	graph = None

	def __init__(self, timetable, conf=None, cached_graph=None, timer_func=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.conf, self.log = conf or EngineConf(), u.get_logger('tb')
		self.timer_wrapper = timer_func if timer_func else lambda f,*a,**k: f(*a,**k)
		self.jtrips_to_journeys = ft.partial(self.timer_wrapper, jtrips_to_journeys)

		if not cached_graph:
			lines = self.timetable_lines(timetable)
			transfers = self.precalc_transfer_set(timetable, lines)
			graph = t.base.Graph(timetable, lines, transfers)
		else:
			graph = self.timer_wrapper(t.base.Graph.load, cached_graph, timetable)
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
		for trip in timetable.trips:
			dts_chk = -1
			for ts in trip: # sanity check
				if not (ts.dts_arr >= dts_chk and ts.dts_arr <= ts.dts_dep):
					u.log_lines( self.log.debug,
						[('Time jumps backwards for stops of the trip: {}', trip)]
						+ list(('  {}', ts) for ts in trip) )
					raise TimetableError('Time jumps backwards for stops of the trip', trip)
				dts_chk = ts.dts_dep
			line_trips[line_stops(trip)].append(trip)

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
				else: # failed to find line to group trip into
					lines_for_stopseq.append(t.base.Line(trip_a))

			lines.add(*lines_for_stopseq)

		return lines

	@timer
	def precalc_transfer_set(self, timetable, lines):
		# Steps here are merged from 3 separate steps in the paper
		transfers = t.base.TransferSet()

		def update_min_time(min_time_map, stop, dts):
			if dts < min_time_map.get(stop, u.inf):
				min_time_map[stop] = dts
				return True
			return False

		counts, progress = Counter(), self.progress_iter('pre-initial-set', len(timetable.trips))
		for n, trip_t in enumerate(timetable.trips):
			progress.send([ 'transfer-set-size={:,} processed-trips={:,}, discarded'
				' u-turns={:,} subopt={:,}', len(transfers), n, counts['uturns'], counts['worse'] ])
			min_time_arr, min_time_ch = dict(), dict()

			for i in range(len(trip_t)-1, 0, -1): # first stop of the trip is skipped
				ts_p = trip_t[i]

				reachable_stops = list()
				update_min_time(min_time_arr, ts_p.stop, ts_p.dts_arr)
				for stop_q, fp in timetable.footpaths.to_stops_from(ts_p.stop):
					fp_delta = fp.get_shortest(dts_src=ts_p.dts_arr)
					if fp_delta is None: continue
					dts_q = ts_p.dts_arr + fp_delta
					update_min_time(min_time_arr, stop_q, dts_q)
					update_min_time(min_time_ch, stop_q, dts_q)
					reachable_stops.append((stop_q, fp_delta, dts_q))

				for stop_q, transfer_fp_delta, dts_q in reachable_stops:
					for j, line in lines.lines_with_stop(stop_q):
						if j == len(line[0]) - 1: continue # transfers to last stop make no sense
						trip_u = line.earliest_trip(j, dts_q)
						if not trip_u: continue # all trips for L(q) have departed by dts_q
						ts_q = trip_u[j]

						if not (
							line != lines.line_for_trip(trip_t)
							or trip_u.compare(trip_t) is t.public.SolutionStatus.non_dominated
							or j < i ): continue

						# U-turn transfers
						try: ts_t, ts_u = trip_t[i-1], trip_u[j+1]
						except IndexError: continue # transfers from-start/to-end of t/u trips
						if ts_t.stop == ts_u.stop:
							delta = timetable.footpaths.time_delta(
								ts_t.stop, ts_u.stop, dts_src=ts_t.dts_arr, dts_dst=ts_u.dts_dep )
							if delta is not None and ts_t.dts_arr + delta <= ts_u.dts_dep:
								counts['uturns'] += 1
								continue

						# No-improvement transfers
						keep = False
						for k in range(j+1, len(trip_u)):
							ts = trip_u[k]
							keep = keep | update_min_time(min_time_arr, ts.stop, ts.dts_arr)
							for stop, fp in timetable.footpaths.to_stops_from(ts_u.stop):
								fp_delta = fp.get_shortest(dts_src=ts_u.dts_arr)
								if fp_delta is None: continue
								dts = ts_u.dts_arr + fp_delta
								keep = keep | update_min_time(min_time_arr, stop, dts)
								keep = keep | update_min_time(min_time_ch, stop, dts)
						if not keep:
							counts['worse'] += 1
							continue

						transfers.add(t.base.Transfer(ts_p, ts_q, transfer_fp_delta))

		self.log.debug( 'Discarded u-turns={:,}'
			' no-improvement={:,}', counts['uturns'], counts['worse'] )
		self.log.debug('Resulting transfer set size: {:,}', len(transfers))
		return transfers


	@timer
	def query_earliest_arrival(self, stop_src, stop_dst, dts_src):
		'''Algorithm 4: Earliest arrival query.
			Actually a bicriteria query that finds
				min-transfer journeys as well, just called that in the paper.'''
		# XXX: special case of profile-query, should be merged into that
		timetable, lines, transfers = self.graph

		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')
		results = t.pareto.QueryResultParetoSet()
		R, Q = dict(), dict()

		def enqueue(trip, i, n, jtrips, _ss=t.public.SolutionStatus):
			i_max = len(trip) - 1 # for the purposes of "infinity" here
			if i >= R.get(trip, i_max): return
			Q.setdefault(n, list()).append(
				TripSegment(trip, i, R.get(trip, i_max), jtrips.copy()) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				R[trip_u] = min(i, R.get(trip_u, i_max))

		# Trips-to-destintaion index is used here instead of lines-to-destintaion,
		#  because footpath time deltas are tied to each trip stop times, and can't be
		#  generalized to lines with multiple of arrival-times for stop, as it is in the algo.
		trips_to_dst = dict() # {trip: (i, fp_delta)}
		for stop_q, fp in timetable.footpaths.from_stops_to(stop_dst):
			if stop_q == stop_dst: fp = None
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line:
					fp_delta = 0 if fp is None else fp.get_shortest(dts_src=trip[i].dts_arr)
					if fp_delta is None: continue
					trips_to_dst[trip] = i, fp_delta

		# Queue initial set of trips (reachable from stop_src) to examine
		for stop_q, fp in timetable.footpaths.to_stops_from(stop_src):
			fp_delta = fp.get_shortest(dts_src=dts_src) if stop_q != stop_src else 0
			if fp_delta is None: continue
			dts_q, jtrips = dts_src + fp_delta, list()
			if stop_q == stop_dst:
				results.add(t.base.QueryResult(dts_q, 0, jtrips))
				continue # can't be beaten on time or transfers - can only be extended
			for i, line in lines.lines_with_stop(stop_q):
				trip = line.earliest_trip(i, dts_q)
				if trip: enqueue(trip, i, 0, jtrips)

		# Main loop
		t_min, n = u.inf, 0
		while Q:
			for trip, b, e, jtrips in Q.pop(n):
				jtrips = jtrips + [trip]

				# Check if trip reaches stop_dst (or its footpath-vicinity) directly
				if trip in trips_to_dst:
					i_dst, fp_delta = trips_to_dst[trip]
					if b < i_dst: # can't reach previous stops, and b->b trips make no sense
						dts_dst = trip[i_dst].dts_arr + fp_delta
						if dts_dst < t_min:
							t_min = dts_dst
							results.add(t.base.QueryResult(dts_dst, n, jtrips))

				for i in range(b+1, e+1): # b < i <= e
					if trip[i].dts_arr >= t_min: break # after +1 transfer, it's guaranteed to be dominated
					for transfer in transfers.from_trip_stop(trip[i]):
						if transfer.ts_to.dts_arr >= t_min: continue
						enqueue(transfer.ts_to.trip, transfer.ts_to.stopidx, n+1, jtrips)

			n += 1

		return self.jtrips_to_journeys(
			timetable.footpaths, stop_src, stop_dst, dts_src, results )


	@timer
	def query_profile(self, stop_src, stop_dst, dts_edt=None, dts_ldt=None, max_transfers=15):
		'''Profile query, returning a list of pareto-optimal JourneySet results with Journeys
				from stop_src to stop_dst, with departure at stop_src in a day-time (dts) interval
				from dts_edt (earliest departure time) to dts_ldt (latest).'''
		timetable, lines, transfers = self.graph
		if dts_edt is None: dts_edt = timetable.dts_parse('00:00')
		if dts_ldt is None: dts_ldt = timetable.dts_parse('24:00')

		DepartureCriteriaCheck = namedtuple('DCCheck', 'trip stopidx dts_src journey')
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')

		results = t.pareto.QueryResultParetoSet()
		R, Q = dict(), dict()

		def enqueue(trip, i, n, jtrips, _ss=t.public.SolutionStatus):
			i_max = len(trip) - 1 # for the purposes of "infinity" here
			# Labels here are set for "n, trip" instead of "trip", so that
			#  they can be reused after n jumps back to 0 (see main loop below).
			if i >= R.get((n, trip), i_max): return
			Q.setdefault(n, list()).append(
				TripSegment(trip, i, R.get((n, trip), i_max), jtrips.copy()) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				i_min = min(i, R.get((n, trip_u), i_max))
				for m in range(n, max_transfers): R[m, trip_u] = i_min

		# Trips-to-destintaion index is used here instead of lines-to-destintaion,
		#  because footpath time deltas are tied to each trip stop times, and can't be
		#  generalized to lines with multiple of arrival-times for stop, as it is in the algo.
		trips_to_dst = dict() # {trip: (i, fp_delta)}
		for stop_q, fp in timetable.footpaths.from_stops_to(stop_dst):
			if stop_q == stop_dst: fp = None
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line:
					fp_delta = 0 if fp is None else fp.get_shortest(dts_src=trip[i].dts_arr)
					if fp_delta is None: continue
					trips_to_dst[trip] = i, fp_delta

		# Same as with earliest-arrival, queue set of trips reachable from stop_src,
		#  but instead of queuing all checks (one for each trip) with same departure time,
		#  queue one DepartureCriteriaCheck for every departure time of each trip from
		#  these reachable stops.
		profile_queue = list()
		for stop_q, fp in timetable.footpaths.to_stops_from(stop_src):
			if stop_q == stop_src: fp = None
			if stop_q == stop_dst:
				# Direct src-to-dst footpath can't be easily compared to
				#  other results, as it has no fixed departure/arrival times,
				#  hence added here as a special "exceptional" result.
				results.add_exception(t.base.QueryResult(None, 0, list()))
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line:
					fp_delta = 0 if fp is None else\
						fp.get_shortest(dts_src=dts_edt, dts_dst=trip[i].dts_dep)
					if fp_delta is None: continue
					dts_min, dts_max = trip[i].dts_arr - fp_delta, trip[i].dts_dep - fp_delta
					if not (dts_edt <= dts_max and dts_ldt >= dts_min): continue
					profile_queue.append(DepartureCriteriaCheck(trip, i, min(dts_ldt, dts_max), list()))
		# Latest departures are processed first because labels (R) are reused for the whole query,
		#  and journeys with later-dep-time dominate earlier, so they are processed first and all
		#  trips with earlier departure not improving on arrival time (not passing check in enqueue())
		#  will be suboptimal anyway, hence skipped.
		profile_queue.sort(key=op.attrgetter('dts_src'), reverse=True) # latest-to-earliest

		t_min_idx = dict() # indexed by n, so that it can be reused, same as R.
		for dts_src, checks in it.groupby(profile_queue, op.attrgetter('dts_src')):
			# Each iteration of this loop is same as an earliest-arrival query,
			#  with starting set of trips (with same departure time) pulled from profile_queue.
			# Labels for trips (R) can be reused, cutting down amount of work for 2+ checks dramatically.
			n = 0
			for trip, stopidx, dts_src, jtrips in checks: enqueue(trip, stopidx, n, jtrips)

			while Q and n < max_transfers:
				t_min = t_min_idx.get(n, u.inf)
				for trip, b, e, jtrips in Q.pop(n):
					jtrips = jtrips + [trip]

					# Check if trip reaches stop_dst (or its footpath-vicinity) directly
					if trip in trips_to_dst:
						i_dst, fp_delta = trips_to_dst[trip]
						if b < i_dst: # can't reach previous stops, and b->b trips make no sense
							dts_dst = trip[i_dst].dts_arr + fp_delta
							if dts_dst < t_min:
								t_min_idx[n] = dts_dst
								results.add(t.base.QueryResult(dts_dst, n, jtrips, dts_src))

					# Check if trip can lead to nondominated journeys, and queue trips reachable from it
					for i in range(b+1, e+1): # b < i <= e
						if trip[i].dts_arr >= t_min: break # after +1 transfer, it's guaranteed to be dominated
						for transfer in transfers.from_trip_stop(trip[i]):
							if transfer.ts_to.dts_arr >= t_min: continue
							enqueue(transfer.ts_to.trip, transfer.ts_to.stopidx, n+1, jtrips)

				n += 1
			Q.clear() # to flush n > max_transfers leftovers there

		return self.jtrips_to_journeys(
			timetable.footpaths, stop_src, stop_dst, dts_edt, results )


	def query_profile_all_to_all(self, max_transfers=15):
		'Run all-to-all profile query, yielding (stop_src, stop_labels) tuples.'
		# To avoid duplicating paper-1 algos' weird naming/types here:
		#  R -> trip_labels: Mapping[(n, Trip), int]
		#  Q -> queue: Sequence[TripSegment] (no point using Q-mapping here)

		timetable, lines, transfers = self.graph

		DepartureCriteriaCheck = namedtuple('DCCheck', 'trip stopidx dts_src ts_list')
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b ts_list')
		StopLabelSet = ft.partial( t.pareto.ParetoSet,
			lambda v: (v.dts_arr, len(v.ts_list) - 1, v.dts_dep) )

		stop_labels = dict() # {stop: ts_list (all TripStops on the way from stop_src to stop)}
		trip_tails_checked = dict() # {trip: earliest_checked_stopidx}

		def enqueue(trip, i, ts_list, _ss=t.public.SolutionStatus):
			'Ensures that each TripStop is only ever processed once via trip_tails_checked index.'
			n, i_max = len(ts_list), len(trip) - 1
			if i >= trip_tails_checked.get((n, trip), i_max): return
			queue.append(TripSegment(trip, i, trip_tails_checked.get((n, trip), i_max), ts_list.copy()))
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				i_min = min(i, trip_tails_checked.get((n, trip_u), i_max))
				for m in range(n, max_transfers+1): trip_tails_checked[m, trip_u] = i_min

		for stop_src in timetable.stops:
			stop_labels.clear()
			trip_tails_checked.clear()

			profile_queue = list()
			for stop_q, fp in timetable.footpaths.to_stops_from(stop_src):
				if stop_q == stop_src: fp = None
				for i, line in lines.lines_with_stop(stop_q):
					for trip in line:
						fp_delta = 0 if fp is None else fp.get_shortest(dts_dst=trip[i].dts_dep)
						if fp_delta is None: continue
						profile_queue.append(
							DepartureCriteriaCheck(trip, i, trip[i].dts_dep - fp_delta, list()) )
			profile_queue.sort(key=op.attrgetter('dts_src'), reverse=True) # latest-to-earliest

			for dts_src, checks in it.groupby(profile_queue, op.attrgetter('dts_src')):
				queue = list()
				for trip, stopidx, dts_src, ts_list in checks: enqueue(trip, stopidx, ts_list)

				for n in range(0, max_transfers):
					if not queue: break
					queue_prev, queue = queue, list()
					for trip, b, e, ts_list in queue_prev:
						ts_list = ts_list + [trip[b]] # trip[b] is transfer.ts_to - internal tree node
						for i in range(b+1, e+1): # b < i <= e
							ts = trip[i]

							# Update labels for all stops reachable from this TripStop
							for stop_q, fp in timetable.footpaths.to_stops_from(ts.stop):
								if stop_q == stop_src: continue
								fp_delta = fp.get_shortest(dts_src=trip[i].dts_arr)
								if fp_delta is None: continue
								stop_q_arr = ts.dts_arr + fp_delta
								if stop_q not in stop_labels: stop_labels[stop_q] = StopLabelSet()
								stop_labels[stop_q].add(t.base.StopLabel(dts_src, stop_q_arr, ts_list))

							for transfer in transfers.from_trip_stop(ts):
								enqueue(transfer.ts_to.trip, transfer.ts_to.stopidx, ts_list)

			yield stop_src, stop_labels

	@timer
	def build_tp_tree(self, **query_kws):
		'''Run all-to-all profile query to build Transfer-Patterns
			prefix-tree of stop_dst->stop_src Line connections.'''
		timetable, lines, transfers = self.graph

		tree = t.tp.TPTree() # adj-lists, with nodes being either Stop or Line objects
		subtree_stats = Counter()

		progress = self.progress_iter('transfer-patterns', len(timetable.stops))
		for stop_src, stop_labels in self.query_profile_all_to_all(**query_kws):
			means = subtree_stats['count']
			if means == 0: means = [0, 0, 0]
			else: means = list(int(subtree_stats[k] / means) for k in ['nodes', 'depth', 'dst'])
			progress.send([
				'tree-nodes={:,} (unique={:,}),'
					' subtree means: nodes={:,} depth={:,} breadth/dst-count={:,}',
				sum(tree.stats.total.values()), len(tree.stats.total) ] + means)

			subtree, subtree_depth = tree[stop_src], list()
			node_src = subtree.node(stop_src, t='src')
			for stop_dst, sl_set in stop_labels.items():
				node_dst = subtree.node(stop_dst)
				for sl in sl_set:
					node, depth = node_dst, 0
					for ts in reversed(sl.ts_list):
						node_prev, node = node, subtree.node(
							t.base.LineStop(lines.line_for_trip(ts.trip).id, ts.stopidx), no_path_to=node )
						node_prev.edges_to.add(node)
						depth += 1
					node.edges_to.add(node_src)
					subtree_depth.append(depth)
			subtree_stats.update(dict( count=1, dst=len(stop_labels),
				depth=u.max(subtree_depth, 0), nodes=tree.stats.prefix[stop_src] ))

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
		self.conf, self.log = conf or EngineConf(), u.get_logger('tb.tp')
		self.graph, self.tree = graph, tp_tree
		self.timer_wrapper = timer_func if timer_func else lambda f,*a,**k: f(*a,**k)
		self.jtrips_to_journeys = ft.partial(self.timer_wrapper, jtrips_to_journeys)

	@timer
	def build_query_tree(self, stop_src, stop_dst):
		query_tree = t.tp.TPTree(prefix=stop_src)
		subtree = self.tree[stop_src]

		queue = [(subtree[stop_dst], list())]
		while queue:
			queue_prev, queue = queue, list()
			for node, path in queue_prev:
				path = path + [node]
				for k in node.edges_to:
					node_k = subtree[k]
					if node_k.value != stop_src:
						queue.append((node_k, path.copy()))
						continue

					# Add src->...->dst path to query_tree, reusing LineStop nodes
					node = query_tree.node(node_k)
					for node_next in reversed(path): # reverse() because tp_tree has dst->...->src paths
						node_next = query_tree.node(node_next, no_path_to=node)
						node.edges_to.add(node_next)
						node = node_next

		qt_stats = query_tree.stat_counts()
		self.log.debug(
			'Query-tree stats: nodes={0.nodes:,} (unique={0.nodes_unique:,},'
				' stops={0.t_stop:,}, line-stops={0.t_line:,}), edges={0.edges:,}', qt_stats )
		return query_tree if qt_stats.nodes > 0 else None

	@timer
	def query_profile(self, stop_src, stop_dst, dts_edt, dts_ldt, query_tree=..., max_transfers=15):
		timetable, lines, transfers = self.graph
		if query_tree is ...: query_tree = self.build_query_tree(stop_src, stop_dst)
		if not query_tree: return list()

		NodeLabel = namedtuple('NodeLabel', 'dts_start ts n journey')
		NodeLabelCheck = namedtuple('NodeLabelChk', 'node label')

		node_labels = defaultdict(ft.partial(t.pareto.ParetoSet, 'ts.dts_arr n dts_start'))
		prio_queue = t.pareto.PrioQueue(lambda v: (-v.label.dts_start, v.label.ts.dts_arr, v.label.n))
		results = t.pareto.QueryResultParetoSet()

		# Queue starting points for each trip of the lines reachable from stop_src node
		for node in query_tree[stop_src].edges_to:
			if node.value == stop_dst:
				# Direct src-to-dst footpath can't be easily compared to
				#  other results, as it has no fixed departure/arrival times,
				#  hence added here as a special "exceptional" result.
				results.add_exception(t.base.QueryResult(None, 0, list()))
				continue
			ls_line, ls_stopidx = lines[node.value.line_id], node.value.stopidx
			ls_stop = ls_line.stops[ls_stopidx]
			fp = None if ls_stop == stop_src else timetable.footpaths.get(stop_src, ls_stop)
			for trip in ls_line:
				ts = trip[ls_stopidx]
				fp_delta = 0 if fp is None else fp.get_shortest(dts_src=dts_edt, dts_dst=ts.dts_dep)
				if fp_delta is None: continue
				dts_min, dts_max = ts.dts_arr - fp_delta, ts.dts_dep - fp_delta
				if not (dts_edt <= dts_max and dts_ldt >= dts_min): continue
				prio_queue.push(NodeLabelCheck(
					node, NodeLabel(min(dts_ldt, dts_max), ts, 0, [trip]) ))

		# Main loop
		while prio_queue:
			node_src, label_src = prio_queue.pop()

			for node in node_src.edges_to:
				if node.value != stop_dst:
					ls_line, ls_stopidx = lines[node.value.line_id], node.value.stopidx
					stop = ls_line.stops[ls_stopidx]
				else: ls_line, stop = None, node.value # ... -> stop_dst

				if not ls_line: # lineN -> stop_dst
					dts = min(
						(ts.dts_arr + timetable.footpaths.time_delta(
							ts.stop, stop, dts_src=ts.dts_arr, default=u.inf ))
						for ts in label_src.ts.trip[label_src.ts.stopidx+1:] )
					assert dts < u.inf # must be at least one, otherwise tp_tree is wrong
					node_label = NodeLabel( label_src.dts_start,
						t.public.TripStop.dummy_for_stop(stop, dts_arr=dts),
						label_src.n, label_src.journey )

				else: # lineN -> lineN+1
					node_transfers = list( transfer
						for ts in label_src.ts.trip[label_src.ts.stopidx+1:]
						for transfer in transfers.from_trip_stop(ts)
						if transfer.ts_to.stopidx == ls_stopidx
							and lines.line_for_trip(transfer.ts_to.trip) == ls_line )
					if node_transfers:
						transfer = min(node_transfers, key=op.attrgetter('ts_to.dts_arr'))
						node_label = NodeLabel( label_src.dts_start,
							transfer.ts_to, label_src.n+1, label_src.journey + [transfer.ts_to.trip] )
					else: node_label = None # only possible for other trips of node_src

				if node_label and node_labels[node].add(node_label):
					prio_queue.push(NodeLabelCheck(node, node_label))

		for label in node_labels[query_tree[stop_dst]]:
			results.add(t.base.QueryResult(label.ts.dts_arr, label.n, label.journey, label.dts_start))
		return self.jtrips_to_journeys(timetable.footpaths, stop_src, stop_dst, dts_edt, results)
