import itertools as it, operator as op, functools as ft
from collections import defaultdict, namedtuple, deque

from . import utils as u, types as t


@u.attr_struct(vals_to_attrs=True)
class EngineConf:
	log_progress_for = None # or a set/list of prefixes
	log_progress_steps = 30


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
			graph = t.internal.Graph(timetable, lines, transfers)
		self.graph = graph


	def timer(self_or_func, func=None, *args, **kws):
		'Calculation call wrapper for timer/progress logging.'
		if not func: return lambda s,*a,**k: s.timer_wrapper(self_or_func, s, *a, **k)
		return self_or_func.timer_wrapper(func, *args, **kws)

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

		lines, progress = t.internal.Lines(), self.progress_iter('lines', len(line_trips))
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
					lines_for_stopseq.append(t.internal.Line(trip_a))

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
		transfers = t.internal.TransferSet()

		progress = self.progress_iter('pre_initial_set', len(timetable.trips))
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
							line is not lines.line_for_trip(trip_t)
							or trip_u.compare(trip_t) is t.public.SolutionStatus.non_dominated
							or j < i ): continue
						transfers.add(t.internal.Transfer(trip_t[i], trip_u[j], dt_fp))

		self.log.debug('Initial transfer set size: {:,}', len(transfers))
		return transfers

	@timer
	def _pre_remove_u_turns(self, transfers, footpaths):
		'Algorithm 2: Remove U-turn transfers.'
		discard_count = 0
		for transfer in transfers:
			try:
				ts_t = transfer.ts_from.trip[transfer.ts_from.stopidx-1]
				ts_u = transfer.ts_to.trip[transfer.ts_to.stopidx+1]
			except IndexError: continue # transfers from-start/to-end of t/u trips
			if ts_t.stop is ts_u.stop:
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

		discard_count, progress = 0, self.progress_iter('pre_reduction', len(timetable.trips))
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

				for transfer in transfers.from_trip_stop(ts_p):
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


	@timer
	def query_earliest_arrival(self, stop_src, stop_dst, dts_src):
		'''Algorithm 4: Earliest arrival query.
			Actually a bicriteria query that finds
				min-transfer journeys as well, just called that in the paper.'''
		# XXX: special case of profile-query, should be merged into that
		timetable, lines, transfers = self.graph

		TripTransferCheck = namedtuple('TTCheck', 'dt_fp trip stopidx n journey')
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')

		journeys = t.public.JourneySet()
		R, Q = dict(), dict()

		## Note: this sub-queue is used fix original algo's quirk where
		##   additional unnecessary footpaths are not factored into optimality.
		##  In original paper, first transfer to other TripSegment to be enqueue()'d
		##   "wins" for all of the stops on it (by updating index R),
		##    regardless of later-enqueue()'d segments with more optimal journeys.
		subqueue = list() # Sequence[TripTransferCheck]
		def subqueue_flush():
			'enqueue() all TripTransferCheck segments in a most-optimal-first order.'
			subqueue.sort(key=op.attrgetter('dt_fp', 'stopidx', 'trip.id'))
			for tt_chk in subqueue: enqueue(tt_chk.trip, tt_chk.stopidx, tt_chk.n, tt_chk.journey)
			subqueue.clear()

		def enqueue(trip, i, n, journey, _ss=t.public.SolutionStatus):
			i_max = len(trip) - 1 # for the purposes of "infinity" here
			if i >= R.get(trip, i_max): return
			Q.setdefault(n, deque()).append(
				TripSegment(trip, i, R.get(trip, i_max), journey) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				R[trip_u] = min(i, R.get(trip_u, i_max))

		lines_to_dst = dict() # (i, line, dt) indexed by trip
		for stop_q, dt_fp in timetable.footpaths.from_stops_to(stop_dst):
			if stop_q is stop_dst: dt_fp = 0
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line: lines_to_dst.setdefault(trip, list()).append((i, line, dt_fp))
		for line_infos in lines_to_dst.values(): # so that all "i > b" come up first
			line_infos.sort(reverse=True, key=op.itemgetter(0))

		# Queue initial set of trips (reachable from stop_src) to examine
		for stop_q, dt_fp in timetable.footpaths.to_stops_from(stop_src):
			if stop_q is stop_src: dt_fp = 0
			dts_q = dts_src + dt_fp
			journey = t.public.Journey(dts_src)
			journey.append_fp(stop_src, stop_q, dt_fp)
			if stop_q is stop_dst:
				journeys.add(journey)
				continue # can't be beaten on time or transfers - can only be extended
			for i, line in lines.lines_with_stop(stop_q):
				## Note: "t ← earliest trip" is usually not desirable as a first trip.
				##  I.e. you'd usually prefer to pick latest trip possible to min dep-to-arr time.
				trip = line.earliest_trip(i, dts_q)
				if trip: subqueue.append(TripTransferCheck(dt_fp, trip, i, 0, journey))
		subqueue_flush()

		# Main loop
		t_min, n = u.inf, 0
		while Q:
			for trip, b, e, journey in Q.pop(n):

				# Check if trip reaches stop_dst (or its footpath-vicinity) directly
				for i_dst, line, dt_fp in lines_to_dst.get(trip, list()):
					if i_dst <= b: break # can't reach previous stop
					line_dts_dst = trip[i_dst].dts_arr + dt_fp
					if line_dts_dst < t_min:
						t_min = line_dts_dst
						jn_dst = journey.copy().append_trip(trip[b], trip[i_dst])
						jn_dst.append_fp(trip[i_dst].stop, stop_dst, dt_fp)
						journeys.add(jn_dst, dts_dep_criteria=False)

				# Check if trip can lead to nondominated journeys, and queue trips reachable from it
				if trip[b+1].dts_arr < t_min:
					for i in range(b+1, e+1): # b < i <= e
						for transfer in transfers.from_trip_stop(trip[i]):
							ts_u, dt_fp = transfer.ts_to, transfer.dt
							jn_u = journey.copy().append_trip(trip[b], trip[i])
							stop_i, stop_j = trip[i].stop, ts_u.stop
							jn_u.append_fp(stop_i, stop_j, dt_fp)
							subqueue.append(TripTransferCheck(dt_fp, ts_u.trip, ts_u.stopidx, n+1, jn_u))

			subqueue_flush()
			n += 1

		return journeys


	@timer
	def query_profile(self, stop_src, stop_dst, dts_edt, dts_ldt, max_transfers=15):
		'''Profile query, returning a list of pareto-optimal JourneySet results with Journeys
				from stop_src to stop_dst, with departure at stop_src in a day-time (dts) interval
				from dts_edt (earliest departure time) to dts_ldt (latest).'''
		timetable, lines, transfers = self.graph

		DepartureCriteriaCheck = namedtuple('DCCheck', 'trip stopidx dts_src journey')
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')

		journeys = t.public.JourneySet()
		R, Q = dict(), dict()

		def enqueue(trip, i, n, journey, _ss=t.public.SolutionStatus):
			i_max = len(trip) - 1 # for the purposes of "infinity" here
			if i >= R.get((n, trip), i_max): return
			Q.setdefault(n, deque()).append(
				TripSegment(trip, i, R.get((n, trip), i_max), journey) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, _ss.non_dominated, _ss.equal):
				i_min = min(i, R.get((n, trip_u), i_max))
				for n in range(n, max_transfers): R[n, trip_u] = i_min

		lines_to_dst = dict() # (i, line, dt) indexed by trip
		for stop_q, dt_fp in timetable.footpaths.from_stops_to(stop_dst):
			if stop_q is stop_dst: dt_fp = 0
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line: lines_to_dst.setdefault(trip, list()).append((i, line, dt_fp))
		for line_infos in lines_to_dst.values(): # so that all "i > b" come up first
			line_infos.sort(reverse=True, key=op.itemgetter(0))

		profile_queue = list()
		for stop_q, dt_fp in timetable.footpaths.to_stops_from(stop_src):
			if stop_q is stop_src: dt_fp = 0
			# XXX: special fp-only journeys that work anytime
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line:
					dts_trip = trip[i].dts_dep
					dts_min, dts_max = dts_trip + dt_fp, dts_trip - dt_fp
					if not (dts_min >= dts_edt and dts_max <= dts_ldt): continue
					journey = t.public.Journey(dts_max)
					journey.append_fp(stop_src, stop_q, dt_fp)
					profile_queue.append(DepartureCriteriaCheck(trip, i, dts_max, journey))
		profile_queue.sort(key=op.attrgetter('dts_src'), reverse=True) # latest-to-earliest

		t_min_idx = dict()
		for dts_src, checks in it.groupby(profile_queue, op.attrgetter('dts_src')):
			n = 0
			for trip, stopidx, dts_src, journey in checks: enqueue(trip, stopidx, n, journey)
			while Q and n < max_transfers:
				t_min = t_min_idx.get(n, u.inf)
				for trip, b, e, journey in Q.pop(n):

					# Check if trip reaches stop_dst (or its footpath-vicinity) directly
					for i_dst, line, dt_fp in lines_to_dst.get(trip, list()):
						if i_dst <= b: break # can't reach previous stop
						line_dts_dst = trip[i_dst].dts_arr + dt_fp
						if line_dts_dst < t_min:
							t_min_idx[n] = line_dts_dst
							jn_dst = journey.copy().append_trip(trip[b], trip[i_dst])
							jn_dst.append_fp(trip[i_dst].stop, stop_dst, dt_fp)
							journeys.add(jn_dst)

					# Check if trip can lead to nondominated journeys, and queue trips reachable from it
					if trip[b+1].dts_arr < t_min:
						for i in range(b+1, e+1): # b < i <= e
							for transfer in transfers.from_trip_stop(trip[i]):
								ts_u, dt_fp = transfer.ts_to, transfer.dt
								jn_u = journey.copy().append_trip(trip[b], trip[i])
								stop_i, stop_j = trip[i].stop, ts_u.stop
								jn_u.append_fp(stop_i, stop_j, dt_fp)
								enqueue(ts_u.trip, ts_u.stopidx, n+1, jn_u)

				n += 1
			Q.clear()

		return journeys
