import itertools as it, operator as op, functools as ft
from collections import defaultdict, namedtuple, deque

from . import utils as u, types as t


class TBRoutingEngine:

	graph = None
	dt_ch = 5*60 # fixed time-delta overhead for changing trips

	def __init__(self, timetable=None, cached_graph=None, timer_func=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.log = u.get_logger('tb')
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
	def progress_iter(self, prefix, n_max, steps=30, n=0):
		'Progress logging helper coroutine for long calculations.'
		steps = min(n_max, steps)
		step_n = n_max / steps
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

		lines = t.internal.Lines()
		for trips in line_trips.values():
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
		transfers = self._pre_remove_u_turns(transfers) # Algorithm 2
		transfers = self._pre_reduction(timetable, transfers) # Algorithm 3
		self.log.debug('Precalculated transfer set size: {:,}', len(transfers))
		return transfers

	@timer
	def _pre_initial_set(self, timetable, lines):
		'Algorithm 1: Initial transfer computation.'
		transfers = t.internal.TransferSet()

		for trip_t in timetable.trips:
			for i, ts_p in enumerate(trip_t):
				if i == 0: continue # "do not add any transfers from the first stop ..."

				for stop_q in timetable.stops:
					try: dt_fp_pq = timetable.footpaths[ts_p.stop, stop_q]
					except KeyError: continue # p->q is impossible on foot
					dts_q = ts_p.dts_arr + dt_fp_pq

					for j, line in lines.lines_with_stop(stop_q):
						if j == len(line[0]) - 1: continue # "do not add any transfers ... to the last stop"
						for trip_u in line:
							if dts_q <= trip_u[j].dts_dep: break
						else: continue # all trips for L(q) have departed by dts_q
						transfers.add(t.internal.Transfer(trip_t, i, trip_u, j))

		self.log.debug('Initial transfer set size: {:,}', len(transfers))
		return transfers

	@timer
	def _pre_remove_u_turns(self, transfers):
		'Algorithm 2: Remove U-turn transfers.'
		transfers_discard = list()
		for k, (trip_t, i, trip_u, j) in transfers:
			try: ts_t, ts_u = trip_t[i-1], trip_u[j+1]
			except IndexError: continue
			if ( ts_t.stop == ts_u.stop
					and ts_t.dts_arr + self.dt_ch <= ts_u.dts_dep ):
				transfers_discard.append(k)
		transfers.discard(transfers_discard)
		self.log.debug('Discarded U-turns: {:,}', len(transfers_discard))
		return transfers

	@timer
	def _pre_reduction(self, timetable, transfers):
		'Algorithm 3: Transfer reduction.'

		def update_min_time(min_time_map, stop, dts):
			if dts < min_time_map.get(stop, u.inf):
				min_time_map[stop] = dts
				return True
			return False

		discarded_n, progress = 0, self.progress_iter('pre_reduction', len(timetable.trips))
		for trip_t in timetable.trips:
			min_time_arr, min_time_ch = dict(), dict()
			progress.send([ 'transfer set size: {:,},'
				' discarded (so far): {:,}', len(transfers), discarded_n ])

			for i in range(len(trip_t)-1, 0, -1): # first stop is skipped here as well
				ts_p, transfers_discard = trip_t[i], list()
				update_min_time(min_time_arr, ts_p.stop, ts_p.dts_arr)

				for stop_q in timetable.stops:
					try: dt_fp_pq = timetable.footpaths[ts_p.stop, stop_q]
					except KeyError: continue
					dts_q = ts_p.dts_arr + dt_fp_pq

					update_min_time(min_time_arr, stop_q, dts_q)
					update_min_time(min_time_ch, stop_q, dts_q)

				for transfer_id, (_, _, trip_u, j) in transfers.from_trip_stop(trip_t, i):
					keep = False

					for k in range(j+1, len(trip_u)):
						ts_u = trip_u[k]
						keep = keep | update_min_time(min_time_arr, ts_u.stop, ts_u.dts_arr)

						for stop_q in timetable.stops: # XXX: add/use footpath-reachable index here
							try: dt_fp_pq = timetable.footpaths[ts_u.stop, stop_q]
							except KeyError: continue
							dts_q = ts_u.dts_arr + dt_fp_pq
							keep = keep | update_min_time(min_time_arr, stop_q, dts_q)
							keep = keep | update_min_time(min_time_ch, stop_q, dts_q)

					if not keep: transfers_discard.append(transfer_id)

				transfers.discard(transfers_discard)
				discarded_n += len(transfers_discard)

		self.log.debug('Discarded no-improvement transfers: {:,}', discarded_n)
		return transfers


	@timer
	def query_earliest_arrival(self, stop_src, stop_dst, dts_src):
		'''Algorithm 4: Earliest arrival query.
			Actually a bicriteria query that also finds
				min-transfer journeys as well, just called that in the paper.'''
		timetable, lines, transfers = self.graph

		R, Q = dict(), dict()
		TripSegment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b journey')

		JourneyTrip = namedtuple('JTrip', 'trip_stop_from trip_stop_to')
		JourneyFp = namedtuple('JFootpath', 'stop_from stop_to dt')
		Journey = namedtuple('Journey', 'dts_arr trips segments')
		journeys = list()

		def enqueue(trip, i, n, journey, ss=t.public.SolutionStatus):
			if i >= R.get(trip, u.inf): return
			Q.setdefault(n, deque()).append(
				TripSegment(trip, i, R.get(trip, len(trip)-1), journey) )
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, ss.non_dominated, ss.equal):
				R[trip_u] = min(i, R.get(trip_u, u.inf))

		lines_to_dst = dict() # (i, line, dt) indexed by trip
		for stop_q in timetable.stops: # XXX: add/use footpath-reachable index here
			if stop_q is stop_dst: dt_fp = 0
			else:
				try: dt_fp = timetable.footpaths[stop_q, stop_dst]
				except KeyError: continue
			for i, line in lines.lines_with_stop(stop_q):
				for trip in line: lines_to_dst.setdefault(trip, list()).append((i, line, dt_fp))
		for line_infos in lines_to_dst.values(): # so that all "i > b" come up first
			line_infos.sort(reverse=True, key=op.itemgetter(0))

		# Queue initial set of trips (reachable from stop_src) to examine
		for stop_q in timetable.stops: # XXX: add/use footpath-reachable index here
			journey = list()
			if stop_q is stop_src: dt_fp = 0
			else:
				try: dt_fp = timetable.footpaths[stop_src, stop_q]
				except KeyError: continue
				else: journey.append(JourneyFp(stop_src, stop_q, dt_fp))
			dts_q = dts_src + dt_fp
			for i, line in lines.lines_with_stop(stop_q):
				trip = line.earliest_trip(i, dts_q)
				## Note: footpath to first stop is not considered as +1 transfer here.
				##   Only first-by-index fp-reachable stop on the Line will be queued.
				##   Maybe some min(dt_fp) pre-enqueue filtering should be added here.
				if trip: enqueue(trip, i, 0, journey)

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
						jn_dst = journey.copy()
						jn_dst.append(JourneyTrip(trip[b], trip[i_dst]))
						if dt_fp: jn_dst.append(JourneyFp(trip[i_dst].stop, stop_dst, dt_fp))
						journeys.append(Journey(t_min, n, jn_dst))

				# Check if trip can lead to nondominated journeys, and queue trips reachable from it
				if trip[b+1].dts_arr < t_min:
					for i in range(b+1, e+1): # b < i <= e
						for k, (_, _, trip_u, j) in transfers.from_trip_stop(trip, i):
							jn_u = journey.copy()
							jn_u.append(JourneyTrip(trip[b], trip[i]))
							stop_i, stop_j = trip[i].stop, trip_u[j].stop
							if stop_i is not stop_j:
								jn_u.append(JourneyFp(stop_i, stop_j, timetable.footpaths[stop_i, stop_j]))
							enqueue(trip_u, j, n+1, journey)

			n += 1

		return journeys
