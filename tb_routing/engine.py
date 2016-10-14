import itertools as it, operator as op, functools as ft
from collections import defaultdict, namedtuple, deque

from . import utils as u, types as t


class TBRoutingEngine:

	dt_ch = 5*60 # fixed time-delta overhead for changing trips

	def __init__(self, timetable, timer_func=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.log = u.get_logger('tb')
		self.timer_wrapper = timer_func if timer_func else lambda f,*a,**k: func(*a,**k)

		pickle_cache = None
		if u.use_pickle_cache: pickle_cache = u.pickle_load()
		if pickle_cache: graph = pickle_cache
		else:
			lines = self.timetable_lines(timetable)
			transfers = self.precalc_transfer_set(timetable, lines)
			graph = t.internal.Graph(timetable, lines, transfers)
		if u.use_pickle_cache and not pickle_cache: u.pickle_dump(graph)
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
		transfers = self._pre_initial_set(timetable, lines)
		transfers = self._pre_remove_u_turns(transfers)
		transfers = self._pre_reduction(timetable, transfers)
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
		stop_arr, stop_ch = dict(), dict()

		def update_min_value(stop_map, stop, dts):
			if dts < stop_map.get(stop, u.inf):
				stop_map[stop] = dts
				return True
			return False

		discarded_n, progress = 0, self.progress_iter('pre_reduction', len(timetable.trips))
		for trip_t in timetable.trips:
			progress.send([ 'transfer set size: {:,},'
				' discarded (so far): {:,}', len(transfers), discarded_n ])

			for i in range(len(trip_t)-1, 0, -1): # first stop is skipped here as well
				ts_p, transfers_discard = trip_t[i], list()
				update_min_value(stop_arr, ts_p.stop, ts_p.dts_arr)

				for stop_q in timetable.stops:
					try: dt_fp_pq = timetable.footpaths[ts_p.stop, stop_q]
					except KeyError: continue
					dts_q = ts_p.dts_arr + dt_fp_pq

					update_min_value(stop_arr, stop_q, dts_q)
					update_min_value(stop_ch, stop_q, dts_q)

				for transfer_id, (_, _, trip_u, j) in transfers.from_trip_stop(trip_t, i):
					keep = False

					for k in range(j+1, len(trip_u)):
						ts_u = trip_u[k]
						keep = keep | update_min_value(stop_arr, ts_u.stop, ts_u.dts_arr)

						for stop_q in timetable.stops:
							try: dt_fp_pq = timetable.footpaths[ts_u.stop, stop_q]
							except KeyError: continue
							dts_q = ts_u.dts_arr + dt_fp_pq
							keep = keep | update_min_value(stop_arr, stop_q, dts_q)
							keep = keep | update_min_value(stop_ch, stop_q, dts_q)

					if not keep: transfers_discard.append(transfer_id)

				transfers.discard(transfers_discard)
				discarded_n += len(transfers_discard)

		self.log.debug('Discarded no-improvement transfers: {:,}', discarded_n)
		return transfers


	@timer
	def query_earliest_arrival(self, stop_src, stop_dst, dts_src):
		timetable, lines, transfers = self.graph
		R, Q = defaultdict(lambda: u.inf), dict()
		trip_segment = namedtuple('TripSeg', 'trip stopidx_a stopidx_b')

		def enqueue(trip, i, n, ss=t.public.SolutionStatus):
			if i >= R[trip]: return
			Q.setdefault(n, deque()).append(trip_segment(trip, i, R[trip]))
			for trip_u in lines.line_for_trip(trip)\
					.trips_by_relation(trip, ss.non_dominated, ss.equal):
				R[trip_u] = min(R[trip_u], i)

		lines_to_dst = list()
		for stop_q in timetable.stops:
			if stop_q is stop_dst: dt_fp = 0
			else:
				try: dt_fp = timetable.footpaths[stop_q, stop_dst]
				except KeyError: continue
			lines_to_dst.extend((i, line, dts_q) for i, line in lines[stop_q])
		lines_to_dst.sort(reverse=True, key=op.itemgetter(0))

		for stop_q in timetable.stops:
			if stop_q is stop_src: dt_fp = 0
			else:
				try: dt_fp = timetable.footpaths[stop_src, stop_q]
				except KeyError: continue
				dts_q = dts_src + dt_fp
				for i, line in lines[stop_q]:
					trip = line.earliest_trip(dts_q)
					if trip: enqueue(trip, i, 0)

		t_min, n, results = u.inf, 0, list()
		while Q:
			for trip, b, e in Q[n]:
				for i, line, dts_hop in lines_to_dst:
					if i <= b: break
					line_dts = trip[i].dts_arr + dts_hop
					if line_dts < t_min:
						t_min = line_dts
						results.append((t_min, n))
					if trip[b+1] < t_min:
						for i in range(b+1, e+1):
							for k, (_, _, trip_u, j) in transfers.from_trip_stop(trip, i):
								enqueue(trip_u, j, n+1)
			n += 1

		return results
