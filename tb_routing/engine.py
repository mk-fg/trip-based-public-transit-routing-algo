import itertools as it, operator as op, functools as ft
from collections import defaultdict

from . import utils as u, types as t


class TBRoutingEngine:

	dt_ch = 5*60 # fixed time-delta overhead for changing trips

	def __init__(self, timetable, timer=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.log = u.get_logger('tb')
		self.timer_wrapper = timer if timer else lambda f,*a,**k: func(*a,**k)

		lines = self.timetable_lines(timetable)
		transfers = self.precalc_transfer_set(timetable, lines)

		self.log.debug('Resulting transfer set size: {:,}', len(transfers))
		u.pickle_dump([timetable, lines, transfers])
		raise NotImplementedError

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
	def timetable_lines(self, tt):
		'Line (pre-)calculation from Timetable data.'

		line_trips = defaultdict(list)
		line_stops = lambda trip: tuple(map(op.attrgetter('stop'), trip))
		for trip in tt.trips: line_trips[line_stops(trip)].append(trip)

		lines = t.internal.Lines()
		for trips in line_trips.values():
			lines_for_stopseq = list()

			# Split same-stops trips into non-overtaking groups
			for a in trips:
				for line in lines_for_stopseq:
					for b in line:
						# XXX: move into t.Line
						overtake_check = set( # True: a ≺ b, False: b ≺ a, None: a == b
							(None if sa.dts_arr == sb.dts_arr else sa.dts_arr <= sb.dts_arr)
							for sa, sb in zip(a, b) ).difference([None])
						if len(overtake_check) == 1: continue # can be ordered
						if not overtake_check: a = None # discard exact duplicates
						break # can't be ordered - split into diff line
					else:
						line.add(a)
						break
					if not a: break
				else: lines_for_stopseq.append(t.internal.Line(a)) # failed to find line to group trip into

			lines.add(*lines_for_stopseq)

		return lines


	@timer
	def precalc_transfer_set(self, tt, lines):
		# Precalculation steps here are not merged and not parallelized in any way.
		transfers = self._pre_initial_set(tt, lines)
		transfers = self._pre_remove_u_turns(transfers)
		transfers = self._pre_reduction(tt, transfers)
		return transfers

	@timer
	def _pre_initial_set(self, tt, lines):
		'Algorithm 1: Initial transfer computation.'
		transfers = t.internal.TransferSet()

		for trip_t in tt.trips:
			for i, ts_p in enumerate(trip_t):
				if i == 0: continue # "do not add any transfers from the first stop ..."

				for stop_q in tt.stops:
					try: dt_fp_pq = tt.footpaths[ts_p.stop, stop_q]
					except KeyError: continue # p->q is impossible on foot
					dts_q = ts_p.dts_arr + dt_fp_pq

					for j, line in lines[stop_q.id]:
						if j == len(line[0]) - 1: continue # "do not add any transfers ... to the last stop"
						for trip_u in line:
							# XXX: do mod() for dt on comparisons to wrap-around into next day
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
	def _pre_reduction(self, tt, transfers, inf=float('inf')):
		'Algorithm 3: Transfer reduction.'
		stop_arr, stop_ch = dict(), dict()

		def set_min(stop_map, stop_id, dts):
			if dts < stop_map.get(stop_id, inf):
				stop_map[stop_id] = dts
				return True
			return False

		discarded_n, progress = 0, self.progress_iter('pre_reduction', len(tt.trips))
		for trip_t in tt.trips:
			progress.send([ 'transfer set size: {:,},'
				' discarded (so far): {:,}', len(transfers), discarded_n ])

			for i in range(len(trip_t)-1, 0, -1): # first stop is skipped here as well
				ts_p, transfers_discard = trip_t[i], list()
				set_min(stop_arr, ts_p.stop.id, ts_p.dts_arr)

				for stop_q in tt.stops:
					try: dt_fp_pq = tt.footpaths[ts_p.stop, stop_q]
					except KeyError: continue
					dts_q = ts_p.dts_arr + dt_fp_pq

					set_min(stop_arr, stop_q.id, dts_q)
					set_min(stop_ch, stop_q.id, dts_q)

				for transfer_id, (_, _, trip_u, j) in transfers.from_trip_stop(trip_t, i):
					keep = False

					for k in range(j+1, len(trip_u)):
						ts_u = trip_u[k]
						keep = keep | set_min(stop_arr, ts_u.stop.id, ts_u.dts_arr)

						for stop_q in tt.stops:
							try: dt_fp_pq = tt.footpaths[ts_u.stop, stop_q]
							except KeyError: continue
							dts_q = ts_u.dts_arr + dt_fp_pq
							keep = keep | set_min(stop_arr, stop_q.id, dts_q)
							keep = keep | set_min(stop_ch, stop_q.id, dts_q)

					if not keep: transfers_discard.append(transfer_id)

				transfers.discard(transfers_discard)
				discarded_n += len(transfers_discard)

		self.log.debug('Discarded no-improvement transfers: {:,}', discarded_n)
		return transfers
