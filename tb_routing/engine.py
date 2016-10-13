import itertools as it, operator as op, functools as ft
from collections import defaultdict

from . import utils as u, types as t


class TBRoutingEngine:

	def __init__(self, conf, timetable, cache=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.conf, self.log = conf, u.get_logger('tb')
		self.cache_wrapper = cache.run if cache else lambda f,*a,**k: func(*a,**k)

		lines = self.timetable_lines(timetable)
		transfers = self.precalc_transfer_set(timetable, lines)

		self.log.debug('Resulting transfer set size: {:,}', len(transfers))
		raise NotImplementedError

	def cached(self_or_dec, func=None, *args, **kws):
		'''Calculation call wrapper for caching and benchmarking stuff. Can be used as a decorator.'''
		if not func: return lambda s,*a,**k: s.cache_wrapper(self_or_dec, s, *a, **k)
		return self_or_dec.cache_wrapper(func, *args, **kws)


	@cached
	def timetable_lines(self, tt):
		'''Line (pre-)calculation from Timetable data.

			Lines - trips with identical stop sequences,
				ordered from earliest-to-latest by arrival time on ALL stops.
			If one trip overtakes another (making
				such strict ordering impossible), they will be split into different lines.'''

		line_trips = defaultdict(list)
		line_stops = lambda trip: tuple(map(op.attrgetter('stop'), trip))
		for trip in tt.trips: line_trips[line_stops(trip)].append(trip)

		stop_lines = defaultdict(list)
		for trips in line_trips.values():
			lines_for_stopseq = list()

			# Split same-stops trips into non-overtaking groups
			for a in trips:
				for line in lines_for_stopseq:
					for b in line:
						overtake_check = set( # True: a ≺ b, False: b ≺ a, None: a == b
							(None if sa.dts_arr == sb.dts_arr else sa.dts_arr <= sb.dts_arr)
							for sa, sb in zip(a, b) ).difference([None])
						if len(overtake_check) == 1: continue # can be ordered
						if not overtake_check: a = None # discard exact duplicates
						break # can't be ordered - split into diff line
					else:
						line.append(a)
						break
					if not a: break
				else: lines_for_stopseq.append([a]) # failed to find line to group trip into

			for line in lines_for_stopseq:
				line.sort(key=lambda trip: sum(map(op.attrgetter('dts_arr'), trip)))
				for n, ts in enumerate(line[0]): stop_lines[ts.stop.id].append((n, line))

		return dict(stop_lines.items())


	@cached
	def precalc_transfer_set(self, tt, lines):
		# Precalculation steps here are not merged and not parallelized in any way.
		transfers = self._pre_initial_set(tt, lines)
		transfers = self._pre_remove_u_turns(transfers)
		transfers = self._pre_reduction(tt, transfers)
		return transfers

	@cached
	def _pre_initial_set(self, tt, lines):
		'Algorithm 1: Initial transfer computation.'
		transfers = dict()

		for trip_t in tt.trips:
			for i, ts_p in enumerate(trip_t):
				if i == 0: continue # "do not add any transfers from the first stop ..."

				for stop_q in tt.stops.values():
					try: dt_fp_pq = tt.footpaths[t.stop_pair_key(ts_p.stop, stop_q)]
					except KeyError: continue # p->q is impossible on foot
					dts_q = ts_p.dts_arr + dt_fp_pq

					for j, line in lines[stop_q.id]:
						if j == len(line[0]) - 1: continue # "do not add any transfers ... to the last stop"
						for trip_u in line:
							# XXX: do mod() for dt on comparisons to wrap-around into next day
							if dts_q <= trip_u[j].dts_dep: break
						else: continue # all trips for L(q) have departed by dts_q
						transfers[len(transfers)] = trip_t, i, trip_u, j

		self.log.debug('Initial transfer set size: {:,}', len(transfers))
		return transfers

	@cached
	def _pre_remove_u_turns(self, transfers):
		'Algorithm 2: Remove U-turn transfers.'
		transfers_discard = list()
		for k, (trip_t, i, trip_u, j) in transfers.items():
			try: ts_t, ts_u = trip_t[i-1], trip_u[j+1]
			except IndexError: continue
			if ( ts_t.stop == ts_u.stop
					and ts_t.dts_arr + self.conf.dt_ch <= ts_u.dts_dep ):
				transfers_discard.append(k)
		for k in transfers_discard: del transfers[k]
		self.log.debug('Discarded U-turns: {:,}', len(transfers_discard))
		return transfers

	@cached
	def _pre_reduction(self, tt, transfers, inf=float('inf')):
		'Algorithm 3: Transfer reduction.'
		transfers_discard, stop_arr, stop_ch = list(), dict(), dict()

		def set_min(stop_map, stop_id, dts):
			if dts < stop_map.get(stop_id, inf):
				stop_map[stop_id] = dts
				return True
			return False

		for trip_t in tt.trips:
			for i in range(len(trip_t)-1, 0, -1): # first stop is skipped here as well
				ts_p = trip_t[i]
				set_min(stop_arr, ts_p.stop.id, ts_p.dts_arr)

				for stop_q in tt.stops.values():
					try: dt_fp_pq = tt.footpaths[t.stop_pair_key(ts_p.stop, stop_q)]
					except KeyError: continue
					dts_q = ts_p.dts_arr + dt_fp_pq

					set_min(stop_arr, stop_q.id, dts_q)
					set_min(stop_ch, stop_q.id, dts_q)

				for k, (trip_t, i, trip_u, j) in transfers.items():
					keep = False

					for k in range(j+1, len(trip_u)):
						ts_u = trip_u[k]
						keep = keep | set_min(stop_arr, ts_u.stop.id, ts_u.dts_arr)

						for stop_q in tt.stops.values():
							try: dt_fp_pq = tt.footpaths[t.stop_pair_key(ts_u.stop, stop_q)]
							except KeyError: continue
							dts_q = ts_u.dts_arr + dt_fp_pq
							keep = keep | set_min(stop_arr, stop_q.id, dts_q)
							keep = keep | set_min(stop_ch, stop_q.id, dts_q)

					if not keep: transfers_discard.append(k)

		for k in transfers_discard: del transfers[k]
		self.log.debug('Discarded no-improvement transfers: {:,}', len(transfers_discard))
		return transfers
