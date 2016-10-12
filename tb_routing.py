import itertools as it, operator as op, functools as ft
from collections import defaultdict
import logging

import attr


### Misc helpers/boilerplate

class LogMessage:
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger._log(level, LogMessage(msg, args, kws), (), log_kws)

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))

def attr_struct(cls):
	try:
		keys = cls.keys
		del cls.keys
	except AttributeError: pass
	else:
		if isinstance(keys, str): keys = keys.split()
		for k in keys: setattr(cls, k, attr.ib())
	return attr.s(cls, slots=True)


### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."

@attr_struct
class Timetable: keys = 'stops footpaths trips'

@attr_struct
class Stop: keys = 'id name lon lat'

@attr_struct
class TripStop: keys = 'stop dts_arr dts_dep'

stop_pair_key = lambda a,b: '\0'.join(sorted([a.id, b.id]))


### Routing engine

@attr_struct
class StopLine: keys = 'stopidx line'

@attr_struct
class Transfer: keys = 'trip_a stopidx_a trip_b stopidx_b'


class TBRoutingEngine:

	def __init__(self, timetable, cache=None):
		'''Creates Trip-Based Routing Engine from Timetable data.'''
		self.cache_wrapper = cache.run if cache else lambda f,*a,**kw: func(*a,**kw)
		self.log = get_logger('tb')

		lines = self.run(self.timetable_lines, timetable)
		transfers = self.run(self.precalc_transfer_set, timetable, lines)

		self.log.debug('Resulting transfer set size: {:,}', len(transfers))
		raise NotImplementedError

	def run(self, func, *args, **kws):
		'Calculation call wrapper for caching and benchmarking stuff.'
		return self.cache_wrapper(func, *args, **kws)


	def timetable_lines(self, tt):
		'''Line (pre-)calculation from Timetable data.

			Lines - trips with identical stop sequences,
				ordered from earliest-to-latest by arrival time.
			If one trip overtakes another (making
				ordering impossible), it will be split into diff line.'''

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
				for n, ts in enumerate(line[0]):
					stop_lines[ts.stop].append(StopLine(n, line))

		return dict(stop_lines.items())


	def precalc_transfer_set(self, tt, lines):
		transfers = list()

		## Algorithm 1: Initial transfer computation
		for trip_t in tt.trips:
			for i, ts_p in enumerate(trip_t[1:]):
				for stop_q in tt.stops.values():

					try: dt_fp_pq = tt.footpaths[stop_pair_key(ts_p.stop, stop_q)]
					except KeyError: continue # p->q is impossible on foot
					dts_q = ts_p.dts_arr + dt_fp_pq

					for j, line in map(attr.astuple, lines[stop_q]):
						for trip_u in line:
							# XXX: do mod() for dt on comparisons to wrap-around into next day
							if dts_q <= trip_u[j].dts_dep: break
						else: continue # all trips for L(q) have departed by dts_q
						transfers.append((trip_t, i, trip_u, j))

		self.log.debug('Initial transfer set size: {:,}', len(transfers))
		return transfers
