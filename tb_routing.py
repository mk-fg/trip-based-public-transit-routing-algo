import itertools as it, operator as op, functools as ft
from collections import defaultdict

import attr


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


### Lines (pre-)calculation from Timetable data
# Can be cached alongside input data and passed to TBRoutingEngine

@attr_struct
class Lines: keys = 'by_stop'

@attr_struct
class StopLine: keys = 'stopidx line'

def timetable_lines(tt):
	# Lines - trips with identical stop sequences, ordered from earliest-to-latest by arrival time
	# If one trip overtakes another (making ordering impossible) - split into diff line
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
			for n, ts in enumerate(line[0]): stop_lines[ts.stop].append(StopLine(n, line))

	return Lines(dict(stop_lines.items()))


### Routing engine

@attr_struct
class Transfer: keys = 'trip_a stopidx_a trip_b stopidx_b'

class TBRoutingEngine:

	def __init__(self, tt, lines=None):
		if not lines: lines = timetable_lines(tt)
		self.transfers = self.precalc_transfer_set(tt, lines)

	def precalc_transfer_set(self, tt, lines):
		transfers = list()

		## Algorithm 1: Initial transfer computation
		for trip_t in tt.trips:
			for i, ts_p in enumerate(trip_t[1:]):
				for stop_q in tt.stops.values():

					try: dt_fp_pq = tt.footpaths[stop_pair_key(ts_p.stop, stop_q)]
					except KeyError: continue # p->q is impossible on foot
					dts_q = ts_p.dts_arr + dt_fp_pq

					for j, line in map(attr.astuple, lines.by_stop[stop_q]):
						for trip_u in line:
							# XXX: do mod() for dt on comparisons to wrap-around into next day
							if dts_q <= trip_u[j].dts_dep: break
						else: continue # all trips for L(q) have departed by dts_q
						transfers.append(Transfer(trip_t, i, trip_u, j))
