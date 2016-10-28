import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict, UserList
import bisect, enum, datetime

from .. import utils as u


class SolutionStatus(enum.Enum):
	'Used as a result for solution (e.g. Trip) comparisons.'
	dominated = False
	non_dominated = True
	equal = None
	undecidable = ...


### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."


@u.attr_struct(hash=False)
class Stop:
	keys = 'id name lon lat'
	def __hash__(self): return hash(self.id)

class Stops:
	def __init__(self): self.set_idx = dict()
	def add(self, stop):
		if stop.id in self.set_idx: stop = self.set_idx[stop.id]
		else: self.set_idx[stop.id] = stop
		return stop
	def __getitem__(self, stop_id): return self.set_idx[stop_id]
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.values())


class Footpaths:

	# XXX: these are currently bi-directional
	# Which is why to_stops_from == from_stops_to and
	#  "k1.id > k2.id" filter in stat_mean_dt gets "unique footpaths".

	def __init__(self): self.set_idx = defaultdict(dict)

	def add(self, stop_a, stop_b, dt):
		self.set_idx[stop_a][stop_b] = dt
		self.set_idx[stop_b][stop_a] = dt

	def discard_longer(self, dt_max):
		items = list(sorted( (v,(k1,k2))
			for k1,v1 in self.set_idx.items() for k2,v in v1.items() ))
		n = bisect.bisect_left(items, (dt_max, ()))
		for v,(k1,k2) in items[n:]:
			del self.set_idx[k1][k2]
			if not self.set_idx[k1]: del self.set_idx[k1]

	def stat_mean_dt(self):
		unique_dt_set = list( v for k1,v1 in
			self.set_idx.items() for k2,v in v1.items() if k1.id > k2.id )
		return sum(unique_dt_set) / len(unique_dt_set)

	def from_stops_to(self, stop): return self.set_idx[stop].items()
	to_stops_from = from_stops_to
	def between(self, stop_a, stop_b): return self.set_idx[stop_a][stop_b]

	def time_delta(self, stop_from, stop_to): return self.set_idx[stop_from][stop_to]

	def __len__(self): return sum(map(len, self.set_idx.values()))


trip_stop_daytime = lambda dts: dts % (24 * 3600)

@u.attr_struct(hash=False, repr=False)
class TripStop:
	trip = u.attr_init()
	stopidx = u.attr_init()
	stop = u.attr_init()
	dts_arr = u.attr_init(convert=trip_stop_daytime)
	dts_dep = u.attr_init(convert=trip_stop_daytime)

	def __hash__(self): return hash((self.trip, self.stopidx))
	def __repr__(self): # mostly to avoid recursion
		return ( 'TripStop('
			'trip_id={0.trip.id}, stopidx={0.stopidx}, stop={0.stop},'
			' dts_arr={0.dts_arr}, dts_dep={0.dts_dep})' ).format(self)

@u.attr_struct(hash=False)
class Trip:
	stops = u.attr_init(list)
	id = u.attr_init(lambda seq=iter(range(2**40)): next(seq))
	def __hash__(self): return hash(self.id)

	def add(self, stop): self.stops.append(stop)

	def compare(self, trip):
		'Return SolutionStatus for this trip as compared to other trip.'
		check = set(
			(None if sa.dts_arr == sb.dts_arr else sa.dts_arr < sb.dts_arr)
			for sa, sb in zip(self, trip) ).difference([None])
		if len(check) == 1: return SolutionStatus(check.pop())
		if not check: return SolutionStatus.equal
		return SolutionStatus.undecidable

	def __getitem__(self, n): return self.stops[n]
	def __len__(self): return len(self.stops)
	def __iter__(self): return iter(self.stops)

class Trips(UserList):
	def add(self, trip): self.append(trip)
	def stat_mean_stops(self): return sum(len(t) for t in self) / len(self)


@u.attr_struct
class Timetable: keys = 'stops footpaths trips'



### TBRoutingEngine query result

JourneyTrip = namedtuple('JTrip', 'ts_from ts_to')
JourneyFp = namedtuple('JFootpath', 'stop_from stop_to dt')

@u.attr_struct(slots=False, hash=False, repr=False)
class Journey:
	dts_start = u.attr_init()
	segments = u.attr_init(list)

	_stats_cache = None
	def _stats(self):
		if not self._stats_cache:
			dts_arr = trip_count = fp_count = 0
			dts_dep, dts_dep_fp = None, 0
			for seg in self.segments:
				if isinstance(seg, JourneyTrip):
					trip_count += 1
					dts_arr = seg.ts_to.dts_arr
					if dts_dep is None: dts_dep = seg.ts_from.dts_dep - dts_dep_fp
				elif isinstance(seg, JourneyFp):
					fp_count += 1
					dts_arr = dts_arr + seg.dt
					if dts_dep is None: dts_dep_fp += seg.dt
			if dts_dep is None: # no trips, only footpaths
				dts_dep, dts_arr = self.dts_start, self.dts_start + dts_arr
			self._stats_cache = dts_arr, dts_dep, trip_count, fp_count
		return self._stats_cache

	@property
	def dts_arr(self): return self._stats()[0]
	@property
	def dts_dep(self): return self._stats()[1]
	@property
	def trip_count(self): return self._stats()[2]
	@property
	def fp_count(self): return self._stats()[3]

	def copy(self): return Journey(self.segments.copy())

	def append_trip(self, *jtrip_args, **jtrip_kws):
		self.segments.append(JourneyTrip(*jtrip_args, **jtrip_kws))
		self._stats_cache = None
		return self

	def append_fp(self, stop_from, stop_to, dt):
		if stop_from is not stop_to:
			self.segments.append(JourneyFp(stop_from, stop_to, dt))
			self._stats_cache = None
		return self

	def compare(self, jn2, _ss=SolutionStatus):
		'Return SolutionStatus for this journey as compared to other journey.'
		if self.dts_arr == jn2.dts_arr and self.trip_count == jn2.trip_count:
			# Consider footpath_count only if dts_arr and trip_count are equal
			if self.fp_count == jn2.fp_count: return _ss.equal
			return [_ss.dominated, _ss.non_dominated][self.fp_count < jn2.fp_count]
		if self.dts_arr >= jn2.dts_arr and self.trip_count >= jn2.trip_count: return _ss.dominated
		if self.dts_arr <= jn2.dts_arr and self.trip_count <= jn2.trip_count: return _ss.non_dominated

	def __len__(self): return len(self.segments)
	def __iter__(self): return iter(self.segments)
	def __hash__(self): return id(self)

	@staticmethod
	def dts_format(dts):
		dts = int(dts)
		return datetime.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1)

	def __repr__(self):
		points = list()
		for seg in self.segments:
			if isinstance(seg, JourneyTrip):
				if not points:
					points.append(
						'{0.stopidx}:{0.stop.id}:{0.stop.name} [{dts_dep}]'\
						.format(seg.ts_from, dts_dep=self.dts_format(seg.ts_from.dts_dep)) )
				points.append(
					'{0.stopidx}:{0.stop.id}:{0.stop.name} [{dts_arr}]'\
					.format(seg.ts_to, dts_arr=self.dts_format(seg.ts_to.dts_arr)) )
			elif isinstance(seg, JourneyFp):
				points.append('(fp-to={0.id}:{0.name} dt={1})'.format(
					seg.stop_to, datetime.timedelta(seconds=int(seg.dt)) ))
		return '<Journey[ {} ]>'.format(' - '.join(points))

	def pretty_print(self, indent=0, **print_kws):
		p = lambda tpl,*a,**k: print(' '*indent + tpl.format(*a,**k), **print_kws)
		p( 'Journey {:x} (arrival: {}, trips: {}):',
			id(self), self.dts_format(self.dts_arr), self.trip_count )
		for seg in self.segments:
			if isinstance(seg, JourneyTrip):
				p('  trip [{}]:', seg.ts_from.trip.id)
				p( '    from (dep at {dts_dep}): {0.stopidx}:{0.stop.name} [{0.stop.id}]',
					seg.ts_from, dts_dep=self.dts_format(seg.ts_from.dts_dep) )
				p( '    to (arr at {dts_arr}): {0.stopidx}:{0.stop.name} [{0.stop.id}]',
					seg.ts_to, dts_arr=self.dts_format(seg.ts_to.dts_arr) )
			elif isinstance(seg, JourneyFp):
				p('  footpath (time: {}):', datetime.timedelta(seconds=int(seg.dt)))
				p('    from: {0.name} [{0.id}]', seg.stop_from)
				p('    to: {0.name} [{0.id}]', seg.stop_to)


@u.attr_struct
class JourneySet:
	journeys = u.attr_init(set)

	def add(self, journey):
		'''Add Journey, maintaining pareto-optimality of the set.'''
		for jn2 in list(self.journeys):
			ss = journey.compare(jn2)
			if ss is SolutionStatus.dominated: break
			if ( ss is SolutionStatus.non_dominated
				and jn2.compare(journey) is SolutionStatus.dominated ): self.journeys.remove(jn2)
		else: self.journeys.add(journey)

	def __len__(self): return len(self.journeys)
	def __iter__(self): return iter(self.journeys)

	def pretty_print(self, indent=0, **print_kws):
		print(' '*indent + 'Journey set ({}):'.format(len(self.journeys)))
		for journey in self.journeys:
			print()
			journey.pretty_print(indent=indent+2, **print_kws)
