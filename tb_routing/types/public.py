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

	@classmethod
	def better_if(cls, check):
		return [cls.dominated, cls.non_dominated][bool(check)]


### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."


@u.attr_struct(repr=False, cmp=False)
class Stop:
	keys = 'id name lon lat'
	def __hash__(self): return hash(self.id)
	def __eq__(self, stop): return u.same_type_and_id(self, stop)
	def __repr__(self):
		if self.id == self.name: return '<Stop {}>'.format(self.id)
		return '<Stop {} [{}]>'.format(self.name, self.id)

class Stops:
	def __init__(self): self.set_idx = dict()

	def add(self, stop):
		if stop.id in self.set_idx: stop = self.set_idx[stop.id]
		else: self.set_idx[stop.id] = stop
		return stop

	def get(self, stop):
		if isinstance(stop, Stop): stop = stop.id
		if stop not in self.set_idx: return
		return self.set_idx[stop]

	def __getitem__(self, stop_id): return self.set_idx[stop_id]
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.values())


class Footpaths:

	def __init__(self):
		self.set_idx_to, self.set_idx_from = dict(), dict()

	def add(self, stop_a, stop_b, dt):
		self.set_idx_to.setdefault(stop_a, dict())[stop_b] = dt
		self.set_idx_from.setdefault(stop_b, dict())[stop_a] = dt
		self._stats_cache = None

	def discard_longer(self, dt_max):
		items = sorted(
			((v,(k1,k2)) for k1,v1 in self.set_idx_to.items() for k2,v in v1.items()),
			key=op.itemgetter(0) )
		n = bisect.bisect_left(items, (dt_max, ()))
		for v,(k1,k2) in items[n:]:
			try:
				del self.set_idx_to[k1][k2]
				if not self.set_idx_to[k1]: del self.set_idx_from[k2]
			except KeyError: pass
			try:
				del self.set_idx_from[k2][k1]
				if not self.set_idx_from[k2]: del self.set_idx_from[k1]
			except KeyError: pass
		self._stats_cache = None

	def to_stops_from(self, stop): return self.set_idx_to[stop].items()
	def from_stops_to(self, stop): return self.set_idx_from[stop].items()

	def time_delta(self, stop_from, stop_to, default=...):
		if default is ...: return self.set_idx_to[stop_from][stop_to]
		return self.set_idx_to[stop_from].get(stop_to, default)

	def between(self, stop_a, stop_b):
		'Return footpath dt in any direction between two stops.'
		try: return self.set_idx_to[stop_a][stop_b]
		except KeyError: return self.set_idx_to[stop_b][stop_a]

	_stats_cache = None
	def _stats(self):
		if not self._stats_cache:
			dt_sum = dt_count = ch_count = 0
			for k1,v1 in self.set_idx_to.items():
				for k2,v in v1.items():
					dt_sum, dt_count = dt_sum + v, dt_count + 1
					if k1 == k2: ch_count += 1
			self._stats_cache = dt_sum, dt_count, ch_count
		return self._stats_cache

	def stat_mean_dt(self):
		dt_sum, dt_count, ch_count = self._stats()
		return dt_sum / dt_count
	def stat_same_stop_count(self): return self._stats()[2]

	def __len__(self): return self._stats()[1]


@u.attr_struct(repr=False)
class TripStop:
	trip = u.attr_init()
	stopidx = u.attr_init()
	stop = u.attr_init()
	dts_arr = u.attr_init()
	dts_dep = u.attr_init()

	@classmethod
	def dummy_for_stop(cls, stop, dts_arr=0, dts_dep=0):
		return cls(None, 0, stop, dts_arr, dts_dep)

	def __hash__(self): return hash((self.trip, self.stopidx))
	def __repr__(self): # mostly to avoid recursion
		return ( 'TripStop('
				'trip_id={line_id_hint}{trip_id}, stopidx={0.stopidx},'
				' stop_id={0.stop.id}, dts_arr={0.dts_arr}, dts_dep={0.dts_dep})' )\
			.format( self,
				trip_id=self.trip.id if self.trip else None,
				line_id_hint='{}:'.format(self.trip.line_id_hint) if self.trip.line_id_hint else '' )

@u.attr_struct(cmp=False)
class Trip:
	stops = u.attr_init(list)
	id = u.attr_init_id()
	line_id_hint = u.attr_init(None) # can be set for introspection/debugging

	def __hash__(self): return hash(self.id)
	def __eq__(self, trip): return self.id == trip.id

	def add(self, stop):
		assert stop.dts_arr <= stop.dts_dep
		assert self.stops[-1].dts_dep < stop.dts_arr
		self.stops.append(stop)

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
	def add(self, trip):
		assert len(trip) >= 2, trip
		self.append(trip)
	def stat_mean_stops(self):
		if not len(self): return 0
		return (sum(len(t) for t in self) / len(self))


@u.attr_struct
class Timetable: keys = 'stops footpaths trips'



### TBRoutingEngine query result

JourneyTrip = namedtuple('JTrip', 'ts_from ts_to')
JourneyFp = namedtuple('JFootpath', 'stop_from stop_to dt')

@u.attr_struct(slots=False, repr=False)
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

	def copy(self):
		attrs = u.attr.asdict(self)
		attrs['segments'] = self.segments.copy()
		return Journey(**attrs)

	def append_trip(self, *jtrip_args, **jtrip_kws):
		self.segments.append(JourneyTrip(*jtrip_args, **jtrip_kws))
		self._stats_cache = None
		return self

	def append_fp(self, stop_from, stop_to, dt):
		if not (stop_from == stop_to or dt == 0):
			self.segments.append(JourneyFp(stop_from, stop_to, dt))
			self._stats_cache = None
		return self

	def compare(self, jn2, _ss=SolutionStatus):
		'Return SolutionStatus for this journey as compared to other journey.'
		jn1 = self
		if jn1.dts_arr == jn2.dts_arr and jn1.trip_count == jn2.trip_count:
			if jn1.dts_dep != jn2.dts_dep: return _ss.better_if(jn1.dts_dep > jn2.dts_dep)
			if jn1.fp_count != jn2.fp_count: return _ss.better_if(jn1.fp_count < jn2.fp_count)
			return _ss.equal
		if jn1.dts_arr >= jn2.dts_arr and jn1.trip_count >= jn2.trip_count: return _ss.dominated
		if jn1.dts_arr <= jn2.dts_arr and jn1.trip_count <= jn2.trip_count: return _ss.non_dominated

	def __len__(self): return len(self.segments)
	def __iter__(self): return iter(self.segments)
	def __hash__(self): return id(self)

	def __repr__(self):
		points = list()
		for seg in self.segments:
			if isinstance(seg, JourneyTrip):
				if not points:
					points.append(
						'{0.trip.id}:{0.stopidx}:{0.stop.id}:{0.stop.name} [{dts_dep}]'\
						.format(seg.ts_from, dts_dep=u.dts_format(seg.ts_from.dts_dep)) )
				points.append(
					'{0.trip.id}:{0.stopidx}:{0.stop.id}:{0.stop.name} [{dts_arr}]'\
					.format(seg.ts_to, dts_arr=u.dts_format(seg.ts_to.dts_arr)) )
			elif isinstance(seg, JourneyFp):
				points.append('(fp-to={0.id}:{0.name} dt={1})'.format(
					seg.stop_to, datetime.timedelta(seconds=int(seg.dt)) ))
		return '<Journey[ {} ]>'.format(' - '.join(points))

	def pretty_print(self, indent=0, **print_kws):
		p = lambda tpl,*a,**k: print(' '*indent + tpl.format(*a,**k), **print_kws)
		stop_id_ext = lambda stop:\
			' [{}]'.format(stop.id) if stop.id != stop.name else ''

		p( 'Journey {:x} (arrival: {}, trips: {}):',
			id(self), u.dts_format(self.dts_arr), self.trip_count )
		for seg in self.segments:
			if isinstance(seg, JourneyTrip):
				trip_id = seg.ts_from.trip.id
				if seg.ts_from.trip.line_id_hint:
					trip_id = '{}:{}'.format(seg.ts_from.trip.line_id_hint, trip_id)
				p('  trip [{}]:', trip_id)
				p( '    from (dep at {dts_dep}): {0.stopidx}:{0.stop.name}{stop_id}',
					seg.ts_from,
					stop_id=stop_id_ext(seg.ts_from.stop),
					dts_dep=u.dts_format(seg.ts_from.dts_dep) )
				p( '    to (arr at {dts_arr}): {0.stopidx}:{0.stop.name}{stop_id}',
					seg.ts_to,
					stop_id=stop_id_ext(seg.ts_to.stop),
					dts_arr=u.dts_format(seg.ts_to.dts_arr) )
			elif isinstance(seg, JourneyFp):
				p('  footpath (time: {}):', datetime.timedelta(seconds=int(seg.dt)))
				p('    from: {0.name}{stop_id}', seg.stop_from, stop_id=stop_id_ext(seg.stop_from))
				p('    to: {0.name}{stop_id}', seg.stop_to, stop_id=stop_id_ext(seg.stop_to))


@u.attr_struct
class JourneySet:
	journeys = u.attr_init(set)

	def add(self, journey, dts_dep_criteria=True):
		'''Add Journey, maintaining pareto-optimality of the set.'''
		# XXX: remove this - either use ParetoSet here or for raw QueryResults
		for jn2 in list(self.journeys):
			ss = journey.compare(jn2)
			if ss is SolutionStatus.dominated or ss is SolutionStatus.equal: break
			if ( ss is SolutionStatus.non_dominated
				and (not dts_dep_criteria or journey.dts_arr == jn2.dts_arr)
				and SolutionStatus.dominated ): self.journeys.remove(jn2)
		else: self.journeys.add(journey)

	def __len__(self): return len(self.journeys)
	def __iter__(self): return iter(self.journeys)

	def pretty_print(self, indent=0, **print_kws):
		print(' '*indent + 'Journey set ({}):'.format(len(self.journeys)))
		for journey in self.journeys:
			print()
			journey.pretty_print(indent=indent+2, **print_kws)
