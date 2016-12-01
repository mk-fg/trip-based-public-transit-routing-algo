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


@u.attr_struct(cmp=False)
class Footpath:
	keys = 'dt dts_min dts_max'
	def __hash__(self): return hash((self.dt, self.dts_min, self.dts_max))
	def __eq__(self, fp): return hash(self) == hash(fp)

class Footpaths:
	# Never returns Footpath tuples, only best (minimal) time deltas for stops

	_stats_cache_t = namedtuple(
		'StatsCache', 'dt_sum dt_count ch_count fp_count fp_keys' )
	_stats_cache = None

	def __init__(self):
		self.set_idx_to, self.set_idx_from = dict(), dict()

	def __getstate__(self):
		state = self.__dict__
		state.pop('_stats_cache', None)
		return state

	def _add_to_idx(self, fp, idx, k1, k2):
		fp_list = idx.setdefault(k1, dict()).setdefault(k2, list())
		fp_list.append(fp)
		fp_list.sort()

	def add(self, stop_a, stop_b, dt, dts_min=0, dts_max=u.inf):
		fp = Footpath(dt, dts_min, dts_max)
		self._add_to_idx(fp, self.set_idx_to, stop_a, stop_b)
		self._add_to_idx(fp, self.set_idx_from, stop_b, stop_a)
		self._stats_cache = None

	def _discard_longer_from_idx(self, dt_max, idx, k1, k2):
		try: fp_list = idx[k1][k2]
		except KeyError: return
		for n, fp in enumerate(list(fp_list)):
			if fp.dt > dt_max: fp_list.pop(n)
		if not fp_list:
			del idx[k1][k2]
			if not idx[k1]: del idx[k1]

	def discard_longer(self, dt_max):
		items = sorted(
			( (fp.dt,(k1,k2))
				for k1,v1 in self.set_idx_to.items()
				for k2,fp_set in v1.items() for fp in fp_set ),
			key=op.itemgetter(0) )
		keys = set(k12 for v,k12 in items[bisect.bisect_left(items, (dt_max, ())):])
		for k1, k2 in keys:
			self._discard_longer_from_idx(dt_max, self.set_idx_to, k1, k2)
			self._discard_longer_from_idx(dt_max, self.set_idx_from, k2, k1)
		self._stats_cache = None

	def _check_if_valid(self, fp, dts_src=None, dts_dst=None):
		if dts_src is None and dts_dst is None: return True
		if dts_src is None: dts_src = dts_dst - fp.dt
		if dts_dst is None: dts_dst = dts_src + fp.dt
		return not (dts_src < fp.dts_min or dts_dst > fp.dts_max)

	def _filtered_stop_dt_tuples(self, idx_items, dts_src=None, dts_dst=None):
		for stop, fp_list in idx_items:
			for fp in fp_list:
				if not self._check_if_valid(fp, dts_src, dts_dst): continue
				yield stop, fp.dt
				break # footpaths are sorted, so first one should be minimal

	def to_stops_from(self, stop, dts_src=None, dts_dst=None):
		return self._filtered_stop_dt_tuples(
			self.set_idx_to.get(stop, dict()).items(), dts_src, dts_dst )

	def from_stops_to(self, stop, dts_src=None, dts_dst=None):
		return self._filtered_stop_dt_tuples(
			self.set_idx_from.get(stop, dict()).items(), dts_src, dts_dst )

	def time_delta(self, stop_from, stop_to, dts_src=None, dts_dst=None, default=...):
		try: fp_list = self.set_idx_to[stop_from][stop_to]
		except KeyError:
			if default is ...: raise
			return default
		for fp in fp_list:
			if not self._check_if_valid(fp, dts_src, dts_dst): continue
			return fp.dt
		else:
			if default is not ...: return default
			raise KeyError(stop_from, stop_to, dts_src, dts_dst)

	def between(self, stop_a, stop_b, dts_src=None, dts_dst=None, default=...):
		'Return footpath dt in any direction between two stops.'
		try: return self.time_delta(stop_a, stop_b, dts_src, dts_dst)
		except KeyError: return self.time_delta(stop_b, stop_a, dts_src, dts_dst, default=default)

	def _stats(self):
		if not self._stats_cache:
			dt_sum = dt_count = ch_count = fp_count = fp_keys = 0
			for k1, fps_from_k1 in self.set_idx_to.items():
				for k2, fp_list in fps_from_k1.items():
					dt_sum, dt_count = dt_sum + fp_list[0].dt, dt_count + 1
					fp_count, fp_keys = fp_count + len(fp_list), fp_keys + 1
					if k1 == k2: ch_count += 1
			self._stats_cache = self._stats_cache_t(
				dt_sum, dt_count, ch_count, fp_count, fp_keys )
		return self._stats_cache

	def stat_mean_dt(self):
		s = self._stats()
		return (s.dt_sum / s.dt_count) if s.dt_count else 0
	def stat_mean_count_for_stops(self):
		s = self._stats()
		return (s.fp_count / s.fp_keys) if s.fp_keys else 0
	def stat_same_stop_count(self): return self._stats().ch_count

	def __len__(self): return self._stats().dt_count # number connected a->b pairs


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
				line_id_hint='{}:'.format(self.trip.line_id_hint)
					if self.trip and self.trip.line_id_hint else '' )

@u.attr_struct(repr=False, cmp=False)
class Trip:
	stops = u.attr_init(list)
	id = u.attr_init_id()
	line_id_hint = u.attr_init(None) # can be set for introspection/debugging

	def __hash__(self): return hash(self.id)
	def __eq__(self, trip): return u.same_type_and_id(self, trip)
	def __repr__(self): # mostly to avoid recursion
		return 'Trip(id={line_id_hint}{0.id}, stops={stops})'.format(
			self, stops=len(self.stops),
			line_id_hint='{}:'.format(self.line_id_hint) if self.line_id_hint else '' )

	def add(self, stop):
		assert stop.dts_arr <= stop.dts_dep
		assert not self.stops or self.stops[-1].dts_dep <= stop.dts_arr
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


@u.attr_struct(defaults=None)
class TimespanInfo:
	keys = 'dt_start dt_min service_days date_map date_min_str date_max_str'

@u.attr_struct
class Timetable:
	stops = u.attr_init()
	footpaths = u.attr_init()
	trips = u.attr_init()
	timespan = u.attr_init(TimespanInfo)

	def dts_relative(self, dts, dt=None):
		if not self.timespan.dt_min: return dts
		if not dt: dt = self.timespan.dt_start
		return dts + (dt - self.timespan.dt_min).total_seconds()

	def dts_parse(self, day_time_str, dt=None):
		return self.dts_relative(u.dts_parse(day_time_str), dt)



### TBRoutingEngine query result

JourneyTrip = namedtuple('JTrip', 'ts_from ts_to')
JourneyFp = namedtuple('JFootpath', 'stop_from stop_to dt')

@u.attr_struct(slots=False, repr=False, cmp=False)
class Journey:
	dts_start = u.attr_init()
	segments = u.attr_init(list)

	_stats_cache_t = namedtuple(
		'StatsCache', 'id dts_arr dts_dep trip_count fp_count' )
	_stats_cache = None

	def _stats(self):
		if not self._stats_cache:
			dts_arr = trip_count = fp_count = 0
			dts_dep, dts_dep_fp, hash_vals = None, 0, list()
			for seg in self.segments:
				if isinstance(seg, JourneyTrip):
					trip_count += 1
					dts_arr = seg.ts_to.dts_arr
					hash_vals.append(seg.ts_from.trip)
					if dts_dep is None: dts_dep = seg.ts_from.dts_dep - dts_dep_fp
				elif isinstance(seg, JourneyFp):
					fp_count += 1
					dts_arr = dts_arr + seg.dt
					hash_vals.append(seg)
					if dts_dep is None: dts_dep_fp += seg.dt
			if dts_dep is None: # no trips, only footpaths
				dts_dep, dts_arr = self.dts_start, self.dts_start + dts_arr
			self._stats_cache = self._stats_cache_t(
				hash(tuple(hash_vals)), dts_arr, dts_dep, trip_count, fp_count )
		return self._stats_cache

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
	def __hash__(self): return self.id
	def __eq__(self, journey): return u.same_type_and_id(self, journey)

	def __getattr__(self, k):
		if k in self._stats_cache_t._fields: return getattr(self._stats(), k)
		return super(Journey, self).__getattr__(k)

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
			self.id, u.dts_format(self.dts_arr), self.trip_count )
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

	def add(self, journey): self.journeys.add(journey)

	def __len__(self): return len(self.journeys)
	def __iter__(self): return iter(self.journeys)

	def pretty_print(self, indent=0, **print_kws):
		print(' '*indent + 'Journey set ({}):'.format(len(self.journeys)))
		for journey in sorted( self.journeys,
				key=op.attrgetter('dts_dep', 'dts_arr', 'dts_start', 'id') ):
			print()
			journey.pretty_print(indent=indent+2, **print_kws)
