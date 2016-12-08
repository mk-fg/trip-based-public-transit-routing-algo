import itertools as it, operator as op, functools as ft
from collections import namedtuple, defaultdict
import enum, datetime, contextlib

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


class Footpath:

	def __init__(self):
		self.delta_tuples = list() # [(delta, dts_min, dts_max), ...]

	def add(self, delta, dts_min, dts_max):
		self.delta_tuples.append((delta, dts_min, dts_max))
	def discard_longer(self, delta_max):
		self.delta_tuples = list(filter(lambda t: t[0] <= delta_max, self.delta_tuples))
	def finalize(self):
		if len(self.delta_tuples) > 1: self.delta_tuples.sort()
		self.delta_tuples = tuple(self.delta_tuples)

	def _check_src_dst(self, delta, dts_min, dts_max, dts_src, dts_dst):
		'''Return True if dts min/max are within src/dst constraints.
			I.e. whether footpath can take place with
				specified arrival-to-src and departure-from-dst times.
			None in place of src/dst times is interpreted as "any".
			Full length (delta) of footpath must fit into dts_min/max interval for it to be valid.'''
		src, dst = dts_src is not None, dts_dst is not None
		if not (src or dst): return True
		if not src: dts_src = dts_min
		elif dts_min: dts_src = max(dts_min, dts_src)
		if not dst: dts_dst = dts_max
		elif dts_max: dts_dst = min(dts_max, dts_dst)
		return dts_dst - dts_src >= delta

	def filtered_deltas(self, dts_src=None, dts_dst=None):
		'Return asc-sorted time deltas for footpaths within given constraints.'
		for delta, dts_min, dts_max in self.delta_tuples:
			if not self._check_src_dst(delta, dts_min, dts_max, dts_src, dts_dst): continue
			yield delta

	def get_shortest(self, **fp_constraints):
		'''Return shortest time delta for valid footpath
			between stops within given constraints, or None if it cannot be found.'''
		try: return next(self.filtered_deltas(**fp_constraints))
		except StopIteration: return None

	def valid_at(self, **fp_constraints):
		'Return (as bool) whether footpath is valid between stops within given constraints.'
		return self.get_shortest(**fp_constraints) is not None

	def stat_delta_sum(self):
		return 0 if not self.delta_tuples else\
			sum(map(op.itemgetter(0), self.delta_tuples))

	def __len__(self): return len(self.delta_tuples)


class Footpaths:

	_stats_cache_t = namedtuple(
		'StatsCache', 'delta_sum delta_count ch_count conn_count' )
	_stats_cache = None

	def __init__(self):
		self.set_idx_to, self.set_idx_from = dict(), dict()
		self.fp0 = Footpath()

	def __getstate__(self):
		state = self.__dict__
		state.pop('_stats_cache', None)
		return state

	def _add(self, stop_a, stop_b, delta, dts_min=0, dts_max=u.inf):
		try: fp = self.set_idx_to[stop_a][stop_b]
		except KeyError:
			fp = Footpath()
			self.set_idx_to.setdefault(stop_a, dict())[stop_b] = fp
			self.set_idx_from.setdefault(stop_b, dict())[stop_a] = fp
		fp.add(delta, dts_min, dts_max)

	@contextlib.contextmanager
	def populate(self):
		try: yield self._add
		finally:
			for k1, k2, fp in self: fp.finalize()
			self._stats_cache = None

	def get(self, stop_from, stop_to):
		try: return self.set_idx_to[stop_from][stop_to]
		except KeyError: return self.fp0

	def _filtered_stop_fp_tuples(self, idx_items, fp_constraints):
		for stop, fp in idx_items:
			if not fp.valid_at(**fp_constraints): continue
			yield stop, fp

	def to_stops_from(self, stop, **fp_constraints):
		'''Return (stop, fp) tuples only for
			stops that have valid footpaths within given constraints.'''
		return self._filtered_stop_fp_tuples(
			self.set_idx_to.get(stop, dict()).items(), fp_constraints )

	def from_stops_to(self, stop, **fp_constraints):
		'''Return (stop, fp) tuples only for
			stops that have valid footpaths within given constraints.'''
		return self._filtered_stop_fp_tuples(
			self.set_idx_from.get(stop, dict()).items(), fp_constraints )

	def time_delta(self, stop_from, stop_to, default=None, **fp_constraints):
		delta = self.get(stop_from, stop_to).get_shortest(**fp_constraints)
		if delta is None: delta = default
		return delta

	def connected(self, stop_a, stop_b, **fp_constraints):
		'''Return (as bool) whether footpath in any
			direction exists between two stops within given constraints.'''
		for a, b in [(stop_a, stop_b), (stop_b, stop_a)]:
			delta = self.time_delta(a, b, **fp_constraints)
			if delta is not None: break
		else: return False
		return delta is not u.inf

	def _stats(self):
		if not self._stats_cache:
			delta_sum = delta_count = ch_count = conn_count = 0
			for k1, fps_from_k1 in self.set_idx_to.items():
				for k2, fp in fps_from_k1.items():
					if not fp: continue
					delta_sum += fp.stat_delta_sum()
					delta_count += len(fp)
					conn_count += 1
					if k1 == k2: ch_count += 1
			self._stats_cache = self._stats_cache_t(
				delta_sum, delta_count, ch_count, conn_count )
		return self._stats_cache

	def stat_mean_delta(self):
		s = self._stats()
		return (s.delta_sum / s.delta_count) if s.delta_count else 0
	def stat_mean_delta_count(self):
		s = self._stats()
		return (s.delta_count / s.conn_count) if s.conn_count else 0
	def stat_same_stop_count(self): return self._stats().ch_count

	def __iter__(self):
		for k1, k1_fps in list(self.set_idx_to.items()):
			for k2, fp in list(k1_fps.items()): yield k1, k2, fp
	def __len__(self): return self._stats().conn_count


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

	def __hash__(self): return hash(self.id)
	def __eq__(self, trip): return u.same_type_and_id(self, trip)
	def __repr__(self): # mostly to avoid recursion
		return 'Trip(id={line_id_hint}{0.id}, stops={stops})'.format(
			self, stops=len(self.stops),
			line_id_hint='{}:'.format(self.line_id_hint) if self.line_id_hint else '' )

	def __getitem__(self, n): return self.stops[n]
	def __len__(self): return len(self.stops)
	def __iter__(self): return iter(self.stops)

class Trips:

	def __init__(self): self.set_idx = dict()

	def add(self, trip):
		assert len(trip) >= 2, trip
		self.set_idx[trip.id] = trip

	def stat_mean_stops(self):
		if not len(self): return 0
		return (sum(len(t) for t in self) / len(self))

	def __getitem__(self, trip_id): return self.set_idx[trip_id]
	def __len__(self): return len(self.set_idx)
	def __iter__(self): return iter(self.set_idx.values())


@u.attr_struct(slots=False, defaults=None)
class TimespanInfo:
	keys = 'dt_start dt_min service_days date_map date_min_str date_max_str'

	_dts_start_cache = None
	@property
	def dts_start(self):
		if self._dts_start_cache is not None: return self._dts_start_cache
		self._dts_start_cache = 0 if not self.dt_min\
			else (self.dt_start - self.dt_min).total_seconds()
		return self._dts_start_cache


@u.attr_struct
class Timetable:
	stops = u.attr_init()
	footpaths = u.attr_init()
	trips = u.attr_init()
	timespan = u.attr_init(TimespanInfo)

	def dts_relative(self, dts, dt=None):
		if not self.timespan.dt_min: return dts
		if not dt: return self.timespan.dts_start + dts
		return dts + (dt - self.timespan.dt_min).total_seconds()

	def dts_parse(self, day_time_str, dt=None):
		return self.dts_relative(u.dts_parse(day_time_str), dt)

	def dts_format(self, dts):
		return u.dts_format(dts - self.timespan.dts_start)



### TBRoutingEngine query result

JourneyTrip = namedtuple('JTrip', 'ts_from ts_to')
JourneyFp = namedtuple('JFootpath', 'stop_from stop_to delta')

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
					dts_arr = dts_arr + seg.delta
					hash_vals.append(seg)
					if dts_dep is None: dts_dep_fp += seg.delta
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

	def pretty_print(self, dts_format_func=None, indent=0, **print_kws):
		if not dts_format_func: dts_format_func = u.dts_format
		p = lambda tpl,*a,**k: print(' '*indent + tpl.format(*a,**k), **print_kws)
		stop_id_ext = lambda stop:\
			' [{}]'.format(stop.id) if stop.id != stop.name else ''

		p( 'Journey {:x} (arrival: {}, trips: {}, duration: {}):',
			self.id, dts_format_func(self.dts_arr), self.trip_count,
			u.dts_format(self.dts_arr - self.dts_dep) )
		for seg in self.segments:
			if isinstance(seg, JourneyTrip):
				trip_id = seg.ts_from.trip.id
				if seg.ts_from.trip.line_id_hint:
					trip_id = '{}:{}'.format(seg.ts_from.trip.line_id_hint, trip_id)
				p('  trip [{}]:', trip_id)
				p( '    from (dep at {dts_dep}): {0.stopidx}:{0.stop.name}{stop_id}',
					seg.ts_from,
					stop_id=stop_id_ext(seg.ts_from.stop),
					dts_dep=dts_format_func(seg.ts_from.dts_dep) )
				p( '    to (arr at {dts_arr}): {0.stopidx}:{0.stop.name}{stop_id}',
					seg.ts_to,
					stop_id=stop_id_ext(seg.ts_to.stop),
					dts_arr=dts_format_func(seg.ts_to.dts_arr) )
			elif isinstance(seg, JourneyFp):
				p('  footpath (time: {}):', datetime.timedelta(seconds=int(seg.delta)))
				p('    from: {0.name}{stop_id}', seg.stop_from, stop_id=stop_id_ext(seg.stop_from))
				p('    to: {0.name}{stop_id}', seg.stop_to, stop_id=stop_id_ext(seg.stop_to))


@u.attr_struct
class JourneySet:
	journeys = u.attr_init(set)

	def add(self, journey): self.journeys.add(journey)

	def __len__(self): return len(self.journeys)
	def __iter__(self): return iter(self.journeys)

	def pretty_print(self, dts_format_func=None, indent=0, **print_kws):
		print(' '*indent + 'Journey set ({}):'.format(len(self.journeys)))
		for journey in sorted( self.journeys,
				key=op.attrgetter('dts_dep', 'dts_arr', 'dts_start', 'id') ):
			print()
			journey.pretty_print(dts_format_func=dts_format_func, indent=indent+2, **print_kws)
