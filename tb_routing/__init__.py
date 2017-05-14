import itertools as it, operator as op, functools as ft
from pathlib import Path
import time

from . import engine, vis, gtfs, utils as u, types as t


def calc_timer(func, *args, log=u.get_logger('tb.timer'), timer_name=None, **kws):
	if not timer_name:
		func_base = func if not isinstance(func, ft.partial) else func.func
		timer_name = '.'.join([func_base.__module__.strip('__'), func_base.__qualname__])
	log.debug('[{}] Starting...', timer_name)
	td = time.monotonic()
	data = func(*args, **kws)
	td = time.monotonic() - td
	log.debug('[{}] Finished in: {:.1f}s', timer_name, td)
	return data


def init_gtfs_router(
		tt_path, cache_path=None, tt_path_dump=None,
		conf=None, conf_engine=None, timer_func=None, log=u.get_logger('tb.init') ):
	if not conf: conf = gtfs.GTFSConf()

	timetable_func, router_func = gtfs.parse_timetable,\
		ft.partial(engine.TBRoutingEngine, conf=conf_engine, timer_func=timer_func)
	if timer_func:
		timetable_func, router_func = (
			ft.partial(timer_func, func) for func in [timetable_func, router_func] )

	tt_path = Path(tt_path)
	if tt_path.is_file():
		tt_load = u.pickle_load
		if timer_func: tt_load = ft.partial(timer_func, tt_load, timer_name='timetable_load')
		timetable = tt_load(tt_path, fail=True)
	else:
		timetable = timetable_func(tt_path, conf)
		if tt_path_dump: u.pickle_dump(timetable, tt_path_dump)
	log.debug(
		'Parsed timetable: stops={:,}, footpaths={:,}'
			' (mean-delta={:,.1f}s, mean-options={:,.1f}, same-stop={:,}),'
			' trips={:,} (mean-stops={:,.1f})',
		len(timetable.stops), len(timetable.footpaths),
		timetable.footpaths.stat_mean_delta(),
		timetable.footpaths.stat_mean_delta_count(),
		timetable.footpaths.stat_same_stop_count(),
		len(timetable.trips), timetable.trips.stat_mean_stops() )

	if cache_path: cache_path = Path(cache_path)
	if cache_path and cache_path.exists():
		with open(str(cache_path), 'rb') as src:
			router = router_func(timetable, cached_graph=src)
	else:
		router = router_func(timetable)
		if cache_path:
			graph_dump = router.graph.dump
			if timer_func: graph_dump = ft.partial(timer_func, graph_dump)
			with u.safe_replacement(cache_path, 'wb') as dst: graph_dump(dst)

	return timetable, router
