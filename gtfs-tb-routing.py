#!/usr/bin/env python3

import itertools as it, operator as op, functools as ft
from pathlib import Path
import os, sys, time, re

import tb_routing as tb


def main(args=None):
	conf = tb.gtfs.GTFSConf()
	conf_engine = tb.engine.EngineConf(
		log_progress_for={'lines', 'pre-initial-set', 'pre-reduction', 'transfer-patterns'} )

	import argparse
	parser = argparse.ArgumentParser(
		description='Simple implementation of trip-based graph-db and algorithms.')
	parser.add_argument('gtfs_dir_or_pickle',
		help='Path to gtfs data directory to build'
			' graph from or a pickled timetable object (if points to a file).')

	group = parser.add_argument_group('Basic timetable/parser options')
	group.add_argument('--cache-timetable', metavar='path',
		help='Store parsed timetable data (in pickle format) to specified file.'
			' This file can then be used in place of gtfs dir, and should load much faster.'
			' All of the "Timetable calendar options" only affect how'
				' the timetable data is parsed and that file generated,'
				' and can be dropped after that.')
	group.add_argument('-c', '--cache-precalc', metavar='path',
		help='Precalculation cache file to load (if exists)'
				' or save (if missing) resulting graph data from/to.'
			' This option must only be used with cached'
				' timetable data (see --cache-timetable option).')
	group.add_argument('-s', '--stops-to-stations', action='store_true',
		help='Convert/translate GTFS "stop" ids to "parent_station" ids,'
				' i.e. group all stops on the station into a single one.'
			' Can produce smaller graphs that would be easier to query.')

	group = parser.add_argument_group('Timetable calendar options')
	group.add_argument('-d', '--day', metavar='{ YYYYMMDD | date }',
		help='Specific date when trip is taking place.'
			'Will also make script parse GTFS calendar data and only build'
				' timetable for trips/footpaths/links active on specified day and its vicinity.'
			' Without this option, all trips/etc will be used regardless of calendar info.'
			' Format is either YYYYMMDD or something that "date -d ..." call would parse (e.g.: today).'
			' See also --parse-days-after and --parse-days-before options.')
	group.add_argument('--parse-days-after',
		type=int, default=conf.parse_days, metavar='n',
		help='In addition to date specified with --parse-day,'
				' process trips for specified number of days after it.'
			' This is important to build journeys which e.g. start on 23:00 and end on'
				' the next day - will be impossible to build these without info from there.'
			' Default: %(default)s')
	group.add_argument('--parse-days-before',
		type=int, default=conf.parse_days_pre, metavar='n',
		help='Similar to --parse-days-after, but for loading data from N previous days.'
			' For journeys starting at e.g. 00:10, many trips starting'
				' on a previous day (e.g. just 10min ago) can be useful.'
			' Default: %(default)s')

	group = parser.add_argument_group('Misc/debug options')
	group.add_argument('--dot-for-lines', metavar='path',
		help='Dump Stop/Line graph (in graphviz dot format) to a specified file and exit.')
	group.add_argument('--dot-opts', metavar='yaml-data',
		help='Options for graphviz graph/nodes/edges to use with all'
			' --dot-for-* commands, as a YAML mappings. Example: {graph: {rankdir: LR}}')
	group.add_argument('--engine-conf', metavar='yaml-data',
		help='Override values for EngineConf as a YAML mapping.'
			' Example: {log_progress_steps: 1000}')
	group.add_argument('--debug', action='store_true', help='Verbose operation mode.')

	cmds = parser.add_subparsers(title='Commands', dest='call')


	cmd = cmds.add_parser('cache',
		help='Generate/store all the caches and exit.')


	cmd = cmds.add_parser('query-earliest-arrival',
		help='Run earliest arrival query, output resulting journey set.')
	cmd.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	cmd.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	cmd.add_argument('day_time', nargs='?', default='00:00',
		help='Day time to start journey at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')


	cmd = cmds.add_parser('query-profile',
		help='Run profile query, output resulting journey set.')

	group = cmd.add_argument_group('Query parameters')
	group.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	group.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	group.add_argument('day_time_earliest', nargs='?', default='00:00',
		help='Earliest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')
	group.add_argument('day_time_latest', nargs='?', default='24:00',
		help='Latest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')

	group = cmd.add_argument_group('Limits')
	group.add_argument('-m', '--max-transfers',
		type=int, metavar='n', default=15,
		help='Max number of transfers (i.e. interchanges)'
			' between journey trips allowed in the results. Default: %(default)s')


	cmd = cmds.add_parser('query-transfer-patterns',
		help='Build/load Transfer-Patterns trie and run queries on it.')

	group = cmd.add_argument_group('Query parameters')
	group.add_argument('stop_from', help='Stop ID to query journey from. Example: J22209723_0')
	group.add_argument('stop_to', help='Stop ID to query journey to. Example: J2220952426_0')
	group.add_argument('day_time_earliest', nargs='?', default='00:00',
		help='Earliest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')
	group.add_argument('day_time_latest', nargs='?', default='24:00',
		help='Latest day time to start journey(s) at, either as HH:MM,'
			' HH:MM:SS or just seconds int/float. Default: %(default)s')

	group = cmd.add_argument_group('Limits')
	group.add_argument('-m', '--max-transfers',
		type=int, metavar='n', default=15,
		help='Max number of transfers (i.e. interchanges)'
			' between journey trips allowed in the results. Default: %(default)s')

	group = cmd.add_argument_group('Graph options')
	group.add_argument('--tree-cache', metavar='path',
		help='Pickle cache-file to load (if exists)'
			' or save (if missing) resulting Transfer-Patterns'
			' prefix-tree from/to (see arXiv:1607.01299v2 paper).')

	group = cmd.add_argument_group('Misc/debug options')
	group.add_argument('--dot-for-tp-subtree', metavar='path',
		help='Dump TB-TP subtree graph for specified'
			' stop_from (in graphviz dot format) to a file and exit.')
	group.add_argument('--dot-for-tp-query-tree', metavar='path',
		help='Dump TB-TP query tree graph for specified'
			' stop_from/stop_to pair (in graphviz dot format) to a file and exit.')


	opts = parser.parse_args(sys.argv[1:] if args is None else args)

	tb.u.logging.basicConfig(
		format='%(asctime)s :: %(name)s %(levelname)s :: %(message)s',
		datefmt='%Y-%m-%d %H:%M:%S',
		level=tb.u.logging.DEBUG if opts.debug else tb.u.logging.WARNING )

	tt_path = Path(opts.gtfs_dir_or_pickle)
	cache_path = opts.cache_precalc and Path(opts.cache_precalc)
	if opts.call != 'cache' and (not tt_path.is_file() and cache_path and cache_path.is_file()):
		parser.error( 'Pre-generated --cache-precalc dump can only'
			' be used with cached timetable (see --cache-timetable option).' )

	day = opts.day
	if day:
		m = re.search(r'^\s*(\d{4})\s*-\s*(\d{2})\s*-\s*(\d{2})\s*$', day)
		if m: day = ''.join(m.groups())
		if not (day.isdigit() and len(day) == 8):
			import subprocess
			proc = subprocess.Popen(['date', '-d', day, '+%Y%m%d'], stdout=subprocess.PIPE)
			day = proc.stdout.read().decode().strip()
			if proc.wait() != 0: parser.error('"date -d" failed to parse --day value: {!r}'.format(day))
			log.debug('Parsed --day value via "date -d" subprocess: {!r} -> {}', opts.day, day)

	if opts.stops_to_stations: conf.group_stops_into_stations = True
	if opts.engine_conf:
		import yaml
		for k, v in yaml.safe_load(opts.engine_conf).items():
			if not hasattr(conf_engine, k):
				parser.error('Unrecognized engine conf option: {!r} (value: {!r})'.format(k, v))
			setattr(conf_engine, k, v)

	conf.parse_start_date, conf.parse_days, conf.parse_days_pre =\
		day, opts.parse_days_after, opts.parse_days_before

	timetable, router = tb.init_gtfs_router(
		tt_path, cache_path, tt_path_dump=opts.cache_timetable,
		conf=conf, conf_engine=conf_engine, timer_func=tb.calc_timer )

	dot_opts = dict()
	if opts.dot_opts:
		import yaml
		dot_opts = yaml.safe_load(opts.dot_opts)
	if opts.dot_for_lines:
		with tb.u.safe_replacement(opts.dot_for_lines) as dst:
			tb.vis.dot_for_lines(router.graph.lines, dst, dot_opts=dot_opts)
		return

	if opts.call == 'cache': pass

	elif opts.call == 'query-earliest-arrival':
		dts_start = timetable.dts_parse(opts.day_time)
		a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]
		journeys = router.query_earliest_arrival(a, b, dts_start)
		journeys.pretty_print(timetable.dts_format)

	elif opts.call == 'query-profile':
		dts_edt, dts_ldt = map(timetable.dts_parse, [opts.day_time_earliest, opts.day_time_latest])
		a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]
		journeys = router.query_profile(a, b, dts_edt, dts_ldt, max_transfers=opts.max_transfers)
		journeys.pretty_print(timetable.dts_format)

	elif opts.call == 'query-transfer-patterns':
		dts_edt, dts_ldt = map(timetable.dts_parse, [opts.day_time_earliest, opts.day_time_latest])
		a, b = timetable.stops[opts.stop_from], timetable.stops[opts.stop_to]

		cache_path = opts.tree_cache
		tp_tree = tb.u.pickle_load(cache_path) if cache_path else None
		tp_router = router.build_tp_engine(tp_tree, max_transfers=opts.max_transfers)
		if not tp_tree and cache_path: tb.u.pickle_dump(tp_router.tree, cache_path)

		if opts.dot_for_tp_subtree:
			with tb.u.safe_replacement(opts.dot_for_tp_subtree) as dst:
				tb.vis.dot_for_tp_subtree(tp_router.tree[a], dst, dst_to_src=True, dot_opts=dot_opts)
			return

		query_tree = tp_router.build_query_tree(a, b)
		if opts.dot_for_tp_query_tree:
			with tb.u.safe_replacement(opts.dot_for_tp_query_tree) as dst:
				tb.vis.dot_for_tp_subtree(query_tree, dst, dot_opts=dot_opts)
			return

		journeys = tp_router.query_profile(a, b, dts_edt, dts_ldt, query_tree)
		journeys.pretty_print(timetable.dts_format)

	else: parser.error('Action not implemented: {}'.format(opts.call))

if __name__ == '__main__': sys.exit(main())
