========================================
 trip-based-public-transit-routing-algo
========================================
----------------------------------------------------------------------
 Python implementation of trip-based public transit routing algorithm
----------------------------------------------------------------------

Implementation of the fast graph-based transit-routing algorithm from the
following papers:

- Trip-Based Public Transit Routing (`arXiv:1504.07149v2`_, 2015)
- Trip-Based Public Transit Routing Using Condensed Search Trees
  (`arXiv:1607.01299v2`_, 2016)

...with source data parsed from `GTFS feeds
<https://developers.google.com/transit/gtfs/>`_.

Not focused on performance, only readability and correctness aspects,
i.e. just a proof of concept, not suitable for any kind of production use.

Under heavy development, see `doc/TODO <doc/TODO>`_ file for a general list
of things that are not (yet?) implemented.

|

.. contents::
  :backlinks: none



Usage
-----

There's command-line ``gtfs-tb-routing.py`` script that builds timetable from
GTFS source data, initializes routing engine with it and runs queries on that,
but routing engine itself can be used separately.

Regardless of interface, highly recommend using PyPy3 (3.3+) to run the thing,
as it gives orders-of-magnitude performance boost here over CPython, and
transfer-set pre-calculation with large datasets can take a while.

Also, as mentioned, no attempt at multi-thread or memory optimizations is made
here, so it will take much longer than necessary and eat all the RAM regardless.


Command-line script
```````````````````

Usage: ``./gtfs-tb-routing.py gtfs-data-dir stop-id-src stop-id-dst [start-time]``

That runs earliest-arrival/min-transfers bi-criteria query on (unpacked) GTFS
data from specified dir and pretty-prints resulting pareto-optimal JourneySet to
stdout.

Some sample GTFS data zips can be found in ``test/`` directory.

Links to many open real-world GTFS feeds are available at `transit.land
<https://transit.land/>`_ repository.

Example usage::

  % unzip test/gtfs_shizuoka.data.2016-10-13.zip -d gtfs-shizuoka
  Archive:  test/gtfs_shizuoka.data.2016-10-13.zip
    inflating: gtfs-shizuoka/agency.txt
    inflating: gtfs-shizuoka/routes.txt
    inflating: gtfs-shizuoka/trips.txt
    inflating: gtfs-shizuoka/stops.txt
    inflating: gtfs-shizuoka/calendar_dates.txt
    inflating: gtfs-shizuoka/stop_times.txt
    inflating: gtfs-shizuoka/shapes.txt

  % time ./gtfs-tb-routing.py gtfs-shizuoka -c gtfs-shizuoka.cache.pickle \
      query-earliest-arrival J22209723_0 J2220952426_0

  Journey set (1):
    Journey 5596f26afe50 (arrival: 08:43:00, trips: 2):
      trip [95]:
        from (dep at 06:10:00): 10:小川 [J22209723_0]
        to (arr at 06:55:00): 49:島田駅 北口２番のりば [J222093340_2]
      trip [97]:
        from (dep at 08:35:00): 20:島田駅 北口２番のりば [J222093340_2]
        to (arr at 08:43:00): 28:ばらの丘一丁目 [J2220952426_0]
  ./gtfs-tb-routing.py ... 8.39s user 0.06s system 99% cpu 8.454 total

  % time ./gtfs-tb-routing.py gtfs-shizuoka -c gtfs-shizuoka.cache.pickle \
      query-earliest-arrival J22209843_0 J222093345_0

  Journey set (1):
    Journey 5555e3e3c020 (arrival: 07:41:00, trips: 2):
      trip [129]:
        from (dep at 07:02:00): 1:田代環境プラザ [J22209843_0]
        to (arr at 07:26:00): 20:島田駅 北口１番のりば [J222093340_1]
      footpath (time: 0:02:16):
        from: 島田駅 北口１番のりば [J222093340_1]
        to: 島田駅 北口２番のりば [J222093340_2]
      trip [7]:
        from (dep at 07:33:00): 38:島田駅 北口２番のりば [J222093340_2]
        to (arr at 07:41:00): 45:島田市民病院 [J222093345_0]
  ./gtfs-tb-routing.py ... 0.85s user 0.04s system 99% cpu 0.894 total


  % ./gtfs-tb-routing.py -c gtfs-shizuoka.cache.pickle -d gtfs-shizuoka \
      query-profile J22209723_0 J2220952426_0

  Journey set (8):

    Journey 555699453210 (arrival: 19:44:00, trips: 2):
      trip [28]:
        from (dep at 18:50:00): 10:小川 [J22209723_0]
        to (arr at 19:33:00): 48:本通三丁目 [J222093339_0]
      footpath (time: 0:02:10):
        from: 本通三丁目 [J222093339_0]
        to: 本通三丁目 [J222093339_1]
      trip [115]:
        from (dep at 19:37:00): 21:本通三丁目 [J222093339_1]
        to (arr at 19:44:00): 28:ばらの丘一丁目 [J2220952426_0]

    Journey 555696d859b8 (arrival: 18:24:00, trips: 2):
      trip [8]:
        from (dep at 16:30:00): 10:小川 [J22209723_0]
        to (arr at 17:15:00): 49:島田駅 北口２番のりば [J222093340_2]
      trip [14]:
        from (dep at 18:16:00): 20:島田駅 北口２番のりば [J222093340_2]
        to (arr at 18:24:00): 28:ばらの丘一丁目 [J2220952426_0]
  ...


Note that second query is much faster due to ``--cache gtfs-shizuoka.cache.pickle``
option, which allows to reuse pre-calculated data from the first query.

Use ``-d/--debug`` option to see pre-calculation progress (useful for large
datasets) and misc other stats and logging.


Routing engine
``````````````

``tb_routing.engine`` module implements actual routing, and can be used with any
kind of timetable data source, passed as a ``tb_routing.types.public.Timetable``
to it on init.

Subsequent queries to engine instance return ``tb_routing.types.public.JourneySet``.

See `test/simple.py <test/simple.py>`_ for example of how such Timetable can be
constructed and queried with trivial test-data.


Requirements
````````````

- Python 3.x
- `attrs <https://attrs.readthedocs.io/en/stable/>`_
- (for tests only) `PyYAML <http://pyyaml.org/>`_
- (for Python<3.4 only) `pathlib2 <https://pypi.python.org/pypi/pathlib2/>`_
- (for Python<3.4 only) `enum34 <https://pypi.python.org/pypi/enum34/>`_

To install all these on any random system (to ``~/.local/`` with --user)::

  % python3 --version
  Python 3.3.5 (ea9979b550eeae87924dc4bef06070e8f8d0e22f, Oct 12 2016, 11:31:15)
  [PyPy 5.5.0-alpha0 with GCC 6.2.1 20160830]

  % python3 -m ensurepip --user
  % python3 -m pip install --user attrs pyyaml

   ## For python<3.4 only, but safe to run on later ones as well
  % python3 -m pip install --user pathlib2 enum34

   ## Done, run the app/tests
  % ./gtfs-tb-routing.py --help
  ...
  % python3 -m unittest test.all

Alternatively, run ``python3 -m virtualenv tb-routing-venv &&
. tb-routing-venv/bin/activate`` before above commands to have these modules
installed into "tb-routing-venv" dir, if `virtualenv <https://virtualenv.pypa.io/>`_
module is installed/available (can be installed via pip same as others above).



Notes
-----

Some less obvious things are described in this section.


Journey Optimality Criterias
````````````````````````````

Trip-Based algorithm, as described in the `arXiv:1504.07149v2`_ paper optimizes
earliest-arrival queries for two criterias:

- Earliest arrival time.
- Minimal number of transfers.

Profile queries there have additional criteria - latest departure time.

Result of this algorithm is a pareto-optimal set of trip-sequences (i.e. graph
nodes) that lead to optimal set of these parameters.

To construct journey info from such nodes (trips) in a deterministic and
somewhat sensible fashion, additional "minmal footpath time" criteria is used to
pick optimal edges (footpaths/interchanges), with earliest optimal footpath
preferred over later ones in case of ties.


Caching
```````

For large datasets, using pickle cache (``-c/--cache`` cli option) to
(de-)serialize graphs can be slower than re-calculating whole thing from
scratch, so might not be worth using.


Tests
`````

Commands to run tests from checkout directory::

  % python3 -m unittest test.all
  % python3 -m unittest test.gtfs_shizuoka
  % python3 -m unittest -vf test.simple

``test.all.case`` also provides global index of all test cases by name::

  % python3 -m unittest test.all.case.test_journeys_J22209723_J2220952426
  % python3 -m unittest test.all.case.testMultipleRoutes


Performance Optimization
````````````````````````

Pre-calculation in Trip-Based routing algorithm, as noted in paper, is very
suitable for further optimization from how it's presented there - i.e. three
separate "steps" can be merged into one loop, running processing of transfers
for each trip in parallel with minimal synchronization.

Python does not provide an easy way to optimize such processing, especially due
to slow serialization of high-level objects and lack of support for cpu-bound
threads working in shared memory.

Workarounds are possible, but it's probably not worth considering python code
for any kind of production use.


Mock / testing timetable from json-dgc graph
````````````````````````````````````````````

`json-dgc <https://github.com/eimink/json-dgc/>`_ is a simple d3-based tool to
interactively draw and save/load directed graphs to/from JSON.

It can be used to draw some testing transport network, using nodes as stops,
positioning them as they'd be on a flat map (to auto-generate footpaths to ones
that are close) and naming/connecting them according to trip-lines.

``timetable-from-json-dgc.py`` script can then be used to convert saved JSON
graph into a pickled timetable, with trips auto-generated to run with regular
intervals (and some fixed speed) along drawn lines, and footpaths connecting
stops that are close enough.

Script requires node names to have following format::

  L<line1>-<seq1>[/L<line2>-<seq2>]...

Where "line" is an arbitrary id for line (group of non-overtaking trips over
same stops at diff times), and "seq" is a string to sort stops for this line by,
e.g. stops/nodes [L1-a, L1-b, L1-c] will be grouped into same line with 3 stops
in that "a-b-c" order (alphasort).

Names like "L1-f/L5-a/L3-m" can be used when multiple lines pass through same stop.
Drawn edges aren't actually used by the script, node names/positions should have
all the necessary info.

See script itself for all the constants like train/footpath speeds, line trips
first/last times, intervals, stop arrival-departure deltas, etc.

``timetable-from-json-dgc.example.json`` is an example JSON graph, as produced
by json-dgc, and can be loaded/tweaked there or used as a template to generate
with some other tool (just two lists of all nodes / edges).



Links
-----

Papers/docs directly related to this project:

- Trip-Based Public Transit Routing (`arXiv:1504.07149v2`_, 2015)

- Trip-Based Public Transit Routing Using Condensed Search Trees
  (`arXiv:1607.01299v2`_, 2016)

- `General Transit Feed Specification (GTFS) format info
  <https://developers.google.com/transit/gtfs/>`_

More on the subject:

- `Topical github awesome-transit list-repo <https://github.com/luqmaan/awesome-transit>`_

- `OpenTripPlanner (OTP) project <http://www.opentripplanner.org/>`_ + `Bibliography.md there
  <https://github.com/opentripplanner/OpenTripPlanner/blob/master/docs/Bibliography.md>`_

  Includes implementation of `RAPTOR
  <https://www.microsoft.com/en-us/research/wp-content/uploads/2012/01/raptor_alenex.pdf>`_ -like
  RoundBasedProfileRouter (see RepeatedRaptorProfileRouter.java and PR-1922 there).

- `Graphserver project <https://github.com/graphserver/graphserver/>`_

- `transit.land open GTFS transit data feeds/repository <https://transit.land/>`_

- Github orgs/groups related to transportation maps/routing:

  - `open-track <https://github.com/open-track>`_
  - `OpenTransport <https://github.com/OpenTransport>`_


.. _arXiv\:1504.07149v2: https://arxiv.org/abs/1504.07149
.. _arXiv\:1607.01299v2: https://arxiv.org/abs/1607.01299
