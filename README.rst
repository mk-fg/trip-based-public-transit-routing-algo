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

Not focused on performance, more on readability and correctness aspects,
i.e. just to have something working.

Under heavy development.

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

  % time ./gtfs-tb-routing.py gtfs-shizuoka -c gtfs-shizuoka.cache.pickle J22209723_0 J2220952426_0
  Journey set (1):
    Journey 5596f26afe50 (arrival: 08:43:00, trips: 2):
      trip [95]:
        from (dep at 06:10:00): 10:小川 [J22209723_0]
        to (arr at 06:55:00): 49:島田駅 北口２番のりば [J222093340_2]
      trip [97]:
        from (dep at 08:35:00): 20:島田駅 北口２番のりば [J222093340_2]
        to (arr at 08:43:00): 28:ばらの丘一丁目 [J2220952426_0]
  ./gtfs-tb-routing.py ... 8.39s user 0.06s system 99% cpu 8.454 total

  % time ./gtfs-tb-routing.py gtfs-shizuoka -c gtfs-shizuoka.cache.pickle J22209843_0 J222093345_0
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
- (for Python<3.4 only) `pathlib <https://pypi.python.org/pypi/pathlib2/>`_
- (for Python<3.4 only) `enum34 <https://pypi.python.org/pypi/enum34/>`_



Notes
-----

Some less obvious things are described in this section.


Tests
`````

Commands to run tests from checkout directory::

  % python3 -m unittest test.all
  % python3 -m unittest test.gtfs_shizuoka
  % python3 -m unittest -v test.simple

``test.all.case`` also provides global index of all test cases by name::

  % python3 -m unittest test.all.case.test_journeys_J22209723_J2220952426
  % python3 -m unittest test.all.case.testMultipleRoutes



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
