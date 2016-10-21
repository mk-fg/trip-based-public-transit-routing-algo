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

Not focused on performance yet, more on readability and correctnes aspects
first, i.e. to have something working.

Under heavy development, not really usable yet.


Requirements
------------

- Python 3.x
- `attrs <https://attrs.readthedocs.io/en/stable/>`_
- (for tests only) `PyYAML <http://pyyaml.org/>`_
- (for Python<3.4 only) `pathlib <https://pypi.python.org/pypi/pathlib2/>`_
- (for Python<3.4 only) `enum34 <https://pypi.python.org/pypi/enum34/>`_


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

- `open-track project <https://github.com/open-track>`_


.. _arXiv\:1504.07149v2: https://arxiv.org/abs/1504.07149
.. _arXiv\:1607.01299v2: https://arxiv.org/abs/1607.01299
