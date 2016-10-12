from . import utils as u


### TBRoutingEngine input data

# "We consider public transit networks defined by an aperiodic
#  timetable, consisting of a set of stops, a set of footpaths and a set of trips."

@u.attr_struct
class Timetable: keys = 'stops footpaths trips'

@u.attr_struct
class Stop: keys = 'id name lon lat'

@u.attr_struct
class TripStop: keys = 'stop dts_arr dts_dep'

stop_pair_key = lambda a,b: '\0'.join(sorted([a.id, b.id]))
