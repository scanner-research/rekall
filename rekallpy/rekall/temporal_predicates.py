"""Binary predicates on Temporal Ranges.

Before and After:
    These s optionally take a min_dist and max_dist. They check if
    the distance between intrvl1 and intrvl2 is in the range
    [min_dist, max_dist] (in the right direction). Note that by default, this
    includes intervals that meet each other.

OverlapsBefore and OverlapsAfter:
    The strict Allen interval definition of overlapping in either direction.
    Returns true if intrvl1 and intrvl2 have the following relationship:
      
      |-----|
         |-----|
    
    OverlapsAfter requires that intrvl1 start after intrvl2 (and end after
    intrvl2).

Starts and StartsInv:
    True if intrvl1 has same start time as intrvl2 and ends before intrvl2 
    (flip intrvl1 and intrvl2 for the inverse).

Finishes and FinishesInv:
    True if intrvl1 has the same finish time as intrvl2 and starts before 
    intrvl2 (flip for inverse).

During and DuringInv:
    True if intrvl1 starts sintrvlictly after intrvl2 and ends strictly before
    intrvl2 (flip for inverse).

MeetsBefore and MeetsAfter:
    True if intrvl1 starts when intrvl2 ends (flip for inverse).

Equal:
    True if intrvl1 and intrvl2 start and end at the same time.

Overlaps:
    Sugar for a more colloquial version of overlapping. Includes Starts/Inv,
    Finishes/Inv, During/Inv, Equal, and OverlapsBefore/After.
"""

from rekall.common import *

def before(min_dist=0, max_dist=INFTY):
    def fn(intrvl1, intrvl2):
        time_diff = intrvl2.start - intrvl1.end
        return (time_diff >= min_dist and
            (max_dist == INFTY or time_diff <= max_dist))

    return fn

def after(min_dist=0, max_dist=INFTY):
    def fn(intrvl1, intrvl2):
        time_diff = intrvl1.start - intrvl2.end
        return (time_diff >= min_dist and
            (max_dist == INFTY or time_diff <= max_dist))

    return fn

def overlaps():
    return lambda intrvl1, intrvl2: ((intrvl1.start < intrvl2.start and intrvl1.end > intrvl2.start) or
            (intrvl1.start < intrvl2.end and intrvl1.end > intrvl2.end) or
            (intrvl1.start <= intrvl2.start and intrvl1.end >= intrvl2.end) or
            (intrvl1.start >= intrvl2.start and intrvl1.end <= intrvl2.end))

def overlaps_before():
    return lambda intrvl1, intrvl2: (intrvl1.end > intrvl2.start and intrvl1.end < intrvl2.end and
            intrvl1.start < intrvl2.start)

def overlaps_after():
    return lambda intrvl1, intrvl2: (intrvl1.start > intrvl2.start and intrvl1.start < intrvl2.end and
            intrvl1.end > intrvl2.end)

def starts(epsilon=0):
    return lambda intrvl1, intrvl2: (abs(intrvl1.start - intrvl2.start) <= epsilon
            and intrvl1.end < intrvl2.end)

def starts_inv(epsilon=0):
    return lambda intrvl1, intrvl2: (abs(intrvl1.start - intrvl2.start) <= epsilon
            and intrvl2.end < intrvl1.end)

def finishes(epsilon=0):
    return lambda intrvl1, intrvl2: (abs(intrvl1.end - intrvl2.end) <= epsilon
            and intrvl1.start > intrvl2.start)

def finishes_inv(epsilon=0):
    return lambda intrvl1, intrvl2: (abs(intrvl1.end - intrvl2.end) <= epsilon
            and intrvl2.start > intrvl1.start)

def during():
    return lambda intrvl1, intrvl2: intrvl1.start > intrvl2.start and intrvl1.end < intrvl2.end

def during_inv():
    return lambda intrvl1, intrvl2: intrvl2.start > intrvl1.start and intrvl2.end < intrvl1.end

def meets_before(epsilon=0):
    return lambda intrvl1, intrvl2: abs(intrvl1.end-intrvl2.start) <= epsilon

def meets_after(epsilon=0):
    return lambda intrvl1, intrvl2: abs(intrvl2.end-intrvl1.start) <= epsilon

def equal():
    return lambda intrvl1, intrvl2: intrvl1.start == intrvl2.start and intrvl1.end == intrvl2.end

