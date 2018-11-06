import itertools
from rekall.common import *
from rekall.temporal_predicates import *

'''
A helper function that, given two objects, returns the payload field of the first
one.
'''
def intrvl1_payload(intrvl1, intrvl2):
    return intrvl1.payload

'''
A helper function that, given two objects, returns the payload field of the first
one.
'''
def intrvl2_payload(intrvl1, intrvl2):
    return intrvl2.payload

'''
A Interval has a start time, end time, and payload.
'''
class Interval:
    '''
    Construct an temporal range from a start time, end time, and integer payload.
    '''
    def __init__(self, start, end, payload):
        self.start = start
        self.end = end
        self.payload = payload

    def sort_key(intrvl):
        return (intrvl.start, intrvl.end, intrvl.payload)

    def __repr__(self):
        return "<Interval start:{} end:{} payload:{}>".format(self.start,
                self.end, self.payload)

    def copy(self):
        return Interval(self.start, self.end, self.payload)

    '''
    Computes the interval difference between self and other and returns results
    in an array.
    If there is no overlap between self and other, [self.copy()] is returned.
    If self is completely contained by other, [] is returned.
    Otherwise, returns a list l of intervals such that the members of l
    maximally cover self without overlapping other.
    The payloads of the members of l are determined by
    payload_producer_fn(self, other).
    '''
    def minus(self, other, payload_producer_fn=intrvl1_payload):
        if overlaps()(self, other):
            payload = payload_producer_fn(self, other)
            if during()(self, other) or equal()(self, other):
                return []
            if overlaps_before()(self, other):
                return [Interval(self.start, other.start, payload)]
            if overlaps_after()(self, other):
                return [Interval(other.end, self.end, payload)]
            if during()(other, self):
                return [Interval(self.start, other.start, payload),
                        Interval(other.end, self.end, payload)]
            error_string = "Reached unreachable point in minus with {} and {}"
            error_string = error_string.format(self, other)
            assert False, error_string
        else:
            return [self.copy()]

    '''
    Computes the interval overlap between self and other.
    If there is no overlap between self and other, returns None.
    Otherwise, it returns an interval that maximally overlaps both self and
    other, with payload produced by lable_producer_fn(self, other).
    '''
    def overlap(self, other, payload_producer_fn=intrvl1_payload):
        if overlaps()(self, other):
            payload = payload_producer_fn(self, other)
            if (during()(self, other) or equal()(self, other)
                    or starts()(self, other) or finishes()(self, other)):
                return Interval(self.start, self.end, payload)
            if overlaps_before()(self, other):
                return Interval(other.start, self.end, payload)
            if overlaps_after()(self, other):
                return Interval(self.start, other.end, payload)
            if (during()(other, self) or starts()(other, self)
                    or finishes()(other, self)):
                return Interval(other.start, other.end, payload)
            error_string = "Reached unreachable point in overlap with {} and {}"
            error_string = error_string.format(self, other)
            assert False, error_string
        else:
            return None

    '''
    Computes the minimum interval that contains both self and other.
    '''
    def merge(self, other, payload_producer_fn=intrvl1_payload):
        payload = payload_producer_fn(self, other)
        return Interval(min(self.start, other.start),
                max(self.end, other.end), payload)

    '''
    Getters.
    '''
    def get_start(self):
        return self.start
    def get_end(self):
        return self.end
    def get_payload(self):
        return self.payload


'''
A IntervalList is a wrapper around a list of Temporal Ranges that contains
a number of useful helper functions.
'''
class IntervalList:
    def __init__(self, intrvls):
        self.intrvls = sorted([intrvl if isinstance(intrvl, Interval)
                else Interval(intrvl[0], intrvl[1], intrvl[2]) for intrvl in intrvls],
                key = Interval.sort_key)

    def __repr__(self):
        return str(self.intrvls)
    
    '''
    Gets number of Temporal Ranges stored by this IntervalList.
    '''
    def size(self):
        return len(self.intrvls)

    '''
    Return an ordered list of the Intervals.
    '''
    def get_intervals(self):
        return self.intrvls

    ''' Get total time. '''
    def get_total_time(self):
        total = 0
        for intrvl in self.intrvls:
            total += intrvl.end - intrvl.start
        return total

    '''
    Combine the temporal ranges in self with the temporal ranges in other.
    '''
    def set_union(self, other):
        return IntervalList(self.intrvls + other.intrvls)

    '''
    Recursively merge all overlapping or touching temporal ranges.

    If require_same_payload is True, then only merge ranges that have the same
    payload.
    '''
    def coalesce(self, require_same_payload=False):
        if len(self.intrvls) == 0:
            return self
        new_intrvls = []
        first_by_payload = {}
        for intrvl in self.intrvls:
            if require_same_payload:
                payload = intrvl.payload
            else:
                payload = 0
            if payload in first_by_payload:
                first = first_by_payload[payload]
                if intrvl.start >= first.start and intrvl.start <= first.end:
                    # intrvl overlaps with first
                    if intrvl.end > first.end:
                        # need to push the upper bound of first
                        first.end = intrvl.end
                else:
                    # intrvl does not overlap with first
                    new_intrvls.append(first_by_payload[payload])
                    first_by_payload[payload] = intrvl.copy()
            else:
                first_by_payload[payload] = intrvl.copy()
        for intrvl in first_by_payload.values():
            new_intrvls.append(intrvl)

        return IntervalList(new_intrvls)

    '''
    Expand every temporal range. An temporal range [start, end, i] will turn into
    [start - window, end + window, i].
    '''
    def dilate(self, window):
        return IntervalList(
            [Interval(intrvl.start - window, intrvl.end + window, intrvl.payload) 
                for intrvl in self.intrvls])

    '''
    Filter every temporal range by fn. fn takes in an Interval and returns true or
    false.
    '''
    def filter(self, fn):
        return IntervalList([intrvl.copy() for intrvl in self.intrvls if fn(intrvl)])

    '''
    Filter temporal ranges so that only temporal ranges of length between min_length and
    max_length are left.
    '''
    def filter_length(self, min_length=0, max_length=INFTY):
        def filter_fn(intrvl):
            length = intrvl.end - intrvl.start
            return length >= min_length and (max_length == INFTY
                    or length <= max_length)

        return self.filter(filter_fn)

    '''
    Filter the ranges in self against the ranges in other, only keeping the
    ranges in self that satisfy predicate with at least one other range in
    other.
    '''
    def filter_against(self, other, predicate=true_pred()):
        def filter_fn(intrvl):
            for intrvlother in other.intrvls:
                if predicate(intrvl, intrvlother):
                    return True
            return False

        return self.filter(filter_fn)

    '''
    Calculate the difference between the temporal ranges in self and the temporal ranges
    in other.

    The difference between two intervals can produce up to two new intervals.
    If recursive_diff is True, difference operations will recursively be
    applied to the resulting intervals.
    If recursive_diff is False, the results of each difference operation
    between every valid pair of intervals in self and other will be emitted.

    For example, suppose the following interval is in self:

    |--------------------------------------------------------|

    and that the following two intervals are in other:

              |--------------|     |----------------|
    
    If recursive_diff is True, this function will produce three intervals:

    |---------|              |-----|                |--------|

    If recursive_diff is False, this function will produce four intervals, some
    of which are overlapping:

    |---------|              |-------------------------------|
    |------------------------------|                |--------|

    Only computes differences for pairs that overlap and that satisfy
    predicate.

    If an interval in self overlaps no pairs in other such that the two
    satisfy predicate, then the interval is reproduced in the output.

    Labels the resulting intervals with payload_producer_fn. For recursive_diff,
    the intervals passed in to the payload producer function are the original
    interval and the first interval that touches the output interval.
    '''
    def minus(self, other, recursive_diff = True, predicate = true_pred(),
            payload_producer_fn = intrvl1_payload):
        if not recursive_diff:
            output = []
            for intrvl1 in self.intrvls:
                found_overlap = False
                for intrvl2 in other.intrvls:
                    if overlaps()(intrvl1, intrvl2) and predicate(intrvl1, intrvl2):
                        found_overlap = True
                        candidates = intrvl1.minus(intrvl2)
                        if len(candidates) > 0:
                            output = output + candidates
                if not found_overlap:
                    output.append(intrvl1.copy())
            return IntervalList(output)
        else:
            output = []
            for intrvl1 in self.intrvls:
                # For each interval in self.intrvls, get all the overlapping
                #   intervals from other.intrvls
                overlapping = []
                for intrvl2 in other.intrvls:
                    if intrvl1 == intrvl2:
                        continue
                    if before()(intrvl1, intrvl2):
                        break
                    if (overlaps()(intrvl1, intrvl2) and
                        predicate(intrvl1, intrvl2)):
                        overlapping.append(intrvl2)

                if len(overlapping) == 0:
                    output.append(intrvl1.copy())
                
                # Create a sorted list of all start to end points between
                #   intrvl1.start and intrvl1.end, inclusive
                endpoints_set = set([intrvl1.start, intrvl1.end])
                for intrvl in overlapping:
                    if intrvl.start > intrvl1.start:
                        endpoints_set.add(intrvl.start)
                    if intrvl.end < intrvl1.end:
                        endpoints_set.add(intrvl.end)
                endpoints_list = sorted(list(endpoints_set))

                # Calculate longest subsequence endpoint pairs
                longest_subsequences = []
                last_j = -1
                for i in range(len(endpoints_list)):
                    if i <= last_j:
                        continue
                    start = endpoints_list[i]
                    valid = True
                    for intrvl in overlapping:
                        if intrvl.start > start:
                            break
                        if intrvl.start < start and intrvl.end > start:
                            valid = False
                            break
                    if not valid:
                        continue
                    max_j = len(endpoints_list) - 1
                    for j in range(max_j, i, -1):
                        end = endpoints_list[j]
                        intrvl_candidate = Interval(start, end, 0)
                        valid = True
                        for intrvl in overlapping:
                            if intrvl.start > end:
                                break
                            if overlaps()(intrvl, intrvl_candidate):
                                valid = False
                                break
                        if valid:
                            longest_subsequences.append((start, end))
                            last_j = j

                # Figure out which intervals from overlapping to use to
                # construct new intervals
                for subsequence in longest_subsequences:
                    start = subsequence[0]
                    end = subsequence[1]
                    for intrvl in overlapping:
                        if intrvl.end == start or intrvl.start == end:
                            payload = payload_producer_fn(intrvl1, intrvl)
                            output.append(Interval(start, end, payload))
                            break

            return IntervalList(output)


    '''
    Get the overlapping intervals between self and other.

    Only processes pairs that overlap and that satisfy predicate.

    Labels the resulting intervals with payload_producer_fn.
    '''
    def overlaps(self, other, predicate = true_pred(), payload_producer_fn =
            intrvl1_payload):
        return IntervalList([intrvl1.overlap(intrvl2, payload_producer_fn)
                for intrvl1 in self.intrvls for intrvl2 in other.intrvls
                if (overlaps()(intrvl1, intrvl2) and
                    predicate(intrvl1, intrvl2))])

    '''
    Merges pairs of intervals in self and other that satisfy payload_producer_fn.

    Only processes pairs that satisfy predicate.

    Labels the resulting intervals with payload_producer_fn.
    '''
    def merge(self, other, predicate = true_pred(), payload_producer_fn =
            intrvl1_payload):
        return IntervalList([intrvl1.merge(intrvl2, payload_producer_fn)
                for intrvl1 in self.intrvls for intrvl2 in other.intrvls
                if predicate(intrvl1, intrvl2)])

    '''
    Generates a new IntervalList from the cross product of self and other;
    pairs are processed using the udf.
    '''
    def cross_udf(self, other, udf):
        return IntervalList(list(itertools.chain.from_iterable([
                udf(intrvl1, intrvl2) for intrvl1 in self.intrvls for intrvl2 in other.intrvls
            ])))
