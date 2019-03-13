"""DEPRECATED: Use rekall.interval_set_3d instead."""

import itertools
from functools import reduce
from rekall.common import *
from rekall.helpers import *
from rekall.temporal_predicates import *
from rekall.logical_predicates import *
from rekall.merge_ops import *

class Interval:
    """DEPRECATED: Use Interval3D in rekall.interval_set_3d instead.

    A Interval has a start time, end time, and payload.
    """
    def __init__(self, start, end, payload):
        """
        Construct an temporal range from a start time, end time, and integer payload.
        """
        self.start = start
        self.end = end
        self.payload = payload

    def sort_key(intrvl):
        return (intrvl.start, intrvl.end)

    def __repr__(self):
        return "<Interval start:{} end:{} payload:{}>".format(self.start,
                self.end, self.payload)

    def copy(self):
        return Interval(self.start, self.end, self.payload)

    def minus(self, other, payload_merge_op=payload_first):
        """
        Computes the interval difference between self and other and returns results
        in an array.
        If there is no overlap between self and other, [self.copy()] is returned.
        If self is completely contained by other, [] is returned.
        Otherwise, returns a list l of intervals such that the members of l
        maximally cover self without overlapping other.
        The payloads of the members of l are determined by
        payload_merge_op(self, other).
        """
        if overlaps()(self, other):
            payload = payload_merge_op(self.payload, other.payload)
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

    def overlap(self, other, payload_merge_op=payload_first):
        """
        Computes the interval overlap between self and other.
        If there is no overlap between self and other, returns None.
        Otherwise, it returns an interval that maximally overlaps both self and
        other, with payload produced by lable_producer_fn(self, other).
        """
        if overlaps()(self, other):
            payload = payload_merge_op(self.payload, other.payload)
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

    def merge(self, other, payload_merge_op=payload_first):
        """
        Computes the minimum interval that contains both self and other.
        """
        payload = payload_merge_op(self.payload, other.payload)
        return Interval(min(self.start, other.start),
                max(self.end, other.end), payload)

    def get_start(self):
        return self.start
    def get_end(self):
        return self.end
    def get_payload(self):
        return self.payload
    def length(self):
        return self.end-self.start


class IntervalList:
    """DEPRECATED: Use IntervalSet3D in rekall.interval_set_3d instead.

    A IntervalList is a wrapper around a list of Intervals.

    IntervalList provides core operations map, fold, join, coalesce, and minus.
    See the respective functions for what each does.

    IntervalList also provides sugared functions merge and overlaps, which are
    common ways to use join in a temporal domain.

    OPTIMIZATION: If there are more than 1000 intervals, join operations will
        work over a "working window" equal to 1/100th of the total timespan
        covered by the IntervalList. Instead of computing the full cross
        product for a join and then filtering those pairs by the predicate,
        it only runs the predicate for intervals that differ in time by less
        than the working window. On large IntervalLists, this can increase
        performance dramatically. Users can set the working_window manually
        for all join operations.
    """
    def __init__(self, intrvls):
        if isinstance(intrvls, IntervalList):
            intrvls = intrvls.intrvls
        self.intrvls = sorted([intrvl if isinstance(intrvl, Interval)
                else Interval(intrvl[0], intrvl[1], intrvl[2]) for intrvl in intrvls],
                key = Interval.sort_key)
        if len(self.intrvls) > 0:
            max_end = max([intrvl.end for intrvl in self.intrvls])
            if len(self.intrvls) > 1000:
                self.working_window = (max_end - self.intrvls[0].start) / 100
            else:
                self.working_window = (max_end - self.intrvls[0].start)
        else:
            self.working_window = 0

    def __repr__(self):
        return str(self.intrvls)
    
    # ============== GETTERS ==============
    def size(self):
        """ Gets number of Intervals stored by this IntervalList.
        """
        return len(self.intrvls)

    def get_intervals(self):
        """ Return an ordered list of the Intervals.
        """
        return self.intrvls

    def get_total_time(self):
        """ Get total time. """
        total = 0
        for intrvl in self.intrvls:
            total += intrvl.end - intrvl.start
        return total

    # ============= GENERAL LIST FUNCTIONS =============
    def map(self, map_fn):
        """
        Maps all the intervals in intrvls.

        map_fn takes in an Interval and returns an Interval
        """
        return IntervalList([map_fn(intrvl) for intrvl in self.intrvls])

    def join(self, other, merge_op, predicate, working_window=None):
        """
        Joins self.intrvls with other.intrvls on predicate and produces new
        Intervals based on merge_op.

        merge_op takes in two Intervals and returns a list of Intervals
        predicate takes in two Intervals and returns True or False
        """
        if working_window == None:
            working_window = self.working_window
        output = []
        start_index = 0
        for intrvl in self.intrvls:
            new_start_index = None
            for idx, intrvlother in enumerate(other.intrvls[start_index:]):
                if new_start_index is None:
                    if intrvl.start < intrvlother.start:
                        new_start_index = idx
                    elif intrvl.start - working_window <= intrvlother.end:
                        new_start_index = idx
                if intrvlother.start - working_window > intrvl.end:
                    break
                if predicate(intrvl, intrvlother):
                    new_intrvls = merge_op(intrvl, intrvlother)
                    if len(new_intrvls) > 0:
                        output += new_intrvls
            if new_start_index is not None:
                start_index += new_start_index
        return IntervalList(output)

    def fold(self, fold_fn, init_acc):
        """
        Computes a fold across intrvls.

        fold_fn takes in an accumulator and an Interval.
        """
        return reduce(fold_fn, self.intrvls, init_acc)

    def fold_list(self, fold_fn, init_acc):
        """
        Computes a fold whose accumulator is a list and returns an
        IntervalList.
        """
        return IntervalList(self.fold(fold_fn, init_acc))

    # ============== FUNCTIONS THAT MODIFY SELF ==============
    def coalesce(self, payload_merge_op=payload_first,
            predicate=true_pred(arity=2)):
        """
        Recursively merge all overlapping or touching intervals that satisfy
        predicate. predicate must be an equivalence relation over the intervals
        (must be reflexive, symmetric, and transitive - all the properties of
        a binary relationship that acts like equality in some way).

        """
        if len(self.intrvls) == 0:
            return self
        new_intrvls = []
        current_intervals = []
        for intrvl in self.intrvls:
            for cur in current_intervals:
                # Add any intervals that end before intrvl.start
                if cur.end < intrvl.start:
                    new_intrvls.append(cur)
            current_intervals = [
                cur
                for cur in current_intervals
                if cur.end >= intrvl.start
            ]
            if len(current_intervals) == 0:
                current_intervals.append(intrvl.copy())
                continue
            
            matched_interval = None
            for cur in current_intervals:
                if predicate(cur, intrvl):
                    matched_interval = cur
                    break
            if matched_interval is None:
                current_intervals.append(intrvl)
            else:
                cur.payload = payload_merge_op(cur.payload, intrvl.payload)
                if intrvl.end > cur.end:
                    cur.end = intrvl.end

        for cur in current_intervals:
            new_intrvls.append(cur)

        return IntervalList(new_intrvls)

    def dilate(self, window):
        """
        Expand every temporal range. An temporal range [start, end, i] will turn into
        [start - window, end + window, i].
        """
        return IntervalList(
            [Interval(intrvl.start - window, intrvl.end + window, intrvl.payload) 
                for intrvl in self.intrvls])

    def filter(self, fn):
        """
        Filter every temporal range by fn. fn takes in an Interval and returns true or
        false.
        """
        return IntervalList([intrvl.copy() for intrvl in self.intrvls if fn(intrvl)])

    def filter_length(self, min_length=0, max_length=INFTY):
        """
        Filter temporal ranges so that only temporal ranges of length between min_length and
        max_length are left.
        """
        def filter_fn(intrvl):
            length = intrvl.end - intrvl.start
            return length >= min_length and (max_length == INFTY
                    or length <= max_length)

        return self.filter(filter_fn)

    # ============= FUNCTIONS THAT JOIN WITH ANOTHER INTERVAL LIST =============
    def set_union(self, other):
        """ Combine the temporal ranges in self with the temporal ranges in other.
        """
        return IntervalList(self.intrvls + other.intrvls)

    def filter_against(self, other, predicate=true_pred(arity=2),
            working_window=None):
        """
        Filter the ranges in self against the ranges in other, only keeping the
        ranges in self that satisfy predicate with at least one other range in
        other.
        """
        if working_window == None:
            working_window = self.working_window

        start_index = 0
        output = []
        for intrvl in self.intrvls:
            new_start_index = None
            add_intrvl = False
            for idx, intrvlother in enumerate(other.intrvls[start_index:]):
                if new_start_index is None:
                    if intrvl.start < intrvlother.start:
                        new_start_index = idx
                    elif intrvl.start - working_window <= intrvlother.end:
                        new_start_index = idx
                if intrvlother.start - working_window > intrvl.end:
                    break
                if predicate(intrvl, intrvlother):
                    add_intrvl = True
                    break
            if new_start_index is not None:
                start_index += new_start_index
            if add_intrvl:
                output.append(intrvl.copy())

        return IntervalList(output)

    def minus(self, other, recursive_diff = True, predicate = true_pred(arity=2),
            payload_merge_op = payload_first, working_window=None):
        """
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

        Labels the resulting intervals with payload_merge_op. For recursive_diff,
        the intervals passed in to the payload producer function are the original
        interval and the first interval that touches the output interval.
        """
        if not recursive_diff:
            return self.join(other, lambda intrvl1, intrvl2:
                    intrvl1.minus(intrvl2, payload_merge_op),
                    and_pred(overlaps(), predicate, arity=2),
                    working_window)
        else:
            if working_window == None:
                working_window = self.working_window

            start_index = 0
            output = []
            for intrvl1 in self.intrvls:
                # For each interval in self.intrvls, get all the overlapping
                #   intervals from other.intrvls
                overlapping = []
                new_start_index = None
                for idx, intrvl2 in enumerate(other.intrvls[start_index:]):
                    if new_start_index is None:
                        if intrvl1.start < intrvl2.start:
                            new_start_index = idx
                        elif intrvl1.start - working_window <= intrvl2.end:
                            new_start_index = idx
                    if (overlaps()(intrvl1, intrvl2) and
                        predicate(intrvl1, intrvl2)):
                        overlapping.append(intrvl2)
                if new_start_index is not None:
                    start_index += new_start_index

                if len(overlapping) == 0:
                    output.append(intrvl1.copy())

                # Special case where where intrvl1 has length 0
                if intrvl1.length() == 0 and len(overlapping) is not 0:
                    continue
                
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
                            payload = payload_merge_op(intrvl1.payload, intrvl.payload)
                            output.append(Interval(start, end, payload))
                            break

            return IntervalList(output)


    def overlaps(self, other, predicate = true_pred(arity=2), payload_merge_op =
            payload_first, working_window=None):
        """
        Get the overlapping intervals between self and other.

        Only processes pairs that overlap and that satisfy predicate.

        Labels the resulting intervals with payload_merge_op.
        """
        def merge_op(intrvl1, intrvl2):
            return [intrvl1.overlap(intrvl2, payload_merge_op)]
        return self.join(other, merge_op, and_pred(overlaps(), predicate,
            arity=2), working_window)

    def merge(self, other, predicate = true_pred(arity=2), payload_merge_op =
            payload_first, working_window=None):
        """
        Merges pairs of intervals in self and other that satisfy predicate.

        Only processes pairs that satisfy predicate.

        Labels the resulting intervals with payload_merge_op.
        """
        def merge_op(intrvl1, intrvl2):
            return [intrvl1.merge(intrvl2, payload_merge_op)]
        return self.join(other, merge_op, predicate, working_window)

