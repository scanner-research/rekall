from rekall.interval_list import Interval
import rekall.interval_set_3d_utils as utils
from rekall.temporal_predicates import *
from rekall.logical_predicates import *
from rekall.merge_ops import * 
from functools import reduce
import constraint as constraint

class Interval3D:
    """
      An Interval3D has bounds on t, x, y dimensions and a payload.
      X, Y values are between [0,1], and t values can be arbitrary.
    """
    def __init__(self, t, x=(0,1), y=(0,1), payload=None):
        self.t = t
        self.x = x
        self.y = y
        self.payload = payload

    def __repr__(self):
        return ("<Interval3D t: [{0},{1}] "
                "x: [{2},{3}] y: [{4},{5}] payload:{6}>").format(
                self.t[0], self.t[1], self.x[0], self.x[1],
                self.y[0], self.y[1], self.payload)

    def copy(self):
        return Interval3D(self.t, self.x,
                self.y, self.payload)

    # Returns another interval that is a combination of self and other
    def combine(self, other, t_combiner, x_combiner, y_combiner,
                payload_merge_op = payload_first):
        payload = payload_merge_op(self.payload, other.payload)
        t = t_combiner(self.t, other.t)
        x = x_combiner(self.x, other.x)
        y = y_combiner(self.y, other.y)
        return Interval3D(t,x,y,payload)

    def merge(self, other, payload_merge_op=payload_first):
        return self.combine(other,
                utils.merge_bound,
                utils.merge_bound,
                utils.merge_bound,
                payload_merge_op)

    # Returns None if self and other are not overlapping in time.
    def overlap_time_merge_space(self, other, payload_merge_op=payload_first):
        if utils.T(overlaps())(self, other):
            return self.combine(other,
                    utils.overlap_bound,
                    utils.merge_bound,
                    utils.merge_bound,
                    payload_merge_op)
        else:
            return None

    # Returns another interval that has the full spatial range.
    def expand_to_frame(self):
        return Interval3D(self.t, (0,1), (0,1), self.payload)

    def length(self):
        return utils.bound_size(self.t)
    def width(self):
        return utils.bound_size(self.x)
    def height(self):
        return utils.bound_size(self.y)

class IntervalSet3D:
    def __init__(self, intrvls):
        self._intrvls = intrvls
        self._sort()
        self._time_window = self._get_time_window()

    def __repr__(self):
        return str(self._intrvls)

    # Sort the intervals in the class.
    def _sort(self):
        self._intrvls.sort(key=utils.sort_key_time_x_y)

    # Compute the default time_window based on the current intervals
    def _get_time_window(self):
        NUM_INTRVLS_THRESHOLD = 1000
        DEFAULT_FRACTION = 1/100
        n = len(self._intrvls)
        if n > 0:
            max_end = max([i.t[1] for i in self._intrvls])
            min_start = self._intrvls[0].t[0]
            if n > NUM_INTRVLS_THRESHOLD:
                return (max_end - min_start) * DEFAULT_FRACTION
            else:
                return max_end - min_start
        else:
            return 0

    # Returns all intervals in the set ordered by (t1, t2, x1, x2, y1, y2)
    def get_intervals(self):
        return self._intrvls

    def map(self, map_fn):
        """
        map_fn takes an Interval3D and returns an Interval3D
        """
        return IntervalSet3D([map_fn(intrvl) for intrvl in self._intrvls])

    def union(self, other):
        return IntervalSet3D(self._intrvls + other._intrvls)

    def join(self, other, predicate, merge_op, time_window=None):
        """
        Joins intervals in self with those in other on predicate and produces
        new Intervals based on merge_op.

        merge_op takes in two Intervals and returns a list of Intervals
        predicate takes in two Intervals and returns True or False
        """
        if time_window == None:
            time_window = self._time_window
        output = []
        start_index = 0
        for intrvl in self.get_intervals():
            new_start_index = None
            for idx, intrvlother in enumerate(
                    other.get_intervals()[start_index:]):
                t1, t2 = intrvl.t
                v1, v2 = intrvlother.t
                if t1 - time_window <= v2:
                    # All following intervals in self will start after t1
                    if new_start_index is None:
                        new_start_index = idx + start_index
                    # All following intervals in other will start after v1
                    if v1 - time_window > t2:
                        break
                    if predicate(intrvl, intrvlother):
                        new_intrvls = merge_op(intrvl, intrvlother)
                        if len(new_intrvls) > 0:
                            output += new_intrvls
            # All intervals left in other end too early to be in the receptive
            # field of all following intervals in self.
            if new_start_index is None:
                break
            start_index = new_start_index
        return IntervalSet3D(output)

    def filter(self, predicate):
        """
        Filter every interval by predicate.
        """
        return IntervalSet3D([
            intrvl.copy() for intrvl in self.get_intervals() if 
            predicate(intrvl)])

    def fold(self, reducer, init=None, sort_key=utils.sort_key_time_x_y):
        """
        Perform fold with given reducer and init on the list of intervals
        ordered by sort_key
        """
        lst = sorted(self.get_intervals(), key=sort_key)
        if init is None:
            return reduce(reducer, lst)
        else:
            return reduce(reducer, lst, init)

    def group_by(self, key, merge):
        """
        Group by all intervals by `key` and merge each group into a new
        interval.
        `key` takes an Interval3D and returns a key.
        `merge` takes a key and an IntervalSet3D and returns an Interval3D
        """
        def add_to_group(group, i):
            k = key(i) 
            if k not in group:
                group[k] = [i]
            else:
                group[k].append(i)
            return group
        groups = self.fold(add_to_group, {})
        output = []
        for k, intervals in groups.items():
            output.append(merge(k, IntervalSet3D(intervals)))
        return IntervalSet3D(output)

    def minus(self, other):
        """
        Calculate the difference between the temporal ranges in self and the
        temporal ranges in other. The spatial dimensions of each interval is
        not changed.

        The difference between two intervals can produce up to two new
        intervals.

        For example, suppose the following interval is in self:

        |--------------------------------------------------------|

        and that the following two intervals are in other:

                  |--------------|     |----------------|
        
        this function will produce three intervals:

        |---------|              |-----|                |--------|

        If an interval in self overlaps no pairs in other such that the two
        satisfy predicate, then the interval is reproduced in the output.

        """
        # Only allow full-frame intervals in other
        for intrvl in other.get_intervals():
            if intrvl.x != (0,1) or intrvl.y != (0,1):
                raise NotImplementedError
        output = []
        start_index = 0
        for intrvl in self.get_intervals():
            new_start_index = None
            overlapped = []
            for idx, otherintrvl in enumerate(
                    other.get_intervals()[start_index:]):
                t1, t2 = intrvl.t
                v1, v2 = otherintrvl.t
                if v2 >= t1:
                    # All following intervals in self will start after t1
                    if new_start_index is None:
                        new_start_index = idx + start_index
                    # All following intervals in other will start after v1
                    if v1 > t2:
                        break
                    overlapped.append(otherintrvl)
            if new_start_index is None:
                output.append(intrvl.copy())
                start_index = len(other.get_intervals())
                continue
            else:
                start_index = new_start_index
                if len(overlapped)==0:
                    output.append(intrvl.copy())
                    continue
                # Take only nontrivial overlaps
                overlapped = [i for i in overlapped if i.length() > 0]
                
                start = intrvl.t[0]
                overlapped_index = 0
                while start < intrvl.t[1]:
                    # Each iteration proposes an interval starting at `start`
                    # If no overlapped interval goes acoss `start`, then it is
                    # a valid start for an interval after the subtraction.
                    intervals_across_start = []
                    first_interval_after_start = None
                    new_overlapped_index = None
                    for idx,overlap in enumerate(
                            overlapped[overlapped_index:]):
                        v1, v2 = overlap.t
                        if new_overlapped_index is None and v2 > start:
                            new_overlapped_index = idx + overlapped_index
                        if v1 <= start and v2 > start:
                            intervals_across_start.append(overlap)
                        elif v1 > start:
                            # overlap is sorted by (t1,t2)
                            first_interval_after_start = overlap
                            break
                    if len(intervals_across_start) == 0:
                        # start is valid, now finds an end point
                        if first_interval_after_start is None:
                            end = intrvl.t[1]
                            new_start = end
                        else:
                            end = first_interval_after_start.t[0]
                            new_start = first_interval_after_start.t[1]
                        if end > start:
                            output.append(Interval3D(
                                (start, end), intrvl.x, intrvl.y,
                                intrvl.payload))
                        start = new_start
                    else:
                        # start is invalid, now propose another start
                        start = max([i.t[1] for i in intervals_across_start])
                    if new_overlapped_index is not None:
                        overlapped_index = new_overlapped_index
        return IntervalSet3D(output)

    def match(self, pattern, exact=False):
        """
        Pattern matching among the intervals in the set.
        pattern is a list of constraints, where each constraint is a pair
          (names, predicates)
          A name is a string that names the variable in the constraint.
          A predicate is a function that takes len(names) Interval3Ds as
             arguments and returns True or False.
        If exact is True, only consider pattern matching a success if all
        intervals in the set are assigned names.

        Returns a list of solutions for successful matchings, where each
            solution is a dictionary from name to the assigned interval.
        """
        nodes, constraints = utils.parse_pattern(pattern)
        intervals = self.get_intervals()
        if exact and len(intervals) != len(nodes):
            return []
        if len(nodes) == 0:
            return []

        def satisfies_all(preds, items):
            for pred in preds:
                if not pred(*items):
                    return False
            return True

        def wrap_preds(preds, intervals):
            def pred(*args):
                new_args = [intervals[i] for i in args]
                return satisfies_all(preds, new_args)
            return pred

        prob = constraint.Problem()
        for name, predicates in nodes.items():
            # Pre-compute single variable constraints
            candidates = [i for i, intrvl in enumerate(intervals)
                    if satisfies_all(predicates, [intrvl])]
            if len(candidates) == 0:
                return []
            prob.addVariable(name, candidates)
        prob.addConstraint(constraint.AllDifferentConstraint())
        # Add multi-variable constraints.
        for names, predicates in constraints:
            prob.addConstraint(wrap_preds(predicates, intervals), names)

        solutions = prob.getSolutions()
        if solutions is None:
            return []
        ret = []
        for sol in solutions:
            d = {}
            for key, idx in sol.items():
                d[key] = intervals[idx]
            ret.append(d)
        return ret

