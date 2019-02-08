from rekall.interval_list import Interval
import rekall.interval_set_3d_utils as utils
from rekall.temporal_predicates import *
from rekall.logical_predicates import *
from rekall.merge_ops import * 
from functools import reduce
import constraint as constraint
import multiprocessing as mp
import cloudpickle

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

    def size(self):
        return len(self._intrvls)

    def empty(self):
        return self.size() == 0

    def map(self, map_fn):
        """
        map_fn takes an Interval3D and returns an Interval3D
        """
        return IntervalSet3D([map_fn(intrvl) for intrvl in self._intrvls])

    def union(self, other):
        return IntervalSet3D(self._intrvls + other._intrvls)

    def fold(self, reducer, init=None, sort_key=utils.sort_key_time_x_y):
        """
        Perform fold with given reducer and init on the list of intervals
        ordered by sort_key
        """
        lst = self.get_intervals()
        if sort_key != utils.sort_key_time_x_y:
            lst = sorted(lst, key=sort_key)
        if init is None:
            return reduce(reducer, lst)
        else:
            return reduce(reducer, lst, init)

    def fold_to_set(self, reducer, init=None,
            acc_to_set = lambda acc:IntervalSet3D(acc),
            sort_key = utils.sort_key_time_x_y):
        """
        Runs acc_to_set on result of fold and returns a IntervalSet3D.
        """
        return acc_to_set(self.fold(reducer, init, sort_key))

    def _map_with_other_within_time_window(self, other,
            mapper, time_window=None):
        """
        Map over a list where elements are:
            (interval_in_self, [intervals_in_other])
            where intervals_in_other are those in other that are within
            time_window of interval_in_self, and the list is ordered by
            (t1,t2) of interval_in_self
        """
        if time_window is None:
            time_window = self._time_window
    
        # State is (other_start_index, outputs, done_flag)
        def update_state(state, intrvlself):
            start_index, outputs, done = state
            intervals_in_other = []
            if not done:
                new_start_index = None
                for idx, intrvlother in enumerate(
                        other.get_intervals()[start_index:]):
                    t1, t2 = intrvlself.t
                    v1, v2 = intrvlother.t
                    if t1 - time_window <= v2:
                        if new_start_index is None:
                            new_start_index = idx + start_index
                        if v1 - time_window > t2:
                            break
                        intervals_in_other.append(intrvlother)
                if new_start_index is None:
                    done = True
                else:
                    start_index = new_start_index
            outputs.append(mapper(intrvlself, intervals_in_other))
            return start_index, outputs, done
        state = (0, [], False)
        _,outputs,_ =  self.fold(update_state, state)
        return [r for results in outputs for r in results]

    def join(self, other, predicate, merge_op, time_window=None):
        """
        Joins intervals in self with those in other on predicate and produces
        new Intervals based on merge_op.

        merge_op takes in two Intervals and returns a list of Intervals
        predicate takes in two Intervals and returns True or False
        """
        def map_output(intrvlself, intervals_in_other):
            out = []
            for intrvlother in intervals_in_other:
                if predicate(intrvlself, intrvlother):
                    new_intrvls = merge_op(intrvlself, intrvlother)
                    if len(new_intrvls) > 0:
                        out += new_intrvls
            return out
        return IntervalSet3D(self._map_with_other_within_time_window(
            other, map_output, time_window))

    def filter(self, predicate):
        """
        Filter every interval by predicate.
        """
        return IntervalSet3D([
            intrvl.copy() for intrvl in self.get_intervals() if 
            predicate(intrvl)])

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
        # Returns a list of intervals that are what is left of intrvl after
        # subtracting all overlapped_intervals.
        def compute_difference(intrvl, overlapped_intervals):
            start = intrvl.t[0]
            overlapped_index = 0
            output = []
            while start < intrvl.t[1]:
                # Each iteration proposes an interval starting at `start`
                # If no overlapped interval goes acoss `start`, then it is
                # a valid start for an interval after the subtraction.
                intervals_across_start = []
                first_interval_after_start = None
                new_overlapped_index = None
                for idx,overlap in enumerate(
                        overlapped_intervals[overlapped_index:]):
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
            return output
        
        def map_output(intrvl, overlapped):
            # Take only nontrivial overlaps
            to_subtract = [i for i in overlapped if i.length()>0]
            if len(to_subtract) == 0:
                return [intrvl.copy()]
            else:
                return compute_difference(intrvl, to_subtract)

        return IntervalSet3D(self._map_with_other_within_time_window(
            other, map_output, 0))

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
        # Parser for pattern matching
        # pattern: list of (names, constraints)
        # Returns:
        #   Dict of (VariableName, [SingleVariablePredicate])
        #   List of (VariableNames, Predicates)
        def parse_pattern(pattern):
            node_defs = {}
            multi_node_constraints = []
            for names, constraints in pattern:
                if len(names) == 1:
                    name = names[0]
                    if name not in node_defs:
                        node_defs[name] = list(constraints)
                    else:
                        node_defs[name] += constraints
                else:
                    multi_node_constraints.append((names, constraints))
                    for name in names:
                        if name not in node_defs:
                            node_defs[name] = []
            return node_defs, multi_node_constraints

        nodes, constraints = parse_pattern(pattern)
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

    def filter_against(self, other, predicate, time_window=None):
        """
        Return intervals in self that satisify predicate with at least one
        interval in other.
        """
        def map_output(intrvlself, intrvlothers):
            for intrvlother in intrvlothers:
                if predicate(intrvlself, intrvlother):
                    return [intrvlself.copy()]
            return []
        return IntervalSet3D(self._map_with_other_within_time_window(
            other, map_output, time_window))

    def map_payload(self, fn):
        def map_fn(intrvl):
            return Interval3D(intrvl.t,
                    intrvl.x, intrvl.y,
                    fn(intrvl.payload))
        return self.map(map_fn)
    
    @staticmethod
    def _check_axis_value(axis):
        if axis not in {'T','X','Y'}:
            raise ValueError("axis must be one of 'T', 'X', 'Y'")

    def dilate(self, window, axis='T'):
        """
        Expand every interval along specified axis in both directions by
          amount of `window`.
        axis: one of 'T','X','Y'
        """
        IntervalSet3D._check_axis_value(axis)
        def get_map_fn(window, axis):
            def dilate_bound(b):
                return (b[0]-window, b[1]+window)
            def dilate_t(i):
                return Interval3D(
                        dilate_bound(i.t),
                        i.x, i.y, i.payload)
            def dilate_x(i):
                return Interval3D(i.t, 
                        dilate_bound(i.x),
                        i.y, i.payload)
            def dilate_y(i):
                return Interval3D(i.t, i.x,
                        dilate_bound(i.y),
                        i.payload)
            if axis == 'T':
                return dilate_t
            if axis == 'X':
                return dilate_x
            if axis == 'Y':
                return dilate_y
            raise RuntimeError("Axis not in ['T','X','Y']")
        return self.map(get_map_fn(window, axis))

    def filter_size(self, min_size=0, max_size=INFTY, axis='T'):
        def getter(axis):
            if axis=='T':
                return Interval3D.length
            if axis=='X':
                return Interval3D.width
            if axis=='Y':
                return Interval3D.height
            raise RuntimeError("Axis not in ['T','X','Y']")
        def get_pred(min_size, max_size, axis):
            func = getter(axis)
            def pred(intrvl):
                val = func(intrvl)
                return val >= min_size and (
                       max_size==INFTY or val<=max_size)
            return pred
        IntervalSet3D._check_axis_value(axis)
        return self.filter(get_pred(min_size, max_size, axis))

    def merge(self, other, predicate, payload_merge_op=payload_first,
            time_window=None):
        """
        Merges pairs of intervals in self and other that satisfy predicate.

        Only processes pairs that satisfy predicate.
        """
        def merge_op(i1, i2):
            return [i1.merge(i2, payload_merge_op)]
        return self.join(other, predicate, merge_op, time_window)

    def group_by_time(self):
        """
        Group intervals by the time span.
            Nest each group under a new interval with the common time span
            and full spatial span.
        """
        def key_fn(intrvl):
            return intrvl.t
        def merge_fn(key, intervals):
            return Interval3D(key,
                    payload=intervals)
        return self.group_by(key_fn, merge_fn)

    def collect_by_interval(self, other, predicate,
            filter_empty=True,time_window=None):
        """
        For each interval in self, nest under it all intervals in other
            that satisfy the predicate and are within time_window
        If filter_empty, only keep intervals in self that have corresponding
            intervals in other.
        """
        def map_output(intrvlself, intrvlothers):
            intrvls_to_nest = IntervalSet3D([
                i for i in intrvlothers if predicate(intrvlself, i)])
            if not intrvls_to_nest.empty() or not filter_empty:
                return [Interval3D(
                    intrvlself.t,
                    intrvlself.x,
                    intrvlself.y,
                    payload=intrvls_to_nest)]
            return []
        return IntervalSet3D(self._map_with_other_within_time_window(
            other, map_output, time_window))

    def temporal_coalesce(self, payload_merge_op=payload_first, epsilon=0):
        """
        Recursively merge all temporally overlapping or touching intervals,
            or intervals that are up to epsilon apart.
        """
        def update_output(output, intrvl):
            if len(output)==0:
                output.append(intrvl)
            else:
                merge_candidate = output[-1]
                if utils.T(utils.or_preds(
                    overlaps(),
                    before(max_dist=epsilon)))(merge_candidate, intrvl):
                    output[-1] = merge_candidate.merge(
                            intrvl, payload_merge_op)
                else:
                    output.append(intrvl)
            return output
        return IntervalSet3D(self.fold(update_output, []))

