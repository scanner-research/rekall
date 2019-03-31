""" Spatiotemporal 3D interval sets

This module provides:
    Interval3D: Class for a single 3D spatiotemporal volume.
    IntervalSet3D: Class for a set of spatiotemporal volumes.
"""
from rekall.interval_list import Interval
import rekall.interval_set_3d_utils as utils
from rekall.temporal_predicates import *
from rekall.logical_predicates import *
from rekall.merge_ops import * 
from functools import reduce
import constraint as constraint
import copy

class Interval3D:
    """A single 3D spatiotemporal volume

    The volume is defined by bounds on each of the three dimensions and a
    payload of arbitrary type.

    Attributes:
        t: Temporal bounds represented as a pair (start, end).
        x: Spatial bounds along x-axis represented as a pair.
        y: Spatial bounds along y-aixs represented as a pair.
        payload: A metadata field of arbitrary type.
    """
    def __init__(self, t, x=(0,1), y=(0,1), payload=None):
        """Initializes an interval with 3d bounds and payload.

        Args:
            t: Temporal bounds as (start, end) pair.
            x (optional): Spatial bounds along x-axis as a pair.
                Defaults to the full relative bounds from 0 to 1.
            y (optional): Spatial bounds along y-axis as a pair.
                Defaults to the full relative bounds from 0 to 1.
            payload (optional): Metadata of arbitrary type.
                Defaults to None.
        """
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
        """Returns a copy of the interval.
        
        Returns:
            An Interval3D that is a copy.

        Notes:
            While the 3D bounds are copied to the return value, the payload
            may only be passed by reference.
        """
        return Interval3D(self.t, self.x,
                self.y, self.payload)

    def combine(self, other, t_combiner, x_combiner, y_combiner,
                payload_merge_op = payload_first):
        """Combines two intervals into one.

        Args:
            other (Interval3D): The other interval to combine with.
            t_combiner: A function that takes two bounds and returns one.
                Used to combine temporal bounds.
            x_combiner: A function that takes two bounds and returns one.
                Used to combine spatial bounds on x-axis.
            y_combiner: A function that takes two bounds and returns one.
                Used to combine spatial bounds on y-axis.
            payload_merge_op: A function that takes the payload in self and
                the payload in other and returns the payload for the combined
                interval.

        Returns:
            A new Interval3D that is the combination of self and other.
        """
        payload = payload_merge_op(self.payload, other.payload)
        t = t_combiner(self.t, other.t)
        x = x_combiner(self.x, other.x)
        y = y_combiner(self.y, other.y)
        return Interval3D(t,x,y,payload)

    def merge(self, other, payload_merge_op=payload_first):
        """Merge two intervals into one by merging bounds in each dimension.

        Merging two bounds means producing a new bound that minimally covers
        both bounds. More formally, the new bound is: 
        (min(start1, start2), max(end1, end2)).
        
        Args:
            other (Interval3D): The other interval to merge with.
            payload_merge_op: A function that takes the payload in self and
                the payload in other and returns the payload for the combined
                interval.

        Returns:
            A new Interval3D that is the merge of self and other.
        """
        return self.combine(other,
                utils.merge_bound,
                utils.merge_bound,
                utils.merge_bound,
                payload_merge_op)

    def overlap_time_merge_space(self, other, payload_merge_op=payload_first):
        """Combines two intervals by overlapping in time and merging in space.
        
        Example:
            {t:(0,2), x:(0,1), y:(1,2)} and {t: (1,3), x:(1,2), y:(0,1)} will
            produce the spatiotemporal volume:
            {t:(1,2), x:(0,2), y:(0,2)}

        Args:
            other (Interval3D): The other interval to combine with.
            payload_merge_op: A function that takes the payload in self and
                the payload in other and returns the payload for the combined
                interval.
        
        Returns:
            A new Interval3D that is the temporal overlap and spatial merge
            of self and other.

        Raises:
            ValueError: If self and other do not overlap in time.
        """
        if utils.T(overlaps())(self, other):
            return self.combine(other,
                    utils.overlap_bound,
                    utils.merge_bound,
                    utils.merge_bound,
                    payload_merge_op)
        else:
            raise ValueError("{0} and {1} do not overlap in time".format(
                self, other))

    def expand_to_frame(self):
        """Expands the interval to the full spatial extent.

        Assuming the spatial bounds are relative bounds within (0,1), this
        operations expands the interval to have the spatial extent of the
        entire frame.

        Returns:
            A new Interval3D with (0,1) as both x- and y-spatial bounds.
        """
        return Interval3D(self.t, (0,1), (0,1), self.payload)

    def length(self):
        """Returns the duration of the interval."""
        return utils.bound_size(self.t)

    def width(self):
        """Returns the length along x-axis of the interval."""
        return utils.bound_size(self.x)

    def height(self):
        """Returns the length along y-axis of the interval."""
        return utils.bound_size(self.y)

    def to_json(self):
        """EXPERIMENTAL. DO NOT USE. Will be removed in the future.

        Returns a python representation of a JSON object
        To customize conversion for payload, add a to_json method
        to the payload.
        """
        return {
            "t": self.t,
            "x": self.x,
            "y": self.y,
            "payload": (self.payload.to_json() if 
                hasattr(self.payload,'to_json') else self.payload)
        }

class IntervalSet3D:
    """A set of Interval3Ds.

    IntervalSet3D provides core operations to transform and combine sets of
    Interval3D such as map, fold, join, union, minus, temporal_coalesce etc.

    Note:
        When the number of intervals is larger than NUM_INTRVLS_THRESHOLD,
        binary operations `join`, `collect_by_interval` and `filter_against`
        will no longer operate on the full cross product of the intervals, and
        instead only look at pairs that are within a temporal neightbourhood
        defined by a time_window that defaults to DEFAULT_FRACTION of the
        overall temporal span of the interval set.
        Each of these operations also has a time_window argument to override
        the default time_window.
    """
    def __init__(self, intrvls):
        """Initializes IntervalSet3D with a list of Interval3Ds.
        
        Args:
            intrvls: a list of Interval3Ds to put in the set.
        """
        self._intrvls = intrvls
        self._sort()
        self._time_window = self._get_time_window()

    def __repr__(self):
        return str(self._intrvls)

    def __len__(self):
        return len(self._intrvls)

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

    def get_intervals(self):
        """Returns a list of Interval3Ds ordered by (t1,t2,x1,x2,y1,y2)."""
        return self._intrvls

    def size(self):
        """Returns the number of intervals in the set."""
        return len(self)

    def empty(self):
        """Returns whether the set is empty."""
        return self.size() == 0

    def map(self, map_fn):
        """Maps a function over all intervals in the set.

        Args:
            map_fn: A function that takes an Interval3D and returns an
                Interval3D

        Returns:
            A new IntervalSet3D with the transformed intervals.
        """
        return IntervalSet3D([map_fn(intrvl) for intrvl in self._intrvls])

    def split(self, split_fn):
        """Splits each interval into a set of intervals.

        Args:
            split_fn: A function that takes an Interval3D and returns an
                IntervalSet3D.

        Returns:
            A new IntervalSet3D with all intervals after the split.
        """
        nested = [split_fn(intrvl).get_intervals() for intrvl in 
                    self._intrvls]
        return IntervalSet3D([i for intrvls in nested for i in intrvls])

    def union(self, other):
        """Set union of two IntervalSet3Ds.

        Args:
            other (IntervalSet3D): the other set to union with.

        Returns:
            A new IntervalSet3D with all intervals in self and other.
        """
        return IntervalSet3D(self._intrvls + other._intrvls)

    def fold(self, reducer, init=None, sort_key=utils.sort_key_time_x_y):
        """Folds a reducer over an ordered list of intervals in the set.
        
        Args:
            reducer: A function that takes a previous state and an interval
                and returns an updated state.
            init (optional): The initial state to use in fold.
                Defaults to None, which means using the first interval as the
                initial state and run reduction from the second interval.
            sort_key (optional): A function that takes an Interval3D and
                returns a value as the sort key that defines the order of the
                list to fold over.
                Default to ascending order by (t1,t2,x1,x2,y1,y2).
        Return:
            The final value of the state after folding all intervals in set.
        """
        lst = self.get_intervals()
        if sort_key != utils.sort_key_time_x_y:
            lst = sorted(lst, key=sort_key)
        if init is None:
            return reduce(reducer, lst)
        else:
            # Avoid taking a reference of the argument
            init = copy.deepcopy(init)
            return reduce(reducer, lst, init)

    def fold_to_set(self, reducer, init=None,
            acc_to_set = lambda acc:IntervalSet3D(acc),
            sort_key = utils.sort_key_time_x_y):
        """Fold over intervals in the set to produce a new IntervalSet3D

        The same as `fold` method except it returns a IntervalSet3D by running
        acc_to_set on the final state of the fold, making it an in-system
        operation.

        Args:
            reducer, init, sort_key: Same as in `fold`.
            acc_to_set (optional): A function that takes the final state of the
                fold and returns a IntervalSet3D.
                Defaults to assuming the state is a list of Interval3Ds and
                constructs a IntervalSet3D on that.
        Returns:
            A new IntervalSet3D that is the result of acc_to_set on the output
            of fold.
        """
        return acc_to_set(self.fold(reducer, init, sort_key))

    def _map_with_other_within_time_window(self, other,
            mapper, time_window=None):
        """Internal helper to deal with pairs of intervals in cross product.

        Map over a list where elements are:
            (interval_in_self, [intervals_in_other])
        where intervals_in_other are those in other that are within time_window
        of interval_in_self. 
        The list is ordered by (t1,t2) of interval_in_self

        Args:
            other (IntervalSet3D): The other IntervalSet3D to do cross product
                with.
            mapper: A function that takes
                (interval_in_self, [intervals_in_other])
                and returns a list of arbitrary type.
            time_window (optional): Restrict interval pairs to those within
                time_window of each other.
                Defaults to None which means using the default time_window
                associated withe self. See class Documentation for more detail.

        Returns:
            A flattend list of mapper outputs.
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
        """Cross-products two sets and combines pairs that pass the predicate.

        Named after the database JOIN operation, this method takes the cross-
        product of two IntervalSet3Ds, filter the resulting set of pairs and 
        form a new IntervalSet3D by combining each pair into a new Interval3D.

        Note:
            `join` does not take the full cross-product but instead only
            consider pairs within time_window of each other. See notes in class
            documentation for more details.

        Args:
            other (IntervalSet3D): The interval set to join with.
            predicate: A function that takes two Interval3Ds and returns a
                bool.
            merge_op: A function that takes two Interval3Ds and returns a list
                of new Interval3Ds.
            time_window (optional): Restrict cross-product to pairs within
                time_window of each other.
                Defaults to None which means using the default time_window
                associated withe self. See class Documentation for more detail.

        Returns:
            A new IntervalSet3D from the intervals produced by merge_op on
            pairs that pass the predicate.
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
        """Filter the set and keep intervals that pass the predicate.

        Args:
            predicate: A function that takes an Interval3D and returns a bool.

        Returns:
            A new IntervalSet3D which is the filtered set.
        """
        return IntervalSet3D([
            intrvl.copy() for intrvl in self.get_intervals() if 
            predicate(intrvl)])

    def group_by(self, key, merge):
        """Group intervals by key and produces a new interval for each group.

        Args:
            key: A function that takes an Interval3D and returns a value as
                the key to group by.
            merge: A function that takes a group key and an IntervalSet3D of
                the group, and returns one Interval3D.

        Returns:
            A new IntervalSet3D with the results of merge on each group.
        """
        def add_to_group(group, i):
            k = key(i) 
            if k not in group:
                group[k] = [i]
            else:
                group[k].append(i)
            return group
        groups = self.fold(add_to_group, {})
        output = [merge(k, IntervalSet3D(intervals))
            for k, intervals in groups.items()]
        return IntervalSet3D(output)

    def minus(self, other):
        """Set minus on two IntervalSet3Ds.

        Calculate the difference between intervals in self and the
        temporal ranges in other. The spatial dimensions of intervals in self
        are not changed.

        The difference between two intervals can produce up to two new
        intervals.

        For example, suppose the following interval is in self:

        |--------------------------------------------------------|

        and that the following two temporal intervals are in other:

                  |--------------|     |----------------|
        
        this function will produce three intervals:

        |---------|              |-----|                |--------|

        If an interval in self overlaps no pairs in other along the temporal
        dimension, then the interval is reproduced in the output.

        Note:
            Although it is possible to allow set-minus of arbitrary 3D volumes,
            it is unclear how to break the resulting set into bounding volumes.
            Therefore we currently only allow full-frame volumes in `other`,
            essentially only allowing 1D range subtraction on temporal
            dimension.

        Args:
            other (IntervalSet3D): A full-frame IntervalSet3D to subtract from.

        Returns:
            A new IntervalSet3D with the results from set-minus. It may contain
            more intervals than before the subtraction.

        Raises:
            NotImplementedError: If `other` contains intervals without
            full-frame spatial extent.
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
        """Pattern matching among the intervals in the set.

        A pattern is a list of constraints, where each constraint is a pair
          (names, predicates)
          A name is a string that names the variable in the constraint.
          A predicate is a function that takes len(names) Interval3Ds as
             arguments and returns True or False.

        A solution is a directionary of assignments, where the key is the name
        of the variable defined in the pattern and the key is the interval in
        the set that gets assigned to that variable. The assignments in a
        solution will satisfy all constraints in the pattern.

        Example:
            pattern = [
                (["harry"], [XY(height_at_least(0.5)), name_is("harry")]),
                (["ron"], [XY(height_at_least(0.5)), name_is("ron")]),
                (["harry","ron"], [XY(same_height()), XY(left_of())]),
            ]

            # solutions are cases in intervalset where harry and ron are at
            # least half the screen height and have the same height and harry
            # is on the left of ron.
            solutions = intervalset.match(pattern)

            # solutions == [
            #   {"harry": Interval3D(...), "ron": Interval3D(...)},
            #   {"harry": Interval3D(...), "ron": Interval3D(...)},
            # ]

        Args:
            pattern: A pattern specified by a list of constraints. See above
                for more details.
            exact (optional): Whether all intervals in the set need to have
                an assignment for it to be considered a solution.
                Defaults to False.

        Returns:
            A list of solutions as described above.
        """
        # Parser for pattern matching
        # pattern: list of (names, constraints)
        # Returns:
        #   A pair of:
        #       Dict of (VariableName, [SingleVariablePredicate])
        #       List of (VariableNames, Predicates)
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
        """Filter intervals in self against intervals in other.

        Keep only intervals in `self` that satisfy the predicate with at least
        one interval in `other`.

        Note:
            We do not check the full cross-product of self and other and
            instead only check pairs that are within time_window of each other.
            See class documentation for more details.

        Args:
            other (IntervalSet3D): The other interval set to filter against.
            predicate: A function that takes one interval in self and an
                interval in other and returns a bool.
            time_window (optional): Restrict cross-product to pairs within
                time_window of each other.
                Defaults to None which means using the default time_window
                associated withe self. See class Documentation for more detail.
        
        Returns:
            A new IntervalSet3D with intervals in self that satisify predicate
            with at least one interval in other.
        """
        def map_output(intrvlself, intrvlothers):
            for intrvlother in intrvlothers:
                if predicate(intrvlself, intrvlother):
                    return [intrvlself.copy()]
            return []
        return IntervalSet3D(self._map_with_other_within_time_window(
            other, map_output, time_window))

    def map_payload(self, fn):
        """Maps a function over payloads of all intervals in the set.

        Args:
            fn: A function that takes the payload of an interval and returns
                the new payload.

        Returns:
            A new IntervalSet3D of intervals with the same 3D bounds but with
            transformed payloads.
        """
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
        """Expand the range of every interval in the set along some axis.

        Args:
            window (Number): The amount to extend at each end-point of the
                range. The actual interval will grow by 2*window. Use negative
                number to shrink intervals.
            axis (string, optional): One of 'X', 'Y', 'T'.
                Defaults to 'T'.

        Returns:
            A new IntervalSet3D with the dilated intervals.

        Raises:
            ValueError: If `axis` has invalid value.
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
        """Filter the intervals by length of the bounds.

        Args:
            min_size (Number, optional): minimum size to pass the filter.
                Defaults to 0.
            max_size (Number, optional): maximum size to pass the filter.
                Defaults to `rekall.common.INFTY`.
            axis (string, optional): One of 'T','X' and 'Y'.
                Defaults to 'T'.

        Returns:
            A new IntervalSet3D of intervals with bound range length within
            [min_size, max_size] along the given axis.

        Raises:
            ValueError: If `axis` has invalid value.
        """
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
        """Merges pairs of intervals in self and other that satisfy predicate.

        See documentation on Interval3D.merge for more details on merge.

        Note:
            We do not check the full cross-product of self and other and
            instead only check pairs that are within time_window of each other.
            See class documentation for more details.

        Args:
            other (IntervalSet3D): The other set to merge intervals from.
            predicate: A function that takes one interval in self and one in
                other and returns a bool.
            payload_merge_op (optional): A function that takes a payload from
                self and one from other and returns a new payload.
                Defaults to using payload in self.
            time_window (optional): Restrict cross-product to pairs within
                time_window of each other.
                Defaults to None which means using the default time_window
                associated withe self. See class Documentation for more detail.

        Returns:
            A new IntervalSet3D with the merged intervals.
        """
        def merge_op(i1, i2):
            return [i1.merge(i2, payload_merge_op)]
        return self.join(other, predicate, merge_op, time_window)

    def group_by_time(self):
        """Group intervals by the time span.

        For each group, create a full-frame Interval3D with the same time span,
        and set the group (an IntervalSet3D) to be its payload.

        Returns:
            A new IntervalSet3D of full-frame intervals that have the set of
            intervals in self with the same time span as the payload.
        """
        def key_fn(intrvl):
            return intrvl.t
        def merge_fn(key, intervals):
            return Interval3D(key,
                    payload=intervals)
        return self.group_by(key_fn, merge_fn)

    def collect_by_interval(self, other, predicate,
            filter_empty=True,time_window=None):
        """Collect intervals in other and nest them under intervals in self.

        For each interval in self, nest under it all intervals in other
        that satisfy the predicate.
        The payload of each resulting interval will become a pair
        (T, IntervalSet3D) where T is the original payload in self, and 
        IntervalSet3D is the nested intervals.

        Note:
            We do not check the full cross-product of self and other and
            instead only check pairs that are within time_window of each other.
            See class documentation for more details.

        Args:
            other (IntervalSet3D): Set of intervals to collect.
            predicate: A function that takes one interval in self and one in
                other and returns a bool.
            filter_empty (optional): Whether to remove intervals in self if no
                intervals in other satisfies the predicate with it.
                If False, such intervals will have payload
                (T, IntervalSet3D([])) where T is the original payload.
                Defaults to True.
            time_window (optional): Restrict cross-product to pairs within
                time_window of each other.
                Defaults to None which means using the default time_window
                associated withe self. See class Documentation for more detail.
        
        Returns:
            A new IntervalSet3D of intervals in self but with nested
            interval set from other in the payload.
        """
        def map_output(intrvlself, intrvlothers):
            intrvls_to_nest = IntervalSet3D([
                i for i in intrvlothers if predicate(intrvlself, i)])
            if not intrvls_to_nest.empty() or not filter_empty:
                return [Interval3D(
                    intrvlself.t,
                    intrvlself.x,
                    intrvlself.y,
                    payload=(intrvlself.payload, intrvls_to_nest))]
            return []
        return IntervalSet3D(self._map_with_other_within_time_window(
            other, map_output, time_window))

    def temporal_coalesce(self, payload_merge_op=payload_first, epsilon=0):
        """Recursively merge all temporally touching or overlapping intervals.

        Merge intervals in set if their temporal projections meet, overlap, or
        are up to epsilon apart. Repeat the process until all such intervals
        are merged.

        See documentation on Interval3D.merge for more details on merge.

        Args:
            payload_merge_op (optional): A function that takes two payloads and
                returns a new payload.
                Defaults to selecting the first payload argument.
            epsilon (Number, optional): The slack for judging if intervals
                meet or overlap. Needs to be nonnegative.
                Defaults to 0 (no slack).

        Returns:
            A new IntervalSet3D of intervals that are temporally disjoint and
            are at least epsilon apart.
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

    def to_json(self):
        """EXPERIMENTAL. DO NOT USE. Will be removed in the future.

        Returns a python representation of a JSON object
        """
        return [i.to_json() for i in self._intrvls]
