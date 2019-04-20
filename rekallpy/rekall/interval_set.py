"""An IntervalSet is a set of Intervals with a number of core operations to
transform and combine sets of Intervals.
"""

from rekall.bounds import Bounds
from rekall.interval import Interval
from rekall.helpers import INFTY
from rekall.predicates import *
from functools import reduce
import constraint as constraint
import copy

class IntervalSet:
    """A set of Intervals.

    IntervalSet provides core operations to transform and combine sets of
    Intervals such as map, fold, join, union, minus, coalesce, etc.

    Note:
        This class does not obey the uniqueness semantics of a Set.

        When the number of intervals is larger than NUM_INTRVLS_THRESHOLD,
        binary operations `join`, `collect_by_interval` and `filter_against`
        will no longer operate on the full cross product of the intervals, and
        instead only look at pairs that are within a certain neighborhood of
        each other along the primary axis (the primary axis of the `first`
        Interval passed in during construction). This neighborhood defaults
        to DEFAULT_FRACTION of the overall range along that axis of the
        IntervalSet.
        
        Specifically, we compute ``DEFAULT_FRACTION`` of the
        overall range of that axis and set ``optimization_window`` to that
        fraction of the range. The optimized operations (``join``,
        ``collect_by_interval``, and ``filter_against``) only look at pairs
        whose distance from each other along the primary axis is less than
        ``optimization_window``.
    """
    NUM_INTRVLS_THRESHOLD = 1000
    DEFAULT_FRACTION = 1/100

    def __init__(self, intrvls):
        """Initializes IntervalSet with a list of Intervals.
    
        Args:
            intrvls: a list of Intervals to put in the set.
        """
        self._intrvls = sorted(list(intrvls))
        self._primary_axis = None
        if len(self._intrvls) > 0:
            self._primary_axis = self._intrvls[0]['bounds'].primary_axis()
            self._optimization_window = self._get_optimization_window()

    def __repr__(self):
        """String representation is a list of Intervals."""
        return str(self._intrvls)

    def __len__(self):
        """Get length."""
        return len(self._intrvls)

    # Compute the default optimization_window based on the current intervals
    def _get_optimization_window(self):
        n = len(self._intrvls)
        if n > 0:
            max_end = max([i[self._primary_axis[1]] for i in self._intrvls])
            min_start = min([i[self._primary_axis[0]] for i in self._intrvls])
            if n > IntervalSet.NUM_INTRVLS_THRESHOLD:
                return (max_end - min_start) * IntervalSet.DEFAULT_FRACTION
            else:
                return max_end - min_start
        else:
            return 0

    def get_intervals(self):
        """Returns a list of Intervals, ordered by their Bounds (which are
        sortable).
        """
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
            map_fn: A function that takes an Interval and returns an Interval.

        Returns:
            A new IntervalSet with the mapped intervals.
        """
        return IntervalSet([map_fn(intrvl) for intrvl in self._intrvls])

    def split(self, split_fn):
        """Splits each Interval into an IntervalSet, and returns the union of
        all the IntervalSets.

        Args:
            split_fn: A function that takes an Interval and returns an
            IntervalSet.

        Returns:
            A new IntervalSet with the union of all the IntervalSets generated
            by split_fn applied to each Interval.
        """
        lists = [split_fn(intrvl).get_intervals() for intrvl in self._intrvls]
        return IntervalSet([i for l in lists for i in l])

    def union(self, other):
        """Set union of two IntervalSets.

        Args:
            other: The other IntervalSet to union with.

        Returns:
            A new IntervalSet with all intervals in self and other.
        """
        return IntervalSet(self._intrvls + other._intrvls)

    def fold(self, reducer, init=None, sort_key=None):
        """Folds a reducer over an ordered list of intervals in the set.
        
        Args:
            reducer: A function that takes a previous state and an interval
                and returns an updated state.
            init (optional): The initial state to use in fold.
                Defaults to None, which means using the first interval as the
                initial state and run reduction from the second interval.
            sort_key (optional): A function that takes an Interval and
                returns a value as the sort key that defines the order of the
                list to fold over. If None, uses the ``primary_axis`` of the
                Bound of an Interval in the IntervalSet.

        Return:
            The final value of the state after folding all intervals in set.
        """
        lst = self.get_intervals()
        if sort_key is not None:
            lst = sorted(lst, key=sort_key)
        if init is None:
            return reduce(reducer, lst)
        else:
            # Avoid taking a reference of the argument
            init = copy.deepcopy(init)
            return reduce(reducer, lst, init)

    def fold_to_set(self, reducer, init=None, sort_key = None,
            acc_to_set = lambda acc:IntervalSet(acc)):
        """Fold over intervals in the set to produce a new IntervalSet.

        The same as `fold` method except it returns a IntervalSet by running
        ``acc_to_set`` on the final state of the fold, making it an in-system
        operation.

        Args:
            reducer: A function that takes a previous state and an interval
                and returns an updated state.
            init (optional): The initial state to use in fold.
                Defaults to None, which means using the first interval as the
                initial state and run reduction from the second interval.
            sort_key (optional): A function that takes an Interval and
                returns a value as the sort key that defines the order of the
                list to fold over. If None, uses the ``primary_axis`` of the
                Bound of an Interval in the IntervalSet.
            acc_to_set (optional): A function that takes the final state of the
                fold and returns an IntervalSet. Defaults to a function that
                takes in a list of Intervals and constructs an IntervalSet with
                that.
        Returns:
            A new IntervalSet that is the result of acc_to_set on the output
            of fold.
        """
        return acc_to_set(self.fold(reducer, init, sort_key))

    def _map_with_other_within_primary_axis_window(self, other,
            mapper, window=None):
        """Internal helper to deal with cross products limited to some window
        around a primary axis.

        Map over a list where elements are:
            (interval_in_self, [intervals_in_other])
        where intervals_in_other are those in other that are within window
        of interval_in_self along each's primary axis. 

        Args:
            other (IntervalSet): The other IntervalSet to do cross product with.
            mapper: A function that takes
                (interval_in_self, [intervals_in_other])
                and returns a list of arbitrary type.
            window (optional): Restrict interval pairs to those within
                window of each other along the primary axis.
                Defaults to None which means using the default optimization
                window associated with self. See class Documentation for more
                detail.

        Returns:
            A flattened list of mapper outputs.
        """
        if window is None:
            window = self._optimization_window

        self_pa = self._primary_axis
        other_pa = other._primary_axis
    
        # State is (other_start_index, outputs, done_flag)
        def update_state(state, intrvlself):
            start_index, outputs, done = state
            intervals_in_other = []
            if not done:
                new_start_index = None
                for idx, intrvlother in enumerate(
                        other.get_intervals()[start_index:]):
                    self_start = intrvlself[self_pa[0]]
                    self_end = intrvlself[self_pa[1]]
                    other_start = intrvlother[other_pa[0]]
                    other_end = intrvlother[other_pa[1]]
                    if self_start - window <= other_end:
                        if new_start_index is None:
                            new_start_index = idx + start_index
                        if other_start - window > other_end:
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

    def join(self, other, predicate, merge_op, window=None):
        """Cross-products two sets and combines pairs that pass the predicate.

        Named after the database JOIN operation, this method takes the cross-
        product of two IntervalSets, filters the resulting set of pairs and 
        forms a new IntervalSet by combining each pair into a new Interval.

        Note:
            ``join`` does not take the full cross-product but instead only
            consider pairs within window of each other. See notes in class
            documentation for more details.

        Args:
            other (IntervalSet): The interval set to join with.
            predicate: A function that takes two Intervals and returns a
                bool.
            merge_op: A function that takes two Intervals and returns a single
                new Interval.
            window (optional): Restrict interval pairs to those within
                window of each other along the primary axis.
                Defaults to None which means using the default optimization
                window associated with self. See class Documentation for more
                detail.

        Returns:
            A new IntervalSet from the intervals produced by merge_op on
            pairs that pass the predicate.
        """
        def map_output(intrvlself, intervals_in_other):
            out = []
            for intrvlother in intervals_in_other:
                if predicate(intrvlself, intrvlother):
                    new_intrvl = merge_op(intrvlself, intrvlother)
                    out.append(new_intrvl)
            return out
        return IntervalSet(self._map_with_other_within_primary_axis_window(
            other, map_output, window))

    def filter(self, predicate):
        """Filter the set and keep intervals that pass the predicate.

        Args:
            predicate: A function that takes an Interval and returns a bool.

        Returns:
            A new IntervalSet which is the filtered set.
        """
        return IntervalSet([
            intrvl.copy() for intrvl in self.get_intervals() if 
            predicate(intrvl)])

    def group_by(self, key, merge):
        """Group intervals by key and produces a new interval for each group.

        Args:
            key: A function that takes an Interval and returns a value as
                the key to group by.
            merge: A function that takes a group key and an IntervalSet of
                the group, and returns one Interval.

        Returns:
            A new IntervalSet with the results of merge on each group.
        """
        def add_to_group(group, i):
            k = key(i) 
            if k not in group:
                group[k] = [i]
            else:
                group[k].append(i)
            return group
        groups = self.fold(add_to_group, {})
        output = [merge(k, IntervalSet(intervals))
                for k, intervals in groups.items()]
        return IntervalSet(output)

    def minus(self, other, axis=None, window=None):
        """Subtract one IntervalSet from the other across some axis.

        Calculates the interval difference between intervals in self and
        intervals in other on some axis. The other dimensions are not changed.

        In particular, ``a.minus(b)`` will produce a new set of Intervals that
        maximally covers the Intervals in ``a`` without intersecting any of
        the intervals in ``b`` on ``axis``.

        The difference between two intervals can produce up to two new
        intervals.

        Suppose we try to subtract two sets of intervals from each other::

            # Suppose the following interval is in self
            |--------------------------------------------------------|

            # and that the following two intervals are in other
                      |--------------|     |----------------|
    
            # this function will produce three intervals
            |---------|              |-----|                |--------|

        If an interval in self overlaps no pairs in other along the temporal
        dimension, then the interval is reproduced in the output.

        Note:
            Although it is possible to compute difference on arbitrary volumes,
            it is unclear how to break the resulting set into bounding volumes.
            Therefore we currently only allow difference to be computed along
            a single axis.

        Args:
            other: The IntervalSet to subtract from ``self``.
            axis (optional): The axis to subtract on. Represented as a pair of
                co-ordinates, such as ``('t1', 't2')``. Defaults to ``None``,
                which uses the ``primary_axis`` of ``self``.

        Returns:
            A new IntervalSet with the results from the subtraction. It may
            contain more intervals than before the subtraction, but none of
            the intervals will overlap with the intervals in ``other`` along
            ``axis``.
        """
        if axis is None:
            axis = self._primary_axis
        def compute_difference(intrvl, overlapped_intervals):
            """Returns a list of intervals that are what is left of intrvl
            after subtracting all overlapped_intervals.
            
            Expects overlapped_intervals to be sorted by (axis[0], axis[1]).
            """
            start = intrvl[axis[0]]
            overlapped_index = 0
            output = []
            while start < intrvl[axis[1]]:
                # Each iteration proposes an interval starting at `start`
                # If no overlapped interval goes acoss `start`, then it is
                # a valid start for an interval after the subtraction.
                intervals_across_start = []
                first_interval_after_start = None
                new_overlapped_index = None
                for idx,overlap in enumerate(
                        overlapped_intervals[overlapped_index:]):
                    v1 = overlap[axis[0]]
                    v2 = overlap[axis[1]]
                    if new_overlapped_index is None and v2 > start:
                        new_overlapped_index = idx + overlapped_index
                    if v1 <= start and v2 > start:
                        intervals_across_start.append(overlap)
                    elif v1 > start:
                        # overlap is sorted by (axis[0], axis[1])
                        first_interval_after_start = overlap
                        break
                if len(intervals_across_start) == 0:
                    # start is valid, now finds an end point
                    if first_interval_after_start is None:
                        end = intrvl[axis[1]]
                        new_start = end
                    else:
                        end = first_interval_after_start[axis[0]]
                        new_start = first_interval_after_start[axis[1]]
                    if end > start:
                        new_bounds = intrvl['bounds'].copy()
                        new_bounds[axis[0]] = start
                        new_bounds[axis[1]] = end
                        output.append(Interval(new_bounds, intrvl['payload']))
                    start = new_start
                else:
                    # start is invalid, now propose another start
                    start = max([i[axis[1]] for i in intervals_across_start])
                if new_overlapped_index is not None:
                    overlapped_index = new_overlapped_index
            return output
        
        def map_output(intrvl, overlapped):
            # Take only nontrivial overlaps
            to_subtract = sorted(
                [i for i in overlapped if (i.size(axis) > 0 and
                    Bounds.cast({'t1': axis[0], 't2': axis[1]})(overlaps())
                    (intrvl, i)
                )],
                key = lambda i: (i[axis[0]], i[axis[1]]))
            if len(to_subtract) == 0:
                return [intrvl.copy()]
            else:
                return compute_difference(intrvl, to_subtract)

        return IntervalSet(self._map_with_other_within_primary_axis_window(
            other, map_output, window))

    def match(self, pattern, exact=False):
        """Pattern matching among the intervals in the set.

        A pattern is a list of constraints, where each constraint is a pair of
        ``(names, predicates)``.
        A name is a string that names the variable in the constraint.
        A predicate is a function that takes ``len(names)`` Intervals as
        arguments and returns ``True`` or ``False``.

        A solution is a directionary of assignments, where the key is the name
        of the variable defined in the pattern and the key is the interval in
        the set that gets assigned to that variable. The assignments in a
        solution will satisfy all constraints in the pattern.

        Example:
            Here's a simple example of matching a spatial pattern::

                pattern = [
                    (["harry"], [XY(height_at_least(0.5)), name_is("harry")]),
                    (["ron"], [XY(height_at_least(0.5)), name_is("ron")]),
                    (["harry","ron"], [XY(same_height()), XY(left_of())])
                ]

                # solutions are cases in intervalset where harry and ron are at
                # least half the screen height and have the same height and
                # harry is on the left of ron.
                solutions = intervalset.match(pattern)

                # solutions == [
                #   {"harry": Interval(...), "ron": Interval(...)},
                #   {"harry": Interval(...), "ron": Interval(...)},
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
        def parse_pattern(pattern):
            """
            Parser for pattern matching
            pattern: list of (names, constraints)
            Returns:
              A pair of:
                  Dict of (VariableName, [SingleVariablePredicate])
                  List of (VariableNames, Predicates)
            """
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

    def filter_against(self, other, predicate, window=None):
        """Filter intervals in self against intervals in other.

        Keep only intervals in `self` that satisfy the predicate with at least
        one interval in `other`.

        Note:
            We do not check the full cross-product of self and other and
            instead only check pairs that are within window of each other.
            See class documentation for more details.

        Args:
            other (IntervalSet): The other interval set to filter against.
            predicate: A function that takes one interval in self and an
                interval in other and returns a bool.
            window (optional): Restrict interval pairs to those within
                window of each other along the primary axis.
                Defaults to None which means using the default optimization
                window associated with self. See class Documentation for more
                detail.
        
        Returns:
            A new IntervalSet with intervals in self that satisify predicate
            with at least one interval in other.
        """
        def map_output(intrvlself, intrvlothers):
            for intrvlother in intrvlothers:
                if predicate(intrvlself, intrvlother):
                    return [intrvlself.copy()]
            return []
        return IntervalSet(self._map_with_other_within_primary_axis_window(
            other, map_output, window))

    def map_payload(self, fn):
        """Maps a function over payloads of all intervals in the set.

        Args:
            fn: A function that takes the payload of an interval and returns
                the new payload.

        Returns:
            A new IntervalSet of intervals with the same bounds but with
            transformed payloads.
        """
        def map_fn(intrvl):
            return Interval(intrvl['bounds'], fn(intrvl['payload']))
        return self.map(map_fn)

    def dilate(self, window, axis=None):
        """Expand the range of every interval in the set along some axis.

        Args:
            window (Number): The amount to extend at each end-point of the
                range. The actual interval will grow by 2*window. Use negative
                number to shrink intervals.
            axis (optional): The axis to dilate on. Represented as a pair of
                co-ordinates, such as ``('t1', 't2')``. Defaults to ``None``,
                which uses the ``primary_axis`` of ``self``.

        Returns:
            A new IntervalSet with the dilated intervals.
        """
        if axis is None:
            axis = self._primary_axis
        def dilate_bounds(b, window, axis):
            new_bounds = b.copy()
            new_bounds[axis[0]] -= window
            new_bounds[axis[1]] += window
            return new_bounds
        return self.map(lambda intrvl: Interval(
            dilate_bounds(intrvl['bounds'], window, axis),
            intrvl['payload']))

    def filter_size(self, min_size=0, max_size=INFTY, axis=None):
        """Filter the intervals by length of the bounds along some axis.

        Args:
            min_size (Number, optional): minimum size to pass the filter.
                Defaults to 0.
            max_size (Number, optional): maximum size to pass the filter.
                Defaults to `rekall.common.INFTY`.
            axis (optional): The axis to filter on. Represented as a pair of
                co-ordinates, such as ``('t1', 't2')``. Defaults to ``None``,
                which uses the ``primary_axis`` of ``self``.

        Returns:
            A new IntervalSet of intervals with bound length within
            [min_size, max_size] along the given axis.
        """
        if axis is None:
            axis = self._primary_axis
        return self.filter(lambda intrvl: intrvl.size(axis) >= min_size and(
            max_size==INFTY or intrvl.size(axis)<=max_size))

    def group_by_axis(self, axis, output_bounds):
        """Group intervals by a particular axis.

        For each group, create an Interval with bounds equal to
        ``output_bounds``, except for the co-ordinates in axis. These are set
        to the co-ordinate values for the group. The payload is an IntervalSet
        containing all the intervals in the group.

        Example::

            iset = IntervalSet([
                Interval(Bounds3D(0, 1, 0.5, 0.75, 0.5, 0.75)),
                Interval(Bounds3D(0, 1, 0.3, 0.75, 0.1, 0.3)),
                Interval(Bounds3D(0, 2, 0.1, 0.7, 0.1, 0.2)),
                Interval(Bounds3D(0, 2, 0.05, 0.25, 0.1, 0.12)),
                Interval(Bounds3D(1, 2, 0.6, 0.75, 0.1, 0.2))
            ])

            is_grouped = iset.group_by_axis(('t1', 't2'),
                Bounds3D(0, 1, 0, 1, 0, 1))

            # This is what is_grouped looks like
            is_grouped == IntervalSet([
                Interval(Bounds3D(0, 1, 0, 1, 0, 1), [
                    Interval(Bounds3D(0, 1, 0.5, 0.75, 0.5, 0.75)),
                    Interval(Bounds3D(0, 1, 0.3, 0.75, 0.1, 0.3)),
                ]),
                Interval(Bounds3D(0, 2, 0, 1, 0, 1), [
                    Interval(Bounds3D(0, 2, 0.1, 0.7, 0.1, 0.2)),
                    Interval(Bounds3D(0, 2, 0.05, 0.25, 0.1, 0.12))
                ]),
                Interval(Bounds3D(1, 2, 0, 1, 0, 1), [
                    Interval(Bounds3D(1, 2, 0.6, 0.75, 0.1, 0.2))
                ])
            ])

        Args:
            axis: The axis to group by. Represented as a pair of co-ordinates,
                such as ``('t1', 't2')``.
            output_bounds: Default output bounds for the Intervals in the
                output. The fields in ``axis`` are overwritten by the grouped
                values.

        Returns:
            A new IntervalSet of Intervals grouped by ``axis``, with the full
            IntervalSet of Intervals in the group in the payload.
        """
        def key_fn(intrvl):
            return (intrvl[axis[0]], intrvl[axis[1]])
        def merge_fn(key, intervals):
            new_bounds = output_bounds.copy()
            new_bounds[axis[0]] = key[0]
            new_bounds[axis[1]] = key[1]
            return Interval(new_bounds, intervals)
        return self.group_by(key_fn, merge_fn)

    def collect_by_interval(self, other, predicate,
            filter_empty=True, window=None):
        """Collect intervals in other and nest them under intervals in self.

        For each interval in self, its payload in the output IntervalSet will
        be all the intervals in other that satisfy the predicate.
        The payload of each resulting interval will become a pair
        ``(P, IS)`` where ``P`` is the original payload in self, and ``IS`` is
        an ``IntervalSet`` containing the nested intervals.

        Note:
            We do not check the full cross-product of self and other and
            instead only check pairs that are within ``window`` of each other
            along the primary axis. See class documentation for more details.

        Args:
            other (IntervalSet): Set of intervals to collect.
            predicate: A function that takes one interval in self and one in
                other and returns a bool.
            filter_empty (optional): Whether to remove intervals in self if no
                intervals in other satisfies the predicate with it.
                If ``False``, such intervals will have payload
                ``(P, IntervalSet([]))`` where ``P`` is the original payload.
                Defaults to ``True``.
            window (optional): Restrict interval pairs to those within
                window of each other along the primary axis.
                Defaults to None which means using the default optimization
                window associated with self. See class Documentation for more
                detail.
        
        Returns:
            A new IntervalSet of intervals in self but with nested
            interval set from other in the payload.
        """
        def map_output(intrvlself, intrvlothers):
            intrvls_to_nest = IntervalSet([
                i for i in intrvlothers if predicate(intrvlself, i)])
            if not intrvls_to_nest.empty() or not filter_empty:
                return [Interval(
                    intrvlself['bounds'].copy(),
                    (intrvlself['payload'], intrvls_to_nest))]
            return []
        return IntervalSet(self._map_with_other_within_primary_axis_window(
            other, map_output, window))

    def coalesce(self, axis, bounds_merge_op,
            payload_merge_op=lambda p1, p2: p1, epsilon=0):
        """Recursively merge all intervals that are touching or overlapping
        along ``axis``.

        Merge intervals in self if they meet, overlap, or are up to ``epsilon``
        apart along ``axis``. Repeat the process until all such intervals are
        merged.

        Merges the bounds with ``bounds_merge_op`` and merges payloads with
        ``payload_merge_op``.

        Args:
            axis: The axis to coalesce on.
            bounds_merge_op: A function that takes two bounds and returns a
                merged version of both of them. Along ``axis``, this function
                should return a bound that spans the two bounds.
            payload_merge_op (optional): A function that takes in two payloads
                and merges them. Defaults to a function that returns the first
                of the two payloads.
            epsilon (optional): The slack for judging if Intervals meet or
                overlap. Must be nonnegative. Defaults to 0 (no slack).

        Returns:
            A new IntervalSet of intervals that are disjoint along ``axis`` and
            are at least ``epsilon`` apart.
        """
        def update_output(output, intrvl):
            if len(output) == 0:
                output.append(intrvl)
            else:
                merge_candidate = output[-1]
                if Bounds.cast({ axis[0]: 't1', axis[1]: 't2' })(or_pred(
                    overlaps(),
                    before(max_dist=epsilon)))(merge_candidate, intrvl):
                    output[-1] = Interval(bounds_merge_op(
                        merge_candidate['bounds'], intrvl['bounds'])        
                    )
                else:
                    output.append(intrvl)
            return output
        return IntervalSet(self.fold(update_output, [],
            sort_key = lambda intrvl: (intrvl[axis[0]], intrvl[axis[1]])))
