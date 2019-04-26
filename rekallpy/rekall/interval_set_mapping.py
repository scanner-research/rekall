"""Abstraction for managing multiple different IntervalSets.

This module provides IntervalSetMapping, which is a wrapper around a mapping
from keys to IntervalSets. They keys can be anything that's hashable in Python.

The most common use case is to restrict IntervalSet operations to be from the
same domain; for example, suppose we use frame number in a video as the
temporal dimension of an Interval. Then we would want to differentiate
Intervals from different videos, since the temporal dimension represents
different domains. We could map from video path or video ID in some database to
the IntervalSet for that video.

The key need not be fixed. For example, if we have IntervalSets over
live broadcasts of news, we can use video metadata to recover the absolute UTC
time as the temporal dimension. Then it may make sense to re-group all the
Intervals into one IntervalSet, or use the TV channel name as the domain key
to re-group for downstream processing. IntervalSetMapping provides a convenient
mechanism for dynamic re-grouping.
"""
from collections.abc import MutableMapping
from operator import attrgetter
from types import MethodType
from tqdm import tqdm

from rekall.interval_set import IntervalSet
from rekall.helpers import perf_count

def _empty_set():
    return IntervalSet([])

class IntervalSetMapping(MutableMapping):
    """A wrapper around a dictionary from key to IntervalSet.

    It uses method reflection to expose all the same methods as IntervalSet,
    and delegates the method to the underlying IntervalSet of each domain in
    the collection. When calling binary methods such as ``join`` or ``minus``
    on two IntervalSetMapping's, it will match up IntervalSet's by key and
    perform the method on the underlying IntervalSet's for each domain.

    For each in-system method of IntervalSet (i.e. the return value is an
    IntervalSet), the corresponding method on IntervalSetMapping returns an
    IntervalSetMapping as well.

    For each out-of-system method on IntervalSet, namely ``size``, ``empty``,
    ``fold``, and ``match``, the corresponding method on IntervalSetMapping
    returns a dictionary from the key to the result of the method call on the
    underlying IntervalSet.

    IntervalSetMapping exposes Python's getter/setter paradigm as well, so
    individual IntervalSet's can be referenced using bracket notation and their
    key.

    The methods to wrap from IntervalSet are defined by the class constants:
    UNARY_METHODS, BINARY_METHODS and OUT_OF_SYSTEM_UNARY_METHODS.

    Example:
        Here are some examples of how IntervalSetMapping reflects IntervalSet's
        methods::
        
            ism1 = IntervalSetMapping({
                1: IntervalSet(...),
                2: IntervalSet(...),
                10: IntervalSet(...)
            })
            ism2 = IntervalSetMapping({
                1: IntervalSet(...),
                3: IntervalSet(...),
                10: IntervalSet(...)
            })

            # Unary methods
            ism1.map(mapper) == IntervalSetMapping({
                1: ism1[1].map(mapper),  # IntervalSet
                2: ism1[2].map(mapper),  # IntervalSet
                10: ism1[10].map(mapper) # IntervalSet
            })

            # Binary methods
            ism1.join(ism2, ...) == IntervalSetMapping({
                1: ism1[1].join(ism2[1], ...),   # IntervalSet
                10: ism1[10].join(ism2[10], ...) # IntervalSet
            })

            # Out of system unary methods:
            ism1.size() == {
                1: ism1[1].size(),   # Number
                2: ism1[2].size(),   # Number
                10: ism1[10].size()  # Number
            }

    Atrributes:
        UNARY_METHODS: List of methods that IntervalSetMapping reflects from
            IntervalSet and that will return a IntervalSetMapping where the
            IntervalSet under each group is transformed under the unary
            operation. See IntervalSet documentation for arguments and behavior
            for each method.
        BINARY_METHODS: List of methods that IntervalSetMapping reflects from
            IntervalSet and that will take a second IntervalSetMapping and 
            will return an IntervalSetMapping where the binary operation is
            performed on the two IntervalSets with the same key. See
            IntervalSet documentation for arguments and behavior for each
            method.
        OUT_OF_SYSTEM_UNARY_METHODS: List of methods that IntervalSetMapping
            reflects from IntervalSet and that will return a dictionary
            mapping from IntervalSet keys to return values of the methods.
    """
    UNARY_METHODS = ["filter_size", "map", "filter", "group_by", "fold_to_set",
            "map_payload", "dilate", "group_by_axis", "coalesce", "split"]
    BINARY_METHODS = ["merge", "union", "join", "minus", "filter_against",
            "collect_by_interval"]
    OUT_OF_SYSTEM_UNARY_METHODS = ["size", "empty", "fold", "match"]

    def __new__(cls, *args, **kwargs):
        """Creates class instance and adds IntervalSet methods on it."""
        instance = super(IntervalSetMapping, cls).__new__(cls)
        for method in cls.UNARY_METHODS:
            setattr(instance, method,
                MethodType(
                cls._get_wrapped_unary_method(method),
                instance))
        for method in cls.BINARY_METHODS:
            setattr(instance, method,
                MethodType(
                cls._get_wrapped_binary_method(method),
                instance))
        for method in cls.OUT_OF_SYSTEM_UNARY_METHODS:
            setattr(instance, method,
                MethodType(
                cls._get_wrapped_out_of_system_unary_method(method),
                instance))
        return instance

    def __init__(self, grouped_intervals):
        """Initializes with a dictionary from key to IntervalSet.

        Args:
            grouped_intervals: A dictionary from key to IntervalSet.
        """
        self._grouped_intervals = grouped_intervals

    def __repr__(self):
        return str(self._grouped_intervals)

    # Makes this class pickleable
    def __getstate__(self):
        return self._grouped_intervals
    def __setstate__(self, grouped_intervals):
        self._grouped_intervals = grouped_intervals

    # Dictionary/MutableMapping Interface
    def __getitem__(self, key):
        return self._grouped_intervals.get(key, _empty_set())
    def __setitem__(self, key, value):
        self._grouped_intervals[key] = value
    def __delitem__(self, key):
        del self._grouped_intervals[key]
    def __iter__(self):
        return self._grouped_intervals.__iter__()
    def __len__(self):
        return len(self._grouped_intervals)

    @classmethod
    def from_iterable(cls, iterable, key_parser, bounds_parser, 
            payload_parser=lambda _:None, progress=False, total=None):
        """Constructs an IntervalSetMapping from an iterable.

        Args:
            iterable: An iterable of arbitrary elements. Each element will
                become an interval in the collection.
            key_parser: A function that takes an element in iterable and
                returns the key for the interval.
            bounds_parser: A function that takes an element in iterable and
                returns the bounds for the interval.
            payload_parser (optional): A function that takes an element in
                iterable and returns the payload for the interval.
                Defaults to producing None for all elements.
            progress (Bool, optional): Whether to display a progress bar using
                tqdm. Defaults to False.
            total (int, optional): Total number of elements in iterable.
                Only used to estimate ETA for the progress bar, and only takes 
                effect if progress is True. 

        Returns:
            A IntervalSetMapping constructed from iterable and the parsers
            provided.

        Note:
            Everything in iterable will be materialized in RAM.
        """
        key_to_intervals = {}
        for row in (tqdm(iterable, total=total)
                if progress and total is not None else tqdm(iterable)
                if progress else iterable):
            interval = Interval(bounds_parser(row), p_parser(row))
            key = key_parser(row)
            if key in key_to_intervals:
                key_to_intervals[key].append(interval)
            else:
                key_to_intervals[key] = [interval]
        return cls({key: IntervalSet(intervals) for key, intervals in 
            key_to_intervals.items()})

    @classmethod
    def from_intervalset(cls, intervalset, key_fn):
        """Constructs an IntervalSetMapping from an IntervalSet by grouping
        by ``key_fn``.
        
        Args:
            intervalset (IntervalSet): An interval set containing all
                intervals to put in the mapping.
            key_fn: A function that takes an interval and returns the domain
                key.

        Returns:
            An IntervalSetMapping with the same intervals organized into
            domains by their key accroding to ``key_fn``.
        """
        def reducer(acc, interval):
            key = key_fn(interval)
            if key not in acc:
                acc[key] = [interval]
            else:
                acc[key].append(interval)
            return acc
        grouped = intervalset.fold(reducer, {})
        return cls({k:IntervalSet(v) for k,v in grouped.items()})

    def get_grouped_intervals(self):
        """Get dictionary from key to IntervalSet."""
        return self._grouped_intervals

    def get_flattened_intervalset(self):
        """Get an IntervalSet containing all intervals in all the
        IntervalSets."""
        output = []
        for intervals in self.get_grouped_intervals().values():
            output.extend(intervals.get_intervals())
        return IntervalSet(output)

    def add_key_to_payload(self):
        """Adds key to payload of each interval in each IntervalSet.

        If each interval in an IntervalSet with key K had payload P before, it
        now has the tuple ``(P, K)`` as payload.

        Returns:
            A new IntervalSetMapping with the transformed intervals.

        Note:
            The original IntervalSetMapping is unchanged. This is the
            same behavior as all unary methods of IntervalSet.
        """
        return IntervalSetMapping({
            k: intervalset.map_payload(lambda p:(p,k))
            for k, intervalset in self.get_grouped_intervals().items()})

    @staticmethod
    def _remove_empty_intervalsets(grouped_intervals):
        new_map = {}
        for key, intervalset in grouped_intervals.items():
            if not intervalset.empty():
                new_map[key] = intervalset
        return new_map

    @staticmethod
    def _get_wrapped_unary_method(name):
        def method(self, *args, profile=False, **kwargs):
            with perf_count(name, profile):
                selfmap = self.get_grouped_intervals()
                keys_to_process = selfmap.keys()

                def func(set1):
                    return getattr(IntervalSet, name)(set1,*args,**kwargs)

                results_map = {v:func(selfmap[v]) for v in keys_to_process}
            return IntervalSetMapping(
                    IntervalSetMapping._remove_empty_intervalsets(
                        results_map))
        return method

    @staticmethod
    def _get_wrapped_binary_method(name):
        def method(self, other, *args, profile=False, **kwargs):
            with perf_count(name, profile):
                selfmap = self.get_grouped_intervals()
                othermap = other.get_grouped_intervals()
                keys = set(selfmap.keys()).union(othermap.keys())

                def func(set1, set2):
                    return getattr(IntervalSet, name)(
                            set1,set2,*args,**kwargs)

                results_map = {v:
                        func(
                            selfmap.get(v, IntervalSet([])),
                            othermap.get(v, IntervalSet([]))) for v in keys}
            return IntervalSetMapping(
                    IntervalSetMapping._remove_empty_intervalsets(
                        results_map))
        return method

    @staticmethod
    def _get_wrapped_out_of_system_unary_method(name):
        def method(self, *args, profile=False, **kwargs):
            with perf_count(name, profile):
                selfmap = self.get_grouped_intervals()
                keys = selfmap.keys()

                def func(set1):
                    return getattr(IntervalSet, name)(set1,*args,**kwargs)
            return {v:func(selfmap[v]) for v in keys}
        return method

