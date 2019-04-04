"""Managing intervals from different domains.

This module provides DomainIntervalCollection, a dictionary-like class to aid
managing intervals from different domains. We often want to restrict operations
to intervals from the same domain: for example, suppose we use frame number as
the temporal dimension of the interval, it would make little sense to check
temporal overlap between intervals from different videos. In this case,
video_id is the domain of the intervals, and operations on a set of intervals
such as `join` or `minus` should be performed on a per-video basis.

The domain need not be fixed. For example, if the videos are live-broadcasts
and we can use video metadata to recover the absolute UTC time to be the
temporal dimension, it may make sense to re-group the intervals into one
interval set, or use TV channel name as the domain key to re-group them for
downstream processing. DomainIntervalCollection also provides convenient
mechanism for such dynamic re-grouping.

Example Usage:
    collection_A = DomainIntervalCollection.from_django_qs(...)
    collection_B = DomainIntervalCollection(...)
    collection_C = collection_A\\
                   .join(collection_B, ...)\\
                   .map(...)\\
                   .filter(...)\\
                   .union(collection_B.minus(collection_A))
"""
from collections.abc import MutableMapping
from types import MethodType
from tqdm import tqdm

from rekall.interval_set_3d import IntervalSet3D, Interval3D
from rekall.interval_set_3d_utils import perf_count
from rekall.video_interval_collection import VideoIntervalCollection as VIC

def _empty_set():
    return IntervalSet3D([])

class DomainIntervalCollection(MutableMapping):
    """A dictionary from domain key to IntervalSet3D.

    It exposes all the same methods as IntervalSet3D, and delegates the method
    to the underlying IntervalSet3D of each domain in the collection.
    In particular, when calling binary methods such as `join` or `minus` on
    two DomainIntervalCollections, it will perform the method on the underlying
    IntervalSet3Ds separately for each domain.

    For each in-system method of IntervalSet3D (i.e. return value is
    still IntervalSet3D), the corresponding method on DomainIntervalCollection
    returns a DomainIntervalCollection as well.

    For each out-of-system method of IntervalSet3D, namely `size`, `empty`,
    `fold` and `match`, the corresponding method on DomainIntervalCollection
    returns a dictionary from domain key to the result from the underlying
    IntervalSet3D.

    The methods to wrap from IntervalSet3D are defined by the class constants:
    UNARY_METHODS, BINARY_METHODS and OUT_OF_SYSTEM_UNARY_METHODS
    """
    # These methods will return a DomainIntervalCollection where the
    # IntervalSet3D under each group is transformed under the unary operation.
    UNARY_METHODS = ["filter_size", "map", "filter", "group_by", "fold_to_set",
            "map_payload", "dilate", "group_by_time", "temporal_coalesce",
            "split"]
    # These methods takes a second DomainIntervalCollection and 
    # will return a DomainIntervalCollection where the
    # binary operation is performed on the two IntervalSet3Ds with the same key
    BINARY_METHODS = ["merge", "union", "join", "minus", "filter_against",
            "collect_by_interval"]
    # These methods will return a dictionary from domain key to results for 
    # that group.
    OUT_OF_SYSTEM_UNARY_METHODS = ["size", "empty", "fold", "match"]

    def __new__(cls, *args, **kwargs):
        """Creates class instance and adds IntervalSet3D methods on it."""
        instance = super(DomainIntervalCollection, cls).__new__(cls)
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
        """Initializes with a dictionary from domain key to IntervalSet3D"""
        self._grouped_intervals = grouped_intervals

    def __repr__(self):
        return "<DomainIntervalCollection domains={0}".format(
                self._grouped_intervals.keys())

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
    def from_iterable(cls, iterable, v_accessor, t_accessor,
            x_accessor=lambda _:(0,1), y_accessor=lambda _:(0,1),
            p_accessor=lambda _:None, progress=False, total=None):
        """Constructs a DomainIntervalCollection from an iterable.

        Args:
            iterable: An iterable of arbitrary elements. Each element will
                become an interval in the collection.
            v_accessor: A function that takes an element in iterable and
                returns the domain key for the interval.
            t_accessor: A function that takes an element in iterable and
                returns the temporal bounds for the interval.
            x_accessor (optional): A function that takes an element in iterable
                and returns the x-axis spatial bounds for the interval.
                Defaults to the full relative bounds of [0,1].
            y_accessor (optional): A function that takes an element in iterable
                and returns the y-axis spatial bounds for the interval.
                Defaults to the full relative bounds of [0,1].
            p_accessor (optional): A function that takes an element in iterable
                and returns the payload for the interval.
                Defaults to producing None for all elements.
            progress (Bool, optional): Whether to display a progress bar.
                Defaults to False.
            total (int, optional): Total number of elements in iterable.
                Only used to estimate ETA for the progress bar, and only takes 
                effect if progress is True. 

        Returns:
            A DomainIntervalCollection constructed from iterable and the
            accessors provided.

        Note:
            Everything in iterable will be materialized in RAM.
        """
        key_to_intervals = {}
        for row in (tqdm(iterable, total=total)
                if progress and total is not None else tqdm(iterable)
                if progress else iterable):
            interval = Interval3D(t_accessor(row),
                    x_accessor(row), y_accessor(row), p_accessor(row))
            key = v_accessor(row)
            if key in key_to_intervals:
                key_to_intervals[key].append(interval)
            else:
                key_to_intervals[key] = [interval]
        return cls({key: IntervalSet3D(intervals) for key, intervals in 
            key_to_intervals.items()})

    @classmethod
    def from_django_qs(cls, qs, schema={}, with_payload=None, progress=False):
        """Constructs a DomainIntervalCollection from django QuerySet

        Args:
            qs: A django QuerySet where each record will become an interval
            schema (optional): A dictionary that defines how to transform each
                record to an interval.
                The keys to use are:
                    'domain', 't1', 't2', 'x1', 'x2', 'y1', 'y2', 'payload'
                The value should be a string that is name of the field
                on the django record for the respective attribute of the
                interval.
                If set, it will override per-key the default schema:
                {
                    "domain": "video_id",
                    "t1": "min_frame",
                    "t2": "max_frame"
                }
            with_payload (optional): A function that takes a django record and
                returns the payload for the interval.
                If set, will override the 'payload' field of the schema.
            progress (Bool, optional): whether to display a progress bar.
                Defaults to False.

        Returns:
            A DomainIntervalCollection constructed from django QuerySet.

        Note:
            All records in the QuerySet are materialized in the function call.
        """
        schema_final = {
          "domain": "video_id",
          "t1": "min_frame",
          "t2": "max_frame",
        }
        schema_final.update(schema)
        def get_accessor(field):
            return lambda row: cls.django_accessor(row, field)
        def get_bounds_accessor(field1, field2):
            return lambda row: (cls.django_accessor(row, field1),
                                cls.django_accessor(row, field2))
        s = schema_final
        v = get_accessor(s['domain'])
        t = get_bounds_accessor(s['t1'], s['t2'])
        kwargs = {}
        if 'x1' in s and 'x2' in s:
            kwargs['x_accessor'] = get_bounds_accessor(s['x1'],s['x2'])
        if 'y1' in s and 'y2' in s:
            kwargs['y_accessor'] = get_bounds_accessor(s['y1'],s['y2'])
        if with_payload is not None:
            kwargs['p_accessor'] = with_payload
        elif 'payload' in s:
            kwargs['p_accessor'] = get_accessor(s['payload'])
        total = None
        if progress:
            total = qs.count()
        return cls.from_iterable(qs, v, t,
                progress=progress, total=total, **kwargs)

    @staticmethod
    def django_accessor(row, field):
        fields = field.split('.')
        output = row
        for field in fields:
            output = VIC.django_accessor(output, field)
        return output

    @staticmethod
    def django_bbox_default_schema():
        """A default schema for a bounding box record in django database."""
        return {
          "domain": "video_id",
          "t1": "min_frame",
          "t2": "max_frame",
          "x1": "bbox_x1",
          "x2": "bbox_x2",
          "y1": "bbox_y1",
          "y2": "bbox_y2",
          "payload": "id"
        }


    @classmethod
    def from_intervalset(cls, intervalset, key_fn):
        """Constructs a DomainIntervalCollection from an IntervalSet3D.
        
        Args:
            intervalset (IntervalSet3D): An interval set containing all
                intervals to put in the collection.
            key_fn: A function that takes an interval and returns the domain
                key.

        Returns:
            A DomainIntervalCollection with the same intervals organized into
            domains.
        """
        def reducer(acc, interval):
            key = key_fn(interval)
            if key not in acc:
                acc[key] = [interval]
            else:
                acc[key].append(interval)
            return acc
        grouped = intervalset.fold(reducer, {})
        return cls({k:IntervalSet3D(v) for k,v in grouped.items()})

    def get_grouped_intervals(self):
        """Get underlying dictionary from domain key to IntervalSet3D."""
        return self._grouped_intervals

    def get_flattened_intervalset(self):
        """Get an IntervalSet3D containing all intervals in collection."""
        output = []
        for intervals in self.get_grouped_intervals().values():
            output.extend(intervals.get_intervals())
        return IntervalSet3D(output)

    def add_domain_to_payload(self):
        """Adds domain key to payload of each interval in collection

        If each interval in domain K had payload P before, it now has the tuple
            (P, K) as payload.

        Returns:
            A new DomainIntervalCollection with the transformed intervals.

        Note:
            The original DomainIntervalCollection is unchanged. This is the
            same behavior as all unary methods of IntervalSet3D.
        """
        return DomainIntervalCollection({
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
                    return getattr(IntervalSet3D, name)(set1,*args,**kwargs)

                results_map = {v:func(selfmap[v]) for v in keys_to_process}
            return DomainIntervalCollection(
                    DomainIntervalCollection._remove_empty_intervalsets(
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
                    return getattr(IntervalSet3D, name)(
                            set1,set2,*args,**kwargs)

                results_map = {v:
                        func(
                            selfmap.get(v, IntervalSet3D([])),
                            othermap.get(v, IntervalSet3D([]))) for v in keys}
            return DomainIntervalCollection(
                    DomainIntervalCollection._remove_empty_intervalsets(
                        results_map))
        return method

    @staticmethod
    def _get_wrapped_out_of_system_unary_method(name):
        def method(self, *args, profile=False, **kwargs):
            with perf_count(name, profile):
                selfmap = self.get_grouped_intervals()
                keys = selfmap.keys()

                def func(set1):
                    return getattr(IntervalSet3D, name)(set1,*args,**kwargs)
            return {v:func(selfmap[v]) for v in keys}
        return method

