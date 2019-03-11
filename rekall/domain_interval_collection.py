from collections.abc import MutableMapping
from types import MethodType
from tqdm import tqdm

from rekall.interval_set_3d import IntervalSet3D, Interval3D
from rekall.interval_set_3d_utils import perf_count
from rekall.video_interval_collection import VideoIntervalCollection as VIC

def _empty_set():
    return IntervalSet3D([])

class DomainIntervalCollection(MutableMapping):
    """
    A DomainIntervalCollection is a grouping of Interval3Ds by some key.
    It is logically a dictionary from key to an IntervalSet3D.
    It exposes the same interface as an IntervaSet3D.
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
        """
        Sets all methods on the instance.
        """
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
        """
        grouped_intervals is a dictionary from key to IntervalSet3D
        """
        self._grouped_intervals = grouped_intervals

    def __repr__(self):
        return "<DomainIntervalCollection domains={0}".format(
                self._grouped_intervals.keys())

    # Makes this class pickleable
    def __getstate__(self):
        return self._grouped_intervals
    def __setstate__(self, grouped_intervals):
        self._grouped_intervals = grouped_intervals

    # Dictionary Interface
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
        """
        Construct a DomainIntervalCollection from an iterable.

        @*_accessor are functions on the item in iterable that returns
            domain_key, temporal bounds, spatial bounds, or payload
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

    @staticmethod
    def django_bbox_default_schema():
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
    def from_django_qs(cls, qs, schema={},
            with_payload=None, progress=False, total=None):
        """
        Constructor for a Django queryset.
        By default, the schema is
        {
            "domain": "video_id",
            "t1": "min_frame",
            "t2": "max_frame",
        }
        """
        schema_final = {
          "domain": "video_id",
          "t1": "min_frame",
          "t2": "max_frame",
        }
        schema_final.update(schema)
        def get_accessor(field):
            return lambda row: VIC.django_accessor(row, field)
        def get_bounds_accessor(field1, field2):
            return lambda row: (VIC.django_accessor(row, field1),
                                VIC.django_accessor(row, field2))
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
        return cls.from_iterable(qs, v, t,
                progress=progress, total=total, **kwargs)

    @classmethod
    def from_intervalset(cls, intervalset, key_fn):
        """
        Group intervals in `intervalset` by the domain given by `key_fn`
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
        """
        Returns the underlying dictionary from domain key to IntervalSet3D
        """
        return self._grouped_intervals

    def get_flattened_intervalset(self):
        """
        Returns a single IntervalSet3D that has all the intervals in the
        collection
        """
        output = []
        for intervals in self.get_grouped_intervals().values():
            output.extend(intervals.get_intervals())
        return IntervalSet3D(output)

    def add_domain_to_payload(self):
        """
        Map payload P to tuple (P, DomainKey) for all intervals in collection
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

