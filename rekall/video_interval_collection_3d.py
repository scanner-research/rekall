"""DEPRECATED. Use rekall.domain_interval_collection instead."""

from rekall.interval_set_3d import Interval3D, IntervalSet3D
from rekall.video_interval_collection import VideoIntervalCollection as VIC
from rekall.interval_set_3d_utils import perf_count
from types import MethodType
import multiprocessing as mp
from tqdm import tqdm

# Functions that ran in worker processes
def _init_workers(context):
    global GLOBAL_CONTEXT
    GLOBAL_CONTEXT = context

def _worker_func_binary(video):
    func, map1, map2 = GLOBAL_CONTEXT
    it1 = map1.get(video, IntervalSet3D([]))
    it2 = map2.get(video, IntervalSet3D([]))
    return (video, func(it1, it2))

def _worker_func_unary(video):
    func, map1 = GLOBAL_CONTEXT
    return (video, func(map1[video]))

class VideoIntervalCollection3D:
    """DEPRECATED. Use DomainIntervalCollection instead.

    A VideoIntervalCollection3D is a wrapper around IntervalSet3D designed for
    videos. Logically, it contains a mapping from video ID's to IntervalSet3D.
    It exposes the same interface as an IntervalList.
    """
    UNARY_METHODS = ["filter_size", "map", "filter", "group_by", "fold_to_set",
            "map_payload", "dilate", "group_by_time", "temporal_coalesce",
            "split"]
    BINARY_METHODS = ["merge", "union", "join", "minus", "filter_against",
            "collect_by_interval"]
    OUT_OF_SYSTEM_UNARY_METHODS = ["size", "empty", "fold", "match"]

    def __new__(cls, *args, **kwargs):
        instance = super(VideoIntervalCollection3D, cls).__new__(cls)
        for method in VideoIntervalCollection3D.UNARY_METHODS:
            setattr(instance, method,
                MethodType(
                VideoIntervalCollection3D._get_wrapped_unary_method(method),
                instance))
        for method in VideoIntervalCollection3D.BINARY_METHODS:
            setattr(instance, method,
                MethodType(
                VideoIntervalCollection3D._get_wrapped_binary_method(method),
                instance))
        for method in VideoIntervalCollection3D.OUT_OF_SYSTEM_UNARY_METHODS:
            setattr(instance, method,
                MethodType(
                VideoIntervalCollection3D\
                        ._get_wrapped_out_of_system_unary_method(method),
                instance))
        return instance

    def __init__(self, video_id_to_intervalset):
        self._video_map = video_id_to_intervalset

    def __repr__(self):
        return "<VideoIntervalCollection3D videos={0}".format(
                self._video_map.keys())

    # Makes this class pickleable
    def __getstate__(self):
        return self._video_map
    def __setstate__(self, video_map):
        self._video_map = video_map


    @classmethod
    def from_iterable(cls, iterable, v_accessor, t_accessor,
            x_accessor=lambda _:(0,1), y_accessor=lambda _:(0,1),
            p_accessor=lambda _:None, progress=False, total=None):
        """
        Construct a VideoIntervalCollection from an iterable collection.

        @iterable is an iterable collection.
        @*_accessor are functions on the item in iterable that returns
            video_id, temporal bounds, spatial bounds, or payload
        """
        video_ids_to_intervals = {}
        for row in (tqdm(iterable, total=total)
                if progress and total is not None else tqdm(iterable)
                if progress else iterable):
            interval = Interval3D(t_accessor(row),
                    x_accessor(row), y_accessor(row), p_accessor(row))
            video_id = v_accessor(row)
            if video_id in video_ids_to_intervals:
                video_ids_to_intervals[video_id].append(interval)
            else:
                video_ids_to_intervals[video_id] = [interval]
        return cls({vid: IntervalSet3D(intervals) for vid, intervals in 
            video_ids_to_intervals.items()})

    @staticmethod
    def django_bbox_default_schema():
        return {
          "video_id": "video_id",
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
            "video_id": "video_id",
            "t1": "min_frame",
            "t2": "max_frame",
        }
        """
        schema_final = {
          "video_id": "video_id",
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
        v = get_accessor(s['video_id'])
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

    @staticmethod
    def _remove_empty_intervalsets(video_map):
        new_map = {}
        for video, intervalset in video_map.items():
            if not intervalset.empty():
                new_map[video] = intervalset
        return new_map

    def default_chunksize(self):
        tasks = len(self._video_map)
        return max(1, int(tasks/mp.cpu_count()))


    @staticmethod
    def _get_wrapped_unary_method(name):
        def method(self, *args, parallel=False, profile=False, **kwargs):
            with perf_count(name, profile):
                selfmap = self.get_allintervals()
                videos_to_process = selfmap.keys()
                def func(set1):
                    return getattr(IntervalSet3D, name)(set1,*args,**kwargs)

                if parallel:
                    # Send func selfmap and othermap to the worker processes as
                    # Global variables.
                    with mp.Pool(initializer=_init_workers,
                            initargs=((func,selfmap),)) as pool:
                        videos_results_list = pool.map(
                                _worker_func_unary, videos_to_process,
                                chunksize=self.default_chunksize())
                else:
                    videos_results_list = [
                            (v,func(selfmap[v])) for v in videos_to_process]
            return VideoIntervalCollection3D(
                        {video: results for video, results in 
                            videos_results_list if not results.empty()})
        return method
    
    @staticmethod
    def _get_wrapped_binary_method(name):
        def method(self, other, *args, parallel=False, profile=False, **kwargs):
            with perf_count(name, profile):
                video_map = {}
                selfmap = self.get_allintervals()
                othermap = other.get_allintervals()
                videos_to_process = set(selfmap.keys()).union(othermap.keys())

                def func(set1, set2):
                    return getattr(IntervalSet3D, name)(
                            set1,set2,*args,**kwargs)

                if parallel:
                # Send func selfmap and othermap to the worker processes as
                # Global variables.
                    with mp.Pool(initializer=_init_workers,
                        initargs=((func,selfmap, othermap),)) as pool:
                        videos_results_list = pool.map(
                            _worker_func_binary, videos_to_process,
                            chunksize=self.default_chunksize())
                else:
                    videos_results_list = [
                            (v, func(selfmap.get(v, IntervalSet3D([])),
                                othermap.get(v, IntervalSet3D([])))) for v
                            in videos_to_process]
            return VideoIntervalCollection3D(
                    {video: results for video, results in videos_results_list
                        if not results.empty()})
        return method

    @staticmethod
    def _get_wrapped_out_of_system_unary_method(name):
        def method(self, *args, parallel=False, profile=False, **kwargs):
            with perf_count(name, profile):
                selfmap = self.get_allintervals()
                videos_to_process = selfmap.keys()
                def func(set1):
                    return getattr(IntervalSet3D, name)(set1,*args,**kwargs)

                if parallel:
                    # Send func selfmap and othermap to the worker processes as
                    # Global variables.
                    with mp.Pool(initializer=_init_workers,
                            initargs=((func,selfmap),)) as pool:
                        videos_results_list = pool.map(
                                _worker_func_unary, videos_to_process,
                                chunksize=self.default_chunksize())
                else:
                    videos_results_list = [
                            (v,func(selfmap[v])) for v in videos_to_process]
            return {video: results for video, results in videos_results_list}
        return method

    def get_allintervals(self):
        return self._video_map

