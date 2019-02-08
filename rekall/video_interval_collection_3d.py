from rekall.interval_set_3d import Interval3D, IntervalSet3D
from types import MethodType
import multiprocessing as mp

# Functions that ran in worker processes
def _init_workers(context):
    global GLOBAL_CONTEXT
    GLOBAL_CONTEXT = context

def _worker_func_binary(video):
    func, map1, map2 = GLOBAL_CONTEXT
    return (video, func(map1[video], map2[video]))

def _worker_func_unary(video):
    func, map1 = GLOBAL_CONTEXT
    return (video, func(map1[video]))

class VideoIntervalCollection3D:
    """
    A VideoIntervalCollection3D is a wrapper around IntervalSet3D designed for
    videos. Logically, it contains a mapping from video ID's to IntervalSet3D.
    It exposes the same interface as an IntervalList.
    """
    UNARY_METHODS = ["filter_size", "map", "filter", "group_by",
            "map_payload", "dilate", "group_by_time", "temporal_coalesce"]
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

    @staticmethod
    def _remove_empty_intervalsets(video_map):
        new_map = {}
        for video, intervalset in video_map.items():
            if not intervalset.empty():
                new_map[video] = intervalset
        return new_map

    @staticmethod
    def _get_wrapped_unary_method(name):
        def method(self, *args, **kwargs):
            selfmap = self.get_allintervals()
            videos_to_process = selfmap.keys()
            def func(set1):
                return getattr(IntervalSet3D, name)(set1,*args,**kwargs)

            # Send func selfmap and othermap to the worker processes as
            # Global variables.
            with mp.Pool(initializer=_init_workers,
                    initargs=((func,selfmap),)) as pool:
                videos_results_list = pool.map(
                        _worker_func_unary, videos_to_process)
            return VideoIntervalCollection3D(
                    {video: results for video, results in videos_results_list
                        if not results.empty()})
        return method
    
    @staticmethod
    def _get_wrapped_binary_method(name):
        def method(self, other, *args, **kwargs):
            video_map = {}
            selfmap = self.get_allintervals()
            othermap = other.get_allintervals()
            videos_to_process = []
            for key in selfmap:
                if key in othermap:
                    videos_to_process.append(key)

            def func(set1, set2):
                return getattr(IntervalSet3D, name)(set1,set2,*args,**kwargs)

            # Send func selfmap and othermap to the worker processes as
            # Global variables.
            with mp.Pool(initializer=_init_workers,
                    initargs=((func,selfmap, othermap),)) as pool:
                videos_results_list = pool.map(
                        _worker_func_binary, videos_to_process)
            return VideoIntervalCollection3D(
                    {video: results for video, results in videos_results_list
                        if not results.empty()})
        return method

    @staticmethod
    def _get_wrapped_out_of_system_unary_method(name):
        def method(self, *args, **kwargs):
            selfmap = self.get_allintervals()
            videos_to_process = selfmap.keys()
            def func(set1):
                return getattr(IntervalSet3D, name)(set1,*args,**kwargs)

            # Send func selfmap and othermap to the worker processes as
            # Global variables.
            with mp.Pool(initializer=_init_workers,
                    initargs=((func,selfmap),)) as pool:
                videos_results_list = pool.map(
                        _worker_func_unary, videos_to_process)
            return {video: results for video, results in videos_results_list}
        return method

    def get_allintervals(self):
        return self._video_map

