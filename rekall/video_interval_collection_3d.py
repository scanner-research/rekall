from rekall.interval_set_3d import Interval3D, IntervalSet3D
from types import MethodType

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
            video_map = {}
            for key, intervalset in self._video_map.items():
                video_map[key] = getattr(intervalset, name)(*args, **kwargs)
            return VideoIntervalCollection3D(
                    VideoIntervalCollection3D._remove_empty_intervalsets(
                        video_map))
        return method
    
    @staticmethod
    def _get_wrapped_binary_method(name):
        def method(self, other, *args, **kwargs):
            video_map = {}
            selfmap = self.get_allintervals()
            othermap = other.get_allintervals()
            for key, intervalset in selfmap.items():
                otherset = othermap.get(key, None)
                if otherset is not None:
                    video_map[key] = getattr(intervalset, name)(
                        otherset, *args, **kwargs)
            return VideoIntervalCollection3D(
                    VideoIntervalCollection3D._remove_empty_intervalsets(
                        video_map)) 
        return method

    @staticmethod
    def _get_wrapped_out_of_system_unary_method(name):
        def method(self, *args, **kwargs):
            video_map = {}
            selfmap = self.get_allintervals()
            for key, intervalset in selfmap.items():
                video_map[key] = getattr(intervalset, name)(
                        *args, **kwargs)
            return video_map
        return method

    def get_allintervals(self):
        return self._video_map

