from rekall.video_interval_collection_3d import VideoIntervalCollection3D
from rekall.interval_set_3d import Interval3D, IntervalSet3D
from rekall.temporal_predicates import overlaps
from rekall.merge_ops import payload_first
import rekall.interval_set_3d_utils as utils
import unittest

from pstats import Stats
import cProfile

from rekall.video_interval_collection import VideoIntervalCollection
from rekall.interval_list import Interval, IntervalList

def overlaps_3d():
    return lambda i1, i2: ((i1.t[0] < i2.t[0] and i1.t[1] > i2.t[0]) or
            (i1.t[0] < i2.t[1] and i1.t[1] > i2.t[1]) or
            (i1.t[0] <= i2.t[0] and i1.t[1] >= i2.t[1]) or
            (i1.t[0] >= i2.t[0] and i1.t[1] <= i2.t[1]))

class TestVideoIntervalCollection3D(unittest.TestCase):
    def setUpProfiler(self):
        self.pr = cProfile.Profile()
        self.pr.enable()

    def tearDownProfiler(self):
        p = Stats(self.pr)
        p.strip_dirs()
        p.sort_stats('cumtime')
        print("Stats for TestVideoIntervalCollection3D "+self._testMethodName)
        p.print_stats()

    @staticmethod
    def get_collection():
        NUM_INTERVALS=1500
        NUM_VIDEOS=100
        def get_intervals_for_video(vid):
            intervals = [
                Interval3D((t, t+1),(0,0.5),(0.5,1),vid)
                for t in range(vid, vid+NUM_INTERVALS)]
            return IntervalSet3D(intervals)
        return VideoIntervalCollection3D(
                {vid: get_intervals_for_video(vid) for vid in range(
                    NUM_VIDEOS)})

    def assertCollectionEq(self, c1,c2):
        map1 = c1.get_allintervals()
        map2 = c2.get_allintervals()
        self.assertEqual(map1.keys(), map2.keys())
        for key in map1:
            is1 = map1[key]
            is2 = map2[key]
            list1 = [(i.t, i.x, i.y, i.payload) for i in is1.get_intervals()]
            list2 = [(i.t, i.x, i.y, i.payload) for i in is2.get_intervals()]
            self.assertEqual(list1, list2)

    def test_self_merge_with_dedicated_predicate(self):
        self.setUpProfiler()
        c = TestVideoIntervalCollection3D.get_collection()
        c2 = c.merge(c, overlaps_3d(),
                payload_first, time_window=10)
        self.assertCollectionEq(c2, c)
        self.tearDownProfiler()

    def test_self_merge(self):
        self.setUpProfiler()
        c = TestVideoIntervalCollection3D.get_collection()
        c2 = c.merge(c, utils.T(overlaps()),
                payload_first, time_window=10)
        self.assertCollectionEq(c2, c)
        self.tearDownProfiler()

    def test_filter_size(self):
        c = TestVideoIntervalCollection3D.get_collection()
        c2 = c.filter_size(min_size=1,max_size=2)
        self.assertCollectionEq(c2, c)

    def test_fold(self):
        c = TestVideoIntervalCollection3D.get_collection()
        vids = c.get_allintervals().keys()
        d = c.fold(lambda m, i: max(m, i.payload), -1)
        self.assertEqual(d, {v: v for v in vids})


class TestVideoIntervalCollection(unittest.TestCase):
    def setUpProfiler(self):
        self.pr = cProfile.Profile()
        self.pr.enable()

    def tearDownProfiler(self):
        p = Stats(self.pr)
        p.strip_dirs()
        p.sort_stats('cumtime')
        print("Stats for TestVideoIntervalCollection "+self._testMethodName)
        p.print_stats()

    @staticmethod
    def get_collection():
        NUM_INTERVALS=1500
        NUM_VIDEOS=100
        def get_intervals_for_video(vid):
            intervals = [
                Interval(t, t+1,vid)
                for t in range(vid, vid+NUM_INTERVALS)]
            return intervals
        return VideoIntervalCollection(
                {vid: get_intervals_for_video(vid) for vid in range(
                    NUM_VIDEOS)})

    def assertCollectionEq(self, c1,c2):
        map1 = c1.get_allintervals()
        map2 = c2.get_allintervals()
        self.assertEqual(map1.keys(), map2.keys())
        for key in map1:
            is1 = map1[key]
            is2 = map2[key]
            list1 = [(i.start, i.end, i.payload) for i in is1.intrvls]
            list2 = [(i.start, i.end, i.payload) for i in is2.intrvls]
            self.assertEqual(list1, list2)

    def test_self_merge(self):
        self.setUpProfiler()
        c = TestVideoIntervalCollection.get_collection()
        c2 = c.merge(c, overlaps(), payload_first, working_window=10)
        self.assertCollectionEq(c2, c)
        self.tearDownProfiler()
