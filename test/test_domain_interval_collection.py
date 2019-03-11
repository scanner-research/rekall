from rekall.domain_interval_collection import DomainIntervalCollection
from rekall.interval_set_3d import Interval3D, IntervalSet3D
from rekall.temporal_predicates import overlaps
from rekall.merge_ops import payload_first
import rekall.interval_set_3d_utils as utils
import unittest

from pstats import Stats
import cProfile

from rekall.video_interval_collection import VideoIntervalCollection
from rekall.interval_list import Interval, IntervalList

class TestDomainIntervalCollection(unittest.TestCase):
    @staticmethod
    def get_collection():
        NUM_INTERVALS=150
        NUM_VIDEOS=100
        def get_intervals_for_video(vid):
            intervals = [
                Interval3D((t, t+1),(0,0.5),(0.5,1),vid)
                for t in range(vid, vid+NUM_INTERVALS)]
            return IntervalSet3D(intervals)
        return DomainIntervalCollection(
                {vid: get_intervals_for_video(vid) for vid in range(
                    NUM_VIDEOS)})

    def assertCollectionEq(self, c1,c2):
        self.assertEqual(c1.keys(), c2.keys())
        for key in c1:
            is1 = c1[key]
            is2 = c2[key]
            list1 = [(i.t, i.x, i.y, i.payload) for i in is1.get_intervals()]
            list2 = [(i.t, i.x, i.y, i.payload) for i in is2.get_intervals()]
            self.assertEqual(list1, list2)

    def test_self_merge(self):
        c = TestDomainIntervalCollection.get_collection()
        c2 = c.merge(c, utils.T(overlaps()),
                payload_first, time_window=10)
        self.assertCollectionEq(c2, c)

    def test_filter_size(self):
        c = TestDomainIntervalCollection.get_collection()
        c2 = c.filter_size(min_size=1,max_size=2)
        self.assertCollectionEq(c2, c)

    def test_fold(self):
        c = TestDomainIntervalCollection.get_collection()
        vids = c.keys()
        d = c.fold(lambda m, i: max(m, i.payload), -1)
        self.assertEqual(d, {v: v for v in vids})

    def test_fold_to_set(self):
        c = TestDomainIntervalCollection.get_collection()
        d = c.fold_to_set(lambda acc, i: acc + [i], [])
        self.assertCollectionEq(c, d)

    def test_fold_modify_accumulator_in_place(self):
        def update(acc, i):
            acc.append(i)
            return acc
        c = TestDomainIntervalCollection.get_collection()
        d = c.fold_to_set(update, [])
        self.assertCollectionEq(c, d)

    def test_union(self):
        c= TestDomainIntervalCollection.get_collection()
        c1 = DomainIntervalCollection({v: c[v] for v in c if v % 2 ==0})
        c2 = DomainIntervalCollection({v: c[v] for v in c if v % 2 ==1})
        c3 = c1.union(c2)
        self.assertCollectionEq(c3, c)

    def test_minus(self):
        c= TestDomainIntervalCollection.get_collection()
        c1 = DomainIntervalCollection({v: c[v] for v in c if v % 2 ==0})
        c2 = DomainIntervalCollection({v: c[v] for v in c if v % 2 ==1})
        c3 = c.minus(c1.map(Interval3D.expand_to_frame))
        self.assertCollectionEq(c3,c2)

    def test_collect_by_interval(self):
        c = TestDomainIntervalCollection.get_collection()
        d = DomainIntervalCollection({1: IntervalSet3D([
                Interval3D((t,t)) for t in range(1, 100)])})
        e = c.collect_by_interval(d, utils.T(overlaps()),
                filter_empty=False, time_window=0)
        self.assertEqual(e.keys(), c.keys())



class TestVideoIntervalCollection(unittest.TestCase):
    def setUpProfiler(self):
        self.pr = cProfile.Profile()
        self.pr.enable()

    def tearDownProfiler(self):
        p = Stats(self.pr)
        p.strip_dirs()
        p.sort_stats('cumtime')
        # p.print_stats()

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
