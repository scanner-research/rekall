from rekall import Interval, IntervalSet, IntervalSetMapping
from rekall.bounds import Bounds3D
from rekall.predicates import overlaps
from rekall.stdlib.merge_ops import payload_first
import unittest

from pstats import Stats
import cProfile

class TestIntervalSetMapping(unittest.TestCase):
    @staticmethod
    def get_collection():
        NUM_INTERVALS=150
        NUM_VIDEOS=100
        def get_intervals_for_video(vid):
            intervals = [
                Interval(Bounds3D(t, t+1,0,0.5,0.5,1),vid)
                for t in range(vid, vid+NUM_INTERVALS)]
            return IntervalSet(intervals)
        return IntervalSetMapping(
                {vid: get_intervals_for_video(vid) for vid in range(
                    NUM_VIDEOS)})

    def assertCollectionEq(self, c1,c2):
        self.assertEqual(c1.keys(), c2.keys())
        for key in c1:
            is1 = c1[key]
            is2 = c2[key]
            list1 = [(i['t1'], i['t2'], i['x1'], i['x2'], i['y1'], i['y2'],
                i['payload']) for i in is1.get_intervals()]
            list2 = [(i['t1'], i['t2'], i['x1'], i['x2'], i['y1'], i['y2'],
                i['payload']) for i in is2.get_intervals()]
            self.assertEqual(list1, list2)

    def test_filter_size(self):
        c = TestIntervalSetMapping.get_collection()
        c2 = c.filter_size(min_size=1,max_size=2)
        self.assertCollectionEq(c2, c)

    def test_fold(self):
        c = TestIntervalSetMapping.get_collection()
        vids = c.keys()
        d = c.fold(lambda m, i: max(m, i['payload']), -1)
        self.assertEqual(d, {v: v for v in vids})

    def test_fold_to_set(self):
        c = TestIntervalSetMapping.get_collection()
        d = c.fold_to_set(lambda acc, i: acc + [i], [])
        self.assertCollectionEq(c, d)

    def test_fold_modify_accumulator_in_place(self):
        def update(acc, i):
            acc.append(i)
            return acc
        c = TestIntervalSetMapping.get_collection()
        d = c.fold_to_set(update, [])
        self.assertCollectionEq(c, d)

    def test_union(self):
        c= TestIntervalSetMapping.get_collection()
        c1 = IntervalSetMapping({v: c[v] for v in c if v % 2 ==0})
        c2 = IntervalSetMapping({v: c[v] for v in c if v % 2 ==1})
        c3 = c1.union(c2)
        self.assertCollectionEq(c3, c)

    def test_minus(self):
        c= TestIntervalSetMapping.get_collection()
        c1 = IntervalSetMapping({v: c[v] for v in c if v % 2 ==0})
        c2 = IntervalSetMapping({v: c[v] for v in c if v % 2 ==1})

        c3 = c.minus(c1, window=0)
        self.assertCollectionEq(c3,c2)

    def test_collect_by_interval(self):
        c = TestIntervalSetMapping.get_collection()
        d = IntervalSetMapping({1: IntervalSet([
                Interval(Bounds3D(t,t)) for t in range(1, 100)])})
        e = c.collect_by_interval(d, Bounds3D.T(overlaps()),
                filter_empty=False, window=0)
        self.assertEqual(e.keys(), c.keys())

