import unittest

from rekall.interval_set_3d import Interval3D, IntervalSet3D
from rekall.runtime import (Runtime, get_forked_process_pool_factory,
        get_spawned_process_pool_factory, wrap_interval_set)
from rekall.video_interval_collection_3d import VideoIntervalCollection3D

class TestRuntime(unittest.TestCase):
    @staticmethod
    def dummy_intervalset():
        return IntervalSet3D([Interval3D((1,10))])

    @staticmethod
    def query(vids):
        return VideoIntervalCollection3D({
            vid: TestRuntime.dummy_intervalset() for vid in vids })

    @staticmethod
    def query_that_throws(vids):
        raise RuntimeError()

    @staticmethod
    def query_single_vid(vid):
        return TestRuntime.dummy_intervalset()

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

    def test_single_process_runtime(self):
        vids = list(range(1000))
        rt = Runtime.inline()

        self.assertCollectionEq(
                rt.run(TestRuntime.query, vids),
                TestRuntime.query(vids))

    def test_exception_inline(self):
        vids = list(range(1))
        rt = Runtime.inline()

        with self.assertRaises(RuntimeError):
            rt.run(TestRuntime.query_that_throws, vids)

    def test_forked_children(self):
        vids = list(range(10))
        rt = Runtime(get_forked_process_pool_factory())
        self.assertCollectionEq(
                rt.run(TestRuntime.query, vids, chunksize=3),
                TestRuntime.query(vids))

    def test_forked_children_exception(self):
        vids = list(range(1))
        rt = Runtime(get_forked_process_pool_factory(1))
        with self.assertRaises(RuntimeError):
            rt.run(TestRuntime.query_that_throws, vids)

    def test_spawned_children(self):
        vids = list(range(10))
        rt = Runtime(get_spawned_process_pool_factory())
        self.assertCollectionEq(
                rt.run(TestRuntime.query, vids, chunksize=3),
                TestRuntime.query(vids))

    def test_spawned_children_exception(self):
        vids = list(range(1))
        rt = Runtime(get_spawned_process_pool_factory())
        with self.assertRaises(RuntimeError):
            rt.run(TestRuntime.query_that_throws, vids)

    def test_wrap_interval_set(self):
        vids = list(range(1000))
        rt = Runtime(get_forked_process_pool_factory())
        self.assertCollectionEq(
                rt.run(wrap_interval_set(TestRuntime.query_single_vid),vids),
                TestRuntime.query(vids))

