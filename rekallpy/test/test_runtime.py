import unittest

from rekall import Interval, IntervalSet, IntervalSetMapping
from rekall.bounds import Bounds3D
from rekall.runtime import (Runtime, get_forked_process_pool_factory,
        get_spawned_process_pool_factory, RekallRuntimeException)

class TestRuntime(unittest.TestCase):
    @staticmethod
    def dummy_intervalset():
        return IntervalSet([TestRuntime.dummy_interval()])

    @staticmethod
    def query(vids):
        return IntervalSetMapping({
            vid: TestRuntime.dummy_intervalset() for vid in vids })

    @staticmethod
    def dummy_interval(payload=None):
        return Interval(Bounds3D(1,10), payload=payload)

    @staticmethod
    def query_that_throws_at_0(vids):
        output = []
        for vid in vids:
            if vid == 0:
                raise RuntimeError()
            output.append(TestRuntime.dummy_interval(vid))
        return IntervalSet(output)

    def assertCollectionEq(self, c1,c2):
        map1 = c1.get_grouped_intervals()
        map2 = c2.get_grouped_intervals()
        self.assertEqual(map1.keys(), map2.keys())
        for key in map1:
            is1 = map1[key]
            is2 = map2[key]
            self.assertIntervalSetEq(is1,is2)

    def assertIntervalsEq(self, intrvl1, intrvl2, payload_cmp=None):
        self.assertEqual(intrvl1['bounds'].data, intrvl2['bounds'].data)
        self.assertTrue((payload_cmp is None or
                payload_cmp(intrvl1['payload'], intrvl2['payload'])))

    def assertIntervalSetEq(self, is1, is2, payload_cmp=None):
        self.assertEqual(is1.size(), is2.size())
        for i, j in zip(is1.get_intervals(), is2.get_intervals()):
            self.assertIntervalsEq(i, j)

    def test_single_process_runtime(self):
        vids = list(range(1000))
        rt = Runtime.inline()

        self.assertCollectionEq(
                rt.run(TestRuntime.query, vids)[0],
                TestRuntime.query(vids))

    def test_exception_inline(self):
        vids = list(range(2))
        rt = Runtime.inline()

        _, vids_with_err = rt.run(TestRuntime.query_that_throws_at_0, vids,
                print_error=False)
        self.assertEqual([0], vids_with_err)

    def test_forked_children(self):
        vids = list(range(10))
        rt = Runtime(get_forked_process_pool_factory())
        self.assertCollectionEq(
                rt.run(TestRuntime.query, vids, chunksize=3)[0],
                TestRuntime.query(vids))

    def test_forked_children_exception(self):
        vids = list(range(2))
        rt = Runtime(get_forked_process_pool_factory(1))
        _, vids_with_err = rt.run(TestRuntime.query_that_throws_at_0, vids,
                print_error=False)
        self.assertEqual([0], vids_with_err)

    def test_spawned_children(self):
        vids = list(range(10))
        rt = Runtime(get_spawned_process_pool_factory())
        self.assertCollectionEq(
                rt.run(TestRuntime.query, vids, chunksize=3)[0],
                TestRuntime.query(vids))

    def test_spawned_children_exception(self):
        vids = list(range(2))
        rt = Runtime(get_spawned_process_pool_factory())
        _, vids_with_err = rt.run(TestRuntime.query_that_throws_at_0, vids,
                print_error=False)
        self.assertEqual([0], vids_with_err)

    def test_returning_intervalset(self):
        vids = list(range(1,101))
        rt = Runtime(get_spawned_process_pool_factory())
        answer, _ = rt.run(TestRuntime.query_that_throws_at_0, vids)
        self.assertIntervalSetEq(answer,
                TestRuntime.query_that_throws_at_0(vids))

    def test_iterator(self):
        vids = list(range(1000))
        rt = Runtime(get_forked_process_pool_factory(5))
        gen = rt.get_result_iterator(TestRuntime.query, vids,
                randomize=False)
        for vid, result in zip(vids, gen):
            self.assertCollectionEq(result, TestRuntime.query([vid]))

    def test_inline_iterator(self):
        vids = list(range(1000))
        rt = Runtime.inline()
        gen = rt.get_result_iterator(TestRuntime.query, vids,
                randomize=True)
        for result in gen:
            self.assertCollectionEq(result, TestRuntime.query(result.keys()))
        
    def test_iterator_error(self):
        vids = list(range(2))
        rt = Runtime(get_spawned_process_pool_factory())
        gen = rt.get_result_iterator(TestRuntime.query_that_throws_at_0, vids,
                print_error=False)
        result = next(gen)
        self.assertIntervalSetEq(result,
                TestRuntime.query_that_throws_at_0([1]))
        with self.assertRaises(RekallRuntimeException):
            next(gen)

    def test_all_tasks_fail(self):
        vids = list(range(1))
        rt = Runtime.inline()
        with self.assertRaises(RekallRuntimeException):
            rt.run(TestRuntime.query_that_throws_at_0, vids,
                    print_error=False)






