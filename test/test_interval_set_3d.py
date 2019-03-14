from rekall.interval_set_3d import Interval3D, IntervalSet3D
import rekall.interval_set_3d_utils as utils
from rekall.merge_ops import payload_plus, payload_second
from rekall.temporal_predicates import (overlaps, before, meets_before, equal,
    during_inv)
    
from rekall.bbox_predicates import left_of, above, height_at_least
import unittest
from operator import eq
import json

class TestInterval3D(unittest.TestCase):
    @staticmethod
    def intervalsEq(i1, i2, payload_cmp=None):
        def float_equal(f1, f2):
            return abs(f1-f2)<=1e-7
        def bound_almost_eq(b1,b2):
            return (float_equal(b1[0], b2[0]) and
                   float_equal(b1[1], b2[1]))
            
        return (i1.t==i2.t and bound_almost_eq(i1.x, i2.x) and 
                bound_almost_eq(i1.y,i2.y) and (
                payload_cmp is None or payload_cmp(i1.payload,i2.payload)))

    def assertIntervalEq(self, i1, i2, payload_cmp=None):
        self.assertTrue(TestInterval3D.intervalsEq(i1,i2,payload_cmp),
                msg = "Interval {0} different from Interval {1}".format(
                    repr(i1), repr(i2)))

    def test_merge(self):
        i1 = Interval3D((0,1),(0,0.5),(0,0.5),1)
        i2 = Interval3D((1,2),(0.4,1),(0.2,0.3),2)
        i3 = i1.merge(i2, payload_plus)
        target = Interval3D((0,2),(0,1), (0,0.5), 3)
        self.assertIntervalEq(i3, target, eq)

    def test_combine(self):
        def combiner1(b1, b2):
            return b1
        def combiner2(b1, b2):
            return b2
        i1 = Interval3D((0,1),(0,0.5),(0,0.1))
        i2 = Interval3D((2,3),(2,2.5),(1,1.1))
        i3 = i1.combine(i2, combiner2, combiner1, combiner2)
        target = Interval3D((2,3),(0,0.5),(1,1.1))
        self.assertIntervalEq(i3, target)

    def test_overlap_time_merge_space(self):
        i1 = Interval3D((0,2.5),(0,0.5),(0,0.1))
        i2 = Interval3D((2,3),(2,2.5),(1,1.1))
        i3 = i1.overlap_time_merge_space(i2)
        target = Interval3D((2,2.5),(0,2.5), (0,1.1))
        self.assertIntervalEq(i3, target)

    def test_overlap_time_merge_space_throws(self):
        i1 = Interval3D((0,1),(0,0.5),(0,0.1))
        i2 = Interval3D((2,3),(2,2.5),(1,1.1))
        with self.assertRaises(ValueError):
            i3 = i1.overlap_time_merge_space(i2)

    def test_expand_to_frame(self):
        i1 = Interval3D((0,1),(0.5,0.6),(0.7,0.8)).expand_to_frame()
        self.assertIntervalEq(i1,
                Interval3D((0,1),(0,1),(0,1)))

    def test_dimension_sizes(self):
        i = Interval3D((0,2),(0.5,0.8),(0.9,1.0))
        self.assertAlmostEqual(i.length(), 2)
        self.assertAlmostEqual(i.width(), 0.3)
        self.assertAlmostEqual(i.height(), 0.1)
        
class TestIntervalSet3D(unittest.TestCase):
    def assertIntervalSetEq(self, is1, is2, payload_cmp=None):
        list1 = is1.get_intervals()
        list2 = is2.get_intervals()
        for i1, i2 in zip(list1, list2):
            self.assertTrue(TestInterval3D.intervalsEq(i1,i2,payload_cmp),
                    "Interval {0} different from Interval {1}".format(
                        repr(i1), repr(i2)))
        self.assertFalse(len(list1)<len(list2),
                "2nd IntervalSet has more intervals {0}".format(
                    list2[len(list1):]))
        self.assertFalse(len(list1)>len(list2),
                "1st IntervalSet has more intervals {0}".format(
                    list1[len(list2):]))

    def compare_interval_sets_in_payload(self):
        def payload_cmp(set1, set2):
            # Will throw if not equal
            self.assertIntervalSetEq(set1, set2)
            return True
        return payload_cmp

    def test_map(self):
        is1 = IntervalSet3D([
            Interval3D((0,1),(0.3,0.4),(0.5,0.6)),
            Interval3D((0,0.5),(0.2,0.3),(0.5,0.6)),
            ])
        is1 = is1.map(Interval3D.expand_to_frame)
        target = IntervalSet3D([
            Interval3D((0,0.5),(0,1),(0,1)),
            Interval3D((0,1),(0,1),(0,1)),
            ])
        self.assertIntervalSetEq(is1, target)

    def test_union(self):
        is1 = IntervalSet3D([
            Interval3D((0,1),(0,1),(0,1),1),
            Interval3D((0,0.5),(0.5,1),(0,0.5),1),
            ])
        is2 = IntervalSet3D([
            Interval3D((0.5,1),(0,1),(0,1),2),
            Interval3D((0,1),(0,1),(0,1),2),
            ])
        is3 = is1.union(is2)
        target = IntervalSet3D([
            Interval3D((0,1),(0,1),(0,1),1),
            Interval3D((0,1),(0,1),(0,1),2),
            Interval3D((0,0.5),(0.5,1),(0,0.5),1),
            Interval3D((0.5,1),(0,1),(0,1),2),
            ])
        self.assertIntervalSetEq(is3, target, eq)

    def test_join(self):
        is1 = IntervalSet3D([
            Interval3D((0,1),(0,1),(0,1),1),
            Interval3D((0,0.5),(0.5,1),(0,0.5),2),
            ])
        is2 = IntervalSet3D([
            Interval3D((0.5,1),(0,1),(0,1),4),
            Interval3D((0,1),(0,1),(0,1),8),
            ])
        is3 = is1.join(is2,
                utils.T(overlaps()),
                lambda i1,i2: [i1.overlap_time_merge_space(i2,
                    payload_plus)])
        target = IntervalSet3D([
            Interval3D((0.5,1),(0,1),(0,1),5),
            Interval3D((0,1),(0,1),(0,1),9),
            Interval3D((0,0.5),(0,1),(0,1),10),
            ])
        self.assertIntervalSetEq(is3, target, eq)

    def test_join_with_time_window(self):
        is1 = IntervalSet3D([
            Interval3D((t,t+1), payload=t) for t in range(100)
            ])
        is2 = IntervalSet3D([
            Interval3D((t,t), payload=t) for t in range(100)
            ])
        is3 = is1.join(is2,
                utils.T(before(max_dist=1)),
                lambda i1, i2: [i1.merge(i2, payload_second)],
                time_window=1)
        target = IntervalSet3D([
            Interval3D((t, t+2), payload=t+2) for t in range(98)]+[
            Interval3D((t,t+1), payload=t+1) for t in range(99)])
        self.assertIntervalSetEq(is3, target, eq)

    def test_filter(self):
        is1 = IntervalSet3D([
            Interval3D((0,1),(0,1),(0,1),1),
            Interval3D((0,0.5),(0.5,1),(0,0.5),2),
            Interval3D((0,0.5),(0,1),(0,0.5),3),
            ])
        is1 = is1.filter(lambda i: i.payload > 2)
        target = IntervalSet3D([
            Interval3D((0,0.5),(0,1),(0,0.5),3),
            ])
        self.assertIntervalSetEq(is1, target, eq)

    def test_fold(self):
        def fold_fn(acc, i):
            return acc + i.length()*i.width()*i.height()

        is1 = IntervalSet3D([
            Interval3D((0,1),(0,1),(0,1),1),
            Interval3D((0,0.5),(0.5,1),(0,0.5),2),
            Interval3D((0,0.5),(0,1),(0,0.5),3),
            ])
        self.assertAlmostEqual(is1.fold(fold_fn, 0),
                1.375)

    def test_fold_custom_sortkey(self):
        def sortkey(i):
            return i.x
        def fold_fn(acc, i):
            acc.append(i.payload)
            return acc
        is1 = IntervalSet3D([
            Interval3D((0,1),(0,0.1),(0,1),1),
            Interval3D((0,0.5),(0.5,1),(0,0.5),2),
            Interval3D((0.1,0.4),(0,1),(0,0.5),3),
            ])
        self.assertListEqual(is1.fold(fold_fn, [], sortkey),
                [1,3,2])

    def test_group_by(self):
        is1 = IntervalSet3D([
            Interval3D((0,1)),
            Interval3D((1,2)),
            Interval3D((0,2)),
            Interval3D((1,3)),
            ])

        def merge_intervals(k, intervals):
            merged = intervals.fold(Interval3D.merge)
            merged.payload = intervals
            return merged

        is2 = is1.group_by(lambda i: int(i.t[1]) % 2,
                merge_intervals)
        target = IntervalSet3D([
            Interval3D((0,3), payload = IntervalSet3D([
                Interval3D((0,1)),
                Interval3D((1,3)),])),
            Interval3D((0,2), payload = IntervalSet3D([
                Interval3D((0,2)),
                Interval3D((1,2)),])),
            ])
        self.assertIntervalSetEq(is2, target,
                self.compare_interval_sets_in_payload())

    def test_minus(self):
        is1 = IntervalSet3D([
            Interval3D((1, 10),(0,0.5),(0.2,0.8),1),
            Interval3D((3, 15),(0,1),(0,1),2)])
        is2 = IntervalSet3D([
            Interval3D((2,2.5)),
            Interval3D((2,2.7)),
            Interval3D((2.9,3.5)),
            Interval3D((3.5,3.6)),
            Interval3D((5,7)),
            Interval3D((9,12)),
            ])
        is3 = is1.minus(is2)
        target = IntervalSet3D([
            Interval3D((1,2),(0,0.5),(0.2,0.8),1),
            Interval3D((2.7, 2.9), (0,0.5),(0.2,0.8),1),
            Interval3D((3.6,5),(0,0.5),(0.2,0.8),1),
            Interval3D((7,9),(0,0.5),(0.2,0.8),1),
            Interval3D((3.6,5), payload=2),
            Interval3D((7,9), payload=2),
            Interval3D((12,15), payload=2),
            ])
        self.assertIntervalSetEq(is3, target, eq)

    def test_minus_everything(self):
        is1 = IntervalSet3D([
            Interval3D((1, 10)),
            Interval3D((3, 15))])
        is2 = IntervalSet3D([
            Interval3D((2,2.5)),
            Interval3D((2,2.7)),
            Interval3D((2.9,3.5)),
            Interval3D((3.5,3.6)),
            Interval3D((5,7)),
            Interval3D((9,12)),
            ])
        is3 = is2.minus(is1)
        self.assertIntervalSetEq(is3, IntervalSet3D([]), eq)

    def test_minus_self(self):
        is1 = IntervalSet3D([
            Interval3D((2,2.5)),
            Interval3D((2,2.7)),
            Interval3D((2.9,3.5)),
            Interval3D((3.5,3.6)),
            Interval3D((5,7)),
            Interval3D((9,12)),
            ])
        is1 = is1.minus(is1)
        self.assertIntervalSetEq(is1, IntervalSet3D([]), eq)

    def test_minus_against_nothing(self):
        is1 = IntervalSet3D([
            Interval3D((1, 10),(0,0.5),(0.2,0.8),1),
            Interval3D((3, 15),(0,1),(0,1),2)])
        is2 = IntervalSet3D([
            Interval3D((20,20.5)),
            Interval3D((20,20.7)),
            Interval3D((20.9,23.5)),
            Interval3D((23.5,23.6)),
            Interval3D((25,27)),
            Interval3D((29,32)),
            ])
        is3 = is1.minus(is2)
        self.assertIntervalSetEq(is3, is1, eq)

    def test_minus_with_single_frame(self):
        is1 = IntervalSet3D([
            Interval3D((1,1)),
            Interval3D((3,3)),
            Interval3D((4,4)),
            Interval3D((7,7)),
            Interval3D((10,10)),
            ])
        is2 = IntervalSet3D([
            Interval3D((1,3)),
            Interval3D((5,8)),
            Interval3D((9,9)),
            ])
        is3 = is1.minus(is2)
        target = IntervalSet3D([
            Interval3D((4,4)),
            Interval3D((10,10)),
            ])
        self.assertIntervalSetEq(is3, target)

        is4 = is2.minus(is1)
        self.assertIntervalSetEq(is4, is2)

    def test_match(self):
        left_box_frame_1 = Interval3D((1,1),(0.1,0.4),(0.4,0.8))
        right_box_frame_1 = Interval3D((1,1),(0.6,0.9),(0.3,0.7))
        bottom_left_box_frame_2 = Interval3D((2,2),(0.1,0.3),(0.8,0.9))
        top_right_box_frame_2 = Interval3D((2,2),(0.5,0.7),(0.2,0.7))
        is1 = IntervalSet3D([
            left_box_frame_1,
            right_box_frame_1,
            bottom_left_box_frame_2,
            top_right_box_frame_2])

        pattern = [
                (["left","right"],[
                    # Two boxes on left and right on same frame
                    utils.T(equal()),
                    utils.XY(left_of()),
                ]),
                (["top", "bottom"], [
                    # Two boxes on top and bottom on overlapping frame
                    utils.T(equal()),
                    utils.XY(above()),
                ]),
                (["left", "top"],[
                    # Left-Right pattern comes before Top-Bottom
                    utils.T(meets_before(epsilon=1))
                ])
        ]
        results = is1.match(pattern, exact=True)
        self.assertEqual(len(results),1)
        result = results[0]
        self.assertTrue(TestInterval3D.intervalsEq(
            left_box_frame_1, result['left']))
        self.assertTrue(TestInterval3D.intervalsEq(
            right_box_frame_1, result['right']))
        self.assertTrue(TestInterval3D.intervalsEq(
            top_right_box_frame_2, result['top']))
        self.assertTrue(TestInterval3D.intervalsEq(
            bottom_left_box_frame_2, result['bottom']))

    def test_match_multiple_solutions(self):
        left_box_frame_1 = Interval3D((1,1),(0.1,0.4),(0.4,0.8))
        right_box_frame_1 = Interval3D((1,1),(0.6,0.9),(0.3,0.7))
        bottom_left_box_frame_2 = Interval3D((2,2),(0.1,0.3),(0.8,0.9))
        top_right_box_frame_2 = Interval3D((2,2),(0.5,0.7),(0.2,0.7))
        is1 = IntervalSet3D([
            left_box_frame_1,
            right_box_frame_1,
            bottom_left_box_frame_2,
            top_right_box_frame_2,
            Interval3D((3,3)),
            ])

        pattern = [
                (["left","right"],[
                    # Two boxes on left and right on same frame
                    utils.T(equal()),
                    utils.XY(left_of()),
                ]),
        ]
        results = is1.match(pattern, exact=False)
        self.assertEqual(len(results),2)

        # Add single interval constraints.
        pattern = pattern + [
                (["left"], [utils.XY(height_at_least(0.3))]),
                (["right"], [utils.XY(height_at_least(0.3))]),
                ]
        results = is1.match(pattern, exact=False)
        self.assertEqual(len(results),1)
        result = results[0]
        self.assertTrue(TestInterval3D.intervalsEq(
            left_box_frame_1, result['left']))
        self.assertTrue(TestInterval3D.intervalsEq(
            right_box_frame_1, result['right']))

    def test_filter_against(self):
        is1 = IntervalSet3D([
            Interval3D((0,1)),
            Interval3D((2,5)),
            Interval3D((6,7)),
            ])
        is2 = IntervalSet3D([
            Interval3D((1,1)),
            Interval3D((7,7)),
            ])
        # Take only intervals in is1 that overlaps with some interval in is2
        is3 = is1.filter_against(is2, utils.T(overlaps()),
                time_window=0)
        self.assertIntervalSetEq(is3,
                IntervalSet3D([Interval3D((0,1)),Interval3D((6,7))]))

    def test_map_payload(self):
        is1 = IntervalSet3D([
            Interval3D((0,1),payload=1),
            Interval3D((2,3),payload=2),
            Interval3D((4,5),payload=3),
            ])
        is2 = is1.map_payload(lambda p: p+10)
        target = IntervalSet3D([
            Interval3D((0,1),payload=11),
            Interval3D((2,3),payload=12),
            Interval3D((4,5),payload=13),
            ])
        self.assertIntervalSetEq(is2, target)

    def test_dilate(self):
        is1 = IntervalSet3D([
            Interval3D((2,5)),
            Interval3D((6,7)),
        ])
        is2 = is1.dilate(1)
        target = IntervalSet3D([
            Interval3D((1,6)),
            Interval3D((5,8)),
        ])
        self.assertIntervalSetEq(is2, target)
        self.assertIntervalSetEq(is2.dilate(-1), is1)
        is3 = is1.dilate(-0.1, axis='X')
        target = IntervalSet3D([
            Interval3D((2,5),(0.1, 0.9),(0,1)),
            Interval3D((6,7),(0.1, 0.9),(0,1)),
            ])
        self.assertIntervalSetEq(is3, target)

    def test_filter_size(self):
        is1 = IntervalSet3D([
            Interval3D((1,2),(0.5,0.9),(0.1,0.2)),
            Interval3D((20,30),(0.4,1.0), (0,1)),
            Interval3D((50,55),(0.2,0.3), (0.5,0.9)),
            ])
        is2 = is1.filter_size(min_size=10, axis='T')
        self.assertIntervalSetEq(is2, IntervalSet3D([
            Interval3D((20,30),(0.4,1.0),(0,1))]))

        is3 = is1.filter_size(max_size=0.1, axis='Y')
        self.assertIntervalSetEq(is3, IntervalSet3D([
            Interval3D((1,2),(0.5,0.9),(0.1,0.2))]))

        is4 = is1.filter_size(min_size=0.3, max_size=0.5, axis='X')
        self.assertIntervalSetEq(is3, IntervalSet3D([
            Interval3D((1,2),(0.5,0.9),(0.1,0.2))]))

    def test_merge(self):
        is1 = IntervalSet3D([
            Interval3D((1,10),(0.8,0.9),(0,0.1)),
            Interval3D((20,30),(0.1,0.2),(0.9,1)),
            ])
        is2 = IntervalSet3D([
            Interval3D((11,15),(0.4,0.5),(0.8,0.9)),
            Interval3D((35,36),(0,1),(0,1)),
            ])
        is3 = is1.merge(is2, utils.T(meets_before(epsilon=1)))
        target = IntervalSet3D([
            Interval3D((1,15),(0.4,0.9),(0,0.9)),
            ])
        self.assertIntervalSetEq(is3, target)

    def test_group_by_time(self):
        intervals_1 = [
            Interval3D((1,1), (0.4,0.5),(0.6,0.8), 1),
            Interval3D((1,1), (0.1,0.2),(0.2,0.3), 2),
            Interval3D((1,1), (0.3,0.5),(0.1,0.5), 3),
        ]
        intervals_2 = [
            Interval3D((2,2), (0.3,0.5),(0.6,0.8), 11),
            Interval3D((2,2), (0.2,0.3),(0.2,0.9), 12),
            Interval3D((2,2), (0.3,0.7),(0,0.5), 13),
        ]
        is1 = IntervalSet3D(intervals_1+intervals_2)
        target = IntervalSet3D([
            Interval3D((1,1), payload=IntervalSet3D(intervals_1)),
            Interval3D((2,2), payload=IntervalSet3D(intervals_2)),
            ])
        self.assertIntervalSetEq(is1.group_by_time(), target,
                self.compare_interval_sets_in_payload())

    def test_collect_by_interval(self):
        is1 = IntervalSet3D([
            Interval3D((1,5)),
            Interval3D((10,50)),
            Interval3D((51,52)),
            ])
        intervals = [
            Interval3D((2,2), (0.4,0.6),(0.1,0.2), 1),
            Interval3D((3,3), (0.1,0.6),(0.8,0.9), 2),
            Interval3D((11,23), (0,1),(0.8,0.9), 3),
            ]
        is2 = IntervalSet3D(intervals)

        is3 = is1.collect_by_interval(is2, utils.T(during_inv()),
                filter_empty=True, time_window=None).map_payload(lambda p:p[1])
        target = IntervalSet3D([
            Interval3D((1,5), payload=IntervalSet3D(intervals[:2])),
            Interval3D((10,50), payload=IntervalSet3D(intervals[2:3])),
            ])
        self.assertIntervalSetEq(is3, target,
                self.compare_interval_sets_in_payload())

        is4 = is1.collect_by_interval(is2, utils.T(overlaps()),
                filter_empty=False, time_window=0).map_payload(lambda p:p[1])
        target = IntervalSet3D([
            Interval3D((1,5), payload=IntervalSet3D(intervals[:2])),
            Interval3D((10,50), payload=IntervalSet3D(intervals[2:3])),
            Interval3D((51,52), payload=IntervalSet3D([])),
            ])
        self.assertIntervalSetEq(is4, target,
                self.compare_interval_sets_in_payload())

    def test_temporal_coalesce(self):
        is1 = IntervalSet3D([
            Interval3D((1,10),(0.3,0.4),(0.5,0.6),1),
            Interval3D((2,5),(0.2,0.8),(0.2,0.3),1),
            Interval3D((10,11),(0.2,0.7),(0.3,0.5),1),
            Interval3D((13,15),(0.5,1),(0,0.5),1),
            Interval3D((15,19),(0.5,1),(0,0.5),1),
            Interval3D((20,20),payload=1),
            Interval3D((22,22),payload=1),
            Interval3D((22,23),payload=1),
            ])
        target = IntervalSet3D([
            Interval3D((1,11),(0.2,0.8),(0.2,0.6),3),
            Interval3D((13,19),(0.5,1),(0,0.5),2),
            Interval3D((20,20),payload=1),
            Interval3D((22,23),payload=2),
            ])
        self.assertIntervalSetEq(is1.temporal_coalesce(payload_plus), target)
        self.assertIntervalSetEq(
                is1.temporal_coalesce(payload_plus, epsilon=2),
                IntervalSet3D([Interval3D((1,23),payload=8)]))

    def test_split(self):
        is1 = IntervalSet3D([
            Interval3D((1,5)),
            Interval3D((10,22)),
            ])
        def split_fn(i):
            output = []
            t = i.copy()
            while t.length() > 5:
                output.append(Interval3D((t.t[0],t.t[0]+5)))
                t.t = (t.t[0]+5, t.t[1])
            if t.length() > 0:
                output.append(t)
            return IntervalSet3D(output)
        target = IntervalSet3D([
            Interval3D((1,5)),
            Interval3D((10,15)),
            Interval3D((15,20)),
            Interval3D((20,22)),
            ])
        self.assertIntervalSetEq(target, is1.split(split_fn))

    def test_len(self):
        is1 = IntervalSet3D([
            Interval3D((1,5)),
            Interval3D((10,22)),
            ])
        self.assertEqual(len(is1), 2)
        self.assertEqual(is1.size(), 2)

    def test_json_conversion(self):
        is1 = IntervalSet3D([
            Interval3D((1,5), payload=5),
            Interval3D((10,50),(0,1),(0.5,1),
                payload=IntervalSet3D([
                    Interval3D((20,25))
                    ]))
            ])
        self.assertEqual(
                is1.to_json(),
                [{
                    "t": (1,5),
                    "x": (0,1),
                    "y": (0,1),
                    "payload": 5
                },{
                    "t": (10,50),
                    "x": (0,1),
                    "y": (0.5,1),
                    "payload": [
                        {"t": (20,25), "x":(0,1), "y":(0,1), "payload":None}
                    ]
                }])
