from rekall.predicates import *
from rekall import Interval, IntervalSet
from rekall.bounds import Bounds3D
from rekall.stdlib.merge_ops import *
from operator import eq
import unittest

class TestIntervalSet(unittest.TestCase):
    def assertIntervalsEq(self, intrvl1, intrvl2, payload_cmp=None):
        self.assertEqual(intrvl1['bounds'].data, intrvl2['bounds'].data)
        self.assertTrue((payload_cmp is None or
                payload_cmp(intrvl1['payload'], intrvl2['payload'])))

    def assertIntervalSetEq(self, is1, is2, payload_cmp=None):
        self.assertEqual(is1.size(), is2.size())
        for i, j in zip(is1.get_intervals(), is2.get_intervals()):
            self.assertIntervalsEq(i, j)

    def compare_interval_sets_in_payload(self):
        def payload_cmp(set1, set2):
            # Will throw if not equal
            self.assertIntervalSetEq(set1, set2)
            return True
        return payload_cmp

    def test_map(self):
        def expand_to_frame(intrvl):
            new_bounds = intrvl['bounds'].copy()
            new_bounds['x1'] = 0
            new_bounds['x2'] = 1
            new_bounds['y1'] = 0
            new_bounds['y2'] = 1
            return Interval(new_bounds, intrvl['payload'])

        is1 = IntervalSet([
            Interval(Bounds3D(0,1,0.3,0.4,0.5,0.6)),
            Interval(Bounds3D(0,0.5,0.2,0.3,0.5,0.6))
        ])
        is1 = is1.map(expand_to_frame)
        target = IntervalSet([
            Interval(Bounds3D(0,0.5,0,1,0,1)),
            Interval(Bounds3D(0,1,0,1,0,1)),
        ])
        self.assertIntervalSetEq(is1, target)

    def test_union(self):
        is1 = IntervalSet([
            Interval(Bounds3D(0,1,0,1,0,1),1),
            Interval(Bounds3D(0,0.5,0.5,1,0,0.5),1),
            ])
        is2 = IntervalSet([
            Interval(Bounds3D(0.5,1,0,1,0,1),2),
            Interval(Bounds3D(0,1,0,1,0,1),2),
            ])
        is3 = is1.union(is2)
        target = IntervalSet([
            Interval(Bounds3D(0,1,0,1,0,1),1),
            Interval(Bounds3D(0,1,0,1,0,1),2),
            Interval(Bounds3D(0,0.5,0.5,1,0,0.5),1),
            Interval(Bounds3D(0.5,1,0,1,0,1),2),
            ])
        self.assertIntervalSetEq(is3, target, eq)

    def test_join(self):
        is1 = IntervalSet([
            Interval(Bounds3D(0,1,0,1,0,1),1),
            Interval(Bounds3D(0,0.5,0.5,1,0,0.5),2),
            ])
        is2 = IntervalSet([
            Interval(Bounds3D(0.5,1,0,1,0,1),4),
            Interval(Bounds3D(0,1,0,1,0,1),8),
            ])
        is3 = is1.join(is2,
                Bounds3D.T(overlaps()),
                lambda i1,i2: Interval(
                    i1['bounds'].intersect_time_span_space(i2['bounds']),
                    i1['payload'] + i2['payload']
                ))
        target = IntervalSet([
            Interval(Bounds3D(0.5,1,0,1,0,1),5),
            Interval(Bounds3D(0,1,0,1,0,1),9),
            Interval(Bounds3D(0,0.5,0,1,0,1),10),
            ])
        self.assertIntervalSetEq(is3, target, eq)

    def test_join_with_optimization_window(self):
        is1 = IntervalSet([
            Interval(Bounds3D(t,t+1), t) for t in range(100)
            ])
        is2 = IntervalSet([
            Interval(Bounds3D(t,t), t) for t in range(100)
            ])
        is3 = is1.join(is2,
                Bounds3D.T(before(max_dist=1)),
                lambda i1, i2: Interval(
                    i1['bounds'].span(i2['bounds']), i2['payload']
                ),
                window=1)
        target = IntervalSet([
            Interval(Bounds3D(t, t+2), t+2) for t in range(98)]+[
            Interval(Bounds3D(t,t+1), t+1) for t in range(99)])
        self.assertIntervalSetEq(is3, target, eq)

    def test_filter(self):
        is1 = IntervalSet([
            Interval(Bounds3D(0,1,0,1,0,1),1),
            Interval(Bounds3D(0,0.5,0.5,1,0,0.5),2),
            Interval(Bounds3D(0,0.5,0,1,0,0.5),3),
            ])
        is1 = is1.filter(lambda i: i['payload'] > 2)
        target = IntervalSet([
            Interval(Bounds3D(0,0.5,0,1,0,0.5),3),
            ])
        self.assertIntervalSetEq(is1, target, eq)

    def test_fold(self):
        def fold_fn(acc, i):
            return acc + (i['bounds'].length()*i['bounds'].width()*
                i['bounds'].height())

        is1 = IntervalSet([
            Interval(Bounds3D(0,1,0,1,0,1),1),
            Interval(Bounds3D(0,0.5,0.5,1,0,0.5),2),
            Interval(Bounds3D(0,0.5,0,1,0,0.5),3),
            ])
        self.assertAlmostEqual(is1.fold(fold_fn, 0), 1.375)

    def test_fold_custom_sortkey(self):
        def sortkey(i):
            return (i['x1'], i['x2'])
        def fold_fn(acc, i):
            acc.append(i['payload'])
            return acc
        is1 = IntervalSet([
            Interval(Bounds3D(0,1,0,0.1,0,1),1),
            Interval(Bounds3D(0,0.5,0.5,1,0,0.5),2),
            Interval(Bounds3D(0.1,0.4,0,1,0,0.5),3),
            ])
        self.assertListEqual(is1.fold(fold_fn, [], sortkey),
                [1,3,2])

    def test_group_by(self):
        is1 = IntervalSet([
            Interval(Bounds3D(0,1)),
            Interval(Bounds3D(1,2)),
            Interval(Bounds3D(0,2)),
            Interval(Bounds3D(1,3)),
            ])

        def merge_intervals(k, intervals):
            merged = intervals.fold(
                lambda i1, i2: Interval(i1['bounds'].span(i2['bounds'])))
            merged['payload'] = intervals
            return merged

        is2 = is1.group_by(lambda i: int(i['t2']) % 2, merge_intervals)
        target = IntervalSet([
            Interval(Bounds3D(0,3), payload = IntervalSet([
                Interval(Bounds3D(0,1)),
                Interval(Bounds3D(1,3)),])),
            Interval(Bounds3D(0,2), payload = IntervalSet([
                Interval(Bounds3D(0,2)),
                Interval(Bounds3D(1,2)),])),
            ])
        self.assertIntervalSetEq(is2, target,
                self.compare_interval_sets_in_payload())

    def test_minus(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1, 10,0,0.5,0.2,0.8),1),
            Interval(Bounds3D(3, 15,0,1,0,1),2)])
        is2 = IntervalSet([
            Interval(Bounds3D(2,2.5)),
            Interval(Bounds3D(2,2.7)),
            Interval(Bounds3D(2.9,3.5)),
            Interval(Bounds3D(3.5,3.6)),
            Interval(Bounds3D(5,7)),
            Interval(Bounds3D(9,12)),
            ])
        is3 = is1.minus(is2)
        target = IntervalSet([
            Interval(Bounds3D(1,2,0,0.5,0.2,0.8),1),
            Interval(Bounds3D(2.7, 2.9, 0,0.5,0.2,0.8),1),
            Interval(Bounds3D(3.6,5,0,0.5,0.2,0.8),1),
            Interval(Bounds3D(7,9,0,0.5,0.2,0.8),1),
            Interval(Bounds3D(3.6,5), payload=2),
            Interval(Bounds3D(7,9), payload=2),
            Interval(Bounds3D(12,15), payload=2),
            ])
        self.assertIntervalSetEq(is3, target, eq)

    def test_minus_everything(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1, 10)),
            Interval(Bounds3D(3, 15))])
        is2 = IntervalSet([
            Interval(Bounds3D(2,2.5)),
            Interval(Bounds3D(2,2.7)),
            Interval(Bounds3D(2.9,3.5)),
            Interval(Bounds3D(3.5,3.6)),
            Interval(Bounds3D(5,7)),
            Interval(Bounds3D(9,12)),
            ])
        is3 = is2.minus(is1)
        self.assertIntervalSetEq(is3, IntervalSet([]), eq)

    def test_minus_self(self):
        is1 = IntervalSet([
            Interval(Bounds3D(2,2.5)),
            Interval(Bounds3D(2,2.7)),
            Interval(Bounds3D(2.9,3.5)),
            Interval(Bounds3D(3.5,3.6)),
            Interval(Bounds3D(5,7)),
            Interval(Bounds3D(9,12)),
            ])
        is1 = is1.minus(is1)
        self.assertIntervalSetEq(is1, IntervalSet([]), eq)

    def test_minus_against_nothing(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1, 10,0,0.5,0.2,0.8),1),
            Interval(Bounds3D(3, 15,0,1,0,1),2)])
        is2 = IntervalSet([
            Interval(Bounds3D(20,20.5)),
            Interval(Bounds3D(20,20.7)),
            Interval(Bounds3D(20.9,23.5)),
            Interval(Bounds3D(23.5,23.6)),
            Interval(Bounds3D(25,27)),
            Interval(Bounds3D(29,32)),
            ])
        is3 = is1.minus(is2)
        self.assertIntervalSetEq(is3, is1, eq)

    def test_minus_with_single_frame(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1,1)),
            Interval(Bounds3D(3,3)),
            Interval(Bounds3D(4,4)),
            Interval(Bounds3D(7,7)),
            Interval(Bounds3D(10,10)),
            ])
        is2 = IntervalSet([
            Interval(Bounds3D(1,3)),
            Interval(Bounds3D(5,8)),
            Interval(Bounds3D(9,9)),
            ])
        is3 = is1.minus(is2)
        target = IntervalSet([
            Interval(Bounds3D(4,4)),
            Interval(Bounds3D(10,10)),
            ])
        self.assertIntervalSetEq(is3, target)

        is4 = is2.minus(is1)
        self.assertIntervalSetEq(is4, is2)

    def test_match(self):
        left_box_frame_1 = Interval(Bounds3D(1,1,0.1,0.4,0.4,0.8))
        right_box_frame_1 = Interval(Bounds3D(1,1,0.6,0.9,0.3,0.7))
        bottom_left_box_frame_2 = Interval(Bounds3D(2,2,0.1,0.3,0.8,0.9))
        top_right_box_frame_2 = Interval(Bounds3D(2,2,0.5,0.7,0.2,0.7))
        is1 = IntervalSet([
            left_box_frame_1,
            right_box_frame_1,
            bottom_left_box_frame_2,
            top_right_box_frame_2])

        pattern = [
                (["left","right"],[
                    # Two boxes on left and right on same frame
                    Bounds3D.T(equal()),
                    Bounds3D.XY(left_of()),
                ]),
                (["top", "bottom"], [
                    # Two boxes on top and bottom on overlapping frame
                    Bounds3D.T(equal()),
                    Bounds3D.XY(above()),
                ]),
                (["left", "top"],[
                    # Left-Right pattern comes before Top-Bottom
                    Bounds3D.T(meets_before(epsilon=1))
                ])
        ]
        results = is1.match(pattern, exact=True)
        self.assertEqual(len(results),1)
        result = results[0]
        self.assertIntervalsEq(left_box_frame_1, result['left'])
        self.assertIntervalsEq(right_box_frame_1, result['right'])
        self.assertIntervalsEq(top_right_box_frame_2, result['top'])
        self.assertIntervalsEq(bottom_left_box_frame_2, result['bottom'])

    def test_match_multiple_solutions(self):
        left_box_frame_1 = Interval(Bounds3D(1,1,0.1,0.4,0.4,0.8))
        right_box_frame_1 = Interval(Bounds3D(1,1,0.6,0.9,0.3,0.7))
        bottom_left_box_frame_2 = Interval(Bounds3D(2,2,0.1,0.3,0.8,0.9))
        top_right_box_frame_2 = Interval(Bounds3D(2,2,0.5,0.7,0.2,0.7))
        is1 = IntervalSet([
            left_box_frame_1,
            right_box_frame_1,
            bottom_left_box_frame_2,
            top_right_box_frame_2,
            Interval(Bounds3D(3,3)),
            ])

        pattern = [
                (["left","right"],[
                    # Two boxes on left and right on same frame
                    Bounds3D.T(equal()),
                    Bounds3D.XY(left_of()),
                ]),
        ]
        results = is1.match(pattern, exact=False)
        self.assertEqual(len(results),2)

        # Add single interval constraints.
        pattern = pattern + [
                (["left"], [Bounds3D.XY(height_at_least(0.3))]),
                (["right"], [Bounds3D.XY(height_at_least(0.3))]),
                ]
        results = is1.match(pattern, exact=False)
        self.assertEqual(len(results),1)
        result = results[0]
        self.assertIntervalsEq(left_box_frame_1, result['left'])
        self.assertIntervalsEq(right_box_frame_1, result['right'])

    def test_filter_against(self):
        is1 = IntervalSet([
            Interval(Bounds3D(0,1)),
            Interval(Bounds3D(2,5)),
            Interval(Bounds3D(6,7)),
            ])
        is2 = IntervalSet([
            Interval(Bounds3D(1,1)),
            Interval(Bounds3D(7,7)),
            ])
        # Take only intervals in is1 that overlaps with some interval in is2
        is3 = is1.filter_against(is2, Bounds3D.T(overlaps()), window=0)
        self.assertIntervalSetEq(is3, IntervalSet([
            Interval(Bounds3D(0,1)),Interval(Bounds3D(6,7))
        ]))

    def test_map_payload(self):
        is1 = IntervalSet([
            Interval(Bounds3D(0,1),payload=1),
            Interval(Bounds3D(2,3),payload=2),
            Interval(Bounds3D(4,5),payload=3),
            ])
        is2 = is1.map_payload(lambda p: p+10)
        target = IntervalSet([
            Interval(Bounds3D(0,1),payload=11),
            Interval(Bounds3D(2,3),payload=12),
            Interval(Bounds3D(4,5),payload=13),
            ])
        self.assertIntervalSetEq(is2, target)

    def test_dilate(self):
        is1 = IntervalSet([
            Interval(Bounds3D(2,5)),
            Interval(Bounds3D(6,7)),
        ])
        is2 = is1.dilate(1)
        target = IntervalSet([
            Interval(Bounds3D(1,6)),
            Interval(Bounds3D(5,8)),
        ])
        self.assertIntervalSetEq(is2, target)
        self.assertIntervalSetEq(is2.dilate(-1), is1)
        is3 = is1.dilate(-0.1, axis=('x1', 'x2'))
        target = IntervalSet([
            Interval(Bounds3D(2,5,0.1, 0.9,0,1)),
            Interval(Bounds3D(6,7,0.1, 0.9,0,1)),
            ])
        self.assertIntervalSetEq(is3, target)

    def test_filter_size(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1,2,0.5,0.9,0.1,0.2)),
            Interval(Bounds3D(20,30,0.4,1.0, 0,1)),
            Interval(Bounds3D(50,55,0.2,0.3, 0.5,0.9)),
            ])
        is2 = is1.filter_size(min_size=10)
        self.assertIntervalSetEq(is2, IntervalSet([
            Interval(Bounds3D(20,30,0.4,1.0,0,1))]))

        is3 = is1.filter_size(max_size=0.1, axis=('y1', 'y2'))
        self.assertIntervalSetEq(is3, IntervalSet([
            Interval(Bounds3D(1,2,0.5,0.9,0.1,0.2))]))

        is4 = is1.filter_size(min_size=0.3, max_size=0.5, axis=('x1', 'x2'))
        self.assertIntervalSetEq(is3, IntervalSet([
            Interval(Bounds3D(1,2,0.5,0.9,0.1,0.2))]))

    def test_group_by_axis(self):
        default_bounds = Bounds3D(0, 1, 0, 1, 0, 1)
        intervals_1 = [
            Interval(Bounds3D(1,1, 0.4,0.5,0.6,0.8), 1),
            Interval(Bounds3D(1,1, 0.1,0.2,0.2,0.3), 2),
            Interval(Bounds3D(1,1, 0.3,0.5,0.1,0.5), 3),
        ]
        intervals_2 = [
            Interval(Bounds3D(2,2, 0.3,0.5,0.6,0.8), 11),
            Interval(Bounds3D(2,2, 0.2,0.3,0.2,0.9), 12),
            Interval(Bounds3D(2,2, 0.3,0.7,0,0.5), 13),
        ]
        is1 = IntervalSet(intervals_1+intervals_2)
        target = IntervalSet([
            Interval(Bounds3D(1,1), payload=IntervalSet(intervals_1)),
            Interval(Bounds3D(2,2), payload=IntervalSet(intervals_2)),
            ])
        self.assertIntervalSetEq(
                is1.group_by_axis(('t1', 't2'), default_bounds), target,
                self.compare_interval_sets_in_payload())

    def test_collect_by_interval(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1,5)),
            Interval(Bounds3D(10,50)),
            Interval(Bounds3D(51,52)),
            ])
        intervals = [
            Interval(Bounds3D(2,2, 0.4,0.6,0.1,0.2), 1),
            Interval(Bounds3D(3,3, 0.1,0.6,0.8,0.9), 2),
            Interval(Bounds3D(11,23, 0,1,0.8,0.9), 3),
            ]
        is2 = IntervalSet(intervals)

        is3 = is1.collect_by_interval(is2, Bounds3D.T(during_inv()),
                filter_empty=True, window=None).map_payload(lambda p:p[1])
        target = IntervalSet([
            Interval(Bounds3D(1,5), payload=IntervalSet(intervals[:2])),
            Interval(Bounds3D(10,50), payload=IntervalSet(intervals[2:3])),
            ])
        self.assertIntervalSetEq(is3, target,
                self.compare_interval_sets_in_payload())

        is4 = is1.collect_by_interval(is2, Bounds3D.T(overlaps()),
                filter_empty=False, window=0).map_payload(lambda p:p[1])
        target = IntervalSet([
            Interval(Bounds3D(1,5), payload=IntervalSet(intervals[:2])),
            Interval(Bounds3D(10,50), payload=IntervalSet(intervals[2:3])),
            Interval(Bounds3D(51,52), payload=IntervalSet([])),
            ])
        self.assertIntervalSetEq(is4, target,
                self.compare_interval_sets_in_payload())

    def test_coalesce(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1,10,0.3,0.4,0.5,0.6),1),
            Interval(Bounds3D(2,5,0.2,0.8,0.2,0.3),1),
            Interval(Bounds3D(10,11,0.2,0.7,0.3,0.5),1),
            Interval(Bounds3D(13,15,0.5,1,0,0.5),1),
            Interval(Bounds3D(15,19,0.5,1,0,0.5),1),
            Interval(Bounds3D(20,20),payload=1),
            Interval(Bounds3D(22,22),payload=1),
            Interval(Bounds3D(22,23),payload=1),
            ])
        target = IntervalSet([
            Interval(Bounds3D(1,11,0.2,0.8,0.2,0.6),3),
            Interval(Bounds3D(13,19,0.5,1,0,0.5),2),
            Interval(Bounds3D(20,20),payload=1),
            Interval(Bounds3D(22,23),payload=2),
            ])
        self.assertIntervalSetEq(is1.coalesce(('t1', 't2'), Bounds3D.span,
            payload_plus), target)
        self.assertIntervalSetEq(is1.coalesce(('t1', 't2'), Bounds3D.span,
            payload_plus,epsilon=2),
            IntervalSet([Interval(Bounds3D(1,23),payload=8)]))

    def test_split(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1,5)),
            Interval(Bounds3D(10,22)),
            ])
        def split_fn(i):
            output = []
            t = i.copy()
            while t['bounds'].length() > 5:
                output.append(Interval(Bounds3D(t['t1'],t['t1']+5)))
                t['t1'] = t['t1'] + 5
            if t['bounds'].length() > 0:
                output.append(t)
            return IntervalSet(output)
        target = IntervalSet([
            Interval(Bounds3D(1,5)),
            Interval(Bounds3D(10,15)),
            Interval(Bounds3D(15,20)),
            Interval(Bounds3D(20,22)),
            ])
        self.assertIntervalSetEq(target, is1.split(split_fn))

    def test_len(self):
        is1 = IntervalSet([
            Interval(Bounds3D(1,5)),
            Interval(Bounds3D(10,22)),
            ])
        self.assertEqual(len(is1), 2)
        self.assertEqual(is1.size(), 2)
