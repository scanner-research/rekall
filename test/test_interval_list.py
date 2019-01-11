from rekall.temporal_predicates import *
from rekall.merge_ops import *
from rekall.interval_list import Interval, IntervalList
import unittest

class TestInterval(unittest.TestCase):
    def test_minus(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)
        
        intrvl1minusintrvl2 = intrvl1.minus(intrvl2)

        self.assertEqual(len(intrvl1minusintrvl2), 1)
        self.assertEqual(intrvl1minusintrvl2[0].__repr__(),
                "<Interval start:1.0 end:1.5 payload:1>")

        intrvl1minusintrvl2 = intrvl1.minus(intrvl2, payload_merge_op=payload_second)

        self.assertEqual(len(intrvl1minusintrvl2), 1)
        self.assertEqual(intrvl1minusintrvl2[0].__repr__(),
                "<Interval start:1.0 end:1.5 payload:2>")

        intrvl3 = Interval(1.3, 1.6, 5)

        intrvl1minusintrvl3 = intrvl1.minus(intrvl3)
        self.assertEqual(len(intrvl1minusintrvl3), 2)
        self.assertEqual(intrvl1minusintrvl3[0].__repr__(),
                "<Interval start:1.0 end:1.3 payload:1>")
        self.assertEqual(intrvl1minusintrvl3[1].__repr__(),
                "<Interval start:1.6 end:2.0 payload:1>")

        intrvl4 = Interval(0., 5., 1)
        self.assertEqual(intrvl1.minus(intrvl4), [])

    def test_overlap(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)
        
        self.assertEqual(intrvl1.overlap(intrvl2).__repr__(),
                "<Interval start:1.5 end:2.0 payload:1>")
        self.assertEqual(intrvl1.overlap(intrvl2,
            payload_merge_op=payload_second).__repr__(),
                "<Interval start:1.5 end:2.0 payload:2>")

        intrvl3 = Interval(1.3, 1.6, 5)

        self.assertEqual(intrvl1.overlap(intrvl3).__repr__(),
                "<Interval start:1.3 end:1.6 payload:1>")

        intrvl4 = Interval(0., 5., 1)

        self.assertEqual(intrvl1.overlap(intrvl4).__repr__(),
                "<Interval start:1.0 end:2.0 payload:1>")

    def test_merge(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)
        
        self.assertEqual(intrvl1.merge(intrvl2).__repr__(),
                "<Interval start:1.0 end:2.5 payload:1>")
        self.assertEqual(intrvl1.merge(intrvl2,
            payload_merge_op=payload_second).__repr__(),
                "<Interval start:1.0 end:2.5 payload:2>")

        intrvl3 = Interval(5., 7., 1)
        self.assertEqual(intrvl1.merge(intrvl3).__repr__(),
                "<Interval start:1.0 end:7.0 payload:1>")

        intrvl4 = Interval(0., 0.5, 1)
        self.assertEqual(intrvl1.merge(intrvl4).__repr__(),
                "<Interval start:0.0 end:2.0 payload:1>")

class IntervalListTest(unittest.TestCase):
    def test_init(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)

        intrvls1 = IntervalList([intrvl1, intrvl2])
        self.assertEqual(intrvls1.intrvls[0].__repr__(),
                "<Interval start:1.0 end:2.0 payload:1>")
        self.assertEqual(intrvls1.intrvls[1].__repr__(),
                "<Interval start:1.5 end:2.5 payload:2>")

        intrvls1 = IntervalList([intrvl2, intrvl1])
        self.assertEqual(intrvls1.intrvls[0].__repr__(),
                "<Interval start:1.0 end:2.0 payload:1>")
        self.assertEqual(intrvls1.intrvls[1].__repr__(),
                "<Interval start:1.5 end:2.5 payload:2>")

    def test_set_union(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)

        intrvls1 = IntervalList([intrvl1])
        intrvls2 = IntervalList([intrvl2])

        intrvlsu = intrvls1.set_union(intrvls2)
        self.assertEqual(intrvlsu.intrvls[0].__repr__(),
                "<Interval start:1.0 end:2.0 payload:1>")
        self.assertEqual(intrvlsu.intrvls[1].__repr__(),
                "<Interval start:1.5 end:2.5 payload:2>")

        intrvlsu = intrvls2.set_union(intrvls1)
        self.assertEqual(intrvlsu.intrvls[0].__repr__(),
                "<Interval start:1.0 end:2.0 payload:1>")
        self.assertEqual(intrvlsu.intrvls[1].__repr__(),
                "<Interval start:1.5 end:2.5 payload:2>")

    def test_coalesce(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)

        intrvls1 = IntervalList([intrvl1, intrvl2])
        intrvlscoalesced = intrvls1.coalesce()

        self.assertEqual(len(intrvlscoalesced.intrvls), 1)
        self.assertEqual(intrvlscoalesced.intrvls[0].__repr__(),
                "<Interval start:1.0 end:2.5 payload:1>")

        intrvlscoalesced_samepayload = intrvls1.coalesce(
            predicate=lambda i1, i2: i1.payload == i2.payload)
        self.assertEqual(len(intrvlscoalesced_samepayload.intrvls), 2)
        self.assertEqual(intrvlscoalesced_samepayload.intrvls[0].__repr__(),
                "<Interval start:1.0 end:2.0 payload:1>")
        self.assertEqual(intrvlscoalesced_samepayload.intrvls[1].__repr__(),
                "<Interval start:1.5 end:2.5 payload:2>")

    def test_dilate(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)

        intrvls1 = IntervalList([intrvl1, intrvl2]).dilate(0.2)
        self.assertEqual(intrvls1.intrvls[0].__repr__(),
                "<Interval start:0.8 end:2.2 payload:1>")
        self.assertEqual(intrvls1.intrvls[1].__repr__(),
                "<Interval start:1.3 end:2.7 payload:2>")

    def test_filter(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 2.5, 2)

        intrvls1 = IntervalList([intrvl1, intrvl2])

        intrvls1 = intrvls1.filter(lambda intrvl: intrvl.start > 1.1)
        self.assertEqual(len(intrvls1.intrvls), 1)
        self.assertEqual(intrvls1.intrvls[0].__repr__(),
                "<Interval start:1.5 end:2.5 payload:2>")

    def test_filter_length(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1.5, 3.5, 2)

        intrvls1 = IntervalList([intrvl1, intrvl2])

        intrvls1 = intrvls1.filter_length(min_length=1.1)
        self.assertEqual(len(intrvls1.intrvls), 1)
        self.assertEqual(intrvls1.intrvls[0].__repr__(),
                "<Interval start:1.5 end:3.5 payload:2>")

        intrvls1 = intrvls1.filter_length(max_length=1.8)
        self.assertEqual(len(intrvls1.intrvls), 0)

    def test_filter_against(self):
        intrvl_long1 = Interval(1., 10., 1)
        intrvl_long2 = Interval(3., 15., 2)

        intrvlshort1 = Interval(2., 2.5, 3)
        intrvlshort2 = Interval(2., 2.7, 4)
        intrvlshort3 = Interval(2.9, 3.5, 5)

        intrvlslong = IntervalList([intrvl_long2, intrvl_long1])
        intrvlsshort = IntervalList([
            intrvlshort1,
            intrvlshort2,
            intrvlshort3])

        intrvlsfiltered = intrvlslong.filter_against(intrvlsshort, predicate=during_inv())
        self.assertEqual(len(intrvlsfiltered.intrvls), 1)
        self.assertEqual(intrvlsfiltered.intrvls[0].__repr__(), intrvl_long1.__repr__())

    def test_minus(self):
        intrvl_long1 = Interval(1., 10., 1)
        intrvl_long2 = Interval(3., 15., 2)

        intrvlshort1 = Interval(2., 2.5, 3)
        intrvlshort2 = Interval(2., 2.7, 4)
        intrvlshort3 = Interval(2.9, 3.5, 5)
        intrvlshort4 = Interval(5., 7., 6)
        intrvlshort5 = Interval(9., 12., 7)
        intrvlshort6 = Interval(14., 16., 8)

        intrvlslong = IntervalList([intrvl_long2, intrvl_long1])
        intrvlsshort = IntervalList([
            intrvlshort2,
            intrvlshort5,
            intrvlshort3,
            intrvlshort1,
            intrvlshort4,
            intrvlshort6])

        intrvlsminusrec = intrvlslong.minus(intrvlsshort)
        self.assertEqual(len(intrvlsminusrec.intrvls), 7)
        self.assertEqual(intrvlsminusrec.intrvls[0].__repr__(),
            "<Interval start:1.0 end:2.0 payload:1>")
        self.assertEqual(intrvlsminusrec.intrvls[1].__repr__(),
            "<Interval start:2.7 end:2.9 payload:1>")
        self.assertEqual(intrvlsminusrec.intrvls[2].__repr__(),
            "<Interval start:3.5 end:5.0 payload:1>")
        self.assertEqual(intrvlsminusrec.intrvls[3].__repr__(),
            "<Interval start:3.5 end:5.0 payload:2>")
        self.assertEqual(intrvlsminusrec.intrvls[4].__repr__(),
            "<Interval start:7.0 end:9.0 payload:1>")
        self.assertEqual(intrvlsminusrec.intrvls[5].__repr__(),
            "<Interval start:7.0 end:9.0 payload:2>")
        self.assertEqual(intrvlsminusrec.intrvls[6].__repr__(),
            "<Interval start:12.0 end:14.0 payload:2>")

        intrvlsminusnonrec = intrvlslong.minus(intrvlsshort, recursive_diff = False)
        self.assertEqual(len(intrvlsminusnonrec.intrvls), 15)

        intrvlsminusrec = intrvlslong.minus(intrvlsshort,
                payload_merge_op=payload_second)
        self.assertEqual(len(intrvlsminusrec.intrvls), 7)
        self.assertEqual(intrvlsminusrec.intrvls[0].__repr__(),
            "<Interval start:1.0 end:2.0 payload:3>")
        self.assertEqual(intrvlsminusrec.intrvls[1].__repr__(),
            "<Interval start:2.7 end:2.9 payload:4>")
        self.assertEqual(intrvlsminusrec.intrvls[2].__repr__(),
            "<Interval start:3.5 end:5.0 payload:5>")
        self.assertEqual(intrvlsminusrec.intrvls[3].__repr__(),
            "<Interval start:3.5 end:5.0 payload:5>")
        self.assertEqual(intrvlsminusrec.intrvls[4].__repr__(),
            "<Interval start:7.0 end:9.0 payload:6>")
        self.assertEqual(intrvlsminusrec.intrvls[5].__repr__(),
            "<Interval start:7.0 end:9.0 payload:6>")
        self.assertEqual(intrvlsminusrec.intrvls[6].__repr__(),
            "<Interval start:12.0 end:14.0 payload:7>")

        intrvlsminusrec = intrvlslong.minus(intrvlsshort, predicate=overlaps_before())
        self.assertEqual(len(intrvlsminusrec.intrvls), 2)
        self.assertEqual(intrvlsminusrec.intrvls[0].__repr__(),
            "<Interval start:1.0 end:9.0 payload:1>")
        self.assertEqual(intrvlsminusrec.intrvls[1].__repr__(),
            "<Interval start:3.0 end:14.0 payload:2>")

    def test_minus_against_nothing(self):
        intrvl_long1 = Interval(1., 10., 1)
        intrvl_long2 = Interval(3., 15., 2)
        
        intrvlslong = IntervalList([intrvl_long2, intrvl_long1])

        intrvlshort1 = Interval(20., 20.5, 3)
        intrvlsshort = IntervalList([intrvlshort1])

        intrvlsminusrec = intrvlslong.minus(intrvlsshort)
        self.assertEqual(len(intrvlsminusrec.intrvls), 2)
        self.assertEqual(intrvlsminusrec.intrvls[0].__repr__(),
            "<Interval start:1.0 end:10.0 payload:1>")
        self.assertEqual(intrvlsminusrec.intrvls[1].__repr__(),
            "<Interval start:3.0 end:15.0 payload:2>")


    def test_overlaps(self):
        intrvla1 = Interval(1., 25., 1)
        intrvla2 = Interval(52., 55., 1)
        intrvla3 = Interval(100., 110., 1)
        intrvla4 = Interval(200., 210., 1)
        intrvlb1 = Interval(12., 26., 2)
        intrvlb2 = Interval(50., 53., 2)
        intrvlb3 = Interval(101., 105., 2)
        intrvlb4 = Interval(190., 220., 2)

        intrvlsa = IntervalList([intrvla2, intrvla3, intrvla1, intrvla4])
        intrvlsb = IntervalList([intrvlb2, intrvlb3, intrvlb1, intrvlb4])

        intrvlsoverlap = intrvlsa.overlaps(intrvlsb)
        self.assertEqual(len(intrvlsoverlap.intrvls), 4)
        self.assertEqual(intrvlsoverlap.intrvls[0].__repr__(),
                "<Interval start:12.0 end:25.0 payload:1>")
        self.assertEqual(intrvlsoverlap.intrvls[1].__repr__(),
                "<Interval start:52.0 end:53.0 payload:1>")
        self.assertEqual(intrvlsoverlap.intrvls[2].__repr__(),
                "<Interval start:101.0 end:105.0 payload:1>")
        self.assertEqual(intrvlsoverlap.intrvls[3].__repr__(),
                "<Interval start:200.0 end:210.0 payload:1>")

        intrvlsoverlap = intrvlsb.overlaps(intrvlsa)
        self.assertEqual(len(intrvlsoverlap.intrvls), 4)
        self.assertEqual(intrvlsoverlap.intrvls[0].__repr__(),
                "<Interval start:12.0 end:25.0 payload:2>")
        self.assertEqual(intrvlsoverlap.intrvls[1].__repr__(),
                "<Interval start:52.0 end:53.0 payload:2>")
        self.assertEqual(intrvlsoverlap.intrvls[2].__repr__(),
                "<Interval start:101.0 end:105.0 payload:2>")
        self.assertEqual(intrvlsoverlap.intrvls[3].__repr__(),
                "<Interval start:200.0 end:210.0 payload:2>")

        intrvlsoverlap = intrvlsa.overlaps(intrvlsb, predicate=overlaps_before())
        self.assertEqual(len(intrvlsoverlap.intrvls), 1)
        self.assertEqual(intrvlsoverlap.intrvls[0].__repr__(),
                "<Interval start:12.0 end:25.0 payload:1>")

        intrvlsoverlap = intrvlsa.overlaps(intrvlsb, predicate=overlaps_before(),
                payload_merge_op=payload_second)
        self.assertEqual(len(intrvlsoverlap.intrvls), 1)
        self.assertEqual(intrvlsoverlap.intrvls[0].__repr__(),
                "<Interval start:12.0 end:25.0 payload:2>")

    def test_merge(self):
        intrvla1 = Interval(1., 25., 1)
        intrvla2 = Interval(52., 55., 1)
        intrvla3 = Interval(100., 110., 1)
        intrvla4 = Interval(200., 210., 1)
        intrvlb1 = Interval(12., 26., 2)
        intrvlb2 = Interval(50., 53., 2)
        intrvlb3 = Interval(101., 105., 2)
        intrvlb4 = Interval(190., 220., 2)

        intrvlsa = IntervalList([intrvla2, intrvla3, intrvla1, intrvla4])
        intrvlsb = IntervalList([intrvlb2, intrvlb3, intrvlb1, intrvlb4])

        intrvlsmerge = intrvlsa.merge(intrvlsb, predicate=overlaps())
        self.assertEqual(len(intrvlsmerge.intrvls), 4)
        self.assertEqual(intrvlsmerge.intrvls[0].__repr__(),
                "<Interval start:1.0 end:26.0 payload:1>")
        self.assertEqual(intrvlsmerge.intrvls[1].__repr__(),
                "<Interval start:50.0 end:55.0 payload:1>")
        self.assertEqual(intrvlsmerge.intrvls[2].__repr__(),
                "<Interval start:100.0 end:110.0 payload:1>")
        self.assertEqual(intrvlsmerge.intrvls[3].__repr__(),
                "<Interval start:190.0 end:220.0 payload:1>")

        intrvlsmerge = intrvlsb.merge(intrvlsa, predicate=overlaps())
        self.assertEqual(len(intrvlsmerge.intrvls), 4)
        self.assertEqual(intrvlsmerge.intrvls[0].__repr__(),
                "<Interval start:1.0 end:26.0 payload:2>")
        self.assertEqual(intrvlsmerge.intrvls[1].__repr__(),
                "<Interval start:50.0 end:55.0 payload:2>")
        self.assertEqual(intrvlsmerge.intrvls[2].__repr__(),
                "<Interval start:100.0 end:110.0 payload:2>")
        self.assertEqual(intrvlsmerge.intrvls[3].__repr__(),
                "<Interval start:190.0 end:220.0 payload:2>")

        intrvlsmerge = intrvlsa.merge(intrvlsb, predicate=overlaps_before())
        self.assertEqual(len(intrvlsmerge.intrvls), 1)
        self.assertEqual(intrvlsmerge.intrvls[0].__repr__(),
                "<Interval start:1.0 end:26.0 payload:1>")

        intrvlsmerge = intrvlsa.merge(intrvlsb, predicate=overlaps_before(),
                payload_merge_op=payload_second)
        self.assertEqual(len(intrvlsmerge.intrvls), 1)
        self.assertEqual(intrvlsmerge.intrvls[0].__repr__(),
                "<Interval start:1.0 end:26.0 payload:2>")

        intrvla1 = Interval(1., 25., 1)
        intrvla2 = Interval(52., 55., 1)
        intrvla3 = Interval(100., 110., 1)
        intrvla4 = Interval(200., 210., 1)
        intrvlb1 = Interval(25., 31., 2)
        intrvlb2 = Interval(56., 90., 2)
        intrvlb3 = Interval(101., 105., 2)
        intrvlb4 = Interval(190., 220., 2)

        intrvlsa = IntervalList([intrvla2, intrvla3, intrvla1, intrvla4])
        intrvlsb = IntervalList([intrvlb2, intrvlb3, intrvlb1, intrvlb4])

        intrvlsmerge = intrvlsa.merge(intrvlsb, predicate=meets_before())
        self.assertEqual(len(intrvlsmerge.intrvls), 1)
        self.assertEqual(intrvlsmerge.intrvls[0].__repr__(),
                "<Interval start:1.0 end:31.0 payload:1>")

        intrvlsmerge = intrvlsa.merge(intrvlsb, predicate=before(0.1, 10.0))
        self.assertEqual(len(intrvlsmerge.intrvls), 1)
        self.assertEqual(intrvlsmerge.intrvls[0].__repr__(),
                "<Interval start:52.0 end:90.0 payload:1>")

    def test_map(self):
        intrvla1 = Interval(1., 25., 1)
        intrvla2 = Interval(52., 55., 1)
        intrvla3 = Interval(100., 110., 1)
        intrvla4 = Interval(200., 210., 1)

        intrvlsa = IntervalList([intrvla2, intrvla3, intrvla1, intrvla4])

        intrvlsa = intrvlsa.map(lambda intrvl: Interval(
            intrvl.start + 1, intrvl.end + 1, intrvl.payload))

        self.assertEqual(len(intrvlsa.intrvls), 4)
        self.assertEqual(intrvlsa.intrvls[0].__repr__(),
                "<Interval start:2.0 end:26.0 payload:1>")
        self.assertEqual(intrvlsa.intrvls[1].__repr__(),
                "<Interval start:53.0 end:56.0 payload:1>")
        self.assertEqual(intrvlsa.intrvls[2].__repr__(),
                "<Interval start:101.0 end:111.0 payload:1>")
        self.assertEqual(intrvlsa.intrvls[3].__repr__(),
                "<Interval start:201.0 end:211.0 payload:1>")

    def test_join(self):
        intrvla1 = Interval(1., 25., 1)
        intrvla2 = Interval(52., 55., 1)
        intrvla3 = Interval(100., 110., 1)
        intrvla4 = Interval(200., 210., 1)
        intrvlb1 = Interval(12., 26., 2)
        intrvlb2 = Interval(50., 53., 2)
        intrvlb3 = Interval(101., 105., 2)
        intrvlb4 = Interval(190., 220., 2)

        intrvlsa = IntervalList([intrvla2, intrvla3, intrvla1, intrvla4])
        intrvlsb = IntervalList([intrvlb2, intrvlb3, intrvlb1, intrvlb4])

        def predicate(x, y):
            return x.start == 1. and y.start == 12.

        def merge_op(x, y):
            return [Interval(1., 100., 25)]

        intrvlsudf = intrvlsa.join(intrvlsb, merge_op=merge_op, predicate=predicate)
        self.assertEqual(len(intrvlsudf.intrvls), 1)
        self.assertEqual(intrvlsudf.intrvls[0].__repr__(),
                "<Interval start:1.0 end:100.0 payload:25>")

    def test_fold(self):
        intrvla1 = Interval(1., 25., 1)
        intrvla2 = Interval(52., 55., 1)
        intrvla3 = Interval(100., 110., 1)
        intrvla4 = Interval(200., 210., 1)

        intrvlsa = IntervalList([intrvla2, intrvla3, intrvla1, intrvla4])

        total_payload = intrvlsa.fold(
                lambda acc, intrvl: acc + intrvl.payload, 0.)
        self.assertEqual(total_payload, 4)

        total_length = intrvlsa.fold(
                lambda acc, intrvl: acc + (intrvl.end - intrvl.start), 0.)
        self.assertEqual(total_length, 47.0)

        def fold_fn(acc, intrvl):
            acc.append(intrvl)
            return acc

        intrvlsa = IntervalList(intrvlsa.fold(fold_fn, []))
        self.assertEqual(len(intrvlsa.intrvls), 4)
        self.assertEqual(intrvlsa.intrvls[0].__repr__(),
                "<Interval start:1.0 end:25.0 payload:1>")
        self.assertEqual(intrvlsa.intrvls[1].__repr__(),
                "<Interval start:52.0 end:55.0 payload:1>")
        self.assertEqual(intrvlsa.intrvls[2].__repr__(),
                "<Interval start:100.0 end:110.0 payload:1>")
        self.assertEqual(intrvlsa.intrvls[3].__repr__(),
                "<Interval start:200.0 end:210.0 payload:1>")

