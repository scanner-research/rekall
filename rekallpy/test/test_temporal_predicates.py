from rekall.temporal_predicates import *
from rekall.interval_list import Interval
import unittest

class TestTemporalPredicates(unittest.TestCase):
    def test_before(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(3., 4., 1)
        self.assertTrue(before()(intrvl1, intrvl2))
        self.assertFalse(before()(intrvl2, intrvl1))

        intrvl3 = Interval(2., 4., 1)
        self.assertTrue(before()(intrvl1, intrvl3))

    def test_before_range(self):
        pred = before(10., 20.)
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(3., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(12., 15., 1)
        intrvl4 = Interval(22.5, 25., 1)
        self.assertTrue(pred(intrvl1, intrvl3))
        self.assertFalse(pred(intrvl1, intrvl4))
        self.assertTrue(pred(intrvl2, intrvl4))

    def test_after(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(3., 4., 1)
        self.assertFalse(after()(intrvl1, intrvl2))
        self.assertTrue(after()(intrvl2, intrvl1))

        intrvl3 = Interval(2., 4., 1)
        self.assertTrue(after()(intrvl3, intrvl1))
            
    def test_after_range(self):
        pred = after(10., 20.)
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(3., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(12., 15., 1)
        intrvl4 = Interval(22.5, 25., 1)
        self.assertTrue(pred(intrvl3, intrvl1))
        self.assertFalse(pred(intrvl4, intrvl1))
        self.assertTrue(pred(intrvl4, intrvl2))

    def test_overlaps(self):
        pred = overlaps()
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(3., 4., 1)

        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))

        intrvl3 = Interval(3.5, 4.5, 1)
        intrvl4 = Interval(2.5, 3.5, 1)
        self.assertTrue(pred(intrvl2, intrvl3))
        self.assertTrue(pred(intrvl2, intrvl4))

        intrvl5 = Interval(2.5, 4.5, 1)
        intrvl6 = Interval(3.3, 3.6, 1)
        self.assertTrue(pred(intrvl2, intrvl5))
        self.assertTrue(pred(intrvl2, intrvl6))

        intrvl7 = Interval(3., 3.5, 1)
        intrvl8 = Interval(3., 4.5, 1)
        self.assertTrue(pred(intrvl2, intrvl7))
        self.assertTrue(pred(intrvl2, intrvl8))

        intrvl9 = Interval(3.3, 4., 1)
        intrvl10 = Interval(2.1, 4., 1)
        self.assertTrue(pred(intrvl2, intrvl9))
        self.assertTrue(pred(intrvl2, intrvl10))

        intrvl11 = Interval(2.3, 3., 1)
        intrvl12 = Interval(4., 4.5, 1)
        self.assertFalse(pred(intrvl2, intrvl11))
        self.assertFalse(pred(intrvl2, intrvl12))

        self.assertTrue(pred(intrvl2, intrvl2))

    def test_overlapsbefore(self):
        pred = overlaps_before()
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertTrue(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(2., 5., 1)
        intrvl4 = Interval(2., 3., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_overlapsafter(self):
        pred = overlaps_after()
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertTrue(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_starts(self):
        pred = starts()
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(2., 5., 1)
        intrvl4 = Interval(2., 3., 1)
        self.assertTrue(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_startsinv(self):
        pred = starts_inv()
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(2., 5., 1)
        intrvl4 = Interval(2., 3., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertTrue(pred(intrvl2, intrvl4))

    def test_finishes(self):
        pred = finishes()
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertTrue(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_finishesinv(self):
        pred = finishes_inv()
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))
        
        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertTrue(pred(intrvl2, intrvl4))

    def test_during(self):
        pred = during()
        intrvl1 = Interval(3., 3.5, 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertTrue(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))

        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_duringinv(self):
        pred = during_inv()
        intrvl1 = Interval(3., 3.5, 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertTrue(pred(intrvl2, intrvl1))

        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_meetsbefore(self):
        pred = meets_before()
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertTrue(pred(intrvl1, intrvl2))
        self.assertFalse(pred(intrvl2, intrvl1))

        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_meetsafter(self):
        pred = meets_after()
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(2., 4., 1)
        self.assertFalse(pred(intrvl1, intrvl2))
        self.assertTrue(pred(intrvl2, intrvl1))

        intrvl3 = Interval(1., 4., 1)
        intrvl4 = Interval(2.5, 4., 1)
        self.assertFalse(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl2, intrvl4))

    def test_equal(self):
        pred = equal()
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(2., 4., 1)
        intrvl3 = Interval(2., 4., 5)
        self.assertTrue(pred(intrvl1, intrvl1))
        self.assertTrue(pred(intrvl2, intrvl2))
        self.assertTrue(pred(intrvl3, intrvl3))
        self.assertTrue(pred(intrvl2, intrvl3))
        self.assertFalse(pred(intrvl1, intrvl2))

