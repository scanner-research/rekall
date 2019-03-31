from rekall.logical_predicates import *
from rekall.temporal_predicates import *
from rekall.interval_list import Interval
import unittest

class TestLogicalPredicates(unittest.TestCase):
    def test_true_pred(self):
        intrvl1 = Interval(1, 2, 1)
        intrvl2 = Interval(5, 6, 1)
        self.assertTrue(true_pred(arity=2)(intrvl1, intrvl2))

    def test_false_pred(self):
        intrvl1 = Interval(1, 2, 1)
        intrvl2 = Interval(5, 6, 1)
        self.assertFalse(false_pred(arity=2)(intrvl1, intrvl2))

    def test_and(self):
        intrvl1 = Interval(1., 2., 1)
        intrvl2 = Interval(1., 2., 3)
        self.assertTrue(and_pred(equal(), overlaps(), arity=2)(intrvl1, intrvl2))
        self.assertFalse(and_pred(equal(), overlaps_before(), arity=2)(intrvl1, intrvl2))
        self.assertFalse(and_pred(overlaps_before(), equal(), arity=2)(intrvl1, intrvl2))
        self.assertFalse(and_pred(overlaps_before(), overlaps_after(), arity=2)(intrvl1, intrvl2))

    def test_or(self):
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)

        self.assertTrue(or_pred(before(), overlaps_before(), arity=2)(intrvl1, intrvl2))
        self.assertFalse(or_pred(before(), false_pred(arity=2), arity=2)(intrvl1, intrvl2))
        self.assertTrue(or_pred(overlaps_before(), overlaps(), arity=2)(intrvl1, intrvl2))
        self.assertTrue(or_pred(overlaps_before(), before(), arity=2)(intrvl1, intrvl2))

    def test_not(self):
        intrvl1 = Interval(1., 3., 1)
        intrvl2 = Interval(2., 4., 1)

        self.assertTrue(overlaps()(intrvl1, intrvl2))
        self.assertFalse(not_pred(overlaps(), arity=2)(intrvl1, intrvl2))

