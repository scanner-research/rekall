from rekall.predicates import *
from rekall.bounds import Bounds1D
import unittest

class TestLogicalPredicates(unittest.TestCase):
    def test_true_pred(self):
        bounds1 = Bounds1D(1, 2)
        bounds2 = Bounds1D(5, 6)
        self.assertTrue(true_pred()(bounds1, bounds2))

    def test_false_pred(self):
        bounds1 = Bounds1D(1, 2)
        bounds2 = Bounds1D(5, 6)
        self.assertFalse(false_pred()(bounds1, bounds2))

    def test_and(self):
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(1., 2.)
        self.assertTrue(and_pred(equal(), overlaps())(bounds1, bounds2))
        self.assertFalse(and_pred(equal(), overlaps_before())(bounds1, bounds2))
        self.assertFalse(and_pred(overlaps_before(), equal())(bounds1, bounds2))
        self.assertFalse(and_pred(overlaps_before(), overlaps_after())(bounds1, bounds2))

    def test_or(self):
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)

        self.assertTrue(or_pred(before(), overlaps_before())(bounds1, bounds2))
        self.assertFalse(or_pred(before(), false_pred())(bounds1, bounds2))
        self.assertTrue(or_pred(overlaps_before(), overlaps())(bounds1, bounds2))
        self.assertTrue(or_pred(overlaps_before(), before())(bounds1, bounds2))

    def test_not(self):
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)

        self.assertTrue(overlaps()(bounds1, bounds2))
        self.assertFalse(not_pred(overlaps())(bounds1, bounds2))

