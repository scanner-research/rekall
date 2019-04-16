from rekall.predicates import *
from rekall.bounds import Bounds1D
import unittest

class TestTemporalPredicates(unittest.TestCase):
    def test_before(self):
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(3., 4.)
        self.assertTrue(before()(bounds1, bounds2))
        self.assertFalse(before()(bounds2, bounds1))

        bounds3 = Bounds1D(2., 4.)
        self.assertTrue(before()(bounds1, bounds3))

    def test_before_range(self):
        pred = before(10., 20.)
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(3., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(12., 15.)
        bounds4 = Bounds1D(22.5, 25.)
        self.assertTrue(pred(bounds1, bounds3))
        self.assertFalse(pred(bounds1, bounds4))
        self.assertTrue(pred(bounds2, bounds4))

    def test_after(self):
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(3., 4.)
        self.assertFalse(after()(bounds1, bounds2))
        self.assertTrue(after()(bounds2, bounds1))

        bounds3 = Bounds1D(2., 4.)
        self.assertTrue(after()(bounds3, bounds1))
            
    def test_after_range(self):
        pred = after(10., 20.)
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(3., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(12., 15.)
        bounds4 = Bounds1D(22.5, 25.)
        self.assertTrue(pred(bounds3, bounds1))
        self.assertFalse(pred(bounds4, bounds1))
        self.assertTrue(pred(bounds4, bounds2))

    def test_overlaps(self):
        pred = overlaps()
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(3., 4.)

        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))

        bounds3 = Bounds1D(3.5, 4.5)
        bounds4 = Bounds1D(2.5, 3.5)
        self.assertTrue(pred(bounds2, bounds3))
        self.assertTrue(pred(bounds2, bounds4))

        bounds5 = Bounds1D(2.5, 4.5)
        bounds6 = Bounds1D(3.3, 3.6)
        self.assertTrue(pred(bounds2, bounds5))
        self.assertTrue(pred(bounds2, bounds6))

        bounds7 = Bounds1D(3., 3.5)
        bounds8 = Bounds1D(3., 4.5)
        self.assertTrue(pred(bounds2, bounds7))
        self.assertTrue(pred(bounds2, bounds8))

        bounds9 = Bounds1D(3.3, 4.)
        bounds10 = Bounds1D(2.1, 4.)
        self.assertTrue(pred(bounds2, bounds9))
        self.assertTrue(pred(bounds2, bounds10))

        bounds11 = Bounds1D(2.3, 3.)
        bounds12 = Bounds1D(4., 4.5)
        self.assertFalse(pred(bounds2, bounds11))
        self.assertFalse(pred(bounds2, bounds12))

        self.assertTrue(pred(bounds2, bounds2))

    def test_overlapsbefore(self):
        pred = overlaps_before()
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)
        self.assertTrue(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(2., 5.)
        bounds4 = Bounds1D(2., 3.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_overlapsafter(self):
        pred = overlaps_after()
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertTrue(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_starts(self):
        pred = starts()
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(2., 5.)
        bounds4 = Bounds1D(2., 3.)
        self.assertTrue(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_startsinv(self):
        pred = starts_inv()
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(2., 5.)
        bounds4 = Bounds1D(2., 3.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertTrue(pred(bounds2, bounds4))

    def test_finishes(self):
        pred = finishes()
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertTrue(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_finishesinv(self):
        pred = finishes_inv()
        bounds1 = Bounds1D(1., 3.)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))
        
        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertTrue(pred(bounds2, bounds4))

    def test_during(self):
        pred = during()
        bounds1 = Bounds1D(3., 3.5)
        bounds2 = Bounds1D(2., 4.)
        self.assertTrue(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))

        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_duringinv(self):
        pred = during_inv()
        bounds1 = Bounds1D(3., 3.5)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertTrue(pred(bounds2, bounds1))

        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_meetsbefore(self):
        pred = meets_before()
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(2., 4.)
        self.assertTrue(pred(bounds1, bounds2))
        self.assertFalse(pred(bounds2, bounds1))

        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_meetsafter(self):
        pred = meets_after()
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(2., 4.)
        self.assertFalse(pred(bounds1, bounds2))
        self.assertTrue(pred(bounds2, bounds1))

        bounds3 = Bounds1D(1., 4.)
        bounds4 = Bounds1D(2.5, 4.)
        self.assertFalse(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds2, bounds4))

    def test_equal(self):
        pred = equal()
        bounds1 = Bounds1D(1., 2.)
        bounds2 = Bounds1D(2., 4.)
        bounds3 = Bounds1D(2., 4.)
        self.assertTrue(pred(bounds1, bounds1))
        self.assertTrue(pred(bounds2, bounds2))
        self.assertTrue(pred(bounds3, bounds3))
        self.assertTrue(pred(bounds2, bounds3))
        self.assertFalse(pred(bounds1, bounds2))

