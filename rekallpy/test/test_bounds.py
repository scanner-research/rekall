from rekall.bounds import *
import unittest

class TestBounds(unittest.TestCase):
    def test_constructors(self):
        bounds1d = Bounds1D(0, 1)
        bounds1d = Bounds1D.fromTuple((0, 1))

        bounds3d = Bounds3D(0, 1, 0, 1, 0, 1)
        bounds3d = Bounds3D.fromTuple((0, 1, 0, 1, 0, 1))
        bounds3d = Bounds3D(0, 1)
        bounds3d = Bounds3D.fromTuple((0, 1))

    def test_bounds1d_lt(self):
        self.assertTrue(Bounds1D(0, 1) < Bounds1D(0, 2))
        self.assertTrue(Bounds1D(0, 1) < Bounds1D(1, 1))
        self.assertFalse(Bounds1D(0, 1) > Bounds1D(0, 2))
        self.assertFalse(Bounds1D(0, 1) > Bounds1D(1, 1))
        
        self.assertTrue(Bounds1D.fromTuple((0, 1)) < Bounds1D(0, 2))
        self.assertTrue(Bounds1D.fromTuple((0, 1)) < Bounds1D(1, 1))
        self.assertFalse(Bounds1D.fromTuple((0, 1)) > Bounds1D(0, 2))
        self.assertFalse(Bounds1D.fromTuple((0, 1)) > Bounds1D(1, 1))

    def test_bounds3d_lt(self):
        self.assertTrue(Bounds3D(0, 1) < Bounds3D(0, 2))
        self.assertTrue(Bounds3D(0, 1) < Bounds3D(1, 1))
        self.assertFalse(Bounds3D(0, 1) > Bounds3D(0, 2))
        self.assertFalse(Bounds3D(0, 1) > Bounds3D(1, 1))
        
        self.assertTrue(Bounds3D.fromTuple((0, 1)) < Bounds3D(0, 2))
        self.assertTrue(Bounds3D.fromTuple((0, 1)) < Bounds3D(1, 1))
        self.assertFalse(Bounds3D.fromTuple((0, 1)) > Bounds3D(0, 2))
        self.assertFalse(Bounds3D.fromTuple((0, 1)) > Bounds3D(1, 1))

        self.assertTrue(Bounds3D.fromTuple((0, 1)) < Bounds3D(0, 1, 0, 1, 1, 0))
        self.assertTrue(Bounds3D.fromTuple((0, 1)) < Bounds3D(0, 1, 1, 0, 0, 1))
        self.assertFalse(Bounds3D.fromTuple((0, 1)) > Bounds3D(0, 1, 0, 1, 1, 0))
        self.assertFalse(Bounds3D.fromTuple((0, 1)) > Bounds3D(0, 1, 1, 0, 0, 1))

    def test_bounds3d_casting(self):
        # This predicate tests whether a bound's 't2' value is greater
        # than its 't1' value
        def example_pred(bounds):
            return bounds['t2'] > bounds['t1']

        # t1 = 0, t2 = 1, x1 = 1, x2 = 0, y1 = 1, y2 = 0
        higher_t2_lower_x2 = Bounds3D(0, 1, 1, 0, 1, 0)
        
        self.assertTrue(example_pred(higher_t2_lower_x2))

        self.assertFalse(Bounds3D.X(example_pred)(higher_t2_lower_x2))
        self.assertFalse(Bounds3D.Y(example_pred)(higher_t2_lower_x2))

    def test_bounds_inheritance(self):
        from rekall.predicates import overlaps

        class Bounds2D(Bounds):
            def __init__(self, t1, t2, x1, x2):
                self.data = { 't1': t1, 't2': t2, 'x1': x1, 'x2': x2 }

            def __lt__(self, other):
                return ((self['t1'], self['t2'], self['x1'], self['x2']) <
                        (other['t1'], other['t2'], other['x1'], other['x2']))

            def primary_axis(self):
                return ('t1', 't2')

            def X(pred):
                return Bounds.cast({ 't1': 'x1', 't2': 'x2' })(pred)

        bounds1 = Bounds2D(0, 1, 0.5, 0.7)
        bounds2 = Bounds2D(2, 3, 0.4, 0.6)

        # overlaps expects two objects with fields 't1' and 't2' and
        #   computes whether there is overlap in that dimension

        # This is False, since there is no time overlap
        self.assertFalse(overlaps()(bounds1, bounds2))

        # This is True, since there is overlap in the X dimension
        self.assertTrue(Bounds2D.X(overlaps())(bounds1, bounds2))

        # This is True.
        self.assertTrue(bounds1 < bounds2)

        # This returns ('t1', 't2')
        self.assertEqual(bounds1.primary_axis(), ('t1', 't2'))

