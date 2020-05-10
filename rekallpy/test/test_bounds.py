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

            def __repr__(self):
                pass

            def __lt__(self, other):
                return ((self['t1'], self['t2'], self['x1'], self['x2']) <
                        (other['t1'], other['t2'], other['x1'], other['x2']))

            def primary_axis(self):
                return ('t1', 't2')

            def X(pred):
                return Bounds.cast({ 't1': 'x1', 't2': 'x2' })(pred)
            
            def copy(self):
                pass

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

    def test_span1D(self):
        b1 = Bounds1D(0,1)
        b2 = Bounds1D(1,2)
        b3 = b1.span(b2)
        target = Bounds1D(0,2)
        self.assertEqual(b3.data, target.data)

    def test_intersect1D(self):
        b1 = Bounds1D(0,2.5)
        b2 = Bounds1D(2,3)
        b3 = b1.intersect(b2)
        target = Bounds1D(2,2.5)
        self.assertEqual(b3.data, target.data)

        b1 = Bounds1D(0,1)
        b2 = Bounds1D(2,3)
        b3 = b1.intersect(b2)
        self.assertIsNone(b3)

    def test_combine1D(self):
        def combiner(b1, b2):
            return Bounds1D(b1['t1'] + b2['t1'], b1['t2'] + b2['t2'])
        b1 = Bounds1D(0,1)
        b2 = Bounds1D(1,2)
        b3 = b1.combine(b2, combiner)
        target = Bounds1D(1, 3) 
        self.assertEqual(b3.data, target.data)

    def test_size1D(self):
        self.assertEqual(Bounds1D(1, 10).size(), 9)
        self.assertEqual(Bounds1D(1, 10).size(axis=('t1', 't2')), 9)

    def test_span3D(self):
        b1 = Bounds3D(0,1,0,0.5,0,0.5)
        b2 = Bounds3D(1,2,0.4,1,0.2,0.3)
        b3 = b1.span(b2)
        target = Bounds3D(0,2,0,1,0,0.5)
        self.assertEqual(b3.data, target.data)

    def test_combine3D(self):
        def combiner1(b1, b2):
            return b1
        def combiner2(b1, b2):
            return b2
        b1 = Bounds3D(0,1,0,0.5,0,0.1)
        b2 = Bounds3D(2,3,2,2.5,1,1.1)
        b3 = b1.combine_per_axis(b2, combiner2, combiner1, combiner2)
        target = Bounds3D(2,3,0,0.5,1,1.1)
        self.assertEqual(b3.data, target.data)

    def test_intersect_time_span_space3D(self):
        b1 = Bounds3D(0,2.5,0,0.5,0,0.1)
        b2 = Bounds3D(2,3,2,2.5,1,1.1)
        b3 = b1.intersect_time_span_space(b2)
        target = Bounds3D(2,2.5,0,2.5,0,1.1)
        self.assertEqual(b3.data, target.data)

        b1 = Bounds3D(0,1,0,0.5,0,0.1)
        b2 = Bounds3D(2,3,2,2.5,1,1.1)
        b3 = b1.intersect_time_span_space(b2)
        self.assertIsNone(b3)

    def test_expand_to_frame3D(self):
        b1 = Bounds3D(0,1,0.5,0.6,0.7,0.8).expand_to_frame()
        self.assertEqual(b1.data, Bounds3D(0,1,0,1,0,1).data)

    def test_dimension_sizes3D(self):
        b = Bounds3D(0,2,0.5,0.8,0.9,1.0)
        self.assertAlmostEqual(b.length(), 2)
        self.assertAlmostEqual(b.width(), 0.3)
        self.assertAlmostEqual(b.height(), 0.1)
