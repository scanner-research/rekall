from rekall.payload_predicates import *
from rekall.list_predicates import *
from rekall.interval_list import Interval
import unittest

class TestListPredicates(unittest.TestCase):
    def test_cardinal_predicates(self):
        bboxes = [
            { 'x1': 0.1, 'y1': 0.1, 'x2': 0.3, 'y2': 0.5 },    
            { 'x1': 0.4, 'y1': 0.1, 'x2': 0.6, 'y2': 0.5 },    
            { 'x1': 0.7, 'y1': 0.1, 'x2': 0.9, 'y2': 0.5 }    
        ]
        
        self.assertTrue(length_exactly(3)(bboxes))
        self.assertTrue(length_at_least(3)(bboxes))
        self.assertTrue(length_at_most(3)(bboxes))
        self.assertTrue(length_between(3, 5)(bboxes))
        self.assertFalse(length_exactly(2)(bboxes))
        self.assertFalse(length_at_least(4)(bboxes))
        self.assertFalse(length_at_most(2)(bboxes))
        self.assertFalse(length_between(1, 2)(bboxes))

