from rekall.spatial_predicates import *
from rekall.interval_list import Interval
import unittest

class TestSpatialPredicates(unittest.TestCase):
    def setUp(self):
        self.scene_one = Interval(0., 1., {
            'type': 'bbox_list',
            'objects': [
                { 'x1': 0.1, 'y1': 0.1, 'x2': 0.3, 'y2': 0.5 },    
                { 'x1': 0.4, 'y1': 0.1, 'x2': 0.6, 'y2': 0.5 },    
                { 'x1': 0.7, 'y1': 0.1, 'x2': 0.9, 'y2': 0.5 }    
            ]
        })

    def test_cardinal_predicates(self):
        self.assertTrue(exactly(3)(self.scene_one))
        self.assertTrue(at_least(3)(self.scene_one))
        self.assertTrue(at_most(3)(self.scene_one))
        self.assertTrue(between(3, 5)(self.scene_one))
        self.assertFalse(exactly(2)(self.scene_one))
        self.assertFalse(at_least(4)(self.scene_one))
        self.assertFalse(at_most(2)(self.scene_one))
        self.assertFalse(between(1, 2)(self.scene_one))

    def test_scene_node_constraints(self):
        one_box_graph = {
            'nodes': [
                { 'name': 'box1', 'predicates': [] }
            ],
            'edges': []
        }
        self.assertTrue(frame_contains(one_box_graph)(self.scene_one))
        self.assertFalse(frame_is(one_box_graph)(self.scene_one))
        self.assertTrue(region_is(left_half(), one_box_graph)(self.scene_one))

        one_box_graph['nodes'][0]['predicates'] = [
            position(0.4, 0.1, 0.6, 0.5)
        ]
        self.assertTrue(frame_contains(one_box_graph)(self.scene_one))

        one_box_graph['nodes'][0]['predicates'] = [
            area_at_least(0.1)
        ]
        self.assertFalse(frame_contains(one_box_graph)(self.scene_one))

    def test_scene_graph_edge_constraints(self):
        three_boxes_graph = {
            'nodes': [
                { 'name': 'box1', 'predicates': [] },
                { 'name': 'box2', 'predicates': [] },
                { 'name': 'box3', 'predicates': [] }
            ],
            'edges': []
        }
        self.assertTrue(frame_contains(three_boxes_graph)(self.scene_one))
        self.assertTrue(frame_is(three_boxes_graph)(self.scene_one))
        self.assertTrue(region_contains(top_half(),
            three_boxes_graph)(self.scene_one))
        self.assertTrue(region_is(top_half(),
            three_boxes_graph)(self.scene_one))
        
        self.assertFalse(region_contains(bottom_half(),
            three_boxes_graph)(self.scene_one))
        self.assertFalse(region_contains(top_left(),
            three_boxes_graph)(self.scene_one))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [same_area()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [same_area()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [same_area()] }
        ]
        self.assertTrue(frame_contains(three_boxes_graph)(self.scene_one))
        self.assertTrue(frame_is(three_boxes_graph)(self.scene_one))
        self.assertTrue(region_contains(top_half(),
            three_boxes_graph)(self.scene_one))
        self.assertTrue(region_is(top_half(),
            three_boxes_graph)(self.scene_one))
        
        self.assertFalse(region_contains(bottom_half(),
            three_boxes_graph)(self.scene_one))
        self.assertFalse(region_contains(top_left(),
            three_boxes_graph)(self.scene_one))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [left_of()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [left_of()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [left_of()] }
        ]
        self.assertTrue(frame_contains(three_boxes_graph)(self.scene_one))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [left_of()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [left_of()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [left_of(), right_of()] }
        ]
        self.assertFalse(frame_contains(three_boxes_graph)(self.scene_one))

    def test_scene_node_and_edge_constraints(self):
        three_boxes_graph = {
            'nodes': [
                { 'name': 'box1', 'predicates': [height_at_least(0.3)] },
                { 'name': 'box2', 'predicates': [height_at_least(0.3)] },
                { 'name': 'box3', 'predicates': [height_at_least(0.3)] }
            ],
            'edges': [
                { 'start': 'box1', 'end': 'box2', 'predicates': [same_area()] },
                { 'start': 'box1', 'end': 'box3', 'predicates': [same_area()] },
                { 'start': 'box2', 'end': 'box3', 'predicates': [same_area()] }
            ]
        }
        self.assertTrue(frame_contains(three_boxes_graph)(self.scene_one))
        self.assertTrue(frame_is(three_boxes_graph)(self.scene_one))
        self.assertTrue(region_contains(top_half(),
            three_boxes_graph)(self.scene_one))
        self.assertTrue(region_is(top_half(),
            three_boxes_graph)(self.scene_one))

        three_boxes_graph['nodes'] = [
            { 'name': 'box1', 'predicates': [has_value('x1', 0.1)] },
            { 'name': 'box2', 'predicates': [has_value('x1', 0.4)] },
            { 'name': 'box3', 'predicates': [has_value('x1', 0.7)] }
        ]
        self.assertTrue(frame_contains(three_boxes_graph)(self.scene_one))
        self.assertTrue(frame_is(three_boxes_graph)(self.scene_one))
        self.assertTrue(region_contains(top_half(),
            three_boxes_graph)(self.scene_one))
        self.assertTrue(region_is(top_half(),
            three_boxes_graph)(self.scene_one))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [left_of()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [left_of()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [right_of()] }
        ]
        self.assertFalse(frame_contains(three_boxes_graph)(self.scene_one))
