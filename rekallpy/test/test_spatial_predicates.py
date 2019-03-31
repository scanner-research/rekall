from rekall.payload_predicates import *
from rekall.bbox_predicates import *
from rekall.spatial_predicates import *
from rekall.interval_list import Interval
import unittest

class TestSpatialPredicates(unittest.TestCase):
    def setUp(self):
        self.bboxes = [
            { 'x1': 0.1, 'y1': 0.1, 'x2': 0.3, 'y2': 0.5 },    
            { 'x1': 0.4, 'y1': 0.1, 'x2': 0.6, 'y2': 0.5 },    
            { 'x1': 0.7, 'y1': 0.1, 'x2': 0.9, 'y2': 0.5 }    
        ]

    def test_scene_node_constraints(self):
        one_box_graph = {
            'nodes': [
                { 'name': 'box1', 'predicates': [] }
            ],
            'edges': []
        }
        self.assertTrue(scene_graph(one_box_graph)(self.bboxes))
        self.assertFalse(scene_graph(one_box_graph, exact=True)(self.bboxes))
        self.assertFalse(scene_graph(
            one_box_graph, 
            region=left_half(), 
            exact=True)(self.bboxes))

        one_box_graph['nodes'][0]['predicates'] = [
            position(0.4, 0.1, 0.6, 0.5)
        ]
        self.assertTrue(scene_graph(one_box_graph)(self.bboxes))

        one_box_graph['nodes'][0]['predicates'] = [
            area_at_least(0.1)
        ]
        self.assertFalse(scene_graph(one_box_graph)(self.bboxes))

    def test_scene_graph_edge_constraints(self):
        three_boxes_graph = {
            'nodes': [
                { 'name': 'box1', 'predicates': [] },
                { 'name': 'box2', 'predicates': [] },
                { 'name': 'box3', 'predicates': [] }
            ],
            'edges': []
        }
        self.assertTrue(scene_graph(three_boxes_graph)(self.bboxes))
        self.assertTrue(
                scene_graph(three_boxes_graph, exact=True)(self.bboxes))
        self.assertTrue(scene_graph(
            three_boxes_graph, 
            region=top_half())(self.bboxes))
        self.assertTrue(scene_graph(
            three_boxes_graph, 
            region=top_half(),
            exact=True)(self.bboxes))
        
        self.assertFalse(scene_graph(
            three_boxes_graph, 
            region=bottom_half())(self.bboxes))
        self.assertFalse(scene_graph(
            three_boxes_graph, 
            region=top_left())(self.bboxes))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [same_area()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [same_area()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [same_area()] }
        ]
        self.assertTrue(scene_graph(three_boxes_graph)(self.bboxes))
        self.assertTrue(scene_graph(three_boxes_graph,
            exact=True)(self.bboxes))
        self.assertTrue(scene_graph(three_boxes_graph,
            region=top_half())(self.bboxes))
        self.assertTrue(scene_graph(three_boxes_graph,
            region=top_half(), exact=True)(self.bboxes))
        
        self.assertFalse(scene_graph(three_boxes_graph,
            region=bottom_half())(self.bboxes))
        self.assertFalse(scene_graph(three_boxes_graph,
            region=top_left())(self.bboxes))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [left_of()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [left_of()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [left_of()] }
        ]
        self.assertTrue(scene_graph(three_boxes_graph)(self.bboxes))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [left_of()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [left_of()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [left_of(), right_of()] }
        ]
        self.assertFalse(scene_graph(three_boxes_graph)(self.bboxes))

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
        self.assertTrue(scene_graph(three_boxes_graph)(self.bboxes))
        self.assertTrue(
                scene_graph(three_boxes_graph, exact=True)(self.bboxes))
        self.assertTrue(scene_graph(
            three_boxes_graph, 
            region=top_half())(self.bboxes))
        self.assertTrue(scene_graph(
            three_boxes_graph, 
            region=top_half(),
            exact=True)(self.bboxes))

        three_boxes_graph['nodes'] = [
            { 'name': 'box1', 'predicates': [has_value('x1', 0.1)] },
            { 'name': 'box2', 'predicates': [has_value('x1', 0.4)] },
            { 'name': 'box3', 'predicates': [has_value('x1', 0.7)] }
        ]
        self.assertTrue(scene_graph(three_boxes_graph)(self.bboxes))
        self.assertTrue(
                scene_graph(three_boxes_graph, exact=True)(self.bboxes))
        self.assertTrue(scene_graph(
            three_boxes_graph, 
            region=top_half())(self.bboxes))
        self.assertTrue(scene_graph(
            three_boxes_graph, 
            region=top_half(),
            exact=True)(self.bboxes))

        three_boxes_graph['edges'] = [
            { 'start': 'box1', 'end': 'box2', 'predicates': [left_of()] },
            { 'start': 'box1', 'end': 'box3', 'predicates': [left_of()] },
            { 'start': 'box2', 'end': 'box3', 'predicates': [right_of()] }
        ]
        self.assertFalse(scene_graph(three_boxes_graph)(self.bboxes))
