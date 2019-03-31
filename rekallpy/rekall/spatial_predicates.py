"""DEPRECATED. Use 3D intervals and IntervalSet3D.match instead.

Spatial predicates on payloads. Assumes that the payload is an array of
objects with at least x1, y1, x2, y2 fields:
[
    {'x1': ..., 'y1': ..., 'x2': ..., 'y2': ..., [other fields, ...]},
    {'x1': ..., 'y1': ..., 'x2': ..., 'y2': ..., [other fields, ...]},
    ...
]

Scene graph Predicates
This is a predicate on the arrangement of objects. See how to create scene
graphs below.
    scene_graph(graph, region, exact) - True if the payload has
        objects in the region that staisfy the scene graph

Constructing scene graphs:
A scene graph defines a predicate on a list of bounding boxes. Scene graphs are
graphs with nodes and edges. Nodes are annotated with unary predicates, and
edges are annotated with binary predicates. A list of bounding boxes satisfies
the scene graph if there is some assignment from bounding boxes to nodes such
that:
    * For each assignment from bounding box to node, the bounding box satisfies
      all the node's unary predicates
    * For each edge, the bounding boxes assigned to the start and end node
      satisfy all the edge's binary predicates

The format of the scene graph is the following:
{
    'nodes': [
        { 'name': 'node1', 'predicates': [pred1, pred2, ...] },
        { 'name': 'node2', 'predicates': [pred1, pred2, ...] },
        ...
    ],
    'edges': [
        { 'start': 'node1', 'end': 'node2', 'predicates': [pred1, pred2, ...] },
        { 'start': 'node1', 'end': 'node3', 'predicates': [pred1, pred2, ...] },
        ...
    ]
}

Constructing regions:
Regions are defined by x1, y1, x2, y2. We have some helper functions for
constructing regions:
    region(x1, y1, x2, y2) - get a region with x1, y1, x2, y2
    full_frame() - get the full frame
    left_half(region=full_frame()) - left half of a region
    right_half(region=full_frame()) - right half of a region
    top_half(region=full_frame()) - top half of a region
    bottom_half(region=full_frame()) - bottom half of a region
    top_left(region=full_frame()) - top left quadrant of a region
    top_right(region=full_frame()) - top right quadrant of a region
    bottom_left(region=full_frame()) - bottom left quadrant of a region
    bottom_right(region=full_frame()) - bottom right quadrant of a region
    center(region=full_frame()) - center of a region (returns a region 1/4 the
        size of the full region, proportional to the full region, and in the
        center of the full region)
"""

from constraint import *

'''Constructing regions'''
def make_region(x1, y1, x2, y2):
    return { 'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2 }
def full_frame():
    return make_region(0., 0., 1., 1.)
def left_half(region=full_frame()):
    return make_region(region['x1'], region['y1'],
            (region['x1'] + region['x2']) / 2., region['y2'])
def right_half(region=full_frame()):
    return make_region((region['x1'] + region['x2']) / 2., region['y1'],
            region['x2'], region['y2'])
def top_half(region=full_frame()):
    return make_region(region['x1'], region['y1'],
            region['x2'], (region['y1'] + region['y2']) / 2.)
def bottom_half(region=full_frame()):
    return make_region(region['x1'], (region['y1'] + region['y2']) / 2.,
            region['x2'], region['y2'])
def top_left(region=full_frame()):
    return left_half(top_half(region))
def top_right(region=full_frame()):
    return right_half(top_half(region))
def bottom_left(region=full_frame()):
    return left_half(bottom_half(region))
def bottom_right(region=full_frame()):
    return right_half(bottom_half(region))
def center(region=full_frame()):
    width = region['x2'] - region['x1']
    height = region['y2'] - region['y1']
    return region(region['x1'] + width / 4.,
            region['y1'] + height / 4.,
            region['x2'] - width / 4.,
            region['y2'] - height / 4.)

'''Scene graph predicates'''
def _region_contains_bbox(region, bbox):
    return (bbox['x1'] >= region['x1'] and
        bbox['x2'] <= region['x2'] and
        bbox['y1'] >= region['y1'] and
        bbox['y2'] <= region['y2'])

def _bboxes_in_region(region, bboxes):
    return [bbox for bbox in bboxes if _region_contains_bbox(region, bbox)]

def _bbox_satisfies_node(bbox, node):
    for pred in node['predicates']:
        if not pred(bbox):
            return False
    return True

def _bboxes_satisfy_edge(bbox1, bbox2, edge):
    for pred in edge['predicates']:
        if not pred(bbox1, bbox2):
            return False
    return True

def scene_graph(graph, region=None, exact=False):
    '''
    Returns a predicate that checks if a list of bounding boxes satisfies the
    scene graph.

    @graph: The scene graph to check against. Has this format:
        {
            'nodes': [
                { 'name': 'node1', 'predicates': [pred1, pred2, ...] },
                { 'name': 'node2', 'predicates': [pred1, pred2, ...] },
                ...
            ],
            'edges': [
                { 'start': 'node1', 'end': 'node2',
                    'predicates': [pred1, pred2, ...] },
                { 'start': 'node1', 'end': 'node3',
                    'predicates': [pred1, pred2, ...] },
                ...
            ]
        }
    @region: Restrict the bounding boxes to this region. If None, then the
        region is the whole screen.
    @exact: If True, the list of bounding boxes must have the same number of
        elements as @graph has nodes.
    '''
    if region is None:
        region = full_frame()

    def compute_predicate(bbox_list):
        bbox_candidates = _bboxes_in_region(region, bbox_list)
        if exact and len(graph['nodes']) is not len(bbox_list):
            return False
        if len(graph['nodes']) == 0:
            return True
        
        '''
        This is a classic constraint satisfaction problem. Our variables are
        the nodes, whose domains are all the bounding boxes that satisfy all
        the node's predicates. Our edges define constraints between variables.
        Satisfying the scene graph is equivalent to satisfying the CSP.
        '''

        problem = Problem()
        variables = []
        for node in graph['nodes']:
            # Pre-compute per-node constraints
            candidates = [i for (i, bbox) in enumerate(bbox_candidates)
                    if _bbox_satisfies_node(bbox, node)]
            if len(candidates) == 0:
                return False
            problem.addVariable(node['name'], candidates)
            variables.append(node['name'])
        problem.addConstraint(AllDifferentConstraint())
        # Add edge constraints.
        # Note that `edge` needs to be explicitly passed into lambda since
        # otherwise it captures `edge` by reference which changes value in
        # the next loop iteration.
        for edge in graph['edges']:
            problem.addConstraint((lambda e: (lambda bbox1, bbox2: _bboxes_satisfy_edge(
                bbox_candidates[bbox1], bbox_candidates[bbox2], e)))(edge),
                (edge['start'], edge['end']))

        return problem.getSolution() is not None

    return compute_predicate

