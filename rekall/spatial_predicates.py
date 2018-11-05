from constraint import *

'''
Spatial predicates on Temporal Ranges. Assumes that the payload has the
following structure:
{
    'type': 'bbox_list',
    'objects': [{'x1': ..., 'y1': ..., 'x2': ..., 'y2': ...}, ...]
}

Logical Predicates
TODO(danfu09): figure out how to combine these with temporal and/or
    and_pred_spatial(pred1, pred2) - True if pred1 and pred2 are true
    or_pred_spatial(pred1, pred2) - True if pred1 or pred2 are true

Cardinal(?) Predicates
These are predicates on the number of bboxes in the range
    exactly(n) - True if there are exactly n bboxes
    at_least(n) - True if there are at least n bboxes
    at_most(n) - True if there are at most n bboxes
    between(n1, n2) - True if there are between n1 and n2 bboxes, inclusive

"Scene graph" Predicates
These are predicates on the arrangement of bboxes. See how to create scene
graphs below.
    frame_contains(graph) - True if the entire frame contains a scene graph
    frame_is(graph) - True if the entire frame contains a scene graph, and
        there are no other bboxes in the frame.
    region_contains(region, graph) - True if a region in the frame contains a
        scene graph.
        Region defined by {'x1': ..., 'y1': ..., 'x2': ..., 'y2': ...}
        Some helpers to define regions below.
    region_is(region, graph) - True if a region contains a scene graph, and
        there are no other bboxes in the region.

Constructing scene graphs:
A scene graph defines a predicate on some number of bounding boxes.
Practically, scene graphs are graphs with nodes and edges. The nodes are
bounding boxes, and the edges are relationships between the bounding boxes.

The nodes can be annotated with attributes; these are predicates on the x1, y1,
x2, y2 values of the bounding box such as area, absolute position, etc. Of
course, we also have "and" and "or" attributes.

The edges can be spatial relationships (left of, right of, above, below, etc)
or relationships on the x1, y1, x2, y2 values of the nodes - same x/y, same
area, etc. Again, we have "and" and "or" predicates to compose these
relationships.

The format of a scene graph is a list of nodes, and a list of edges. Each node
needs a name (unique within the scene graph), and has a list of predicates.
Each item in the list is automatically and'ed to get the full predicate on the
node.

Each edge needs to define which two nodes it points two (via the nodes' names),
and a list of predicates. Again, each item in the list is automatically and'ed
to get the full predicate.

So the format of the scene graph is the following:
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

For now, we only list the possible predicates for nodes and edges. In the
future, we'll add functionality to easily construct scene graphs.

Node predicates:
    position(x1, y1, x2, y2, epsilon=0.1) - bbox has positions x1, y1, x2, y2,
        +/- epsilon
    has_value(value_name, value, epsilon=0.1) - value_name (one of x1, y1, x2,
        y2) has value value, +/- epsilon
    area_exactly(area, epsilon=0.1) - area of the bbox is some area
    area_at_least(area) - area is at least some area
    area_at_most(area) - area is at most some area
    area_between(area1, area2) - area is between two values
    width_exactly(width, epsilon=0.1) - width of the bbox is some width
    width_at_least(width) - width is at least some width
    width_at_most(width) - width is at most some width
    width_between(width1, width2) - width is between two values
    height_exactly(height, epsilon=0.1) - height of the bbox is some height
    height_at_least(height) - height is at least some height
    height_at_most(height) - height is at most some height
    height_between(height1, height2) - height is between two values
    and_pred_node(pred1, pred2) - and
    or_pred_node(pred1, pred2) - or

Edge relationships:
    left_of() - start's x2 is less than end's x1
    right_of() - start's x1 is greater than end's x2
    above() - start's y2 is less than end's y1
    below() - start's y1 is greater than end's y2
    same_area(epsilon=0.1) - start and end have same area
    more_area() - start's area greater than end's area
    less_area() - start's area less than end's area
    same_width(epsilon=0.1) - start and end have same width
    more_width() - start's width greater than end's width
    less_width() - start's width less than end's width
    same_height(epsilon=0.1) - start and end have same height
    more_height() - start's height greater than end's height
    less_height() - start's height less than end's height
    same_value(value_name, epsilon=0.1) - start and end have the same value
        (one of x1, y1, x2, y2), +/- epsilon
    and_pred_edge(pred1, pred2) - and
    or_pred_edge(pred1, pred2) - or

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
'''

'''Logical predicates'''
def and_pred_spatial(pred1, pred2):
    return lambda intrvl: pred1(intrvl) and pred2(intrvl)
def or_pred_spatial(pred1, pred2):
    return lambda intrvl: pred1(intrvl) or pred2(intrvl)

'''Cardinal predicates'''
def exactly(n):
    return lambda intrvl: len(intrvl.payload['objects']) == n
def at_least(n):
    return lambda intrvl: len(intrvl.payload['objects']) >= n
def at_most(n):
    return lambda intrvl: len(intrvl.payload['objects']) <= n
def between(n1, n2):
    return lambda intrvl: (len(intrvl.payload['objects']) >= n1 and
            len(intrvl.payload['objects']) <= n2)

'''Node predicates'''
def _area(bbox):
    return (bbox['x2'] - bbox['x1']) * (bbox['y2'] - bbox['y1'])
def _width(bbox):
    return bbox['x2'] - bbox['x1']
def _height(bbox):
    return bbox['y2'] - bbox['y1']
def position(x1, y1, x2, y2, epsilon=0.1):
    return lambda bbox: (abs(bbox['x1'] - x1) < epsilon and
            abs(bbox['y1'] - y1) < epsilon and
            abs(bbox['x2'] - x2) < epsilon and
            abs(bbox['y2'] - y2) < epsilon)
def has_value(value_name, value, epsilon=0.1):
    return lambda bbox: abs(bbox[value_name] - value) < epsilon
def area_exactly(area, epsilon=0.1):
    return lambda bbox: abs(_area(bbox) - area) < epsilon 
def area_at_least(area):
    return lambda bbox: _area(bbox) >= area
def area_at_most(area):
    return lambda bbox: _area(bbox) <= area
def area_between(area1, area2):
    return lambda bbox: _area(bbox) >= area1 and _area(bbox) <= area2
def width_exactly(width, epsilon=0.1):
    return lambda bbox: abs(_width(bbox) - width) < epsilon 
def width_at_least(width):
    return lambda bbox: _width(bbox) >= width
def width_at_most(width):
    return lambda bbox: _width(bbox) <= width
def width_between(width1, width2):
    return lambda bbox: _width(bbox) >= width1 and _width(bbox) <= width2
def height_exactly(height, epsilon=0.1):
    return lambda bbox: abs(_height(bbox) - height) < epsilon 
def height_at_least(height):
    return lambda bbox: _height(bbox) >= height
def height_at_most(height):
    return lambda bbox: _height(bbox) <= height
def height_between(height1, height2):
    return lambda bbox: _height(bbox) >= height1 and _height(bbox) <= height2
def and_pred_node(pred1, pred2):
    return lambda bbox: pred1(bbox) and pred2(bbox)
def or_pred_node(pred1, pred2):
    return lambda bbox: pred1(bbox) or pred2(bbox)

'''Edge predicates'''
def left_of():
    return lambda bbox1, bbox2: bbox1['x2'] < bbox2['x1']
def right_of():
    return lambda bbox1, bbox2: bbox1['x1'] > bbox2['x2']
def above():
    return lambda bbox1, bbox2: bbox1['y2'] < bbox2['y1']
def below():
    return lambda bbox1, bbox2: bbox1['y1'] > bbox2['y2']
def same_area(epsilon=0.1):
    return lambda bbox1, bbox2: abs(_area(bbox1) - _area(bbox2)) < epsilon
def more_area():
    return lambda bbox1, bbox2: _area(bbox1) > _area(bbox2)
def less_area():
    return lambda bbox1, bbox2: _area(bbox1) < _area(bbox2)
def same_width(epsilon=0.1):
    return lambda bbox1, bbox2: abs(_width(bbox1) - _width(bbox2)) < epsilon
def more_width():
    return lambda bbox1, bbox2: _width(bbox1) > _width(bbox2)
def less_width():
    return lambda bbox1, bbox2: _width(bbox1) < _width(bbox2)
def same_height(epsilon=0.1):
    return lambda bbox1, bbox2: abs(_height(bbox1) - _height(bbox2)) < epsilon
def more_height():
    return lambda bbox1, bbox2: _height(bbox1) > _height(bbox2)
def less_height():
    return lambda bbox1, bbox2: _height(bbox1) < _height(bbox2)
def same_value(value_name, epsilon=0.1):
    return lambda bbox1, bbox2: abs(bbox1[value_name] - bbox2[value_name]) < epsilon
def and_pred_edge(pred1, pred2):
    return lambda bbox1, bbox2: pred1(bbox1, bbox2) and pred2(bbox1, bbox2)
def or_pred_edge(pred1, pred2):
    return lambda bbox1, bbox2: pred1(bbox1, bbox2) or pred2(bbox1, bbox2)

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

def region_contains(region, graph):
    def calculate_predicate(intrvl):
        bbox_candidates = _bboxes_in_region(region, intrvl.payload['objects'])
        if len(bbox_candidates) == 0:
            return False

        '''
        This is a classic constraint satisfaction problem.
        Our variables are the nodes, each of which has a domain of the bounding
        boxes.
        Each node has a list of predicates, which are constraints on the
        variables.
        We also have constraints between variables.
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
        # Add edge constraints
        for edge in graph['edges']:
            problem.addConstraint(lambda bbox1, bbox2: _bboxes_satisfy_edge(
                bbox_candidates[bbox1], bbox_candidates[bbox2], edge),
                (edge['start'], edge['end']))

        return problem.getSolution() is not None

    return calculate_predicate

def region_is(region, graph):
    def right_number(intrvl):
        num_bboxes = len(_bboxes_in_region(region, intrvl.payload['objects']))
        return num_bboxes == len(graph['nodes'])
    return and_pred_spatial(region_contains(region, graph), right_number)

def frame_contains(graph):
    return region_contains(full_frame(), graph)

def frame_is(graph):
    return region_is(full_frame(), graph)

