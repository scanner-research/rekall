"""
Unary and Binary predicates on bounding boxes. Assumes objects with structure
{'x1': ..., 'y1': ..., 'x2': ..., 'y2': ...}.

Unary predicates:
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

Binary predicates:
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
"""

'''Unary predicates'''
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

'''Binary predicates'''
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
