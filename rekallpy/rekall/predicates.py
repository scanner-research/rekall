"""
A collection of built-in predicate functions. Each function in this module
takes in some number of arguments and returns an anonymous function that
evaluates to True or False.

Example:
    The ``area_at_least`` predicate returns a predicate that computes whether a
    2D bounding box's area is at least some value. This is how you would use
    it::

        $ bbox = { 'x1': 0.1, 'x2': 0.3, 'y1': 0.1, 'y2': 0.3 }
        $ pred = area_at_least(.03)
        $ bbox_satisfies_pred = pred(bbox)
"""

# Adapters for logical combinations of predicates
def not_pred(pred):
    """Negates the predicate."""
    def new_pred(*args):
        return not pred(*args)
    return new_pred

def and_pred(*preds):
    """ANDs the predicates."""
    def new_pred(*args):
        for pred in preds:
            if not pred(*args):
                return False
        return True
    return new_pred

def or_pred(*preds):
    """ORs the predicates."""
    def new_pred(*args):
        for pred in preds:
            if pred(*args):
                return True
        return False
    return new_pred

def true_pred():
    """Returns a predicate that always returns ``True``."""
    def new_pred(*args):
        return True
    return new_pred

def false_pred():
    """Returns a predicate that always returns ``False``."""
    def new_pred(*args):
        return False
    return new_pred

# Unary bounding box predicates.
def _area(bbox):
    """Computes area of a 2D bounding box.
    
    Args:
        bbox: A dict with keys 'x1', 'x2', 'y1', 'y2' encoding spatial
            co-ordinates.
    
    Returns:
        The area of the bounding box.
    """
    return (bbox['x2'] - bbox['x1']) * (bbox['y2'] - bbox['y1'])

def _width(bbox):
    """Computes width of a 2D bounding box.
    
    Args:
        bbox: A dict with keys 'x1', 'x2', 'y1', 'y2' encoding spatial
            co-ordinates.
    
    Returns:
        The width (in the X dimension) of the bounding box.
    """
    return bbox['x2'] - bbox['x1']

def _height(bbox):
    """Computes height of a 2D bounding box.
    
    Args:
        bbox: A dict with keys 'x1', 'x2', 'y1', 'y2' encoding spatial
            co-ordinates.
    
    Returns:
        The height (in the Y dimension) of the bounding box.
    """
    return bbox['y2'] - bbox['y1']

def position(x1, y1, x2, y2, epsilon=0.1):
    """Returns a function that computes whether a 2D bounding box has certain
    co-ordinates (+/- epsilon).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the absolute difference between the
    dict's values and the specified co-ordinates are all less than ``epsilon``.

    Args:
        x1: Value to compare against the bounding box's 'x1' field.
        y1: Value to compare against the bounding box's 'y1' field.
        x2: Value to compare against the bounding box's 'x2' field.
        y2: Value to compare against the bounding box's 'y2' field.
        epsilon: Maximum difference against specified co-ordinates.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's spatial co-ordinates are all within ``epsilon`` of ``x1``,
        ``y1``, ``x2``, and ``y2``.
    """
    return lambda bbox: (abs(bbox['x1'] - x1) < epsilon and
            abs(bbox['y1'] - y1) < epsilon and
            abs(bbox['x2'] - x2) < epsilon and
            abs(bbox['y2'] - y2) < epsilon)

def has_value(key, target, epsilon=0.1):
    """Returns a function that computes whether a specified value in a dict
    is within ``epsilon`` of ``target``.

    The output function takes in a dict ``d`` and returns ``True`` if the
    absolute difference between ``d[key]`` and ``target`` is less than
    ``epsilon``.

    Args:
        key: Lookup key for the value in the dict to compare.
        target: The value to compare against.
        epsilon: Maximum difference between the two values.
    
    Returns:
        A function that takes a dict and returns ``True`` if the absolute value
        between ``dict[key]`` and ``target`` is less than ``epsilon``.
    """
    return lambda bbox: abs(bbox[key] - target) < epsilon

def area_exactly(area, epsilon=0.1):
    """Returns a function that computes whether a 2D bounding box has a certain
    area (+/- epsilon).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the absolute difference between the
    bounding box's area and the specified area is less than ``epsilon``.

    Args:
        area: Target area value.
        epsilon: Maximum difference between the bounding box's area and ``area``.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's area is within ``epsilon`` of ``area``.
    """
    return lambda bbox: abs(_area(bbox) - area) < epsilon 

def area_at_least(area):
    """Returns a function that computes whether a 2D bounding box's area is at
    least ``area``.

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's area is greater than
    or equal to ``area``.

    Args:
        area: Target area value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's area is greater than or equal to ``area``.
    """
    return lambda bbox: _area(bbox) >= area

def area_at_most(area):
    """Returns a function that computes whether a 2D bounding box's area less
    than or equal to ``area``.

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's area is less than
    or equal to ``area``.

    Args:
        area: Target area value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's area is less than or equal to ``area``.
    """
    return lambda bbox: _area(bbox) <= area

def area_between(area1, area2):
    """Returns a function that computes whether a 2D bounding box's area is
    between ``area1`` and ``area2`` (inclusive).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's area is between
    ``area1`` and ``area2``.

    Args:
        area1: Minimum area value.
        area2: Maximum area value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's area is between ``area1`` and ``area2``.
    """
    return lambda bbox: _area(bbox) >= area1 and _area(bbox) <= area2

def width_exactly(width, epsilon=0.1):
    """Returns a function that computes whether a 2D bounding box has a certain
    width (+/- epsilon).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the absolute difference between the
    bounding box's width and the specified width is less than ``epsilon``.

    Args:
        width: Target width value.
        epsilon: Maximum difference between the bounding box's width and
            ``width``.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's width is within ``epsilon`` of ``width``.
    """
    return lambda bbox: abs(_width(bbox) - width) < epsilon 

def width_at_least(width):
    """Returns a function that computes whether a 2D bounding box's width is at
    least ``width``.

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's width is greater than
    or equal to ``width``.

    Args:
        width: Target width value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's width is greater than or equal to ``width``.
    """
    return lambda bbox: _width(bbox) >= width

def width_at_most(width):
    """Returns a function that computes whether a 2D bounding box's width is 
    less than or equal to ``width``.

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's width is less than
    or equal to ``width``.

    Args:
        width: Target width value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's width is less than or equal to ``width``.
    """
    return lambda bbox: _width(bbox) <= width

def width_between(width1, width2):
    """Returns a function that computes whether a 2D bounding box's width is 
    between ``width1`` and ``width2`` (inclusive).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's width is between
    ``width1`` and ``width2``.

    Args:
        width1: Minimum width value.
        width2: Maximum width value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's width is between ``width1`` and ``width2``.
    """
    return lambda bbox: _width(bbox) >= width1 and _width(bbox) <= width2

def height_exactly(height, epsilon=0.1):
    """Returns a function that computes whether a 2D bounding box has a certain
    height (+/- epsilon).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the absolute difference between the
    bounding box's height and the specified height is less than ``epsilon``.

    Args:
        height: Target height value.
        epsilon: Maximum difference between the bounding box's height and
            ``height``.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's height is within ``epsilon`` of ``height``.
    """
    return lambda bbox: abs(_height(bbox) - height) < epsilon 

def height_at_least(height):
    """Returns a function that computes whether a 2D bounding box's height is
    at least ``height``.

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's height is greater than
    or equal to ``height``.

    Args:
        height: Target height value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's height is greater than or equal to ``height``.
    """
    return lambda bbox: _height(bbox) >= height

def height_at_most(height):
    """Returns a function that computes whether a 2D bounding box's height is 
    less than or equal to ``height``.

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's height is less than
    or equal to ``height``.

    Args:
        height: Target height value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's height is less than or equal to ``height``.
    """
    return lambda bbox: _height(bbox) <= height

def height_between(height1, height2):
    """Returns a function that computes whether a 2D bounding box's height is 
    between ``height1`` and ``height2`` (inclusive).

    The output function takes in a 2D bounding box (dict with keys 'x1', 'x2',
    'y1', 'y2') and returns ``True`` if the bounding box's height is between
    ``height1`` and ``height2``.

    Args:
        height1: Minimum height value.
        height2: Maximum height value.
    
    Returns:
        A function that takes a 2D bounding box and returns ``True`` if the
        bounding box's height is between ``height1`` and ``height2``.
    """
    return lambda bbox: _height(bbox) >= height1 and _height(bbox) <= height2

# Binary bounding box predicates.
def left_of():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly to the left of the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box's 'x2' value
    is less than the second bounding box's 'x1' value.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly to the left of the second one.
    """
    return lambda bbox1, bbox2: bbox1['x2'] < bbox2['x1']

def right_of():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly to the right of the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box's 'x1' value
    is greater than the second bounding box's 'x2' value.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly to the right of the second one.
    """
    return lambda bbox1, bbox2: bbox1['x1'] > bbox2['x2']

def above():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly above the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box's 'y2' value
    is less than the second bounding box's 'y1' value.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly above the second one.
    """
    return lambda bbox1, bbox2: bbox1['y2'] < bbox2['y1']

def below():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly below the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box's 'y1' value
    is greater than the second bounding box's 'y2' value.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly below the second one.
    """
    return lambda bbox1, bbox2: bbox1['y1'] > bbox2['y2']

def same_area(epsilon=0.1):
    """Returns a function that takes two 2D bounding boxes and computes whether
    the difference in their areas is less than ``epsilon``.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the difference in their areas is
    less than ``epsilon``.

    Args:
        epsilon: The maximum difference in area between the two bounding boxes.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        difference in their areas is less than ``epsilon``.
    """
    return lambda bbox1, bbox2: abs(_area(bbox1) - _area(bbox2)) < epsilon

def more_area():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one has strictly more area than the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box has
    strictly more area than the second one.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box has strictly more area than the second one.
    """
    return lambda bbox1, bbox2: _area(bbox1) > _area(bbox2)

def less_area():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one has strictly less area than the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box has
    strictly less area than the second one.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box has strictly less area than the second one.
    """
    return lambda bbox1, bbox2: _area(bbox1) < _area(bbox2)

def same_width(epsilon=0.1):
    """Returns a function that takes two 2D bounding boxes and computes whether
    the difference in their widths is less than ``epsilon``.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the difference in their widths is
    less than ``epsilon``.

    Args:
        epsilon: The maximum difference in area between the two bounding boxes.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        difference in their widths is less than ``epsilon``.
    """
    return lambda bbox1, bbox2: abs(_width(bbox1) - _width(bbox2)) < epsilon

def more_width():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly wider than the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box is
    strictly wider than the second one.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly wider than the second one.
    """
    return lambda bbox1, bbox2: _width(bbox1) > _width(bbox2)

def less_width():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly narrower than the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box is
    strictly narrower than the second one.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly narrower than the second one.
    """
    return lambda bbox1, bbox2: _width(bbox1) < _width(bbox2)

def same_height(epsilon=0.1):
    """Returns a function that takes two 2D bounding boxes and computes whether
    the difference in their heights is less than ``epsilon``.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the difference in their heights is
    less than ``epsilon``.

    Args:
        epsilon: The maximum difference in area between the two bounding boxes.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        difference in their heights is less than ``epsilon``.
    """
    return lambda bbox1, bbox2: abs(_height(bbox1) - _height(bbox2)) < epsilon

def more_height():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly taller than the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box is
    strictly taller than the second one.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly taller than the second one.
    """
    return lambda bbox1, bbox2: _height(bbox1) > _height(bbox2)

def less_height():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is strictly shorter than the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first bounding box is
    strictly shorter than the second one.

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first bounding box is strictly shorter than the second one.
    """
    return lambda bbox1, bbox2: _height(bbox1) < _height(bbox2)

def same_value(key, epsilon=0.1):
    """Returns a function that takes two dicts and computes whether
    the difference between two of their values is less than ``epsilon``.

    The output function takes in two dicts ``d1`` and ``d2`` and returns
    ``True`` if the absolute difference between ``d1[key]`` and ``d2[key]`` is
    less than ``epsilon``.

    Args:
        epsilon: The maximum difference between the two values of the two
            dicts.

    Returns:
        A function that takes two dicts and returns ``True`` if the
        absolute difference between two of their values is less than
        ``epsilon``.
    """
    return lambda bbox1, bbox2: abs(bbox1[value_name] - bbox2[value_name]) < epsilon

def inside():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one is inside the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first one is inside the
    second one (boundaries inclusive).

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first one is inside the second one.
    """
    return lambda bbox1, bbox2: (
        bbox2['x1'] >= bbox1['x1'] and
        bbox2['x2'] <= bbox1['x2'] and
        bbox2['y1'] >= bbox1['y1'] and
        bbox2['y2'] <= bbox1['y2'])

def contains():
    """Returns a function that takes two 2D bounding boxes and computes whether
    the first one contains the second one.

    The output function takes in two 2D bounding boxes (dicts with keys 'x1',
    'x2', 'y1', 'y2') and returns ``True`` if the first one contains the
    second one (boundaries inclusive).

    Returns:
        A function that takes two 2D bounding boxes and returns ``True`` if the
        first one contains the second one.
    """
    return lambda bbox1, bbox2: inside()(bbox2, bbox1)
