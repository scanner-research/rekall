"""
A collection of built-in predicate functions. Each function in this module
takes in some number of arguments and returns an anonymous function that
evaluates to True or False.

Example:
    The ``area_at_least`` predicate returns a predicate that computes whether a
    2D bounding box's area is at least some value. This is how you would use
    it::

        bbox = { 'x1': 0.1, 'x2': 0.3, 'y1': 0.1, 'y2': 0.3 }
        pred = area_at_least(.03)
        bbox_satisfies_pred = pred(bbox)
"""

from rekall.helpers import INFTY

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

# Predicates on payloads.
def payload_satisfies(pred):
    """This wraps a predicate so it is applied to the payloads of intervals.

    The output function expects one or more Intervals as input (depending on
    how many arguments ``pred`` expects) and applies the predicate to the
    payloads of the Intervals instead of the Intervals themselves.

    Arg:
        pred: The predicate to wrap.

    Returns:
        An output function that applies ``pred`` to payloads.
    """
    def new_pred(*interval_args):
        return pred(*[i.payload for i in interval_args])
    return new_pred

def on_key(key, pred):
    """This wraps a predicate so it is applied to a value in a dict instead of
    the dict itself.

    The output function expects one or more dicts as input (depending on how
    many arguments ``pred`` expects) and applies the predicate to ``d[key]``
    for every dict ``d`` of input.

    Arg:
        key: The key of the dict to apply the predicate to.
        pred: The predicate to wrap.

    Returns:
        An output function that applies ``pred`` to keyed values of dict(s).
    """
    def new_pred(*args):
        return pred(*[arg[key] for arg in args])
    return new_pred

# Temporal predicates
def before(min_dist=0, max_dist=INFTY):
    """Returns a function that computes whether a temporal interval is before
    another, optionally filtering the time difference to be between
    ``min_dist`` and ``max_dist`` (inclusive).

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the time difference between the start of the second interval and the end of
    the first interval is between ``min_dist`` and ``max_dist``, inclusive.

    Arg:
        min_dist: The minimum time difference between the two intervals.
            Negative values are undefined.
        max_dist: The maximum time difference between the two intervals. If
            this is ``INFTY``, then the maximum time difference is unbounded.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval is before the second interval.
    """
    def fn(intrvl1, intrvl2):
        time_diff = intrvl2['t1'] - intrvl1['t2']
        return (time_diff >= min_dist and
            (max_dist == INFTY or time_diff <= max_dist))

    return fn

def after(min_dist=0, max_dist=INFTY):
    """Returns a function that computes whether a temporal interval is after
    another, optionally filtering the time difference to be between
    ``min_dist`` and ``max_dist`` (inclusive).

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the time difference between the start of the first interval and the end of
    the second interval is between ``min_dist`` and ``max_dist``, inclusive.

    Arg:
        min_dist: The minimum time difference between the two intervals.
            Negative values are undefined.
        max_dist: The maximum time difference between the two intervals. If
            this is ``INFTY``, then the maximum time difference is unbounded.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval is after the second interval.
    """
    def fn(intrvl1, intrvl2):
        time_diff = intrvl1['t1'] - intrvl2['t2']
        return (time_diff >= min_dist and
            (max_dist == INFTY or time_diff <= max_dist))

    return fn

def overlaps():
    """Returns a function that computes whether a temporal interval overlaps
    another in any way (including just at the endpoints).

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the two intervals overlap in any way

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the two intervals overlap in any way.
    """
    return lambda intrvl1, intrvl2: ((intrvl1['t1'] < intrvl2['t1'] and intrvl1['t2'] > intrvl2['t1']) or
            (intrvl1['t1'] < intrvl2['t2'] and intrvl1['t2'] > intrvl2['t2']) or
            (intrvl1['t1'] <= intrvl2['t1'] and intrvl1['t2'] >= intrvl2['t2']) or
            (intrvl1['t1'] >= intrvl2['t1'] and intrvl1['t2'] <= intrvl2['t2']))

def overlaps_before():
    """Returns a function that computes whether a temporal interval has
    non-zero overlap with another interval, and starts before it.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the first interval starts before the second interval, and the two intervals
    have a non-zero amount of overlap.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval starts before the second interval, and
        the two intervals have non-zero overlap.
    """
    return lambda intrvl1, intrvl2: (intrvl1['t2'] > intrvl2['t1'] and intrvl1['t2'] < intrvl2['t2'] and
            intrvl1['t1'] < intrvl2['t1'])

def overlaps_after():
    """Returns a function that computes whether a temporal interval has
    non-zero overlap with another interval, and starts after it.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the first interval starts after the second interval, and the two intervals
    have a non-zero amount of overlap.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval starts after the second interval, and
        the two intervals have non-zero overlap.
    """
    return lambda intrvl1, intrvl2: (intrvl1['t1'] > intrvl2['t1'] and intrvl1['t1'] < intrvl2['t2'] and
            intrvl1['t2'] > intrvl2['t2'])

def starts(epsilon=0):
    """Returns a function that computes whether a temporal interval has the
    same start time as another interval (+/- epsilon), and ends before it.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the first interval starts at the same time as the second interval (+/-
    ``epsilon``), and the first interval ends before the second interval.

    Args:
        epsilon: The maximum difference between the start time of the first
            interval and the start time of the second interval.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval starts at the same time as the second
        interval, and ends before the second interval ends.
    """
    return lambda intrvl1, intrvl2: (abs(intrvl1['t1'] - intrvl2['t1']) <= epsilon
            and intrvl1['t2'] < intrvl2['t2'])

def starts_inv(epsilon=0):
    """Returns a function that computes whether a temporal interval has the
    same start time as another interval (+/- epsilon), and ends before it.
    This is the inverse of the ``starts`` predicate; it checks whether the
    second interval starts the first interval.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the second interval starts at the same time as the first interval (+/-
    ``epsilon``), and the second interval ends before the first interval.

    Args:
        epsilon: The maximum difference between the start time of the second
            interval and the start time of the first interval.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the second interval starts at the same time as the first
        interval, and ends before the first interval ends.
    """
    return lambda intrvl1, intrvl2: (abs(intrvl1['t1'] - intrvl2['t1']) <= epsilon
            and intrvl2['t2'] < intrvl1['t2'])

def finishes(epsilon=0):
    """Returns a function that computes whether a temporal interval has the
    same end time as another interval (+/- epsilon), and starts after it.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the first interval ends at the same time as the second interval (+/-
    ``epsilon``), and the first interval starts after the second interval.

    Args:
        epsilon: The maximum difference between the end time of the first
            interval and the end time of the second interval.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval ends at the same time as the second
        interval, and starts after the second interval starts.
    """
    return lambda intrvl1, intrvl2: (abs(intrvl1['t2'] - intrvl2['t2']) <= epsilon
            and intrvl1['t1'] > intrvl2['t1'])

def finishes_inv(epsilon=0):
    """Returns a function that computes whether a temporal interval has the
    same end time as another interval (+/- epsilon), and starts after it.
    This is the inverse of the ``finishes`` predicate; it checks whether the
    second interval finishes the first interval.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the second interval ends at the same time as the first interval (+/-
    ``epsilon``), and the second interval starts after the first interval.

    Args:
        epsilon: The maximum difference between the end time of the second
            interval and the end time of the first interval.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the second interval ends at the same time as the first
        interval, and starts after the first interval starts.
    """
    return lambda intrvl1, intrvl2: (abs(intrvl1['t2'] - intrvl2['t2']) <= epsilon
            and intrvl2['t1'] > intrvl1['t1'])

def during():
    """Returns a function that computes whether a temporal interval takes place
    entirely during another temporal interval (i.e. it starts after the other
    interval starts and ends before the other interval ends).

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the first interval starts strictly after the second interval starts and
    ends strictly before the second interval ends.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval takes place strictly during the second
        interval.
    """
    return lambda intrvl1, intrvl2: intrvl1['t1'] > intrvl2['t1'] and intrvl1['t2'] < intrvl2['t2']

def during_inv():
    """Returns a function that computes whether a temporal interval takes place
    entirely during another temporal interval (i.e. it starts after the other
    interval starts and ends before the other interval ends).
    This is the inverse of the ``during`` predicate; it checks whether the
    second interval takes place during the first interval.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the second interval starts strictly after the first interval starts and
    ends strictly before the first interval ends.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the second interval takes place strictly during the first
        interval.
    """
    return lambda intrvl1, intrvl2: intrvl2['t1'] > intrvl1['t1'] and intrvl2['t2'] < intrvl1['t2']

def meets_before(epsilon=0):
    """Returns a function that computes whether a temporal interval ends at the
    same time as another interval starts (+/- epsilon).

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the absolute time difference between the end of the first interval and the
    start of the second interval is less than ``epsilon``.

    Args:
        epsilon: The maximum time difference between the end of the first
            interval and the start of the second interval.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval ends at the same time as the second
        interval starts.
    """
    return lambda intrvl1, intrvl2: abs(intrvl1['t2']-intrvl2['t1']) <= epsilon

def meets_after(epsilon=0):
    """Returns a function that computes whether a temporal interval ends at the
    same time as another interval starts (+/- epsilon).
    This is the inverse of the ``meets_before`` predicate; it checks whether
    the first interval starts when the second interval ends.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the absolute time difference between the start of the first interval and
    the end of the second interval is less than ``epsilon``.

    Args:
        epsilon: The maximum time difference between the start of the first
            interval and the end of the second interval.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the first interval starts at the same time as the second
        interval ends.
    """
    return lambda intrvl1, intrvl2: abs(intrvl2['t2']-intrvl1['t1']) <= epsilon

def equal():
    """Returns a function that computes whether two temporal intervals are
    strictly equal.

    The output function expects two temporal intervals (dicts with keys 't1'
    and 't2' for the start and end times, respectively). It returns ``True`` if
    the two intervals have equal start times and equal end times.

    Returns:
        An output function that takes two temporal intervals and returns
        ``True`` if the two intervals have equal start times and equal end
        times.
    """
    return lambda intrvl1, intrvl2: intrvl1['t1'] == intrvl2['t1'] and intrvl1['t2'] == intrvl2['t2']

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

# List predicates
def length_exactly(n):
    """Returns a function that checks whether a list has length exactly ``n``.

    Args:
        n: The length to check against.

    Returns:
        A function that takes in a list ``l`` and returns ``True`` if
        ``len(l)`` is equal to ``n``.
    """
    return lambda l: len(l) == n

def length_at_least(n):
    """Returns a function that checks whether a list has length at least ``n``.

    Args:
        n: The length to check against.

    Returns:
        A function that takes in a list ``l`` and returns ``True`` if
        ``len(l)`` is greater than or equal to ``n``.
    """
    return lambda l: len(l) >= n

def length_at_most(n):
    """Returns a function that checks whether a list has length less than or
    equal to ``n``.

    Args:
        n: The length to check against.

    Returns:
        A function that takes in a list ``l`` and returns ``True`` if
        ``len(l)`` is less than or equal to ``n``.
    """
    return lambda l: len(l) <= n

def length_between(n1, n2):
    """Returns a function that checks whether a list's length is between ``n1``
    and ``n2`` (inclusive).

    Args:
        n1: The minimum length of the list.
        n2: The maximum length of the list.

    Returns:
        A function that takes in a list ``l`` and returns ``True`` if
        ``len(l)`` is between ``n1`` and ``n2`` (inclusive).
    """
    return lambda l: (len(l) >= n1 and len(l) <= n2)

