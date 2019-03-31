"""Utilities for working with 3D Intervals.

Bounds-related:
    merge_bound: Merges two 1D ranges.
    overlap_bound: Overlaps two 1D ranges.
    bound_size: Length of the 1D range.

Default sortkey:
    sort_key_time_x_y: Returns tuple (t1,t2,x1,x2,y1,y2) from 3D Interval

Predicate-related:
    T: Adapter for 1D-range predicates on temporal dimension of Interval3D(s).
    X: Adapter for 1D-range predicates on x-dimension of Interval3D(s).
    Y: Adapter for 1D-range predicates on y-dimension of Interval3D(s).
    XY: Adapter for bounding-box predicates on xy-projection of Interval3D(s).
    P: Adapter for predicates on payload of Interval3D(s).
    not_pred: Transforms the predicate to output its negation.
    and_preds: Composes predciates by ANDing them.
    or_preds: Composes predicates by ORing them.

Performance-related:
    perf_count: ContextManager that prints the walltime to STDOUT.
"""

from rekall.interval_list import Interval
from contextlib import contextmanager
from time import perf_counter

# Bound Combiners
def merge_bound(bound1, bound2):
    """Merges two 1D bounds.
    
    Args:
        bound1, bound2: Bounds to merge. They are in the form of pair
        (start, end).
    
    Returns:
        The merged bound.
    """
    return (min(bound1[0], bound2[0]),max(bound1[1], bound2[1]))

def overlap_bound(bound1, bound2):
    """Overlaps two 1D bounds.
    
    Args:
        bound1, bound2: Bounds to overlap. They are in the form of pair
        (start, end). They need to overlap for the return value to be a valid
        bound.
    
    Returns:
        The overlap between two bounds.
    """
    return (max(bound1[0], bound2[0]),min(bound1[1], bound2[1]))

def bound_size(b):
    """Length of the given bound."""
    return b[1]-b[0]

# Sortkeys for Interval3D
def sort_key_time_x_y(interval):
    """A sortkey function that sorts by (t1,t2,x1,x2,y1,y2).

    (t1,t2), (x1,x2), (y1,y2) are the 3D bounds on the interval.

    Args:
        interval (Interval3D): the interval to compute sort key for.

    Returns:
        6-Tuple that is the sortkey.
    """
    return (*interval.t,
            *interval.x,
            *interval.y)

# Adapters for spatiotemporal predicates
def T(pred):
    """Adapts predicate to work on temporal range of Interval3D(s).

    Example:
        from rekall.temporal_predicates import overlaps
        # Whether two intervals overlap temporally.
        T(overlaps())(interval1, interval2)

    Args:
        pred: A predicate on some number of 1d ranges defined by
            `start` and `end` attributes on them.

    Returns:
        A predicate on temporal ranges of same number of Interval3Ds
    """
    def new_pred(*interval_3ds):
        return pred(*[
            Interval(i.t[0], i.t[1], i.payload) for i in interval_3ds
        ])
    return new_pred

def X(pred):
    """Adapts predicate to work on spatial x-dimension of Interval3D(s).

    Example:
        from rekall.temporal_predicates import overlaps
        # Whether two intervals overlap horizontally.
        X(overlaps())(interval1, interval2)

    Args:
        pred: A predicate on some number of 1d ranges defined by
            `start` and `end` attributes on them.

    Returns:
        A predicate on x-ranges of same number of Interval3Ds
    """
    def new_pred(*interval_3ds):
        return pred(*[
            Interval(i.x[0], i.x[1], i.payload) for i in interval_3ds
        ])
    return new_pred

def Y(pred):
    """Adapts predicate to work on spatial y-dimension of Interval3D(s).

    Example:
        from rekall.temporal_predicates import overlaps
        # Whether two intervals overlap vertically.
        Y(overlaps())(interval1, interval2)

    Args:
        pred: A predicate on some number of 1d ranges defined by
            `start` and `end` attributes on them.

    Returns:
        A predicate on y-ranges of same number of Interval3Ds
    """
    def new_pred(*interval_3ds):
        return pred(*[
            Interval(i.y[0], i.y[1], i.payload) for i in interval_3ds
        ])
    return new_pred

def _interval_3d_to_bbox(intrvl):
    return {'x1':intrvl.x[0], 'x2':intrvl.x[1],
            'y1':intrvl.y[0], 'y2':intrvl.y[1],
            'payload': intrvl.payload
            }

def XY(pred):
    """Adapts predicate to work on spatial dimensions of Interval3D(s).

    Example:
        from rekall.bbox_predicates import left_of
        # Whether interval1 is to the left of interval2 
        XY(left_of())(interval1, interval2)

    Args:
        pred: A predicate on some number of bounding boxes, each represented
            as a dictionary with keys 'x1','x2','y1','y2'.

    Returns:
        A predicate on xy-projections of same number of Interval3Ds
    """
    def new_pred(*interval_3ds):
        return pred(*[
            _interval_3d_to_bbox(i) for i in interval_3ds 
        ])
    return new_pred

def P(pred):
    """Adapts predicate to work on payloads of Interval3D(s).

    Example:
        # frames_with_faces are set of frames where each frame has a payload
        # that is an IntervalSet3D of faces in that frame.
        # To get frames whose faces satisfy pattern 
        frames_with_faces.filter(P(lambda faces: len(faces.match(pattern))>0))

    Args:
        pred: A predicate on some number of payloads

    Returns:
        A predicate on payloads of same number of Interval3Ds
    """
    def new_pred(*interval_3ds):
        return pred(*[
            i.payload for i in interval_3ds 
        ])
    return new_pred


# Adapters for logical combinations of predicates
def not_pred(pred):
    """Negates the predicate"""
    def new_pred(*args):
        return not pred(*args)
    return new_pred

def and_preds(*preds):
    """ANDs the predicates"""
    def new_pred(*args):
        for pred in preds:
            if not pred(*args):
                return False
        return True
    return new_pred

def or_preds(*preds):
    """ORs the predicates"""
    def new_pred(*args):
        for pred in preds:
            if pred(*args):
                return True
        return False
    return new_pred

# Performance profling util
@contextmanager
def perf_count(name, enable=True):
    """Prints wall time for the code block to STDOUT

    Example:
        with perf_count("test code"):
            sleep(10)
        # Writes to stdout:
        # test code starts.
        # test code ends after 10.01 seconds
    """
    if not enable:
        yield
    else:
        print("{0} starts.".format(name))
        s = perf_counter()
        yield
        t = perf_counter()
        print("{0} ends after {1:.2f} seconds".format(name, t-s))
