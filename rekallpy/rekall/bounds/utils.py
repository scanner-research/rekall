"""Utilities for Bounds."""

# Bound Combiners
def bounds_span(bound1, bound2):
    """Produces the span of two 1D bounds, represented as tuples.
    
    Args:
        bound1, bound2: Bounds to merge. They are in the form of pair
        (start, end).
    
    Returns:
        The span of the bounds bound.
    """
    return (min(bound1[0], bound2[0]),max(bound1[1], bound2[1]))

def bounds_intersect(bound1, bound2):
    """Produces the intersection of two 1D bounds, represented as tuples.
    
    Args:
        bound1, bound2: Bounds to intersect. They are in the form of pair
        (start, end). They need to overlap for the return value to be a valid
        bound.
    
    Returns:
        The overlap between two bounds.
    """
    return (max(bound1[0], bound2[0]),min(bound1[1], bound2[1]))

def bound_size(b):
    """Length of the given bound, represented as a tuple."""
    return b[1]-b[0]
