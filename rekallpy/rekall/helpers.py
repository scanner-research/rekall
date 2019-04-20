"""This module defines a collection of helper functions and constants.

Attributes:
    INFTY: A String representing Infinity for float comparisons in predicates.
"""
import sys
from contextlib import contextmanager

INFTY = "infty"

def panic(message):
    """ Print an error message and exit. """
    print(message, file=sys.stderr)
    sys.exit(1)

# Helper functions to define bboxes representing regions of the screen
def make_bbox(x1, y1, x2, y2):
    """A helper function to make a 2D bounding box.

    Args:
        x1: The x1 value of the bounding box.
        y1: The y1 value of the bounding box.
        x2: The x2 value of the bounding box.
        y2: The y2 value of the bounding box.

    Returns:
        A dict with keys 'x1', 'y1', 'x2', 'y2' representing a bounding box.
    """
    return { 'x1': x1, 'y1': y1, 'x2': x2, 'y2': y2 }

def full_frame():
    """Returns a bounding box representing the full frame."""
    return make_bbox(0., 0., 1., 1.)

def left_half(bbox=full_frame()):
    """Returns a bounding box covering the left half of ``bbox``."""
    return make_bbox(bbox['x1'], bbox['y1'],
            (bbox['x1'] + bbox['x2']) / 2., bbox['y2'])

def right_half(bbox=full_frame()):
    """Returns a bounding box covering the right half of ``bbox``."""
    return make_bbox((bbox['x1'] + bbox['x2']) / 2., bbox['y1'],
            bbox['x2'], bbox['y2'])

def top_half(bbox=full_frame()):
    """Returns a bounding box covering the top half of ``bbox``."""
    return make_bbox(bbox['x1'], bbox['y1'],
            bbox['x2'], (bbox['y1'] + bbox['y2']) / 2.)

def bottom_half(bbox=full_frame()):
    """Returns a bounding box covering the bottom half of ``bbox``."""
    return make_bbox(bbox['x1'], (bbox['y1'] + bbox['y2']) / 2.,
            bbox['x2'], bbox['y2'])

def top_left(bbox=full_frame()):
    """Returns a bounding box covering the top left quadrant of ``bbox``."""
    return left_half(top_half(bbox))

def top_right(bbox=full_frame()):
    """Returns a bounding box covering the top right quadrant of ``bbox``."""
    return right_half(top_half(bbox))

def bottom_left(bbox=full_frame()):
    """Returns a bounding box covering the bottom left quadrant of ``bbox``."""
    return left_half(bottom_half(bbox))

def bottom_right(bbox=full_frame()):
    """Returns a bounding box covering the bottom right quadrant of ``bbox``."""
    return right_half(bottom_half(bbox))

def center(bbox=full_frame()):
    """Returns a bounding box covering a quarter of ``bbox``, starting in the
    middle."""
    width = bbox['x2'] - bbox['x1']
    height = bbox['y2'] - bbox['y1']
    return bbox(bbox['x1'] + width / 4.,
            bbox['y1'] + height / 4.,
            bbox['x2'] - width / 4.,
            bbox['y2'] - height / 4.)

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
