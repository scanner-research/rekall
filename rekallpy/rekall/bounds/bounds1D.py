"""This module defines and implements the Bounds1D one-dimensional bound."""

from rekall.bounds import Bounds, utils
from rekall.predicates import overlaps

class Bounds1D(Bounds):
    """Object representing a one-dimensional (temporal) bound.
    
    This class has co-ordinates 't1' and 't2', representing the start and end
    in a temporal dimension, respectively.
    This class has no built-in casts, since there's only one dimension.
    """
    def __init__(self, t1, t2):
        """Initialize this Bounds1D object by explicitly passing in values for
        't1' and 't2'.

        Args:
            t1: The value for 't1'.
            t2: The value for 't2'.

        Returns:
            A Bounds1D object with 't1' and 't2' co-ordinates.
        """
        self.data = {
            't1': t1,
            't2': t2
        }

    @classmethod
    def fromTuple(cls, t1t2_tuple):
        """Create a Bounds1D object with a tuple of length two.

        Args:
            t1t2_tuple: A tuple of length two. The tuple items can be any type
                with an ordering function. The first tuple item becomes the
                value for 't1', and the second tuple item becomes the value for
                't2'.

        Returns:
            A Bounds1D object with 't1' and 't2' co-ordinates, specified by
            ``t1t2_tuple``.
        """
        return cls(*list(t1t2_tuple))

    def __lt__(self, other):
        """Ordering of a Bounds1D is by 't1' first and then 't2'."""
        return (self['t1'], self['t2']) < (other['t1'], other['t2'])

    def __repr__(self):
        """String representation is ``'t1:val t2:val'``."""
        return 't1:{} t2:{}'.format(self['t1'], self['t2'])

    def primary_axis(self):
        """The primary axis is time."""
        return ('t1', 't2')

    def span(self, other):
        """Returns the minimum Bound spanning both ``self`` and ``other``.
        
        Returns:
            A single Bounds1D spanning ``self`` and ``other``.
        """
        return Bounds1D.fromTuple(utils.bounds_span(
            (self['t1'], self['t2']), 
            (other['t1'], other['t2'])
        ))

    def intersect(self, other):
        """Returns the bound intersecting ``self`` and ``other``, or
        ``None`` if the bounds do not overlap.

        Returns:
            A single Bounds1D covering the intersection of ``self`` and
            ``other``, or ``None`` if the two bounds do not overlap.
        """
        if overlaps()(self, other):
            return Bounds1D.fromTuple(utils.bounds_intersect(
                (self['t1'], self['t2']), 
                (other['t1'], other['t2'])
            ))
        else:
            return None

    def size(self):
        """Returns the size of this bound."""
        return utils.bound_size((self['t1'], self['t2']))

    def copy(self):
        """Returns a copy of this bound."""
        return Bounds1D(self['t1'], self['t2'])

    def T():
        """Returns a tuple representing the time axis."""
        return ('t1', 't2')
