"""This module defines and implements the Bounds1D one-dimensional bound."""

from rekall.bounds import Bounds

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

    def fromTuple(t1t2_tuple):
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
        return Bounds1D(*list(t1t2_tuple))

    def __lt__(self, other):
        """Ordering of a Bounds1D is by 't1' first and then 't2'."""
        return (self['t1'], self['t2']) < (other['t1'], other['t2'])

    def __repr__(self):
        """String representation is ``'t1:val t2:val'``."""
        return 't1:{} t2:{}'.format(self['t1'], self['t2'])

    def primary_axis(self):
        """The primary axis is time."""
        return ('t1', 't2')
