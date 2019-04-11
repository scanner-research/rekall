"""This module defines and implements the Bounds3D three-dimensional bound."""

from rekall.bounds import Bounds

class Bounds3D(Bounds):
    """Object representing a three-dimensional (time, x, y) bound.

    The class has co-ordinates 't1', 't2', 'x1', 'x2', 'y1', 'y2', representing
    start and end co-ordinates in the time, x, and y dimensions respectively.

    This class has two built-in one-dimensional casts - ``X()`` and ``Y()``
    cast the time dimensions to the x and y dimensions so that temporal
    predicates can be used on one-dimensional spatial dimensions.
    """
    def __init__(self, t1, t2, x1=0., x2=1., y1=0., y2=1.):
        """Initialize this Bounds3D object by manually passing in all six
        co-ordinates.

        Args:
            t1: 't1' value.
            t2: 't2' value.
            x1: 'x1' value.
            x2: 'x2' value.
            y1: 'y1' value.
            y2: 'y2' value.

        Returns:
            A Bounds3D object with the six co-ordinates specified by the
            arguments.
        """
        self.data = {
            't1': t1, 't2': t2, 'x1': x1, 'x2': x2, 'y1': y1, 'y2': y2
        }

    def fromTuple(tuple_3d):
        """Initialize a Bounds3D object with a tuple of length two or six.

        Args:
            tuple3d: A tuple of length two or six. The items represent, in order,
                't1', 't2', 'x1', 'x2', 'y1', and 'y2', respectively. If the
                tuple is only of length two, 'x1' and 'y1' get set to 0., and
                'x2' and 'y2' get set to 1.

        Returns:
            A Bounds3D object with the six co-ordinates specified by the six
            items in ``tuple3d``.
        """
        return Bounds3D(*list(tuple_3d))

    def __lt__(self, other):
        """Ordering is by 't1', 't2', 'x1', 'x2', 'y1', 'y2'."""
        return (self['t1'], self['t2'], self['x1'], self['x2'], self['y1'],
                self['y2']) < (other['t1'], other['t2'], other['x1'],
                        other['x2'], other['y1'], other['y2'])

    def __repr__(self):
        """String representation is
        ``'t1:val t2:val x1:val x2:val y1:val y2:val'``."""
        return 't1:{} t2:{} x1:{} x2:{} y1:{} y2:{}'.format(self['t1'], 
                self['t2'], self['x1'], self['x2'], self['y1'], self['y2'])

    def primary_axis(self):
        """Primary axis is time."""
        return ('t1', 't2')

    def X(pred):
        """Returns a function that transforms predicates by casting accesses to
        't1' to 'x1' and accesses to 't2' to 'x2'.

        Example:
            Here is an example of casting an example predicate::
            
                # This predicate tests whether a bound's 't2' value is greater
                # than its 't1' value
                def example_pred(bounds):
                    return bounds['t2'] > bounds['t1']

                # t1 = 0, t2 = 1, x1 = 1, x2 = 0, y1 = 1, y2 = 0
                higher_t2_lower_x2 = Bounds3D(0, 1, 1, 0, 1, 0)
                
                example_pred(higher_t2_lower_x2) # this is True, since t2 > t1

                Bounds3D.X(example_pred)(higher_t2_lower_x2) # this is False, since x2 < x1
        
        Arg:
            pred: The predicate to cast.

        Returns:
            The same predicate as ``pred``, except accesses to 't1' are cast to
            'x1', and accesses to 't2' are cast to 'x2'.
        """
        return Bounds.cast({
            't1': 'x1',
            't2': 'x2'
        })(pred)

    def Y(pred):
        """Returns a function that transforms predicates by casting accesses to
        't1' to 'y1' and accesses to 't2' to 'y2'.

        Example:
            Here is an example of casting an example predicate::
            
                # This predicate tests whether a bound's 't2' value is greater
                # than its 't1' value
                def example_pred(bounds):
                    return bounds['t2'] > bounds['t1']

                # t1 = 0, t2 = 1, x1 = 1, x2 = 0, y1 = 1, y2 = 0
                higher_t2_lower_x2 = Bounds3D(0, 1, 1, 0, 1, 0)
                
                example_pred(higher_t2_lower_y2) # this is True, since t2 > t1

                Bounds3D.X(example_pred)(higher_t2_lower_y2) # this is False, since y2 < y1
        
        Arg:
            pred: The predicate to cast.

        Returns:
            The same predicate as ``pred``, except accesses to 't1' are cast to
            'y1', and accesses to 't2' are cast to 'y2'.
        """
        return Bounds.cast({
            't1': 'y1',
            't2': 'y2'
        })(pred)
