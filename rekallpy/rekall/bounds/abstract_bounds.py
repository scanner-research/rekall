"""This module defines the ``Bounds`` class, which all bounds should inherit
from.
"""

class Bounds:
    """
    The ``Bounds`` class is a simple wrapper around a dictionary. Typically,
    the keys in this dictionary should represent physical bounds, like ``t1``,
    ``t2`` for time or ``x1``, ``x2`` for space.

    Each class that inherits from ``Bounds`` should define a ``data`` dict upon
    initialization. This allows fields from the bounds to be referenced using
    ``[]`` notation.

    Each child class should also implement the following methods:
    
    ``__lt__`` for sorting

    ``__repr__`` for printing

    ``primary_axis`` to specify a tuple representing the major axis for
    optimization. For videos, the primary  will typically be ``('t1', 't2')``.

    ``copy`` for producing a copy of this Bounds

    The ``Bounds`` class comes with a ``combine`` method that takes two Bounds
    instances and a combiner function and combines them into one Bounds.
    Child classes may want to implement common combination functions or provide
    mechanisms to use different combination functions across multiple axes.

    The ``Bounds`` class also provides a ``cast`` mechanism to recast
    predicates to use other dimensions. The ``cast`` mechanism takes in a
    function that expects one or more ``dict``-like objects as input and remaps
    keys of the input.

    Example:
        Let's define a simple function that prints the 't' field of an object::

            def print_t(obj):
                print(obj['t'])

        Now suppose we have an object with a key of 'x' instead::

            my_obj = { 'x': 1 }

        Then we can cast ``print_t`` to print out the 'x' field instead of the
        't' field::

            >>> cast({'t': 'x'})(print_t)(my_obj)
            1
    
    See the example below for a full example of a Bounds class.

    Example:
        The example below defines a two-dimensional Bounds object with two
        dimensions, defined by ``t1``, ``t2``, ``x1``, and ``x2``::

            from rekall.predicates import overlaps

            class Bounds2D(Bounds):
                def __init__(self, t1, t2, x1, x2):
                    self.data = { 't1': t1, 't2': t2, 'x1': x1, 'x2': x2 }

                def __lt__(self, other):
                    return ((self['t1'], self['t2'], self['x1'], self['x2']) <
                            (other['t1'], other['t2'], other['x1'], other['x2']))

                def __repr__(self):
                    return 't1:{} t2:{} x1:{} x2:{}'.format(
                        self['t1'], self['t2'], self['x1'], self['x2'])

                def primary_axis(self):
                    return ('t1', 't2')

                def T(pred):
                    return cast({ 't1': 'x1', 't2': 'x2' })(pred)

            bounds1 = Bounds2D(0, 1, 0.5, 0.7)
            bounds2 = Bounds2D(2, 3, 0.4, 0.6)

            # overlaps expects two objects with fields 't1' and 't2' and
            #   computes whether there is overlap in that dimension

            # This is False, since there is no time overlap
            overlaps()(bounds1, bounds2)

            # This is True, since there is overlap in the X dimension
            Bounds2D.X(overlaps())(bounds1, bounds2)

            # This is True.
            bounds1 < bounds2

            # This returns ('t1', 't2')
            bounds1.primary_axis()

    Attributes:
        data: dict mapping from co-ordinate keys to co-ordinate values
    """

    def __getitem__(self, arg):
        """Get ``arg`` from ``self.data``."""
        return self.data[arg]

    def __setitem__(self, key, item):
        """Set ``self.data[key]`` to ``item``."""
        self.data[key] = item

    def combine(self, other, combiner):
        """Combines two Bounds into a single new Bound using ``combiner``.

        args:
            other: The other Bound to combine with.
            combiner: A function that takes two Bounds as input (``self`` and
                ``other``) and returns a single new Bound.

        Returns:
            The output of ``combiner(self, other)``.
        """
        return combiner(self, other)

    def cast(schema):
        """Return a function that takes in a predicate function and remaps key
        lookups according to ``schema``.
        
        The output function takes in a predicate function ``p`` and returns a
        modified function ``p'``. It expects ``p`` to take in some amount of
        ``dict``-like objects and re-map lookups in those objects according to
        ``schema``.
        
        In particular, let ``obj`` be an argument to ``p``. Then for every key
        ``k`` in ``schema``, ``obj[k]`` will be re-mapped to ``obj[schema[k]]``
        in ``p'``.
        Example:
            Let's define a simple function that prints the 't' field of an
            object::
                def print_t(obj):
                    print(obj['t'])
            Now suppose we have an object with a key of 'x' instead::
                my_obj = { 'x': 1 }
            Then we can cast ``print_t`` to print out the 'x' field instead of
            the 't' field::
                >>> cast({'t': 'x'})(print_t)(my_obj)
                1
        
        Args:
            schema: A ``dict`` representing re-mappings for the target
                function. For every key ``k`` in ``schema``, ``k`` is re-mapped
                to ``schema[k]`` in the transformed function.
        Returns:
            A function that transforms a predicate function by remapping
            key-value lookups in the predicate function's arguments.
        """
        class WrappedArg:
            def __init__(self, orig_obj, schema):
                self.orig_obj = orig_obj
                self.schema = schema

            def __getitem__(self, arg):
                if arg in self.schema:
                    return self.orig_obj[self.schema[arg]]
                else:
                    return self.orig_obj[arg]

        def wrap_pred(pred):
            def new_pred(*args):
                return pred(*[WrappedArg(a, schema) for a in args])
            return new_pred

        return wrap_pred

    def size(self, axis=None):
        """Get the size of the bounds along some axis.
        
        Args:
            axis (optional): The axis to compute size on. Represented as a pair
                of co-ordinates, such as ``('t1', 't2')``. Defaults to ``None``,
                which uses the ``primary_axis`` of ``self``.
        
        Returns:
            The size of the bounds across some axis.
        """
        if axis is None:
            axis = self.primary_axis()
        return self[axis[1]] - self[axis[0]]

    def __lt__(self, other):
        """Method to compare two Bounds. Child classes should implement
        this."""
        pass

    def __repr__(self):
        """Method to get a string representation of a Bound. Child classes
        should implement this."""
        pass

    def primary_axis(self):
        """Method to get the primary axis of a Bound for optimizations. Child
        classes should implement this."""
        pass

    def copy(self):
        """Method to get another Bound that has the same data as this Bound.
        Child classes should implement this."""
        pass
