"""An Interval is a wrapper around a Bounds instance with a payload.
"""

class Interval:
    """A single Interval.

    An Interval is a wrapper around a Bounds and a payload. The payload can be
    referenced with the 'payload' key - i.e. ``interval['payload']``, as can
    the fields of the Bounds. The bounds field itself can also be referenced
    with type 'bounds' key.

    Attributes:
        bounds: Bounds object.
        payload: payload object.
    """
    def __init__(self, bounds, payload=None):
        """Initializes an interval with certain bounds and payload.
        
        Args:
            bounds: Bounds for this Interval.
            payload (optional): Metadata of arbitrary type. Defaults to None.
        """
        self.bounds = bounds
        self.payload = payload

    def __getitem__(self, arg):
        """Access bounds, payload, or a co-ordinate of bounds using key access.

        Strings 'bounds' and 'payload' are hard-coded to return the bounds or
        payload attributes, respectively.
        """
        if arg == 'bounds':
            return self.bounds
        if arg == 'payload':
            return self.payload
        return self.bounds[arg]

    def __setitem__(self, key, item):
        """Set bounds, payload, or a co-ordinate of bounds using key access.

        Strings 'bounds' and 'payload' are hard-coded to reference the bounds
        or payload attributes, respectively.
        """
        if key == 'bounds':
            self.bounds = item
        elif key == 'payload':
            self.payload = item
        else:
            self.bounds[key] = item

    def __repr__(self):
        """String representation is ``<Interval {bounds} payload:{payload}>``."""
        return "<Interval {} payload:{}>".format(self.bounds, self.payload)

    def __lt__(self, other):
        return self['bounds'] < other['bounds']

    def copy(self):
        """Returns a copy of the Interval."""
        return Interval(self.bounds.copy(), self.payload)

    def combine(self, other, bounds_combiner,
            payload_combiner=lambda p1, p2: p1):
        """Combines two Intervals into one by separately combining the bounds
        and the payload.

        Args:
            other: The other Interval to combine with.
            bounds_combiner: The function to combine the bounds. Takes two
                Bounds objects as input and returns one Bounds object.
            payload_combiner: The function to combine the two payloads. Takes
                two payload objects as input and returns one payload object.

        Returns:
            A new Interval combined using ``bounds_combiner`` and
            ``payload_combiner``.
        """
        return Interval(bounds_combiner(self.bounds, other.bounds),
            payload_combiner(self.payload, other.payload))

    def P(pred):
        """This wraps a predicate so it is applied to the payload of Intervals
        instead of the Intervals themselves.

        The output function expects one or more Intervals as input (depending
        on how many arguments ``pred`` expects) and applies the predicate to
        the payloads of the Intervals instead of the Interavls themselves.

        Arg:
            pred: The predicate to wrap.

        Returns:
            An output function that applies ``pred`` to payloads.
        """
        def new_pred(*interval_args):
            return pred(*[i.payload for i in interval_args])
        return new_pred

    def size(self, axis=None):
        """Get the size of the bounds along some axis.
        
        Args:
            axis (optional): The axis to compute size on. Represented as a pair
                of co-ordinates, such as ``('t1', 't2')``. Defaults to ``None``,
                which uses the primary axis of ``self``'s Bounds.
        
        Returns:
            The size of the bounds across some axis.
        """
        return self.bounds.size(axis)
