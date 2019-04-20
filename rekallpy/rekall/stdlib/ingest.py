"""Module for ingesting data from various sources into
``IntervalSetMapping``'s. Provides some common data loading facilities from
data sources that we've seen appear regularly in our use."""

from operator import attrgetter
from rekall.interval_set_mapping import IntervalSetMapping
from rekall.bounds import Bounds1D, Bounds3D
from tqdm import tqdm

def getter_accessor(row, field):
    """Accessor for iterables whose items have implemented __getitem__,
    like Pandas/Spark dataframes. Returns ``row[field]``."""
    return row[field]

def attrgetter_accesor(row, field):
    """Accessor for iterables whose fields are put into class attributes,
    like Django querysets. Returns the equivalent of ``row.field``."""
    return attrgetter(field)(row)

def ism_from_iterable_with_schema_bounds1D(iterable, key_accessor,
        bounds_schema={}, with_payload=lambda x: None, progress=False,
        total=None):
    """Constructs an IntervalSetMapping of Intervals with Bounds1D bounds from
    an iterable based on a schema.
    
    ``bounds_schema`` and ``key_accessor`` define how to access necessary
    ``key`` and co-ordinate fields from items of ``iterable``. In
    particular, for any value ``V`` in ``bounds_schema``,
    ``key_accessor(item, V)`` should access field ``V`` of ``item``.
    ``getter_accessor`` and ``attrgetter_accessor`` are two examples of
    ``key_accessor`` methods.

    By default, this function expects ``iterator`` to have fields ``key``,
    ``t1``, and ``t2``. But ``bounds_schema`` can overwrite those fields. For
    example, if the mapping key, t1, and t2 values are stored in fields
    ``video_id``, ``min_frame``, and ``max_frame`` respectively,
    ``bounds_schema`` should be set to::

        {
            'key': 'video_id',
            't1': 'min_frame',
            't2': 'max_frame'
        }

    And for each item in iterable, the key, t1, and t2 values will be accessed
    by ``key_accessor(item, 'video_id')``, ``key_accessor(item, 'min_frame')``,
    and ``key_accessor(item, 'max_frame')``, respectively.

    If only the key needs to be updated, ``bounds_schema`` should be set to::

        {
            'key': 'video_id'
        }

    Args:
        iterable: An iterable of elements whose relevant fields can be
            accessed by ``key_accessor``.
        key_accessor: A function that takes an element of ``iterable`` and
            a key and returns the value of the key on that element.
        bounds_schema (optional): A dictionary that overrides default field
            keys (``'key'`` for the key, ``'t1'`` for co-ordinate t1, and
            ``'t2'`` for co-ordinate t2).
        with_payload (optional): A function that takes in an item in iterable
            and returns the payload for the Interval from the item. Defaults to
            a function that returns ``None``.
        progress (optional): Whether to display a loading bar from ``tqdm``.
            Defaults to ``False``.
        total (optional): Used in conjunction with ``progress`` to optionally
            display the total number of items in the loading bar if
            ``progress`` is ``True``.

    Returns:
        An IntervalSetMapping with Intervals from each item of iterable.
    """
    schema_final = {
        "key": "key",
        "t1": "t1",
        "t2": "t2"
    }
    schema_final.update(schema)
    def key_parser(item):
        return key_accessor(item, schema_final['key'])
    def bounds_parser(item):
        return Bounds1D(
            key_accessor(item, schema_final['t1']),
            key_accessor(item, schema_final['t2']))
    return IntervalSetMapping.from_iterable(iterable, key_parser,
        bounds_parser, payload_parser, progress, total)
    
def ism_from_iterable_with_schema_bounds3D(iterable, key_accessor,
        bounds_schema={}, with_payload=lambda x: None, progress=False,
        total=None):
    """Constructs an IntervalSetMapping of Intervals with Bounds3D bounds from
    an iterable based on a schema.
    
    ``bounds_schema`` and ``key_accessor`` define how to access necessary
    ``key`` and co-ordinate fields from items of ``iterable``. In
    particular, for any value ``V`` in ``bounds_schema``,
    ``key_accessor(item, V)`` should access field ``V`` of ``item``.
    ``getter_accessor`` and ``attrgetter_accessor`` are two examples of
    ``key_accessor`` methods.

    By default, this function expects ``iterator`` to have fields ``key``,
    ``t1``, and ``t2``. But ``bounds_schema`` can overwrite those fields and
    add new fields (Bounds3D has default values for ``x1``, ``x2``, ``y1``, and
    ``y2`` - ``bounds_schema`` can overwrite these default values). For
    example, if the mapping key, t1, t2, x1, and x2 values are stored in fields
    ``video_id``, ``min_frame``, ``max_frame``, ``bbox_x1``, and ``bbox_x2``
    fields respectively, ``bounds_schema`` should be set to::

        {
            'key': 'video_id',
            't1': 'min_frame',
            't2': 'max_frame',
            'x1': 'bbox_x1',
            'x2': 'bbox_x2'
        }

    And for each item in iterable, the key, t1, t2, x1, and x2 values will be
    accessed by ``key_accessor(item, 'video_id')``,
    ``key_accessor(item, 'min_frame')``, ``key_accessor(item, 'max_frame')``,
    ``key_accessor(item, 'bbox_x1')``, ``key_accessor(item, 'bbox_x2')``,
    respectively.

    If only the key needs to be updated, ``bounds_schema`` should be set to::

        {
            'key': 'video_id'
        }

    In this case, Bounds3D will use default values for x1, x2, y1, and y2.

    Args:
        iterable: An iterable of elements whose relevant fields can be
            accessed by ``key_accessor``.
        key_accessor: A function that takes an element of ``iterable`` and
            a key and returns the value of the key on that element.
        bounds_schema (optional): A dictionary that overrides default field
            keys (``'key'`` for the key, ``'t1'`` for co-ordinate t1, ``'t2'``
            for co-ordinate t2, ``'x1'`` for co-ordinate x1, ``'x2'`` for
            co-ordinate x2, ``'y1'`` for co-ordinate y1, and ``'y2'`` for
            co-ordinate y2).
        with_payload (optional): A function that takes in an item in iterable
            and returns the payload for the Interval from the item. Defaults to
            a function that returns ``None``.
        progress (optional): Whether to display a loading bar from ``tqdm``.
            Defaults to ``False``.
        total (optional): Used in conjunction with ``progress`` to optionally
            display the total number of items in the loading bar if
            ``progress`` is ``True``.

    Returns:
        An IntervalSetMapping with Intervals from each item of iterable.
    """
    schema_final = {
        "key": "key",
        "t1": "t1",
        "t2": "t2"
    }
    schema_final.update(schema)
    def key_parser(item):
        return key_accessor(item, schema_final['key'])
    def bounds_parser(item):
        args = [
            key_accessor(item, schema_final['t1']),
            key_accessor(item, schema_final['t2'])
        ]
        kwargs = {}
        for k in ['x1', 'x2', 'y1', 'y2']:
            if k in schema_final:
                kwargs[k] = key_accessor(item, schema_final[k])
        return Bounds3D(*args, **kwargs)
    return IntervalSetMapping.from_iterable(iterable, key_parser,
        bounds_parser, payload_parser, progress, total)

# from Django QS
def ism_from_django_qs(qs, bounds_class=Bounds3D, bounds_schema={},
        progress=None):
    """Default constructor for Django QuerySets.

    This uses the right accessor for rows in a Django QuerySet and by default
    creates Intervals with Bounds3D bounds.

    The default schema in this case is::

        {
            "key": "video_id",
            "t1": "min_frame",
            "t2": "max_frame"
        }

    The fields in ``bounds_schema`` update the fields of the default schema.

    Payload is set by setting a value in the ``payload`` field of
    ``bounds_schema``. For example::

        {
            "payload": "id"
        }

    This will set the payload of each Interval to the id field of the record.

    This supports nested field names. For example::

        {
            "t1": "face.frame.number",
            "t2": "face.frame.number"
        }

    Args:
        qs: A Django queryset where every record will become an Interval. 
        bounds_class (optional): The bounds that each Interval will have.
            Curently only supports Bounds1D and Bounds3D. Defaults to Bounds3D.
        bounds_schema (optional): A dictionary that overrides the default field
            keys.
        progress (optional): Whether to display a loading bar from ``tqdm``.
            The total for the loading bar is computed using ``qs.count()``.
            Defaults to ``False``.

    Returns:
        An IntervalSetMapping with Intervals from each record of qs.

    Raises:
        NotImplementedError: If ``bounds_class`` is not one of ``Bounds3D`` or
            ``Bounds1D``.
    """
    def django_accessor(row, field):
        fields = field.split('.')
        output = row
        for field in fields:
            output = attrgetter_accessor(output, field)
        return output
    final_schema = {
        "key": "video_id",
        "t1": "min_frame",
        "t2": "max_frame"
    }
    final_schema.update(bounds_schema)
    total = None
    if progress is not None:
        total = qs.count()
    def payload_parser(record):
        if "payload" in final_schema:
            return django_accessor(record, final_schema["payload"])
        else:
            return None
    if bounds_class == Bounds3D:
        return ism_from_iterable_with_schema_bounds3d(qs, django_accessor,
            bounds_schema=final_schema, with_payload=payload_parser,
            progress=progress, total=total)
    elif bounds_class == Bounds1D:
        return ism_from_iterable_with_schema_bounds1d(qs, django_accessor,
            bounds_schema=final_schema, with_payload=payload_parser,
            progress=progress, total=total)
    else:
        raise NotImplementedError("{} not a supported bounds".format(
            bounds_class.__name__))

# from Pandas DF
def ism_from_df(df, bounds_class=Bounds3D, bounds_schema={}, progress=None,
        total=None):
    """Default constructor for Pandas-style dataframes.

    This uses the right accessor for rows in a dataframe and by default creates
    Intervals with Bounds3D bounds.

    The default schema in this case is::

        {
            "key": "video_id",
            "t1": "min_frame",
            "t2": "max_frame"
        }

    The fields in ``bounds_schema`` update the fields of the default schema.

    Payload is set by setting a value in the ``payload`` field of
    ``bounds_schema``. For example::

        {
            "payload": "id"
        }

    This will set the payload of each Interval to the id field of the row.

    Args:
        qs: A Django queryset where every record will become an Interval. 
        bounds_class (optional): The bounds that each Interval will have.
            Curently only supports Bounds1D and Bounds3D. Defaults to Bounds3D.
        bounds_schema (optional): A dictionary that overrides the default field
            keys.
        with_payload (optional): A function that takes in a record in ``qs``
            and returns the payload for the Interval from the record. Defaults
            to a function that returns ``None``.
        progress (optional): Whether to display a loading bar from ``tqdm``.
            Defaults to ``False``.
        total (optional): Used in conjunction with ``progress`` to optionally
            display the total number of items in the loading bar if
            ``progress`` is ``True``.

    Returns:
        An IntervalSetMapping with Intervals from each record of qs.

    Raises:
        NotImplementedError: If ``bounds_class`` is not one of ``Bounds3D`` or
            ``Bounds1D``.
    """
    final_schema = {
        "key": "video_id",
        "t1": "min_frame",
        "t2": "max_frame"
    }
    final_schema.update(bounds_schema)
    def payload_parser(record):
        if "payload" in final_schema:
            return getter_accessor(record, final_schema["payload"])
        else:
            return None
    if bounds_class == Bounds3D:
        return ism_from_iterable_with_schema_bounds3d(qs, getter_accessor,
            bounds_schema=final_schema, with_payload=payload_parser,
            progress=progress, total=total)
    elif bounds_class == Bounds1D:
        return ism_from_iterable_with_schema_bounds1d(qs, getter_accessor,
            bounds_schema=final_schema, with_payload=payload_parser,
            progress=progress, total=total)
    else:
        raise NotImplementedError("{} not a supported bounds".format(
            bounds_class.__name__))

# default schema for bounding boxes
def django_bbox_default_schema():
    """A default schema for bounding box records in a database."""
    return {
        "key": "video_id",
        "t1": "min_frame",
        "t2": "max_frame",
        "x1": "bbox_x1",
        "x2": "bbox_x2",
        "y1": "bbox_y1",
        "y2": "bbox_y2",
        "payload": "id"
    }

