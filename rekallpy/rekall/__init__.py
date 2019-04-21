"""Rekall is a spatiotemporal query language.

It operates over sets of intervals and allows for combining and filtering on
temporal and spatial predicates.

This module defines classes ``Interval``, ``IntervalSet``, and
``IntervalSetMapping``.

An ``Interval`` is a spatiotemporal volume defined by a bounds and a payload of
arbitrary type. An ``IntervalSet`` is a set of such Intervals.

One can perform unary operations such as ``map``, ``filter`` on ``IntervalSet`` as
well as binary operations between sets such as ``join``, ``union`` and ``minus``.

``IntervalSetMapping`` is a collection of ``IntervalSet``s organized by some key.
It exposes the same interface as ``IntervalSet`` and executes operations on the
underlying ``IntervalSet``s. It performs binary operations between ``IntervalSet``s
of the same key.

The ``rekall.bounds`` submodule provides the ``Bounds`` abstraction and two default
``Bounds`` implementations, a one-dimensional ``Bounds1D`` and a three-dimensional
``Bounds3D``. ``Bounds1D`` and ``Bounds3D`` both come with some useful functions on
their co-ordinate systems.

The ``rekall.predicates`` submodule provides a number of useful one-dimensional
and two-dimensional predicate functions. These functions are often used to
filter pairs of ``Intervals`` when joining two ``IntervalSet``s or
``IntervalSetMapping``s.

The ``rekall.stdlib`` submodule provides a number of useful functions that are
not core to Rekall but that we have nevertheless found to be useful.
``rekall.stdlib.ingest`` in particular provides a number of useful functions for
reading from various data sources into an ``IntervalSetMapping``.

The ``rekall.runtime`` submodule provides a library for efficiently executing
rekall queries. Given a function that operates on a batch of inputs, the
runtime divides the long list of inputs into chunks and run each chunk
potentially in parallel, and can combine the results of each chunk at the end.
"""

from rekall.interval import Interval
from rekall.interval_set import IntervalSet
from rekall.interval_set_mapping import IntervalSetMapping
from rekall.bounds import Bounds3D

__all__ = [
    'Interval', 'IntervalSet', 'IntervalSetMapping', 'Bounds3D'
]
