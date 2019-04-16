"""Rekall is a spatiotemporal query language.

It operates over sets of intervals and allows for combining and filtering on
temporal and spatial predicates.

This module defines classes ``Interval``, ``IntervalSet``, and
``IntervalSetMapping``.
"""

from rekall.interval import Interval
from rekall.interval_set import IntervalSet
from rekall.interval_set_mapping import IntervalSetMapping

__all__ = [
    'Interval', 'IntervalSet', 'IntervalSetMapping'
]
