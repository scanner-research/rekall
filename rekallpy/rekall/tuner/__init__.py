"""This module provides some classes for automatically tuning Rekall queries."""

from rekall.tuner.tuner import Tuner
from rekall.tuner.random_tuner import RandomTuner

__all__ = [
    'Tuner', 'RandomTuner'
]
