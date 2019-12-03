"""This module provides some classes for automatically tuning Rekall queries."""

from rekall.tuner.tuner import Tuner
from rekall.tuner.random import RandomTuner
from rekall.tuner.grid import GridTuner
from rekall.tuner.coordinate_descent import CoordinateDescentTuner
from rekall.tuner.successive_halving import SuccessiveHalvingTuner
from rekall.tuner.hyperband import HyperbandTuner
# from rekall.tuner.scipy_nelder_mead import ScipyNelderMeadTuner
# from rekall.tuner.scipy_l_bfgs_b import ScipyLBFGSBTuner

__all__ = [
    'Tuner',
    'RandomTuner', 
    'GridTuner', 
    'CoordinateDescentTuner', 
    'SuccessiveHalvingTuner',
    'HyperbandTuner', 
    # 'ScipyNelderMeadTuner', 
    # 'ScipyLBFGSBTuner'
]
