"""Logical predicates

They take in an optional arity parameter.
"""

from rekall.helpers import *

def and_pred(pred1, pred2, arity=1):
    if arity == 1:
        return lambda intrvl: pred1(intrvl) and pred2(intrvl)
    elif arity == 2:
        return lambda intrvl1, intrvl2: (pred1(intrvl1, intrvl2) and
                pred2(intrvl1, intrvl2))
    else:
        panic('arity {} not supported.'.format(arity))

def or_pred(pred1, pred2, arity=1):
    if arity == 1:
        return lambda intrvl: pred1(intrvl) or pred2(intrvl)
    elif arity == 2:
        return lambda intrvl1, intrvl2: (pred1(intrvl1, intrvl2) or
                pred2(intrvl1, intrvl2))
    else:
        panic('arity {} not supported.'.format(arity))

def true_pred(arity=1):
    if arity == 1:
        return lambda intrvl: True
    elif arity == 2:
        return lambda intrvl1, intrvl2: True
    else:
        panic('arity {} not supported.'.format(arity))

def false_pred(arity=1):
    if arity == 1:
        return lambda intrvl: False
    elif arity == 2:
        return lambda intrvl1, intrvl2: False
    else:
        panic('arity {} not supported.'.format(arity))

def not_pred(pred, arity=1):
    if arity == 1:
        return lambda intrvl: not pred(intrvl)
    elif arity == 2:
        return lambda intrvl1, intrvl2: not pred(intrvl1, intrvl2)
    else:
        panic('arity {} not supported.'.format(arity))
