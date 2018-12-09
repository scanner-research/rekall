from rekall.helpers import *

"""
Predicates on payloads. Right now just two predicates that takes in an interval
and a predicate and applies the payload to the predicate (for both binary and
unary predicates):
    payload_satisfies_unary(pred) - True if the interval's payload satisfies
        pred
    payload_staisfies_binary(pred) - True if the two interval's payloads
        satisfy pred
To apply pred on a named payload, use payload_satisfies_*(on_name(name, pred))
"""

def payload_satisfies(pred, arity=1):
    if arity == 1:
        return lambda intrvl: pred(intrvl.payload)
    elif arity == 2:
        return lambda intrvl1, intrvl2: pred(intrvl1.payload, intrvl2.payload)
    else:
        panic('arity {} not supported.'.format(arity))

def on_name(name, pred):
    """
    Wraps a predicate so that it can be applied to one value in a dict payload
    """
    def apply(*args):
        return pred(*[arg[name] for arg in args])
    return apply
