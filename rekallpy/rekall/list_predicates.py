"""Predicates on lists.

Right now just predicates based on length.
    length_exactly(n) - True if length = n
    length_at_least(n) - True if length >= n
    length_at_most(n) - True if length <= n
    length_between(n1, n2) - True if n1 <= length <= n2
"""

def length_exactly(n):
    return lambda l: len(l) == n
def length_at_least(n):
    return lambda l: len(l) >= n
def length_at_most(n):
    return lambda l: len(l) <= n
def length_between(n1, n2):
    return lambda l: (len(l) >= n1 and len(l) <= n2)
