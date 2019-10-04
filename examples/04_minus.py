from rekall import Interval, IntervalSet, Bounds3D
from rekall.predicates import payload_satisfies

def main():
    '''
    Examples of minus (anti-semi-join).

    The first example subtracts out all the parts of the Intervals in the left
    set that overlap with any part of an interval in the right set.

    The second example uses a predicate to only subtract Intervals with the
    same payload.
    '''
    
    is1 = IntervalSet([
        Interval(Bounds3D(1, 10,0,0.5,0.2,0.8),1),
        Interval(Bounds3D(3, 15,0,1,0,1),2)
    ])
    is2 = IntervalSet([
        Interval(Bounds3D(2,2.5),1),
        Interval(Bounds3D(2,2.7),1),
        Interval(Bounds3D(2.9,3.5),1),
        Interval(Bounds3D(3.5,3.6),1),
        Interval(Bounds3D(5,7),2),
        Interval(Bounds3D(9,12),2),
    ])
    is3 = is1.minus(is2)

    # is3 is equal to target
    target = IntervalSet([
        Interval(Bounds3D(1,2,0,0.5,0.2,0.8),1),
        Interval(Bounds3D(2.7, 2.9, 0,0.5,0.2,0.8),1),
        Interval(Bounds3D(3.6,5,0,0.5,0.2,0.8),1),
        Interval(Bounds3D(7,9,0,0.5,0.2,0.8),1),
        Interval(Bounds3D(3.6,5), payload=2),
        Interval(Bounds3D(7,9), payload=2),
        Interval(Bounds3D(12,15), payload=2),
    ])

    is4 = is1.minus(is2, predicate=payload_satisfies(
        lambda p1, p2: p1 == p2
    )) 
    
    # is4 is equal to target2
    target2 = IntervalSet([
        Interval(Bounds3D(1,2,0,0.5,0.2,0.8),1),
        Interval(Bounds3D(2.7, 2.9, 0,0.5,0.2,0.8),1),
        Interval(Bounds3D(3.6,10,0,0.5,0.2,0.8),1),
        Interval(Bounds3D(3,5), payload=2),
        Interval(Bounds3D(7,9), payload=2),
        Interval(Bounds3D(12,15), payload=2),
    ])
    
    print('is1:')
    print(is1)
    print('is2:')
    print(is2)
    print()

    print('is1 minus is2:')
    print(is3)
    print()

    print('is1 minus is2 filtering on the payload:')
    print(is4)

if __name__ == "__main__":
    main()
