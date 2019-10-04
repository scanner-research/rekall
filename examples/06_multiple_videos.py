from rekall import Interval, IntervalSet, IntervalSetMapping, Bounds3D
from rekall.predicates import *
from rekall.stdlib.merge_ops import *
from rekall.bounds import utils

def main():
    '''
    An example of using IntervalSetMapping to handle annotations from multiple
    videos.
    '''

    # Annotations for one video 
    is1 = IntervalSet([
        Interval(Bounds3D(1,10,0.3,0.4,0.5,0.6),1),
        Interval(Bounds3D(2,5,0.2,0.8,0.2,0.3),1),
        Interval(Bounds3D(10,11,0.2,0.7,0.3,0.5),1),
        Interval(Bounds3D(13,15,0.5,1,0,0.5),1),
        Interval(Bounds3D(15,19,0.5,1,0,0.5),1),
        Interval(Bounds3D(20,20),payload=1),
        Interval(Bounds3D(22,22),payload=1),
        Interval(Bounds3D(22,23),payload=1),
    ])

    # Annotations for a second video
    is2 = IntervalSet([
        Interval(Bounds3D(2,5,0.2,0.8,0.2,0.4),1),
        Interval(Bounds3D(1,10,0.3,0.4,0.3,0.6),1),
        Interval(Bounds3D(9,11,0.16,0.17,0.3,0.5),1),
        Interval(Bounds3D(13,15,0.5,1,0,0.5),1),
        Interval(Bounds3D(14,19,0.5,1,0,0.5),1),
    ])

    # An IntervalSetMapping can store both
    ism = IntervalSetMapping({
        1: is1,
        2: is2
    })

    # An IntervalSetMapping reflects IntervalSet operations
    ism_coalesced = ism.coalesce(('t1', 't2'), Bounds3D.span, payload_plus)

    other_ism = IntervalSetMapping({
        1: IntervalSet([
            Interval(Bounds3D(1, 5, 0, 1, 0, 1))
        ]),
        2: IntervalSet([
            Interval(Bounds3D(10, 15, 0, 1, 0, 1))
        ])
    })

    # For binary operations, IntervalSetMapping sends the operations to the
    # correct underlying IntervalSets
    isms_joined = ism.join(
        other_ism,
        Bounds3D.T(overlaps()),
        lambda i1, i2: Interval(i1['bounds'].combine_per_axis(
            i2['bounds'],
            utils.bounds_intersect,
            utils.bounds_intersect,
            utils.bounds_intersect
        ))
    )
    
    print('ism:')
    print(ism)
    print()

    print('ism coalesced:')
    print(ism_coalesced)
    print()

    print('other_ism:')
    print(other_ism)
    print()

    print('ism joined with other_ism:')
    print(isms_joined)

if __name__ == "__main__":
    main()

