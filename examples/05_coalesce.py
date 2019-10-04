from rekall import Interval, IntervalSet, Bounds3D
from rekall.predicates import and_pred, overlaps
from rekall.stdlib.merge_ops import payload_plus

def main():
    '''
    Some examples of coalesce -- recursively merging overlapping or touching
    intervals.
    '''

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
    is1_coalesced = is1.coalesce(('t1', 't2'), Bounds3D.span, payload_plus)

    # target1 is the same as is1_coalesced
    target1 = IntervalSet([
        Interval(Bounds3D(1,11,0.2,0.8,0.2,0.6),3),
        Interval(Bounds3D(13,19,0.5,1,0,0.5),2),
        Interval(Bounds3D(20,20),payload=1),
        Interval(Bounds3D(22,23),payload=2),
    ])

    is1_coalesced_larger_epsilon = is1.coalesce(('t1', 't2'), Bounds3D.span,
        payload_plus, epsilon = 2)

    # target2 is the same as is1_coalesced_larger_epsilon
    target2 = IntervalSet([Interval(Bounds3D(1, 23), payload=8)])


    is2 = IntervalSet([
        Interval(Bounds3D(2,5,0.2,0.8,0.2,0.4),1),
        Interval(Bounds3D(1,10,0.3,0.4,0.3,0.6),1),
        Interval(Bounds3D(9,11,0.16,0.17,0.3,0.5),1),
        Interval(Bounds3D(13,15,0.5,1,0,0.5),1),
        Interval(Bounds3D(14,19,0.5,1,0,0.5),1),
    ])
    is2_coalesced_overlapping_bboxes = is2.coalesce(
        ('t1', 't2'), Bounds3D.span, payload_plus,
        predicate = and_pred(
            Bounds3D.X(overlaps()),
            Bounds3D.Y(overlaps())
        )
    )

    # target3 is the same as is2_coalesced_overlapping_bboxes
    target3 = IntervalSet([
        Interval(Bounds3D(1,10,0.2,0.8,0.2,0.6),2),
        Interval(Bounds3D(9,11,0.16,0.17,0.3,0.5),1),
        Interval(Bounds3D(13,19,0.5,1,0,0.5),2),
    ])

    print('is1:')
    print(is1)
    print()

    print('is1 coalesced:')
    print(is1_coalesced)
    print()

    print('is1_coalesced_larger_epsilon:')
    print(is1_coalesced_larger_epsilon)
    print()

    print('is2:')
    print(is2)
    print()

    print('is2 coalesced with overlapping bounding boxes:')
    print(is2_coalesced_overlapping_bboxes)

if __name__ == "__main__":
    main()
