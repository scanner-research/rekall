from rekall import Interval, IntervalSet, Bounds3D
from rekall.predicates import overlaps

def main():
    '''
    An example of a join. We filter pairs based on time overlap, and merge
    surviving pairs by taking the intersection over the time bounds and the
    span of the spatial bounds (and adding the payloads).
    '''
    
    is1 = IntervalSet([
        Interval(Bounds3D(0,1,0,1,0,1),1),
        Interval(Bounds3D(0,0.5,0.5,1,0,0.5),2),
    ])
    is2 = IntervalSet([
        Interval(Bounds3D(0.5,1,0,1,0,1),4),
        Interval(Bounds3D(0,1,0,1,0,1),8),
    ])
    is3 = is1.join(is2,
            Bounds3D.T(overlaps()),
            lambda i1,i2: Interval(
                i1['bounds'].intersect_time_span_space(i2['bounds']),
                i1['payload'] + i2['payload']
            ))
    
    print('is1:')
    print(is1)
    print('is2:')
    print(is2)

    print('is1 joined with is2:')
    print(is3)

if __name__ == "__main__":
    main()
