from rekall import Interval, IntervalSet, Bounds3D

def main():
    '''
    Examples of simple functions on IntervalSets.
    '''

    # filter examples
    is1 = IntervalSet([
        Interval(Bounds3D(0,1,0,1,0,1),1),
        Interval(Bounds3D(0,0.5,0.5,1,0,0.5),2),
        Interval(Bounds3D(0,0.5,0,1,0,0.5),3),
    ])
    print('is1:')
    print(is1)
    is1_filtered = is1.filter(lambda i: i['payload'] > 2)
    print('is1 filtered on payload:')
    print(is1_filtered)
    print()

    # map example
    def expand_to_frame(intrvl):
        new_bounds = intrvl['bounds'].copy()
        new_bounds['x1'] = 0
        new_bounds['x2'] = 1
        new_bounds['y1'] = 0
        new_bounds['y2'] = 1
        return Interval(new_bounds, intrvl['payload'])

    is2 = IntervalSet([
        Interval(Bounds3D(0,1,0.3,0.4,0.5,0.6)),
        Interval(Bounds3D(0,0.5,0.2,0.3,0.5,0.6))
    ])
    print('is2:')
    print(is2)
    is2_expanded = is2.map(expand_to_frame)
    print('is2 expanded to the frame:')
    print(is2_expanded)

if __name__ == "__main__":
    main()
