from rekall import Interval, IntervalSet, Bounds3D

def main():
    '''
    Examples of creating some Intervals. Intervals are parameterized by Bounds
    (Bounds3D in these examples) and an optional payload.
    '''

    my_first_bounds = Bounds3D(
        t1 = 5,
        t2 = 10, 
        x1 = 0.1, 
        x2 = 0.4, 
        y1 = 0.3, 
        y2 = 0.9
    )

    # This bounds is the same as my_first_bounds
    my_second_bounds = Bounds3D(5, 10, 0.1, 0.4, 0.3, 0.9)

    # This Interval has the 3D bounds of my_first_bounds and a payload
    my_first_interval = Interval(my_first_bounds, payload = 10)

    # This Interval doesn't have a payload
    my_first_interval_no_payload = Interval(my_first_bounds)

    # IntervalSet contains multiple Intervals
    my_first_interval_set = IntervalSet([
        my_first_interval,
        Interval(my_second_bounds, payload = 'payloads are arbitrary'),
        Interval(Bounds3D(20, 30), payload = 'Bounds3D has default X/Y fields')
    ])
    print(my_first_interval_set)

if __name__ == "__main__":
    main()
