from rekall.interval_list import Interval
from rekall.temporal_predicates import *
from rekall.logical_predicates import *
from rekall.merge_ops import * 

class Interval3D:
    def __init__(self, t1, t2, x1=0, x2=1, y1=0, y2=1, payload=None):
        self.t1 = t1
        self.t2 = t2
        self.x1 = x1
        self.x2 = x2
        self.y1 = y1
        self.y2 = y2
        self.payload = payload

    def sort_key(intrvl):
        return (intrvl.t1, intrvl.t2, intrvl.x1, intrvl.x2, intrvl.y1, intrvl.y2)

    def __repr__(self):
        return "<Interval3D t: [{0},{1}] x: [{2},{3}] y: [{4},{5}] payload:{6}>".format(
                self.t1, self.t2, self.x1, self.x2, self.y1, self.y2, self.payload)

    def copy(self):
        return Interval3D(self.t1, self.t2, self.x1, self.x2, self.y1, self.y2, self.payload)

    def minus(self, other, payload_merge_op=payload_first):
        raise NotImplementedError

    def overlap(self, other, payload_merge_op=payload_first):
        raise NotImplementedError

    def merge(self, other, payload_merge_op=payload_first):
        payload = payload_merge_op(self.payload, other.payload)
        return Interval3D(
                min(self.t1, other.t1),
                max(self.t2, other.t2),
                min(self.x1, other.x1),
                max(self.x2, other.x2),
                min(self.y1, other.y1),
                max(self.y2, other.y2),
                payload)

    def length(self):
        return self.t2-self.t1
    def width(self):
        return self.x2-self.x1
    def height(self):
        return self.y2-self.y1

def T(pred, arity=2):
    def pred1(intrvl):
        return pred(Interval(intrvl.t1, intrvl.t2, intrvl.payload))
    def pred2(intrvl1, intrvl2):
        return pred(Interval(intrvl1.t1, intrvl1.t2, intrvl1.payload),
                           Interval(intrvl2.t1, intrvl2.t2, intrvl2.payload))
    if arity == 1:
        return pred1
    if arity == 2:
        return pred2
    raise NotImplementedError

def X(pred, arity=2):
    def pred1(intrvl):
        return pred(Interval(intrvl.x1, intrvl.x2, intrvl.payload))
    def pred2(intrvl1, intrvl2):
        return pred(Interval(intrvl1.x1, intrvl1.x2, intrvl1.payload),
                           Interval(intrvl2.x1, intrvl2.x2, intrvl2.payload))
    if arity == 1:
        return pred1
    if arity == 2:
        return pred2
    raise NotImplementedError

def Y(pred, arity=2):
    def pred1(intrvl):
        return pred(Interval(intrvl.y1, intrvl.y2, intrvl.payload))
    def pred2(intrvl1, intrvl2):
        return pred(Interval(intrvl1.y1, intrvl1.y2, intrvl1.payload),
                           Interval(intrvl2.y1, intrvl2.y2, intrvl2.payload))
    if arity == 1:
        return pred1
    if arity == 2:
        return pred2
    raise NotImplementedError

def intrvl_to_bbox(intrvl):
    return {'x1':intrvl.x1, 'x2':intrvl.x2,
            'y1':intrvl.y1, 'y2':intrvl.y2,
            'payload': intrvl.payload
            }

def XY(pred, arity=2):
    def pred1(intrvl):
        return pred(intrvl_to_bbox(intrvl))
    def pred2(i1, i2):
        return pred(intrvl_to_bbox(i1), intrvl_to_bbox(i2))
    if arity == 1:
        return pred1
    if arity == 2:
        return pred2
    raise NotImplementedError

def XY_list(pred):
    def fn(intrvls):
        if isinstance(intrvls, IntervalList3D):
            intrvls = intrvls.intrvls
        return pred([intrvl_to_bbox(i) for i in intrvls])
    return fn
    

def overlaps_or_meets():
    return lambda i1, i2: overlaps()(i1,i2) or meets_before()(i1,i2) or meets_after()(i1,i2)

def overlaps_or_meets_3D():
    return lambda int1, int2: T(overlaps_or_meets())(int1, int2) and X(
            overlaps_or_meets())(int1,int2) and Y(
                    overlaps_or_meets())(int1, int2)

def expand_to_frame(intrvl):
    return Interval3D(intrvl.t1, intrvl.t2, payload = intrvl.payload)


class IntervalList3D:
    def __init__(self, intrvls):
        if isinstance(intrvls, IntervalList3D):
            intrvls = intrvls.intrvls
        self.intrvls = sorted([intrvl if isinstance(intrvl, Interval3D)
            else Interval3D(*intrvl) for intrvl in intrvls],
            key=Interval3D.sort_key)
        if len(self.intrvls) > 0:
            max_end = max([intrvl.t2 for intrvl in self.intrvls])
            if len(self.intrvls) > 1000:
                self.working_window = (max_end - self.intrvls[0].t1) / 100
            else:
                self.working_window = (max_end - self.intrvls[0].t1)
        else:
            self.working_window = 0

    def __repr__(self):
        return str(self.intrvls)

    def map(self, map_fn):
        return IntervalList3D([map_fn(intrvl) for intrvl in self.intrvls])


    def join(self, other, merge_op, predicate, working_window=None):
        """
        Joins self.intrvls with other.intrvls on predicate and produces new
        Intervals based on merge_op.

        merge_op takes in two Intervals and returns a list of Intervals
        predicate takes in two Intervals and returns True or False
        """
        if working_window == None:
            working_window = self.working_window
        output = []
        start_index = 0
        for intrvl in self.intrvls:
            new_start_index = None
            for idx, intrvlother in enumerate(other.intrvls[start_index:]):
                if new_start_index is None:
                    if intrvl.t1 < intrvlother.t1:
                        new_start_index = idx
                    elif intrvl.t1 - working_window <= intrvlother.t2:
                        new_start_index = idx
                if intrvlother.t1 - working_window > intrvl.t2:
                    break
                if predicate(intrvl, intrvlother):
                    new_intrvls = merge_op(intrvl, intrvlother)
                    if len(new_intrvls) > 0:
                        output += new_intrvls
            if new_start_index is not None:
                start_index += new_start_index
        return IntervalList3D(output)

    def experimental_join(self, other, predicate,
            interval_generator=lambda i1, i2: [Interval3D.sort_key(i1)],
            payload_generator=lambda i1, i2: (i1.payload, i2), 
            working_window=None):
        """
        Joins self.intrvls with other.intrvls on predicate and produces new
        Intervals based on interval_generator.

        interval_generator takes two Intervals and returns a list of 
        (start, end) tuples. It defaults to the interval span in self.

        payload_generator takes two Intervals and returns the payload for
        each of the newly generated intervals. It defaults to a tuple
        where the first element is the payload in interval in self, and
        second element is the interval in other.
        """
        def merge_op(intvl1, intvl2):
            payload = payload_generator(intvl1, intvl2)
            return [Interval3D(t1,t2,x1,x2,y1,y2, payload) for t1,t2,x1,x2,y1,y2 in
                    interval_generator(intvl1, intvl2)]
        return self.join(other, merge_op, predicate, working_window)

    def experimental_group_by_interval(self):
        """
        Collapses all intervals with the same (start, end) into one
        interval, and set its payload to be the list of all payloads
        of these intervals.
        """
        output = {}
        for intrvl in self.intrvls:
            key = Interval3D.sort_key(intrvl)
            if key not in output:
                output[key] = [intrvl.payload]
            else:
                output[key].append(intrvl.payload)
        return IntervalList3D([Interval3D(*key, payload=payloads) for key, payloads in output.items()])

    def experimental_map_payload(self, func):
        def map_fn(intvl):
            return Interval3D(intvl.t1, intvl.t2,
                    intvl.x1, intvl.x2,
                    intvl.y1, intvl.y2, func(intvl.payload))
        return self.map(map_fn)

    def coalesce(self, payload_merge_op=payload_first,
            predicate=true_pred(arity=2)):
        """
        Recursively merge all overlapping or touching intervals that satisfy
        predicate. predicate must be an equivalence relation over the intervals
        (must be reflexive, symmetric, and transitive - all the properties of
        a binary relationship that acts like equality in some way).

        """
        if len(self.intrvls) == 0:
            return self
        new_intrvls = []
        current_intervals = []
        for intrvl in self.intrvls:
            for cur in current_intervals:
                # Add any intervals that end before intrvl.start
                if cur.t2 < intrvl.t1:
                    new_intrvls.append(cur)
            current_intervals = [
                cur
                for cur in current_intervals
                if cur.t2 >= intrvl.t1
            ]
            if len(current_intervals) == 0:
                current_intervals.append(intrvl.copy())
                continue
            
            matched_interval = None
            for cur in current_intervals:
                if overlaps_or_meets_3D()(cur, intrvl) and predicate(cur, intrvl):
                    matched_interval = cur
                    break
            if matched_interval is None:
                current_intervals.append(intrvl)
            else:
                # Merge intrvl into cur
                cur.payload = payload_merge_op(cur.payload, intrvl.payload)
                cur.t2 = max(cur.t2, intrvl.t2)
                cur.x1 = min(cur.x1, intrvl.x1)
                cur.x2 = max(cur.x2, intrvl.x2)
                cur.y1 = min(cur.y1, intrvl.y1)
                cur.y2 = max(cur.y2, intrvl.y2)

        for cur in current_intervals:
            new_intrvls.append(cur)

        return IntervalList3D(new_intrvls)

    def filter(self, fn):
        """
        Filter every temporal range by fn. fn takes in an Interval and returns true or
        false.
        """
        return IntervalList3D([intrvl.copy() for intrvl in self.intrvls if fn(intrvl)])

    def set_union(self, other):
        """ Combine the temporal ranges in self with the temporal ranges in other.
        """
        return IntervalList3D(self.intrvls + other.intrvls)

    def minus(self, other, recursive_diff=True, predicate = true_pred(arity=2),
            payload_merge_op=payload_first, working_window=None):
        raise NotImplementedError

    def experimental_minus_temporal_only(self, other, recursive_diff = True, predicate = true_pred(arity=2),
            payload_merge_op = payload_first, working_window=None):
        """
        Calculate the difference between the temporal ranges in self and the temporal ranges
        in other.

        The difference between two intervals can produce up to two new intervals.
        If recursive_diff is True, difference operations will recursively be
        applied to the resulting intervals.
        If recursive_diff is False, the results of each difference operation
        between every valid pair of intervals in self and other will be emitted.

        For example, suppose the following interval is in self:

        |--------------------------------------------------------|

        and that the following two intervals are in other:

                  |--------------|     |----------------|
        
        If recursive_diff is True, this function will produce three intervals:

        |---------|              |-----|                |--------|

        If recursive_diff is False, this function will produce four intervals, some
        of which are overlapping:

        |---------|              |-------------------------------|
        |------------------------------|                |--------|

        Only computes differences for pairs that overlap and that satisfy
        predicate.

        If an interval in self overlaps no pairs in other such that the two
        satisfy predicate, then the interval is reproduced in the output.

        Labels the resulting intervals with payload_merge_op. For recursive_diff,
        the intervals passed in to the payload producer function are the original
        interval and the first interval that touches the output interval.
        """
        if not recursive_diff:
            raise NotImplementedError
        else:
            if working_window == None:
                working_window = self.working_window

            start_index = 0
            output = []
            for intrvl1 in self.intrvls:
                # For each interval in self.intrvls, get all the overlapping
                #   intervals from other.intrvls
                overlapping = []
                new_start_index = None
                for idx, intrvl2 in enumerate(other.intrvls[start_index:]):
                    if new_start_index is None:
                        if intrvl1.t1 < intrvl2.t1:
                            new_start_index = idx
                        elif intrvl1.t1 - working_window <= intrvl2.t2:
                            new_start_index = idx
                    if (T(overlaps())(intrvl1, intrvl2) and
                        predicate(intrvl1, intrvl2)):
                        overlapping.append(intrvl2)
                if new_start_index is not None:
                    start_index += new_start_index

                if len(overlapping) == 0:
                    output.append(intrvl1.copy())

                # Special case where where intrvl1 has length 0
                if intrvl1.length() == 0 and len(overlapping) is not 0:
                    continue
                
                # Create a sorted list of all start to end points between
                #   intrvl1.start and intrvl1.end, inclusive
                endpoints_set = set([intrvl1.t1, intrvl1.t2])
                for intrvl in overlapping:
                    if intrvl.t1 > intrvl1.t1:
                        endpoints_set.add(intrvl.t1)
                    if intrvl.t2 < intrvl1.t2:
                        endpoints_set.add(intrvl.t2)
                endpoints_list = sorted(list(endpoints_set))

                # Calculate longest subsequence endpoint pairs
                longest_subsequences = []
                last_j = -1
                for i in range(len(endpoints_list)):
                    if i <= last_j:
                        continue
                    start = endpoints_list[i]
                    valid = True
                    for intrvl in overlapping:
                        if intrvl.t1 > start:
                            break
                        if intrvl.t1 < start and intrvl.t2 > start:
                            valid = False
                            break
                    if not valid:
                        continue
                    max_j = len(endpoints_list) - 1
                    for j in range(max_j, i, -1):
                        end = endpoints_list[j]
                        intrvl_candidate = Interval3D(start, end)
                        valid = True
                        for intrvl in overlapping:
                            if intrvl.t1 > end:
                                break
                            if T(overlaps())(intrvl, intrvl_candidate):
                                valid = False
                                break
                        if valid:
                            longest_subsequences.append((start, end))
                            last_j = j

                # Figure out which intervals from overlapping to use to
                # construct new intervals
                for subsequence in longest_subsequences:
                    start = subsequence[0]
                    end = subsequence[1]
                    for intrvl in overlapping:
                        if intrvl.t2 == start or intrvl.t1 == end:
                            payload = payload_merge_op(intrvl1.payload, intrvl.payload)
                            output.append(Interval3D(start, end, intrvl1.x1, intrvl1.x2, intrvl1.y1, intrvl1.y2, payload))
                            break

            return IntervalList3D(output)
