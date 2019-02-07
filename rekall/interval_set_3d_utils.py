from rekall.interval_list import Interval

# Bound Combiners
def merge_bound(bound1, bound2):
    return (min(bound1[0], bound2[0]),max(bound1[1], bound2[1]))

def overlap_bound(bound1, bound2):
    return (max(bound1[0], bound2[0]),min(bound1[1], bound2[1]))

def bound_size(b):
    return b[1]-b[0]

# Sortkeys for Interval3D
def sort_key_time_x_y(interval):
    return (*interval.t,
            *interval.x,
            *interval.y)

# Adapters for spatiotemporal predicates
# Apply a predicate on temporal dimension in Interval3D(s)
def T(pred):
    def new_pred(*interval_3ds):
        return pred(*[
            Interval(i.t[0], i.t[1], i.payload) for i in interval_3ds
        ])
    return new_pred
# Apply a predicate on x dimension in Interval3D(s)
def X(pred):
    def new_pred(*interval_3ds):
        return pred(*[
            Interval(i.x[0], i.x[1], i.payload) for i in interval_3ds
        ])
    return new_pred
# Apply a predicate on y dimension in Interval3D(s)
def X(pred):
    def new_pred(*interval_3ds):
        return pred(*[
            Interval(i.y[0], i.y[1], i.payload) for i in interval_3ds
        ])
    return new_pred

def _interval_3d_to_bbox(intrvl):
    return {'x1':intrvl.x[0], 'x2':intrvl.x[1],
            'y1':intrvl.y[0], 'y2':intrvl.y[1],
            'payload': intrvl.payload
            }

# Apply a bbox predicate on spatial dimensions in Interval3D(s)
def XY(pred):
    def new_pred(*interval_3ds):
        return pred(*[
            _interval_3d_to_bbox(i) for i in interval_3ds 
        ])
    return new_pred

# Adapters for logical combinations of predicates
def not_pred(pred):
    def new_pred(*args):
        return not pred(*args)
    return new_pred

def and_preds(*preds):
    def new_pred(*args):
        for pred in preds:
            if not pred(*args):
                return False
        return True
    return new_pred

def or_preds(*preds):
    def new_pred(*args):
        for pred in preds:
            if pred(*args):
                return True
        return False
    return new_pred

