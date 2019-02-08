from rekall.interval_list import Interval
import cloudpickle
import multiprocessing as mp

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
def Y(pred):
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

# Apply a predicate on payload
def P(pred):
    def new_pred(*interval_3ds):
        return pred(*[
            i.payload for i in interval_3ds 
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

# Asynchronous utilities
class AsyncWorkDispatcher:
    """
    Helper class to dispatch parallelizable work in batches
    """
    class _SerializedFunc:
        def __init__(self, func):
            self.serialized_func = cloudpickle.dumps(func)
        def __call__(self, batch):
            func = cloudpickle.loads(self.serialized_func)
            return [func(*packed_args) for packed_args in batch]

    def __init__(self, func, total_work, num_workers = mp.cpu_count()):
        if num_workers <= 1 or total_work <= 1:
            self.disable_parallel = True
            self.func = func
            self.outputs = []
        else:
            self.disable_parallel = False
            self.chunk_size = int(total_work/num_workers)+1
            self.serialized_func = AsyncWorkDispatcher._SerializedFunc(func)
            self.pool = mp.Pool(processes=num_workers)
            self.output_batch_futures = []
            self.current_batch = []

    def _dispatch(self):
        self.output_batch_futures.append(
            self.pool.apply_async(self.serialized_func,
                args=(self.current_batch,)))
        self.current_batch = []

    def add_work(self, packed_args):
        if self.disable_parallel:
            self.outputs.append(self.func(*packed_args))
        else:
            self.current_batch.append(packed_args)
            if len(self.current_batch) >= self.chunk_size:
                self._dispatch()
        return self

    def close(self):
        if not self.disable_parallel:
            if len(self.current_batch)>0:
                self._dispatch() 
            self.pool.close()

    def get_all_outputs(self):
        if self.disable_parallel:
            return self.outputs
        else:
            return [r for batch_future in self.output_batch_futures
                      for r in batch_future.get()]



