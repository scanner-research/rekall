import cloudpickle
from math import ceil
import multiprocessing as mp
import random
from tqdm import tqdm

from rekall.interval_set_3d_utils import perf_count
from rekall.video_interval_collection_3d import VideoIntervalCollection3D

# WorkerPool interface
class AbstractWorkerPool():
    def apply_async(self, args, done_callback):
        raise NotImplementedError()
    def shut_down(self):
        raise NotImplementedError()

# A WorkerPool with one worker: the current process.
class InlineSingleProcessPool(AbstractWorkerPool):
    """
    A single-process implmentation of WorkerPool
    """
    class ResultWrapper():
        def __init__(self, result, is_error=False):
            self.is_error = is_error
            self.result = result
        def get(self):
            if self.is_error:
                raise self.result
            else:
                return self.result

    def __init__(self, fn):
        self.fn = fn

    def apply_async(self, args, done_callback):
        try:
            r = self.fn(args)
            done_callback((True, r))
            return InlineSingleProcessPool.ResultWrapper(r)
        except Exception as e:
            done_callback((False, e))
            return InlineSingleProcessPool.ResultWrapper(e, is_error=True)

    def shut_down(self):
        return

# Helper functions for creating child processes:
# We set the function to execute as a global variable on the child process
# to avoid pickling the function.
def _child_process_init(context):
    global GLOBAL_CONTEXT
    GLOBAL_CONTEXT=context

def _apply_global_context_as_function(vids):
    global GLOBAL_CONTEXT
    fn = GLOBAL_CONTEXT
    return fn(vids)

class ForkedProcessPool():
    def __init__(self, fn, num_workers):
        self._pool = mp.get_context("fork").Pool(
                processes=num_workers,
                initializer=_child_process_init,
                initargs=(fn,))

    def apply_async(self, args, done_callback):
        def success(result):
            return done_callback((True, result))
        def error(err):
            return done_callback((False, err))
        return self._pool.apply_async(
                _apply_global_context_as_function,
                args=(args,),
                callback=success,
                error_callback=error)

    def shut_down(self):
        self._pool.terminate()

# When Spawning arguments to initializer are pickled.
# To allow arbitrary lambdas with closure, use cloudpickle to serialize the
# function to execute
def _apply_serialized_function(serialized_func, vids):
    fn = cloudpickle.loads(serialized_func)
    return fn(vids)

class SpawnedProcessPool():
    def __init__(self, fn, num_workers, initializer=None):
        self._pool = mp.get_context("spawn").Pool(
                processes=num_workers,
                initializer=initializer)
        self._pickled_fn = cloudpickle.dumps(fn)

    def apply_async(self, args, done_callback):
        def success(result):
            return done_callback((True, result))
        def error(err):
            return done_callback((False, err))
        return self._pool.apply_async(
                _apply_serialized_function,
                args=(self._pickled_fn, args),
                callback=success,
                error_callback=error)

    def shut_down(self):
        self._pool.terminate()

# WorkerPool Factories
def inline_pool_factory(fn):
    return InlineSingleProcessPool(fn)

# A WorkerPool that fork()s current process to create children processes.
def get_forked_process_pool_factory(num_workers=mp.cpu_count()):
    def factory(fn):
        return ForkedProcessPool(fn, num_workers)
    return factory

# A WorkerPool that spawns clean python interpreter process to create
# children processes.
def get_spawned_process_pool_factory(num_workers=mp.cpu_count()):
    def factory(fn):
        return SpawnedProcessPool(fn, num_workers)
    return factory

class _WorkerPoolContext():
    """ Wrapper class to allow `with` syntax
    """
    def __init__(self, pool):
        self._pool = pool

    def __enter__(self):
        return self._pool

    def __exit__(self, *args):
        self._pool.shut_down()

def _get_callback(pbar, vids):
    work = len(vids)
    # result is (success, error or result) pair
    def callback(pair):
        success, result = pair
        if success:
            pbar.update(work)
        else:
            print("Error when processing {0}:{1}".format(vids, result))
    return callback

def _combine_collections(collections):
    result = {}
    result_keys = set([])
    for c in collections:
        d = c.get_allintervals()
        keys = set(d.keys())
        if not keys.isdisjoint(result_keys):
            repeated_keys = keys.intersection(result_keys)
            raise RuntimeError("Videos {0} found in multiple results".format(
                repeated_keys))
        result_keys = result_keys.union(keys)
        result.update(c.get_allintervals())
    return VideoIntervalCollection3D(result)

class Runtime():
    """
    Runtime creates a pool of workers to execute a rekall query.
    """
    def __init__(self, worker_pool_factory):
        """
        worker_pool_factory should be a function that returns a WorkerPool
        """
        self._get_worker_pool = worker_pool_factory

    @classmethod
    def inline(cls):
        return cls(inline_pool_factory)

    def run(self, query, vids, progress=False, chunksize=1, profile=False):
        """
        Runs the rekall query on given video ids.
        query is a function from a list of video ids to a
            VideoIntervalCollection3D
        """
        with perf_count("Executing query in Runtime", enable=profile):
            with _WorkerPoolContext(self._get_worker_pool(query)) as pool:
                total_work = len(vids)
                with tqdm(total=total_work, disable=not progress) as pbar:
                    with perf_count("Executing in workers", enable=profile):
                        with perf_count("Dispatching tasks", enable=profile):
                            async_results = []
                            random.shuffle(vids)
                            num_tasks = int(ceil(len(vids)/chunksize))
                            for task_i in range(num_tasks):
                                start = chunksize * task_i
                                end = chunksize + start
                                task = vids[start:end]
                                async_results.append(
                                        pool.apply_async(
                                            task,
                                            _get_callback(pbar, task)))
                        task_results = [r.get() for r in async_results]
                    with perf_count("Combining results from workers",
                            enable=profile):
                        return _combine_collections(task_results)

# Wraps a query function from a single vid to an IntervalSet3D
def wrap_interval_set(query):
    def fn(vids):
        return VideoIntervalCollection3D({
            vid: query(vid) for vid in vids})
    return fn
