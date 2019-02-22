import cloudpickle
from math import ceil
import multiprocessing as mp
import random
from tqdm import tqdm

from rekall.interval_set_3d_utils import perf_count
from rekall.video_interval_collection_3d import VideoIntervalCollection3D

# Custom exception type
class TaskException(Exception):
    def __repr__(self):
        return "TaskException from {0}".format(self.__cause__)

def _wrap_for_exception(fn):
    def wrapped(vids):
        try:
            return fn(vids)
        except Exception as e:
            raise TaskException() from e
    return wrapped

# WorkerPool interface
class AbstractWorkerPool():
    def map(self, tasks, done):
        raise NotImplementedError()
    def shut_down(self):
        raise NotImplementedError()

# A WorkerPool with one worker: the current process.
class InlineSingleProcessPool(AbstractWorkerPool):
    """
    A single-process implmentation of WorkerPool
    """
    class Lazy():
        def __init__(self, getter, done):
            self.getter = getter
            self.done = done
        def get(self):
            try:
                r = self.getter()
            except TaskException as e:
                self.done(e)
                raise e
            self.done()
            return r

    def __init__(self, fn):
        self.fn = fn

    def map(self, tasks, done):
        def get_callback(vids):
            def callback(e=None):
                done(vids, e)
            return callback
        def get_getter(fn, vids):
            def getter():
                return fn(vids)
            return getter
        return [InlineSingleProcessPool.Lazy(
                    get_getter(self.fn, task),
                    get_callback(task)) for task in tasks]

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

    def map(self, tasks, done_callback):
        def get_success_callback(vids):
            def success(result):
                done_callback(vids)
            return success
        def get_error_callback(vids):
            def error(err):
                done_callback(vids, err)
            return error
        return [self._pool.apply_async(
                _apply_global_context_as_function,
                args=(task,),
                callback=get_success_callback(task),
                error_callback=get_error_callback(task)) for task in tasks]

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

    def map(self, tasks, done_callback):
        def get_success_callback(vids):
            def success(result):
                done_callback(vids)
            return success
        def get_error_callback(vids):
            def error(err):
                done_callback(vids, err)
            return error
        return [self._pool.apply_async(
                _apply_serialized_function,
                args=(self._pickled_fn, task),
                callback=get_success_callback(task),
                error_callback=get_error_callback(task)) for task in tasks]

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

def _get_callback(pbar, vids_with_err):
    def callback(vids, err=None):
        if err is None:
            pbar.update(len(vids))
        else:
            print("Error when processing {0}:{1}".format(vids, repr(err)))
            vids_with_err.extend(vids)
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

def _create_tasks(vids, chunksize):
    total = len(vids)
    num_tasks = int(ceil(total/chunksize))
    tasks = []
    for task_i in range(num_tasks):
        start = chunksize*task_i
        end = min(total,start+chunksize)
        tasks.append(vids[start:end])
    return tasks

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

    def run(self, query, vids,
            randomize=True, chunksize=1,
            progress=False, profile=False):
        """
        Runs the rekall query on given video ids.
        query is a function from a list of video ids to a
            VideoIntervalCollection3D
        """
        query = _wrap_for_exception(query)
        with perf_count("Executing query in Runtime", enable=profile):
            with _WorkerPoolContext(self._get_worker_pool(query)) as pool:
                total_work = len(vids)
                with tqdm(total=total_work, disable=not progress) as pbar:
                    with perf_count("Executing in workers", enable=profile):
                        vids_with_err = []
                        with perf_count("Dispatching tasks", enable=profile):
                            if randomize:
                                random.shuffle(vids)
                            async_results = pool.map(
                                    _create_tasks(vids, chunksize),
                                    _get_callback(pbar, vids_with_err))
                        task_results = []
                        for future in async_results:
                            try:
                                r = future.get()
                                task_results.append(r)
                            except TaskException:
                                pass
                    with perf_count("Combining results from workers",
                            enable=profile):
                        return (_combine_collections(task_results),
                                vids_with_err)

# Wraps a query function from a single vid to an IntervalSet3D
def wrap_interval_set(query):
    def fn(vids):
        return VideoIntervalCollection3D({
            vid: query(vid) for vid in vids})
    return fn
