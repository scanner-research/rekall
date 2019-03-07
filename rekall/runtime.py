import cloudpickle
from math import ceil
import multiprocessing as mp
import random
from tqdm import tqdm

from rekall.interval_set_3d_utils import perf_count
from rekall.domain_interval_collection import DomainIntervalCollection

# Custom exception type
class TaskException(Exception):
    def __repr__(self):
        return "TaskException from {0}".format(self.__cause__)

class RekallRuntimeException(Exception):
    pass

def _wrap_for_exception(fn):
    def wrapped(domains):
        try:
            return fn(domains)
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

def _get_callback(pbar, args_with_err, print_error=True):
    def callback(task_args, err=None):
        if err is None:
            if pbar is not None:
                pbar.update(len(task_args))
        else:
            if print_error:
                print("Error when processing {0}:{1}".format(
                    task_args, repr(err)))
            args_with_err.extend(task_args)
    return callback

def _create_tasks(args, chunksize):
    total = len(args)
    num_tasks = int(ceil(total/chunksize))
    tasks = []
    for task_i in range(num_tasks):
        start = chunksize*task_i
        end = min(total,start+chunksize)
        tasks.append(args[start:end])
    return tasks

def union_combiner(result1, result2):
    return result1.union(result2)

def disjoint_domain_combiner(result1, result2):
    """ Same as union_combiner but assumes results are 
    DomainIntervalCollections with disjoint keys so is faster.
    """
    d1 = result1.get_grouped_intervals()
    d2 = result2.get_grouped_intervals()
    k1,k2 = set(d1.keys()), set(d2.keys())
    if k1.isdisjoint(k2):
        return DomainIntervalCollection({**d1, **d2})
    intersection = k1 & k2
    raise RekallRuntimeException(
        "DisjointDomainCombiner used on results"
        " with overlapping domains {0}".format(intersection))

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

    def run(self, query, args, combiner=union_combiner,
            randomize=True, chunksize=1,
            progress=False, profile=False,
            print_error=True):
        """
        Runs the rekall query on given args.
        Since args may be long, the runtime may split it into chunks and send
        each chunk to workers that may run in parallel.
        `query` is a function from a batch of args to an IntervalSet3D or
        a DomainIntervalCollection
        Returns (query_output, args_with_err) where query_output is the same
        as applying query on args.
        """
        query = _wrap_for_exception(query)
        with perf_count("Executing query in Runtime", enable=profile):
            with _WorkerPoolContext(self._get_worker_pool(query)) as pool:
                total_work = len(args)
                with tqdm(total=total_work, disable=not progress) as pbar:
                    with perf_count("Executing in workers", enable=profile):
                        args_with_err = []
                        with perf_count("Dispatching tasks", enable=profile):
                            if randomize:
                                random.shuffle(args)
                            async_results = pool.map(
                                    _create_tasks(args, chunksize),
                                    _get_callback(pbar, args_with_err,
                                        print_error))
                        combined_result = None
                        for future in async_results:
                            try:
                                r = future.get()
                            except TaskException:
                                continue
                            if combined_result is None:
                                combined_result = r
                            else:
                                combined_result = combiner(combined_result, r)
                        if combined_result is None and total_work>0:
                            raise RekallRuntimeException("All tasks failed!")
                        return (combined_result, args_with_err)

    def get_result_iterator(self, query, args, randomize=True, chunksize=1, 
            print_error=True):
        """
        Returns a generator for results of running `query` on chunks of `args`

        Since args may be long, the runtime may split it into chunks and send
        each chunk to workers that may run in parallel.
        `query` is a function from a batch of args to an IntervalSet3D or
        a DomainIntervalCollection

        If randomize is True, results are yielded in the same order as args.
        If any chunk encountered error, the error is thrown after all
        successful results are yielded.
        """
        query = _wrap_for_exception(query)
        with _WorkerPoolContext(self._get_worker_pool(query)) as pool:
            total_work = len(args)
            args_with_err = []
            if randomize:
                random.shuffle(args)
            async_results = pool.map(
                _create_tasks(args, chunksize),
                _get_callback(None, args_with_err, print_error))
            for future in async_results:
                try:
                    r = future.get()
                except TaskException:
                    continue
                yield r
            if len(args_with_err) > 0:
                raise RekallRuntimeException(
                        "The following tasks failed: {0}".format(
                    args_with_err))

