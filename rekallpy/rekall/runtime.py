"""An Extensible Parallel Runtime Library for Rekall.

Runtime: the entrypoint for running large tasks.
    Its construction takes a factory function for a worker pool, which are
    described in the following section.

WorkerPool Factories:
    inline_pool_factory: A factory for a pool that executes tasks in sequence
        in the main process.
    get_spawned_process_pool_factory: Returns a factory function for a pool
        that creates worker processes by spawning new python interpreters.
        The worker processes do not inherit any context from the main process.
    get_forked_process_pool_factory: Returns a factory function for a pool that
        creates worker processes by forking. The worker processes thus inherits
        all the global context from the main process such as global variables.
        However, safely forking a multithreaded program is problematic.

Combiners for Runtime:
    Runtime.run() is a MapReduce routine where a large task is divided into
    smaller chunks and each chunk is fed to a worker (the map step).
    A combiner is responsible for merging the results of each chunk, i.e. the 
    reduction step. The provided combiners are the following.

    union_combiner: The default combiner. It calls `union` method on the
        per-chunk results to merge them, assuming the results have type
        IntervalSet3D or DomainIntervalCollection.
    disjoint_domain_combiner: A faster combiner than union_combiner which
        assumes that the results are of type DomainIntervalCollection and
        every chunk produces its own set of domain keys that are disjoint
        from results of other chunks.

WorkerPool classes:
    Besides using the provided factory functions, one can create their own 
    factory by using the provided WorkerPool classes, or even write their own
    implementation of the WorkerPool interface, which are described below.

    InlineSingleProcessPool: This worker pool uses the main process to
        execute tasks in sequence.
    ForkedProcessPool: This pool uses forking to create worker processes.
    SpawnedProcessPool: This pool spawns fresh python interpreter processes.
        It can run custom initializers when creating the child processes.
    AbstractWorkerPool: The abstract interface for all WorkerPool
        implementations.

AsyncTaskResult interface:
    This represents a Future-like object. Custom implementations of WorkerPool
    interface need to return objects with this interface in `map` method.

Exception classes:
    TaskException: raised when a worker throws during task execution.
    RekallRuntimeException: raised when there is error in the Runtime.
"""

import cloudpickle
from math import ceil
import multiprocessing as mp
import random
from tqdm import tqdm

from rekall.interval_set_3d_utils import perf_count
from rekall.domain_interval_collection import DomainIntervalCollection

class TaskException(Exception):
    """Exception to throw when a worker encounters error during task.

    Use `raise from` syntex to wrap the exception around the real error.
    For example:
        try:
            ...
        except Exception as e:
            raise TaskException() from e
    """
    def __repr__(self):
        return "TaskException from {0}".format(repr(self.__cause__))

class RekallRuntimeException(Exception):
    """Exception raised when Runtime encounters error."""
    pass

class AbstractAsyncTaskResult():
    """Definition of the AsyncTaskResult interface

    This represents a Future-like object. Custom implementations of WorkerPool
    interface need to return objects with this interface in `map` method.
    """
    def get(self):
        """Returns the value inside the AsyncTaskResult.

        This blocks until the value is ready.

        Raises:
            TaskException: if the AsyncTaskResult contains error.
        """
        raise NotImplementedError()

    def done(self):
        """Returns whether the value is ready"""
        raise NotImplementedError()

class _FutureWrapper(AbstractAsyncTaskResult):
    """Wraps a mp.pool.AsyncResult object to throw TaskException."""
    def __init__(self, future):
        self._f = future

    def get(self):
        try:
            return self._f.get()
        except Exception as e:
            raise TaskException() from e

    def done(self):
        return self._f.ready()

class AbstractWorkerPool():
    """Definition of the WorkerPool interface
    
    Notes:
        A WorkerPool instance is specialized to running one function, which is
        why the function to execute is not here in the interface but is instead
        passed to the worker pool factory function.
    """
    def map(self, tasks, done):
        """Maps the tasks over the available workers in the pool

        Args:
            tasks: A list of tasks to execute. Each task is a set of arguments
                to run the function with. 
            done: A callback function that is called when any task finishes.
                It takes the set of arguments for the finished task, and
                optionally an error that the task encountered if there is one.

        Returns:
            A list of AsyncTaskResults, one for each task in tasks.
        """
        raise NotImplementedError()

    def shut_down(self):
        """Clean up the worker pool after all tasks have finished.
        
        Implementations should release any resources used by the worker pool.
        """
        raise NotImplementedError()

class InlineSingleProcessPool(AbstractWorkerPool):
    """A single-process implmentation of WorkerPool interface."""
    class _Lazy(AbstractAsyncTaskResult):
        """A wrapper that defers the execution until result is requested"""
        def __init__(self, getter, done_cb):
            self.getter = getter
            self.done_cb = done_cb
        def get(self):
            try:
                r = self.getter()
            except Exception as e:
                self.done_cb(e)
                raise TaskException() from e
            self.done_cb()
            return r
        def done(self):
            return True

    def __init__(self, fn):
        """Initializes with the function to run."""
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
        return [InlineSingleProcessPool._Lazy(
                    get_getter(self.fn, task),
                    get_callback(task)) for task in tasks]

    def shut_down(self):
        pass

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

class ForkedProcessPool(AbstractWorkerPool):
    """A WorkerPool implementation using forking.
    
    The worker processes will inherit the global context from the main process
    such as global variables. However, forking a multithreaded program safely 
    is very tricky. In particular, any global thread pool object in the parent 
    process is forked but the actual threads are not available in the forked 
    child processes.
    """
    def __init__(self, fn, num_workers):
        """Initializes the instance
        Args:
            fn: The function to run in child processes.
            num_workers: Number of child processes to create.
        """
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
        return [_FutureWrapper(self._pool.apply_async(
                _apply_global_context_as_function,
                args=(task,),
                callback=get_success_callback(task),
                error_callback=get_error_callback(task))) for task in tasks]

    def shut_down(self):
        self._pool.terminate()

# When spawning, arguments to initializer are pickled.
# To allow arbitrary lambdas with closure, use cloudpickle to serialize the
# function to execute
def _apply_serialized_function(serialized_func, vids):
    fn = cloudpickle.loads(serialized_func)
    return fn(vids)

class SpawnedProcessPool(AbstractWorkerPool):
    """A WorkerPool implementation using spawning.
    
    It creates worker processes by spawning new python interpreters.
    The worker processes do not inherit any context from the main process.
    In particular, they have no access to the global variables and imported
    modules in the main process.
    """
    def __init__(self, fn, num_workers, initializer=None):
        """Initializes the instance.

        Args:
            fn: The function to run in child processes.
            num_workers: Number of child processes to create.
            initializer: A function to run in the child process after it is 
                created. It can be used to set up necessary resources in the
                worker.
        """
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
        return [_FutureWrapper(self._pool.apply_async(
                _apply_serialized_function,
                args=(self._pickled_fn, task),
                callback=get_success_callback(task),
                error_callback=get_error_callback(task))) for task in tasks]

    def shut_down(self):
        self._pool.terminate()

# WorkerPool Factories
def inline_pool_factory(fn):
    """Creates a InlineSingleProcessPool."""
    return InlineSingleProcessPool(fn)

def get_forked_process_pool_factory(num_workers=mp.cpu_count()):
    """Returns a factory for ForkedProcessPool.

    Args:
        num_workers (optional): Number of child processes to fork.
            Defaults to the number of CPU cores on the machine.
    
    Returns:
        A factory for ForkedProcessPool.
    """
    def factory(fn):
        return ForkedProcessPool(fn, num_workers)
    return factory

def get_spawned_process_pool_factory(num_workers=mp.cpu_count()):
    """Returns a factory for SpawnedProcessPool.

    Args:
        num_workers (optional): Number of child processes to spawn.
            Defaults to the number of CPU cores on the machine.
    
    Returns:
        A factory for SpawnedProcessPool.
    """
    def factory(fn):
        return SpawnedProcessPool(fn, num_workers)
    return factory

class _WorkerPoolContext():
    """ Wrapper class to allow `with` syntax on WorkerPools"""
    def __init__(self, pool):
        self._pool = pool

    def __enter__(self):
        return self._pool

    def __exit__(self, *args):
        self._pool.shut_down()

def _get_callback(pbar, args_with_err, print_error=True):
    """
    Returns a callback that, when called after a task finishes, updates the
    progress bar, and if there is an error, add the task to args_with_err and
    optionally prints the error to stdout.
    """
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
    """Splits args into tasks of `chunksize` each."""
    total = len(args)
    num_tasks = int(ceil(total/chunksize))
    tasks = []
    for task_i in range(num_tasks):
        start = chunksize*task_i
        end = min(total,start+chunksize)
        tasks.append(args[start:end])
    return tasks

def union_combiner(result1, result2):
    """Combiner that calls union method on the result."""
    return result1.union(result2)

def disjoint_domain_combiner(result1, result2):
    """ A faster combiner than union_combiner for DomainIntervalCollection.
        
    Assumes that the results are of type DomainIntervalCollection and every 
    chunk produces its own set of domain keys that are disjoint from results of 
    other chunks.

    Args:
        result1, result2 (DomainIntervalCollection): partial results from
            some chunks of total work.
    
    Returns:
        A DomainIntervalCollection that is the union of the two.
    
    Raises:
        RekallRuntimeException: Raised if results have common domain key.
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

def _pop_future_to_yield(futures):
    """Polling until a future is ready with exponential backoff"""
    MAX_WAIT = 1
    MIN_WAIT = 0.001
    FACTOR = 1.5

    def grow(wait_time):
        return min(MAX_WAIT, wait_time * FACTOR)

    from time import sleep

    wait_time = MIN_WAIT
    while True:
        for i,f in enumerate(futures):
            if f.done():
                return futures.pop(i)
        sleep(wait_time)
        wait_time = grow(wait_time)

class Runtime():
    """Manages execution of function on large number of inputs.

    Given a function that can return results for a batch of inputs, and a
    potentially long list of inputs to run the function with, Runtime helps to
    divide the inputs into small chunks, also called tasks, and dispatches
    the tasks to a pool of workers created by worker_pool_factory. It also
    gracefully handles exceptions in workers and can assemble the partial
    results.

    An example function:
        def query(video_ids):
            # Gets the intervals in the input batch of videos
            frames_with_opposing_faces = ...
            # Returns a DomainIntervalCollection with video_id as domain key.
            return frames_with_opposing_faces

        # A list of 100K video_ids
        ALL_VIDEO_IDS = ...

    In the example, query(ALL_VIDEO_IDS) is not practical to run in one go.
    To get the same results, one can use Runtime in one of two ways.

    The first way is to dispatch all tasks and wait:
        # Running the query on all videos, in chunks of 5 on 16 processes.
        rt = Runtime(get_forked_process_pool_factory(num_workers=16))
        # Will block until everything finishes
        # results is a DomainIntervalCollection with all intervals found.
        results, failed_video_ids = rt.run(
            query, ALL_VIDEO_IDS, combiner=disjoint_domain_combiner,
            chunksize=5)

    The second way is to use iterator:
        # Get an iterator that yields partial results from each chunk of 5.
        rt = Runtime(get_forked_process_pool_factory(num_workers=16))
        gen = rt.get_result_iterator(query, ALL_VIDEO_IDS, chunksize=5)
        # Blocks until the first batch is done.
        # results_from_one_batch is a DomainIntervalCollection with intervals
        # found in one task (a chunk of 5 videos).
        results_from_one_batch = next(gen)
    """
    def __init__(self, worker_pool_factory):
        """Initialized with a WorkerPool Factory
        
        Args:
            worker_pool_factory: A function that takes the query to execute,
                and returns a worker pool to execute the query.
        """
        self._get_worker_pool = worker_pool_factory

    @classmethod
    def inline(cls):
        """Inline Runtime executes each chunk in sequence in one process."""
        return cls(inline_pool_factory)

    def run(self, query, args, combiner=union_combiner,
            randomize=True, chunksize=1,
            progress=False, profile=False,
            print_error=True):
        """Dispatches all tasks to workers and waits until everything finishes.

        See class documentation for an example of how to use run().
        Exception raised in `query` are suppressed and the unsuccessful subset
        of `args` is returned at the end. However, such errors can be printed
        as soon as they occur.

        Args:
            query: A function that can return partial results for any batch of
                input arguments.
            args: A potentially long list of input arguments to execute the
                query with.
            combiner (optional): A function that takes two partial results and
                returns the combination of the two.
                Defaults to union_combiner which assumes the partial results
                have a `union` method.
            randomize (optional): Whether to create and dispatch tasks in
                random order.
                Defaults to True.
            chunksize (optional): The size of the input batch for each task.
                Defaults to 1.
            progress (optional): Whether to display a progress bar.
                Defaults to False.
            profile (optional): Whether to output wall time of various internal
                stages to stdout.
                Defaults to False.
            print_error (optional): Whether to output task errors to stdout.
                Defaults to True.

        Returns:
            A pair (query_output, args_with_err) where
            query_output: The combined results from successful tasks.
            args_with_err: A list that is a subset of args that failed to
                execute.
        """
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
            print_error=True, dispatch_size=mp.cpu_count()):
        """Incrementally dispatches tasks as partial results are consumed.

        See class documentation for an example of how to use 
        get_result_iterator().
        Exception raised in `query` are suppressed and if any tasks failed,
        will raise a RekallRuntimeException after all successful tasks' results
        have been yielded. However, such errors can be printed as soon as they
        occur.

        Args:
            query, args, randomize, chunksize, print_error: Same as in run().
            dispatch_size (int, optional): Number of tasks to dispatch at a
                time. In this mode, tasks are incrementally dispatched
                as partial results from preivous tasks are yielded.
                If not positive, will dispatch all tasks at once.
                Defaults to the number of CPU cores.

        Yields:
            Partial results from each task.

        Raises:
            RekallRuntimeException: Raised after all successful task results
                have been yielded if there have been failed tasks.
        """
        with _WorkerPoolContext(self._get_worker_pool(query)) as pool:
            args_with_err = []
            if randomize:
                random.shuffle(args)
            tasks = _create_tasks(args, chunksize)
            if dispatch_size is None or dispatch_size<=0:
                dispatch_size = len(tasks)
            outstanding_tasks = tasks
            async_results = []
            num_finished_tasks = 0
            while num_finished_tasks < len(tasks):
                # Maybe make a dispatch
                num_to_yield = len(async_results)
                if (num_to_yield <= dispatch_size/2 and 
                    len(outstanding_tasks) > 0):
                    task_batch = outstanding_tasks[:dispatch_size]
                    outstanding_tasks = outstanding_tasks[dispatch_size:]
                    async_results.extend(pool.map(
                        task_batch,
                        _get_callback(None, args_with_err, print_error)))
                if randomize:
                    future_to_yield = _pop_future_to_yield(async_results)
                else:
                    future_to_yield = async_results.pop(0)
                num_finished_tasks += 1
                try:
                    r = future_to_yield.get()
                except TaskException:
                    continue
                yield r
        if len(args_with_err) > 0:
            raise RekallRuntimeException(
                    "The following tasks failed: {0}".format(
                args_with_err))

