"""
Task manager that distributes tasks over MPI processes.

Taken from https://github.com/bccp/nbodykit/blob/master/nbodykit/__init__.py
and https://github.com/bccp/nbodykit/blob/master/nbodykit/batch.py.
"""

import os
import functools
import traceback
import logging
import warnings
from contextlib import contextmanager

import numpy as np
from mpi4py import MPI


class CurrentMPIComm(object):
    """Class to faciliate getting and setting the current MPI communicator."""
    logger = logging.getLogger('CurrentMPIComm')

    _stack = [MPI.COMM_WORLD]

    @staticmethod
    def enable(func):
        """
        Decorator to attach the current MPI communicator to the input
        keyword arguments of ``func``, via the ``mpicomm`` keyword.
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            kwargs.setdefault('mpicomm', None)
            if kwargs['mpicomm'] is None:
                kwargs['mpicomm'] = CurrentMPIComm.get()
            return func(*args, **kwargs)

        return wrapper

    @classmethod
    @contextmanager
    def enter(cls, mpicomm):
        """
        Enter a context where the current default MPI communicator is modified to the
        argument `comm`. After leaving the context manager the communicator is restored.

        Example:

        .. code:: python

            with CurrentMPIComm.enter(comm):
                cat = UniformCatalog(...)

        is identical to

        .. code:: python

            cat = UniformCatalog(..., comm=comm)

        """
        cls.push(mpicomm)

        yield

        cls.pop()

    @classmethod
    def push(cls, mpicomm):
        """Switch to a new current default MPI communicator."""
        cls._stack.append(mpicomm)
        if mpicomm.rank == 0:
            cls.logger.info('Entering a current communicator of size {:d}'.format(mpicomm.size))
        cls._stack[-1].barrier()

    @classmethod
    def pop(cls):
        """Restore to the previous current default MPI communicator."""
        mpicomm = cls._stack[-1]
        if mpicomm.rank == 0:
            cls.logger.info('Leaving current communicator of size {:d}'.format(mpicomm.size))
        cls._stack[-1].barrier()
        cls._stack.pop()
        mpicomm = cls._stack[-1]
        if mpicomm.rank == 0:
            cls.logger.info('Restored current communicator to size {:d}'.format(mpicomm.size))

    @classmethod
    def get(cls):
        """Get the default current MPI communicator. The initial value is ``MPI.COMM_WORLD``."""
        return cls._stack[-1]



class CurrentMPIState(object):

    """
    Descriptor for current MPI state of a Python class.
    - BROADCAST : class "content" (e.g. arrays) broadcast on all ranks
    - SCATTERED : class content scattered on all ranks
    - GATHERED : class content gathered on root rank.
    """
    strs = ['BROADCAST','SCATTERED','GATHERED']
    ints = list(range(len(strs)))

    def __new__(cls, mpistate):
        if isinstance(mpistate,str):
            mpistate = cls.strs.index(mpistate.upper())
        if mpistate not in cls.ints:
            raise MPIError('Unknown MPI state {}; shoud be in {} or {}.'.format(mpistate,cls.strs,cls.ints))
        return mpistate

    @classmethod
    def as_str(cls, mpistate):
        return cls.strs[mpistate]


for i,s in zip(CurrentMPIState.ints,CurrentMPIState.strs):
    setattr(CurrentMPIState,s,i)



class MPIError(Exception):

    """Exception raised when issue with MPI operations."""


def MPIInit(func):

    """:meth:`__init__()` decorator that sets MPI attributes: :attr:`mpiroot`, :attr:`mpicomm` and :attr:`mpistate`."""

    @functools.wraps(func)
    @CurrentMPIComm.enable
    def wrapper(self, *args, mpistate=CurrentMPIState.BROADCAST, mpiroot=0, mpicomm=None, **kwargs):
        self.mpiroot = mpiroot
        self.mpicomm = mpicomm
        self.mpistate = mpistate
        func(self,*args,**kwargs)

    return wrapper


def MPIScatter(func):

    """:meth:`mpi_scatter` decorator that checks whether class is already scattered before scattering, then sets :attr:`mpistate`."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.mpistate == CurrentMPIState.SCATTERED:
            raise MPIError('{} instance already scattered!'.format(self.__class__.__name__))
        func(self,*args,**kwargs)
        self.mpistate = CurrentMPIState.SCATTERED

    return wrapper


def MPIGather(func):

    """:meth:`mpi_gather` decorator that checks whether class is already gathered before gathering, then sets :attr:`mpiroot` and :attr:`mpistate`."""

    @functools.wraps(func)
    def wrapper(self, *args, mpiroot=0, **kwargs):
        if self.mpistate != CurrentMPIState.SCATTERED:
            raise MPIError('{} instance is not scattered!'.format(self.__class__.__name__))
        self.mpiroot = mpiroot
        func(self,*args,**kwargs)
        self.mpistate = CurrentMPIState.GATHERED

    return wrapper


def MPIBroadcast(func):

    """:meth:`mpi_broadcast` decorator that first gathers class on ``mpiroot``, sets :attr:`mpiroot`, :attr:`mpicomm` :attr:`mpistate`."""

    @functools.wraps(func)
    @classmethod
    @CurrentMPIComm.enable
    def wrapper(cls, self, *args, mpiroot=0, mpicomm=None, **kwargs):
        new = cls.__new__(cls)
        isscattered = self is not None and self.is_mpi_scattered()
        if isscattered: self.mpi_gather()
        #if self.mpistate == CurrentMPIState.BROADCAST:
        #    raise MPIError('{} instance already broadcast!'.format(cls.__name__))
        new.mpiroot = mpiroot
        new.mpicomm = mpicomm
        func(new,self,*args,**kwargs)
        new.mpistate = CurrentMPIState.BROADCAST
        #if isscattered: self.mpi_scatter()
        return new

    return wrapper


def enum(*sequential, **named):
    """
    Enumeration values to serve as status tags passed
    between processes
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)



def split_ranks(N_ranks, N, include_all=False):
    """
    Divide the ranks into chunks, attempting to have `N` ranks
    in each chunk. This removes the master (0) rank, such
    that `N_ranks - 1` ranks are available to be grouped

    Parameters
    ----------
    N_ranks : int
        the total number of ranks available
    N : int
        the desired number of ranks per worker
    include_all : bool, optional
        if `True`, then do not force each group to have
        exactly `N` ranks, instead including the remainder as well;
        default is `False`
    """
    available = list(range(1, N_ranks)) # available ranks to do work
    total = len(available)
    extra_ranks = total % N

    if include_all:
        for i, chunk in enumerate(np.array_split(available, max(total//N, 1))):
            yield i, list(chunk)
    else:
        for i in range(total//N):
            yield i, available[i*N:(i+1)*N]

        i = total // N
        if extra_ranks and extra_ranks >= N//2:
            remove = extra_ranks % 2 # make it an even number
            ranks = available[-extra_ranks:]
            if remove: ranks = ranks[:-remove]
            if len(ranks):
                yield i+1, ranks


class MPITaskManager(object):
    """
    A MPI task manager that distributes tasks over a set of MPI processes,
    using a specified number of independent workers to compute each task.

    Given the specified number of independent workers (which compute
    tasks in parallel), the total number of available CPUs will be
    divided evenly.

    The main function is ``iterate`` which iterates through a set of tasks,
    distributing the tasks in parallel over the available ranks.
    """
    logger = logging.getLogger('MPITaskManager')

    @CurrentMPIComm.enable
    def __init__(self, nprocs_per_task=1, use_all_nprocs=False, mpicomm=None):
        """
        Initialize MPITaskManager.

        Parameters
        ----------
        nprocs_per_task : int, optional
            the desired number of processes assigned to compute
            each task
        mpicomm : MPI communicator, optional
            the global communicator that will be split so each worker
            has a subset of CPUs available; default is COMM_WORLD
        use_all_nprocs : bool, optional
            if `True`, use all available CPUs, including the remainder
            if `nprocs_per_task` does not divide the total number of CPUs
            evenly; default is `False`
        """
        self.nprocs_per_task = nprocs_per_task
        self.use_all_nprocs  = use_all_nprocs

        # the base communicator
        self.basecomm = MPI.COMM_WORLD if mpicomm is None else mpicomm
        self.rank     = self.basecomm.rank
        self.size     = self.basecomm.size

        # need at least one
        if self.size == 1:
            raise ValueError('need at least two processes to use a MPITaskManager')

        # communication tags
        self.tags = enum('READY', 'DONE', 'EXIT', 'START')

        # the task communicator
        self.mpicomm = None

        # store a MPI status
        self.status = MPI.Status()

    def __enter__(self):
        """
        Split the base communicator such that each task gets allocated
        the specified number of nranks to perform the task with.
        """
        self.self_worker_ranks = []
        color = 0
        total_ranks = 0
        nworkers = 0

        # split the ranks
        for i, ranks in split_ranks(self.size, self.nprocs_per_task, include_all=self.use_all_nprocs):
            if self.rank in ranks:
                color = i+1
                self.self_worker_ranks = ranks
            total_ranks += len(ranks)
            nworkers = nworkers + 1
        self.other_ranks = [rank for rank in range(self.size) if rank not in self.self_worker_ranks]

        self.workers = nworkers # store the total number of workers
        if self.rank == 0:
            self.logger.info('Entering {} with {:d} workers.'.format(self.__class__.__name__,self.workers))

        # check for no workers!
        if self.workers == 0:
            raise ValueError('no pool workers available; try setting `use_all_nprocs` = True')

        leftover = (self.size - 1) - total_ranks
        if leftover and self.rank == 0:
            self.logger.warning('with `nprocs_per_task` = {:d} and {:d} available rank(s), '\
                                '{:d} rank(s) will do no work'.format(self.nprocs_per_task, self.size-1, leftover))
            self.logger.warning('set `use_all_nprocs=True` to use all available nranks')

        # crash if we only have one process or one worker
        if self.size <= self.workers:
            raise ValueError('only have {:d} ranks; need at least {:d} to use the desired %d workers'.format(self.size, self.workers+1, self.workers))

        # ranks that will do work have a nonzero color now
        self._valid_worker = color > 0

        # split the comm between the workers
        self.mpicomm = self.basecomm.Split(color, 0)
        CurrentMPIComm.push(self.mpicomm)

        return self

    def is_root(self):
        """
        Is the current process the root process?

        Root is responsible for distributing the tasks to the other available ranks
        """
        return self.rank == 0

    def is_worker(self):
        """
        Is the current process a valid worker?

        Workers wait for instructions from the master
        """
        try:
            return self._valid_worker
        except:
            raise ValueError('workers are only defined when inside the ``with MPITaskManager()`` context')

    def _get_tasks(self):
        """Internal generator that yields the next available task from a worker."""

        if self.is_root():
            raise RuntimeError('Root rank mistakenly told to await tasks')

        # logging info
        if self.mpicomm.rank == 0:
            self.logger.debug('worker master rank is {:d} on {} with {:d} processes available'.format(self.rank, MPI.Get_processor_name(), self.mpicomm.size))

        # continously loop and wait for instructions
        while True:
            args = None
            tag = -1

            # have the master rank of the subcomm ask for task and then broadcast
            if self.mpicomm.rank == 0:
                self.basecomm.send(None, dest=0, tag=self.tags.READY)
                args = self.basecomm.recv(source=0, tag=MPI.ANY_TAG, status=self.status)
                tag = self.status.Get_tag()

            # bcast to everyone in the worker subcomm
            args  = self.mpicomm.bcast(args) # args is [task_number, task_value]
            tag   = self.mpicomm.bcast(tag)

            # yield the task
            if tag == self.tags.START:

                # yield the task value
                yield args

                # wait for everyone in task group before telling master this task is done
                self.mpicomm.Barrier()
                if self.mpicomm.rank == 0:
                    self.basecomm.send([args[0], None], dest=0, tag=self.tags.DONE)

            # see ya later
            elif tag == self.tags.EXIT:
                break

        # wait for everyone in task group and exit
        self.mpicomm.Barrier()
        if self.mpicomm.rank == 0:
            self.basecomm.send(None, dest=0, tag=self.tags.EXIT)

        # debug logging
        self.logger.debug('rank %d process is done waiting',self.rank)

    def _distribute_tasks(self, tasks):
        """Internal function that distributes the tasks from the root to the workers."""

        if not self.is_root():
            raise ValueError('only the root rank should distribute the tasks')

        ntasks = len(tasks)
        task_index     = 0
        closed_workers = 0

        # logging info
        self.logger.debug('master starting with {:d} worker(s) with {:d} total tasks'.format(self.workers, ntasks))

        # loop until all workers have finished with no more tasks
        while closed_workers < self.workers:

            # look for tags from the workers
            data = self.basecomm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=self.status)
            source = self.status.Get_source()
            tag = self.status.Get_tag()

            # worker is ready, so send it a task
            if tag == self.tags.READY:

                # still more tasks to compute
                if task_index < ntasks:
                    this_task = [task_index, tasks[task_index]]
                    self.basecomm.send(this_task, dest=source, tag=self.tags.START)
                    self.logger.debug('sending task `{}` to worker {:d}'.format(str(tasks[task_index]),source))
                    task_index += 1

                # all tasks sent -- tell worker to exit
                else:
                    self.basecomm.send(None, dest=source, tag=self.tags.EXIT)

            # store the results from finished tasks
            elif tag == self.tags.DONE:
                self.logger.debug('received result from worker {:d}'.format(source))

            # track workers that exited
            elif tag == self.tags.EXIT:
                closed_workers += 1
                self.logger.debug('worker {:d} has exited, closed workers = {:d}'.format(source,closed_workers))

    def iterate(self, tasks):
        """
        Iterate through a series of tasks in parallel.

        Notes
        -----
        This is a collective operation and should be called by
        all ranks.

        Parameters
        ----------
        tasks : iterable
            An iterable of `task` items that will be yielded in parallel
            across all ranks.

        Yields
        -------
        task :
            The individual items of `tasks`, iterated through in parallel.
        """
        # master distributes the tasks and tracks closed workers
        if self.is_root():
            self._distribute_tasks(tasks)

        # workers will wait for instructions
        elif self.is_worker():
            for tasknum, args in self._get_tasks():
                yield args

    def map(self, function, tasks):
        """
        Apply a function to all of the values in a list and return the list of results.

        If ``tasks`` contains tuples, the arguments are passed to
        ``function`` using the ``*args`` syntax.

        Notes
        -----
        This is a collective operation and should be called by
        all ranks.

        Parameters
        ----------
        function : callable
            The function to apply to the list.
        tasks : list
            The list of tasks.

        Returns
        -------
        results : list
            The list of the return values of ``function``.
        """
        results = []

        # master distributes the tasks and tracks closed workers
        if self.is_root():
            self._distribute_tasks(tasks)

        # workers will wait for instructions
        elif self.is_worker():

            # iterate through tasks in parallel
            for tasknum, args in self._get_tasks():

                # make function arguments consistent with *args
                if not isinstance(args, tuple):
                    args = (args,)

                # compute the result (only worker root needs to save)
                result = function(*args)
                if self.mpicomm.rank == 0:
                    results.append((tasknum, result))

        # put the results in the correct order
        results = self.basecomm.allgather(results)
        results = [item for sublist in results for item in sublist]
        return [r[1] for r in sorted(results, key=lambda x: x[0])]

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit gracefully by closing and freeing the MPI-related variables."""

        if exc_value is not None:
            from . import utils
            utils.exception_handler(exc_type, exc_value, exc_traceback)

        # wait and exit
        self.logger.debug('Rank {:d} process finished'.format(self.rank))
        self.basecomm.Barrier()

        if self.is_root():
            self.logger.debug('master is finished; terminating')

        CurrentMPIComm.pop()

        if self.mpicomm is not None:
            self.mpicomm.Free()


@CurrentMPIComm.enable
def gather_array(data, root=0, mpicomm=None):
    """
    Taken from https://github.com/bccp/nbodykit/blob/master/nbodykit/utils.py
    Gather the input data array from all ranks to the specified ``root``.
    This uses `Gatherv`, which avoids mpi4py pickling, and also
    avoids the 2 GB mpi4py limit for bytes using a custom datatype

    Parameters
    ----------
    data : array_like
        the data on each rank to gather
    mpicomm : MPI communicator
        the MPI communicator
    root : int, or Ellipsis
        the rank number to gather the data to. If root is Ellipsis or None,
        broadcast the result to all ranks.

    Returns
    -------
    recvbuffer : array_like, None
        the gathered data on root, and `None` otherwise
    """
    if root is None: root = Ellipsis

    if np.isscalar(data):
        if root == Ellipsis:
            return np.array(mpicomm.allgather(data))
        gathered = mpicomm.gather(data, root=root)
        if mpicomm.rank == root:
            return np.array(gathered)
        return None

    if not isinstance(data, np.ndarray):
        raise ValueError('`data` must be numpy array in gather_array')

    # need C-contiguous order
    if not data.flags['C_CONTIGUOUS']:
        data = np.ascontiguousarray(data)
    local_length = data.shape[0]

    # check dtypes and shapes
    shapes = mpicomm.allgather(data.shape)
    dtypes = mpicomm.allgather(data.dtype)

    # check for structured data
    if dtypes[0].char == 'V':

        # check for structured data mismatch
        names = set(dtypes[0].names)
        if any(set(dt.names) != names for dt in dtypes[1:]):
            raise ValueError('mismatch between data type fields in structured data')

        # check for 'O' data types
        if any(dtypes[0][name] == 'O' for name in dtypes[0].names):
            raise ValueError('object data types ("O") not allowed in structured data in gather_array')

        # compute the new shape for each rank
        newlength = mpicomm.allreduce(local_length)
        newshape = list(data.shape)
        newshape[0] = newlength

        # the return array
        if root is Ellipsis or mpicomm.rank == root:
            recvbuffer = np.empty(newshape, dtype=dtypes[0], order='C')
        else:
            recvbuffer = None

        for name in dtypes[0].names:
            d = gather_array(data[name], root=root, mpicomm=mpicomm)
            if root is Ellipsis or mpicomm.rank == root:
                recvbuffer[name] = d

        return recvbuffer

    # check for 'O' data types
    if dtypes[0] == 'O':
        raise ValueError('object data types ("O") not allowed in structured data in gather_array')

    # check for bad dtypes and bad shapes
    if root is Ellipsis or mpicomm.rank == root:
        bad_shape = any(s[1:] != shapes[0][1:] for s in shapes[1:])
        bad_dtype = any(dt != dtypes[0] for dt in dtypes[1:])
    else:
        bad_shape = None; bad_dtype = None

    if root is not Ellipsis:
        bad_shape, bad_dtype = mpicomm.bcast((bad_shape, bad_dtype),root=root)

    if bad_shape:
        raise ValueError('mismatch between shape[1:] across ranks in gather_array')
    if bad_dtype:
        raise ValueError('mismatch between dtypes across ranks in gather_array')

    shape = data.shape
    dtype = data.dtype

    # setup the custom dtype
    duplicity = np.product(np.array(shape[1:], 'intp'))
    itemsize = duplicity * dtype.itemsize
    dt = MPI.BYTE.Create_contiguous(itemsize)
    dt.Commit()

    # compute the new shape for each rank
    newlength = mpicomm.allreduce(local_length)
    newshape = list(shape)
    newshape[0] = newlength

    # the return array
    if root is Ellipsis or mpicomm.rank == root:
        recvbuffer = np.empty(newshape, dtype=dtype, order='C')
    else:
        recvbuffer = None

    # the recv counts
    counts = mpicomm.allgather(local_length)
    counts = np.array(counts, order='C')

    # the recv offsets
    offsets = np.zeros_like(counts, order='C')
    offsets[1:] = counts.cumsum()[:-1]

    # gather to root
    if root is Ellipsis:
        mpicomm.Allgatherv([data, dt], [recvbuffer, (counts, offsets), dt])
    else:
        mpicomm.Gatherv([data, dt], [recvbuffer, (counts, offsets), dt], root=root)

    dt.Free()

    return recvbuffer


@CurrentMPIComm.enable
def broadcast_array(data, root=0, mpicomm=None):
    """
    Broadcast the input data array across all ranks, assuming `data` is
    initially only on `root` (and `None` on other ranks).
    This uses ``Scatterv``, which avoids mpi4py pickling, and also
    avoids the 2 GB mpi4py limit for bytes using a custom datatype

    Parameters
    ----------
    data : array_like or None
        on `root`, this gives the data to split and scatter
    mpicomm : MPI communicator
        the MPI communicator
    root : int
        the rank number that initially has the data
    counts : list of int
        list of the lengths of data to send to each rank
    Returns
    -------
    recvbuffer : array_like
        the chunk of `data` that each rank gets
    """

    # check for bad input
    if mpicomm.rank == root:
        isscalar = np.isscalar(data)
    else:
        isscalar = None
    isscalar = mpicomm.bcast(isscalar, root=root)

    if isscalar:
        return mpicomm.bcast(data, root=root)

    if mpicomm.rank == root:
        bad_input = not isinstance(data, np.ndarray)
    else:
        bad_input = None
    bad_input = mpicomm.bcast(bad_input,root=root)
    if bad_input:
        raise ValueError('`data` must by numpy array on root in broadcast_array')

    if mpicomm.rank == root:
        # need C-contiguous order
        if not data.flags['C_CONTIGUOUS']:
            data = np.ascontiguousarray(data)
        shape_and_dtype = (data.shape, data.dtype)
    else:
        shape_and_dtype = None

    # each rank needs shape/dtype of input data
    shape, dtype = mpicomm.bcast(shape_and_dtype, root=root)

    # object dtype is not supported
    fail = False
    if dtype.char == 'V':
        fail = any(dtype[name] == 'O' for name in dtype.names)
    else:
        fail = dtype == 'O'
    if fail:
        raise ValueError('"object" data type not supported in broadcast_array; please specify specific data type')

    # initialize empty data on non-root ranks
    if mpicomm.rank != root:
        np_dtype = np.dtype((dtype, shape))
        data = np.empty(0, dtype=np_dtype)

    # setup the custom dtype
    duplicity = np.product(np.array(shape, 'intp'))
    itemsize = duplicity * dtype.itemsize
    dt = MPI.BYTE.Create_contiguous(itemsize)
    dt.Commit()

    # the return array
    recvbuffer = np.empty(shape, dtype=dtype, order='C')

    # the send offsets
    counts = np.ones(mpicomm.size, dtype='i', order='C')
    offsets = np.zeros_like(counts, order='C')

    # do the scatter
    mpicomm.Barrier()
    mpicomm.Scatterv([data, (counts, offsets), dt], [recvbuffer, dt], root=root)
    dt.Free()
    return recvbuffer



@CurrentMPIComm.enable
def scatter_array(data, counts=None, root=0, mpicomm=None):
    """
    Taken from https://github.com/bccp/nbodykit/blob/master/nbodykit/utils.py
    Scatter the input data array across all ranks, assuming `data` is
    initially only on `root` (and `None` on other ranks).
    This uses ``Scatterv``, which avoids mpi4py pickling, and also
    avoids the 2 GB mpi4py limit for bytes using a custom datatype

    Parameters
    ----------
    data : array_like or None
        on `root`, this gives the data to split and scatter
    mpicomm : MPI communicator
        the MPI communicator
    root : int
        the rank number that initially has the data
    counts : list of int
        list of the lengths of data to send to each rank

    Returns
    -------
    recvbuffer : array_like
        the chunk of `data` that each rank gets
    """
    if counts is not None:
        counts = np.asarray(counts, order='C')
        if len(counts) != mpicomm.size:
            raise ValueError('counts array has wrong length!')

    # check for bad input
    if mpicomm.rank == root:
        bad_input = not isinstance(data, np.ndarray)
    else:
        bad_input = None
    bad_input = mpicomm.bcast(bad_input, root=root)
    if bad_input:
        raise ValueError('`data` must by numpy array on root in scatter_array')

    if mpicomm.rank == root:
        # need C-contiguous order
        if not data.flags['C_CONTIGUOUS']:
            data = np.ascontiguousarray(data)
        shape_and_dtype = (data.shape, data.dtype)
    else:
        shape_and_dtype = None

    # each rank needs shape/dtype of input data
    shape, dtype = mpicomm.bcast(shape_and_dtype, root=root)

    # object dtype is not supported
    fail = False
    if dtype.char == 'V':
        fail = any(dtype[name] == 'O' for name in dtype.names)
    else:
        fail = dtype == 'O'
    if fail:
        raise ValueError('"object" data type not supported in scatter_array; please specify specific data type')

    # initialize empty data on non-root ranks
    if mpicomm.rank != root:
        np_dtype = np.dtype((dtype, shape[1:]))
        data = np.empty(0, dtype=np_dtype)

    # setup the custom dtype
    duplicity = np.product(np.array(shape[1:], 'intp'))
    itemsize = duplicity * dtype.itemsize
    dt = MPI.BYTE.Create_contiguous(itemsize)
    dt.Commit()

    # compute the new shape for each rank
    newshape = list(shape)

    if counts is None:
        newlength = shape[0] // mpicomm.size
        if mpicomm.rank < shape[0] % mpicomm.size:
            newlength += 1
        newshape[0] = newlength
    else:
        if counts.sum() != shape[0]:
            raise ValueError('the sum of the `counts` array needs to be equal to data length')
        newshape[0] = counts[mpicomm.rank]

    # the return array
    recvbuffer = np.empty(newshape, dtype=dtype, order='C')

    # the send counts, if not provided
    if counts is None:
        counts = mpicomm.allgather(newlength)
        counts = np.array(counts, order='C')

    # the send offsets
    offsets = np.zeros_like(counts, order='C')
    offsets[1:] = counts.cumsum()[:-1]

    # do the scatter
    mpicomm.Barrier()
    mpicomm.Scatterv([data, (counts, offsets), dt], [recvbuffer, dt], root=root)
    dt.Free()
    return recvbuffer
