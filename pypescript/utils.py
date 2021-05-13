"""A few utilities."""

import os
import sys
import time
import functools
import logging
import traceback

import numpy as np

from . import mpi


def exception_handler(exc_type, exc_value, exc_traceback):
    # Do not print traceback if the exception has been handled and logged
    _logger_name = 'Exception'
    log = logging.getLogger(_logger_name)
    line = '='*100
    #log.critical(line[len(_logger_name) + 5:] + '\n' + ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + line)
    log.critical('\n' + line + '\n' + ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + line)
    if exc_type == KeyboardInterrupt:
        log.critical('Interrupted by the user.')
    else:
        log.critical('An error occured.')
    # Exit all MPI processes
    os._exit(1)


def setup_logging(level=logging.INFO, stream=sys.stdout, filename=None, filemode='w', **kwargs):
    """
    Set up logging.

    Parameters
    ----------
    level : string, int, default=logging.INFO
        Logging level.

    stream : _io.TextIOWrapper, default=sys.stdout
        Where to stream.

    filename : string, default=None
        If not ``None`` stream to file name.

    filemode : string, default='w'
        Mode to open file, only used if filename is not ``None``.

    kwargs : dict
        Other arguments for :func:`logging.basicConfig`.
    """
    # Cannot provide stream and filename kwargs at the same time to logging.basicConfig, so handle different cases
    # Thanks to https://stackoverflow.com/questions/30861524/logging-basicconfig-not-creating-log-file-when-i-run-in-pycharm
    if isinstance(level,str):
        level = {'info':logging.INFO,'debug':logging.DEBUG,'warning':logging.WARNING}[level]
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)

    t0 = time.time()

    class MyFormatter(logging.Formatter):

        @mpi.CurrentMPIComm.enable
        def format(self, record, mpicomm=None):
            ranksize = '[{:{dig}d}/{:d}]'.format(mpicomm.rank,mpicomm.size,dig=len(str(mpicomm.size)))
            self._style._fmt = '[%09.2f] ' % (time.time() - t0) + ranksize + ' %(asctime)s %(name)-25s %(levelname)-8s %(message)s'
            return super(MyFormatter,self).format(record)

    fmt = MyFormatter(datefmt='%m-%d %H:%M ')
    if filename is not None:
        mkdir(os.path.dirname(filename))
        handler = logging.FileHandler(filename,mode=filemode)
    else:
        handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(fmt)
    logging.basicConfig(level=level,handlers=[handler],**kwargs)
    sys.excepthook = exception_handler


def mkdir(dirname):
    """Try to create ``dirnm`` and catch :class:`OSError`."""
    try:
        os.makedirs(dirname) # MPI...
    except OSError:
        return


def savefile(func):
    """
    Wrapper for a class method that saves a file on disk.
    It creates the file directory (if does not exist).
    """
    @functools.wraps(func)
    def wrapper(self, filename, *args, **kwargs):
        dirname = os.path.dirname(filename)
        mkdir(dirname)
        self.log_info('Saving to {}.'.format(filename),rank=0)
        return func(self,filename,*args,**kwargs)
    return wrapper


def snake_to_pascal_case(snake):
    """Transform string in snake case (name1_name2) into Pascal case (Name1Name2)."""
    words = snake.split('_')
    return ''.join(map(str.title,words))


def addclslogger(cls):

    cls.logger = logging.getLogger(cls.__name__)

    def _make_logger(level):

        @classmethod
        @mpi.CurrentMPIComm.enable
        def logger(cls, *args, rank=None, extra=None, mpicomm=None, **kwargs):
            if rank is None or mpicomm.rank == rank:
                getattr(cls.logger,level)(*args,**kwargs)

        return logger

    for level in ['debug','info','warning','error','critical','log']:
        setattr(cls,'log_{}'.format(level),_make_logger(level))

    return cls


@addclslogger
class BaseTaskManager(object):
    """A dumb task manager, that simply iterates through the tasks in series."""

    @mpi.CurrentMPIComm.enable
    def __init__(self, mpicomm=None):
        self.mpicomm = mpicomm
        self.basecomm = self.mpicomm

    def __enter__(self):
        """Return self."""
        self.log_info('Entering {}.'.format(self.__class__.__name__),rank=0)
        self.self_worker_ranks = [0]
        self.other_ranks = []
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Do nothing."""
        if exc_value is not None:
            exception_handler(exc_type, exc_value, exc_traceback)

    def iterate(self, tasks):
        """
        Iterate through a series of tasks.

        Parameters
        ----------
        tasks : iterable
            An iterable of tasks that will be yielded.

        Yields
        -------
        task :
            The individual items of ```tasks``, iterated through in series.
        """
        for task in tasks:
            yield task

    def map(self, function, tasks):
        """
        Apply a function to all of the values in a list and return the list of results.

        If ``tasks`` contains tuples, the arguments are passed to
        ``function`` using the ``*args`` syntax.

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
        return [function(*(t if isinstance(t,tuple) else (t,))) for t in tasks]


@mpi.CurrentMPIComm.enable
def TaskManager(mpicomm=None, nprocs_per_task=1, **kwargs):
    """
    Switch between non-MPI (ntasks=1) and MPI task managers. To be called as::

        with TaskManager(...) as tm:
            # do stuff

    """
    msg = 'Not enough MPI processes = {:d} for nprocs_per_task = {:d}.'.format(mpicomm.size,nprocs_per_task)
    if mpicomm.size == 1:
        if nprocs_per_task > 1:
            raise ValueError(msg)
        self = BaseTaskManager.__new__(BaseTaskManager)
        self.__init__(mpicomm=mpicomm)
    else:
        if nprocs_per_task > mpicomm.size - 1:
            raise ValueError(msg)
        self = mpi.MPITaskManager.__new__(mpi.MPITaskManager)
        self.__init__(mpicomm=mpicomm,nprocs_per_task=nprocs_per_task,**kwargs)
    return self


class _BaseClass(object):

    def __setstate__(self, state):
        """Set the class state dictionary."""
        self.__dict__.update(state)

    def __getstate__(self):
        """Return this class state dictionary."""
        state = {}
        if hasattr(self,'attrs'): state['attrs'] = self.attrs
        return state

    def is_mpi_root(self):
        return self.mpicomm.rank == self.mpiroot

    def is_mpi_scattered(self):
        return self.mpistate == mpi.CurrentMPIState.SCATTERED

    def is_mpi_gathered(self):
        return self.mpistate == mpi.CurrentMPIState.GATHERED

    def is_mpi_broadcast(self):
        return self.mpistate == mpi.CurrentMPIState.BROADCAST

    def copy(self):
        """Return shallow copy of ``self``."""
        new = self.__class__.__new__(self.__class__)
        new.__dict__.update(self.__dict__)
        return new

    def deepcopy(self):
        import copy
        return copy.deepcopy(self)

@addclslogger
class ScatteredBaseClass(_BaseClass):
    """
    Base template for **pypescript** MPI classes.
    It defines a couple of base methods.
    """

    @mpi.MPIInit
    def __init__(self, **attrs):
        self.attrs = attrs

    @property
    def mpistate(self):
        return self._mpistate

    @mpistate.setter
    def mpistate(self, mpistate):
        self._mpistate = mpi.CurrentMPIState(mpistate)

    @classmethod
    @mpi.CurrentMPIComm.enable
    def from_state(cls, state, mpistate=mpi.CurrentMPIState.GATHERED, mpiroot=0, mpicomm=None):
        """Instantiate and initalise class with state dictionary."""
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        new.mpistate = mpistate
        new.mpiroot = mpiroot
        new.__setstate__(state)
        return new

    def mpi_to_state(self, mpistate):
        mpistate = mpi.CurrentMPIState(mpistate)
        if mpistate == mpi.CurrentMPIState.GATHERED:
            if self.is_mpi_scattered():
                self.mpi_gather()
        if mpistate == mpi.CurrentMPIState.SCATTERED:
            if self.is_mpi_gathered() or self.is_mpi_broadcast():
                self.mpi_scatter()
        if mpistate == mpi.CurrentMPIState.BROADCAST:
            if self.is_mpi_scattered():
                self.mpi_gather()
            if self.is_mpi_gathered():
                self = self.mpi_broadcast(self)
        return self

    @classmethod
    @mpi.CurrentMPIComm.enable
    def load(cls, filename, mpiroot=0, mpistate=mpi.CurrentMPIState.GATHERED, mpicomm=None):
        """Load class from disk."""
        cls.log_info('Loading {}.'.format(filename))
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        new.mpiroot = mpiroot
        if new.is_mpi_root():
            state = np.load(filename,allow_pickle=True)[()]
            new = cls.from_state(state,mpiroot=mpiroot,mpicomm=mpicomm)
        new.mpistate = mpi.CurrentMPIState.GATHERED
        new = new.mpi_to_state(mpistate)
        return new

    @mpi.MPIBroadcast
    def mpi_broadcast(new, self=None, mpiroot=0, mpicomm=None):
        new = mpicomm.bcast(self,mpiroot=mpiroot)
        new.mpicomm = mpicomm
        new.mpistate = mpi.CurrentMPIState.BROADCAST
        new.mpiroot = mpiroot
        return new

    @classmethod
    @mpi.CurrentMPIComm.enable
    def mpi_collect(cls, self=None, sources=None, mpicomm=None):
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        if sources is None:
            issource = self.mpicomm.rank if self is not None else -1
            sources = [rank for rank in new.mpicomm.allgather(issource) if rank >= 0]
        new.mpistate = new.mpicomm.bcast(self.mpistate if new.mpicomm.rank == sources[0] else None,root=sources[0])
        mpiroot = -1
        if (new.mpicomm.rank in sources) and self.is_mpi_root():
            mpiroot = new.mpicomm.rank
        new.mpiroot = [r for r in new.mpicomm.allgather(mpiroot) if r >= 0][0]
        if new.is_mpi_broadcast():
            return cls.mpi_broadcast(self,mpiroot=new.mpiroot,mpicomm=new.mpicomm)
        if new.is_mpi_scattered():
            if new.mpicomm.rank in sources:
                self.mpi_gather()
                self.mpicomm = new.mpicomm
                self.mpiroot = new.mpiroot
                new = self
            new.mpistate = mpi.CurrentMPIState.GATHERED
            new.mpi_scatter()
        return new

    @mpi.CurrentMPIComm.enable
    def mpi_distribute(self, dests, mpicomm=None):
        new = self.copy()
        new.mpicomm = mpicomm
        mpiroot = -1
        if self.mpicomm.rank == dests[0]:
            mpiroot = new.mpicomm.rank
        new.mpiroot = [r for r in self.mpicomm.allgather(mpiroot) if r >= 0][0]
        if self.is_mpi_broadcast():
            if self.mpicomm.rank in dests:
                return new
            return None
        isscattered = self.is_mpi_scattered()
        if isscattered:
            self.mpi_gather()
        if self.is_mpi_gathered():
            if dests[0] != self.mpiroot:
                if self.is_mpi_root():
                    self.mpi_send(dests[0],tag=42)
                if self.mpicomm.rank == dests[0]:
                    new.mpicomm = self.mpicomm
                    new.mpi_recv(self.mpiroot,tag=42)
                    new.mpicomm = mpicomm
        if isscattered:
            new.mpistate = 'gathered'
            if self.mpicomm.rank in dests:
                new.mpi_scatter()
            self.mpi_scatter()
        if self.mpicomm.rank in dests:
            return new
        return None

    @mpi.MPIGather
    def mpi_gather(self):
        raise NotImplementedError

    @mpi.MPIScatter
    def mpi_scatter(self):
        raise NotImplementedError

    def mpi_send(self, dest, tag=42):
        raise NotImplementedError

    def mpi_recv(self, source, tag=42):
        raise NotImplementedError

    @savefile
    def save(self, filename):
        """Save class to disk."""
        isscattered = self.is_mpi_scattered()
        if isscattered: self.mpi_gather()
        if self.is_mpi_root():
            np.save(filename,self.__getstate__())
        if isscattered: self.mpi_scatter()


@addclslogger
class BaseClass(_BaseClass):
    """
    Base template for **pypescript** MPI classes.
    It defines a couple of base methods.
    """

    @property
    def mpistate(self):
        return mpi.CurrentMPIState.BROADCAST

    @mpistate.setter
    def mpistate(self, mpistate):
        self._mpistate = mpi.CurrentMPIState(mpistate)
        if self._mpistate != mpi.CurrentMPIState.BROADCAST:
            raise ValueError('This class does not support MPI state {}.'.format(mpi.CurrentMPIState.as_str(self._mpistate)))

    @property
    def mpiroot(self):
        return getattr(self,'_mpiroot',0)

    @mpiroot.setter
    def mpiroot(self, mpiroot):
        self._mpiroot = mpiroot

    @property
    def mpicomm(self):
        return getattr(self,'_mpicomm',mpi.CurrentMPIComm.get())

    @mpicomm.setter
    def mpicomm(self, mpicomm):
        self._mpicomm = mpicomm

    @classmethod
    @mpi.CurrentMPIComm.enable
    def from_state(cls, state, mpiroot=0, mpicomm=None):
        """Instantiate and initalise class with state dictionary."""
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        new.mpiroot = mpiroot
        new.__setstate__(state)
        return new

    @classmethod
    @mpi.CurrentMPIComm.enable
    def load(cls, filename, mpiroot=0, mpicomm=None):
        """Load class from disk."""
        cls.log_info('Loading {}.'.format(filename))
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        new.mpiroot = mpiroot
        if new.is_mpi_root():
            state = np.load(filename,allow_pickle=True)[()]
        state = new.mpicomm.bcast(state if new.is_mpi_root() else None,root=new.mpiroot)
        new = cls.from_state(state,mpiroot=mpiroot,mpicomm=mpicomm)
        return new

    @savefile
    def save(self, filename):
        """Save class to disk."""
        if self.is_mpi_root():
            np.save(filename,self.__getstate__())


class MemoryMonitor(object):
    """
    Class that monitors memory usage and clock, useful to check for memory leaks.

    >>> with MemoryMonitor() as mem:
            '''do womething'''
            mem()
            '''do something else'''
    """

    def __init__(self, pid=None, msg=''):
        """
        Initalise :class:`MemoryMonitor` and register current memory usage.

        Parameters
        ----------
        pid : int, default=None
            Process identifier. If ``None``, use the identifier of the current process.
        """
        import psutil
        self.proc = psutil.Process(os.getpid() if pid is None else pid)
        self.mem = self.proc.memory_info().rss / 1e6
        self.time = time.time()
        self.msg = msg
        msg = 'using {:.3f} [Mb]'.format(self.mem)
        if self.msg:
            msg = '[{}] {}'.format(self.msg,msg)
        print(msg)

    def __enter__(self):
        """Enter context."""
        pass

    def __call__(self):
        """Update memory usage."""
        mem = self.proc.memory_info().rss / 1e6
        t = time.time()
        msg = 'using {:.3f} [Mb] (increase of {:.3f} [Mb]) after {:.3f} [s]'.format(mem,mem-self.mem,t-self.time)
        if self.msg:
            msg = '[{}] {}'.format(self.msg,msg)
        print(msg)
        self.mem = mem
        self.time = t

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit context."""
        self()
