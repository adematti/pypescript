"""A few utilities."""

import os
import sys
import re
import time
import functools
import logging
import traceback

import numpy as np

from . import mpi


def exception_handler(exc_type, exc_value, exc_traceback):
    """Print exception with a logger."""
    # Do not print traceback if the exception has been handled and logged
    _logger_name = 'Exception'
    log = logging.getLogger(_logger_name)
    line = '='*100
    #log.critical(line[len(_logger_name) + 5:] + '\n' + ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + line)
    log.critical('\n' + line + '\n' + ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + line)
    if exc_type is KeyboardInterrupt:
        log.critical('Interrupted by the user.')
    else:
        log.critical('An error occured.')


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
        level = {'info':logging.INFO,'debug':logging.DEBUG,'warning':logging.WARNING}[level.lower()]
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
        if filename is not None:
            dirname = os.path.dirname(filename)
            mkdir(dirname)
            self.log_info('Saving to {}.'.format(filename),rank=0)
        toret = func(self,filename,*args,**kwargs)
        self.mpicomm.Barrier()
        return toret
    return wrapper


def snake_to_pascal_case(snake):
    """Transform string in snake case (name1_name2) into Pascal case (Name1Name2)."""
    words = snake.split('_')
    return ''.join(map(str.title,words))


def is_of_type(value, types):
    """
    Check type of ``value``.

    Parameters
    ----------
    value : object
        Value to check type of.

    types : list, string, type or class
        Types to check the return value of :meth:`DataBlock.get` against.
        If list or tuple, check whether any of the proposed types matches.
        If a type is string, will search for the corresponding builtin type.

    Returns
    -------
    oftype : bool
        Whether ``value`` is of any of ``types``.
    """
    convert = {'string':'str'}

    def get_type_from_str(type_):
        return __builtins__.get(type_,None)

    def get_nptype_from_str(type_):
        return {'bool':np.bool_,'int':np.integer,'float':np.floating,'str':np.string_}.get(type_,None)

    if not isinstance(types,list):
        types = [types]

    toret = False
    for type_ in types:
        if isinstance(type_,str):
            type_ = convert.get(type_,type_)
            type_py = get_type_from_str(type_)
            if type_py is not None:
                if not isinstance(value,type_py):
                    continue
            else:
                match = re.match('(.*)_array',type_)
                if match is None:
                    continue
                type_ = convert.get(match.group(1),match.group(1))
                if isinstance(value,np.ndarray):
                    type_np = get_nptype_from_str(type_)
                    if type_np is None or not np.issubdtype(value.dtype,type_np):
                        continue
                else:
                    continue
        elif not isinstance(value,type_):
            continue
        toret = True
    return toret


class BaseMetaClass(type):

    """Meta class to add logging attributes to :class:`BaseClass` derived classes."""

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        cls.set_logger()
        return cls

    def set_logger(cls):
        """
        Add attributes for logging:

        - logger
        - methods log_debug, log_info, log_warning, log_error, log_critical
        """
        cls.logger = logging.getLogger(cls.__name__)

        def make_logger(level):

            @classmethod
            @mpi.CurrentMPIComm.enable
            def logger(cls, *args, rank=None, mpicomm=None, **kwargs):
                if rank is None or mpicomm.rank == rank:
                    getattr(cls.logger,level)(*args,**kwargs)

            return logger

        for level in ['debug','info','warning','error','critical']:
            setattr(cls,'log_{}'.format(level),make_logger(level))


class BaseTaskManager(metaclass=BaseMetaClass):
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


class _BaseClass(metaclass=BaseMetaClass):

    _copy_if_datablock_copy = False

    def __setstate__(self, state):
        """Set the class state dictionary."""
        self.__dict__.update(state)

    def __getstate__(self):
        """Return this class state dictionary."""
        state = {}
        if hasattr(self,'attrs'): state['attrs'] = self.attrs
        return state

    @property
    def mpiattrs(self):
        """MPI attributes"""
        return {'mpicomm':self.mpicomm,'mpistate':self.mpistate,'mpiroot':self.mpiroot}

    def is_mpi_root(self):
        return self.mpicomm.rank == self.mpiroot

    def is_mpi_scattered(self):
        return self.mpistate == mpi.CurrentMPIState.SCATTERED

    def is_mpi_gathered(self):
        return self.mpistate == mpi.CurrentMPIState.GATHERED

    def is_mpi_broadcast(self):
        return self.mpistate == mpi.CurrentMPIState.BROADCAST

    def __copy__(self):
        new = self.__class__.__new__(self.__class__)
        new.__dict__.update(self.__dict__)
        if hasattr(self,'attrs'): new.attrs = self.attrs.copy()
        return new

    def copy(self):
        """Return shallow copy of ``self``."""
        return self.__copy__()

    def __deepcopy__(self, memo):
        import copy
        new = self.copy()
        for key,value in self.__dict__.items():
            if key in ['mpicomm','_mpicomm']:
                new.__dict__[key] = value
            else:
                new.__dict__[key] = copy.deepcopy(value,memo)
        return new

    def deepcopy(self):
        import copy
        return copy.deepcopy(self)


class ScatteredBaseClass(_BaseClass):
    """
    Base template for **pypescript** MPI classes.
    It defines a couple of base methods and attributes.

    Attributes
    ----------
    mpistate : CurrentMPIState
        See :class:`CurrentMPIState`.

    mpicomm : MPI communicator
        Current MPI communicator.

    mpiroot : int
        MPI root rank.
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
        """Instantiate and initalize class with state dictionary."""
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        new.mpistate = mpistate
        new.mpiroot = mpiroot
        new.__setstate__(state)
        return new

    def mpi_to_state(self, mpistate):
        """Return instance, changing current MPI state to ``mpistate``."""
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
        cls.log_info('Loading {}.'.format(filename),rank=0)
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
        """Broadcast class from ``mpiroot`` to all ranks."""
        new = mpicomm.bcast(self,mpiroot=mpiroot)
        new.mpicomm = mpicomm
        new.mpistate = mpi.CurrentMPIState.BROADCAST
        new.mpiroot = mpiroot
        return new

    @classmethod
    @mpi.CurrentMPIComm.enable
    def mpi_collect(cls, self=None, sources=None, mpicomm=None):
        """
        Return new instance corresponding to ``self`` on larger ``mpicomm``.

        Parameters
        ----------
        self : object, None
            Instance to spread on ``mpicomm``.

        sources : list, None
            Ranks of processes of ``mpicomm`` where ``self`` lives.
            If ``None``, takes the ranks of processes where ``self`` is not ``None``.

        mpicomm : MPI communicator
            New mpi communicator.

        Returns
        -------
        new : object
        """
        new = cls.__new__(cls)
        new.mpicomm = mpicomm
        if sources is None:
            source_rank = self.mpicomm.rank if self is not None else -1
            sources = [rank for rank in new.mpicomm.allgather(source_rank) if rank >= 0]
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
        """
        Return new instance corresponding to ``self`` on smaller ``mpicomm``.

        Parameters
        ----------
        self : object, None
            Instance to concentrate on ``mpicomm``.

        dests : list, None
            Ranks of processes of :attr:`mpicomm` where to send ``self`` lives.
            If ``None``, takes the ranks of processes where ``self`` is not ``None``.

        mpicomm : MPI communicator
            New mpi communicator.

        Returns
        -------
        new : object, None
        """
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
        """Instantiate and initalize class with state dictionary."""
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
            '''do something'''
            mem()
            '''do something else'''
    """

    def __init__(self, pid=None, msg=''):
        """
        Initalize :class:`MemoryMonitor` and register current memory usage.

        Parameters
        ----------
        pid : int, default=None
            Process identifier. If ``None``, use the identifier of the current process.

        msg : string, default=''
            Additional message.
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
