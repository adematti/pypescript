"""A few utilities."""

import os
import sys
import time
import functools
import logging

import numpy as np


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
    fmt = logging.Formatter(fmt='%(asctime)s %(name)-25s %(levelname)-8s %(message)s',datefmt='%m-%d %H:%M ')
    if filename is not None:
        mkdir(os.path.dirname(filename))
        handler = logging.FileHandler(filename,mode=filemode)
    else:
        handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(fmt)
    logging.basicConfig(level=level,handlers=[handler],**kwargs)


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
        self.logger.info('Saving to {}.'.format(filename))
        return func(self,filename,*args,**kwargs)
    return wrapper


def snake_to_pascal_case(snake):
    """Transform string in snake case (name1_name2) into Pascal case (Name1Name2)."""
    words = snake.split('_')
    return ''.join(map(str.title,words))


class BaseClass(object):
    """
    Base template for **pypescript** classes.
    It defines a couple of base methods below.
    """
    def __setstate__(self,state):
        """Set the class state dictionary."""
        self.__dict__.update(state)

    def __getstate__(self):
        """Return this class state dictionary."""
        state = {}
        if hasattr(self,'attrs'): state['attrs'] = self.attrs
        return state

    @classmethod
    def from_state(cls, state):
        """Instantiate and initalise class with state dictionary."""
        new = cls.__new__(cls)
        new.__setstate__(state)
        return new

    @classmethod
    def load(cls, filename):
        """Load class from disk."""
        cls.logger.info('Loading {}.'.format(filename))
        state = np.load(filename,allow_pickle=True)[()]
        return cls.from_state(state)

    @savefile
    def save(self, filename):
        """Save class to disk."""
        np.save(filename,self.__getstate__())

    def copy(self):
        """Return shallow copy of ``self``."""
        new = self.__class__.__new__(self.__class__)
        new.__dict__.update(self.__dict__)
        return new


class MemoryMonitor(object):
    """
    Class that monitors memory usage, useful to check for memory leaks.

    >>> with MemoryMonitor() as mem:
        '''do womething'''
        mem()
        '''do something else'''
    """

    def __init__(self, pid=None):
        """
        Initalise :class:`MemoryMonitor` and register current memory usage.

        Parameters
        ----------
        pid : int, default=None
            Process identifier. If ``None``, the identifier of the current process is considered.
        """
        import psutil
        self.proc = psutil.Process(os.getpid() if pid is None else pid)
        self.mem = self.proc.memory_info().rss / 1e6
        self.time = time.time()
        print('Using {:10.3f} [Mb]'.format(self.mem))

    def __enter__(self):
        """Enter context."""
        pass

    def __call__(self):
        """Update memory usage."""
        mem = self.proc.memory_info().rss / 1e6
        t = time.time()
        print('Using {:10.3f} [Mb] {:+10.3f} [Mb] {:+10.3f} [s]'.format(mem,mem-self.mem,t-self.time))
        self.mem = mem
        self.time = t

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit context."""
        self()
