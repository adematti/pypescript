"""
Python framework to script pipelines putting together code pieces called *modules* in interaction through
a data container called *data_block*.
"""

from .block import DataBlock, SectionBlock
from .config import ConfigBlock
from .module import BaseModule, BasePipeline
from .parameter import Parameter, BasePrior, ParamBlock
from .utils import setup_logging
from . import section_names

from ._version import __version__
