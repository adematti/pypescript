"""Python framework to script pipelines putting together code pieces called *modules* in interaction through a data container called *data_block*."""

from .block import DataBlock, SectionBlock
from .config import ConfigBlock, ConfigError
from .module import BaseModule
from .pipeline import BasePipeline, MPIPipeline, BatchPipeline
from .utils import setup_logging
from . import section_names
#from . import syntax

from ._version import __version__
