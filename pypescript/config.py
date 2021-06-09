"""Definition of :class:`ConfigBlock`."""

import logging

from .block import BlockMapping, DataBlock
from .lib import block
from . import syntax
from .syntax import Decoder
from .utils import savefile


class ConfigError(Exception):

    pass


class ConfigBlock(DataBlock):
    """
    This class handles the pipeline configurations.
    Extends :class:`DataBlock` with an initialisation from a file.
    """
    logger = logging.getLogger('ConfigBlock')

    def __init__(self, data=None, string=None, parser=None):
        """
        Initialise :class:`ConfigBlock`.

        Parameters
        ----------
        data : string, ConfigBlock, dict, default=None
            Path to configuration file.
            Else, :class:`ConfigBlock` instance to be (shallow) copied.
            Else, a dictionary as for initialisation of a :class:`DataBlock` instance.
            If ``None``, ignored.

        string : string, default=None
            String to be parsed and update ``self`` internal dictionary.

        parser : callable
            Parser which turns a string into a dictionary.
        """
        if isinstance(data,ConfigBlock):
            block.DataBlock.__init__(self,data=data)
            self.raw = data.raw
            return

        if isinstance(data,str) and data.endswith(syntax.block_save_extension):
            new = self.load(data)
            block.DataBlock.__init__(self,data=new)
            self.raw = new.data
            return

        decoder = Decoder(data=data,string=string,parser=parser)
        # filter those entries which match the (section,name) format
        data = {key:value for key,value in decoder.items() if isinstance(value,dict)}
        block.DataBlock.__init__(self,data=data,mapping=BlockMapping(decoder.mapping))
        self.raw = decoder.raw

    def __copy__(self):
        new = self.__class__.__new__(self.__class__)
        block.DataBlock.__init__(self,data=self.data.copy(),mapping=self.mapping)
        new.raw = self.raw
        return new

    copy = __copy__

    def __repr__(self):
        return 'ConfigBlock(data={}, mapping={})'.format(self.data,self.mapping)

    def __str__(self):
        return 'ConfigBlock(data={}, mapping={})'.format(self.data,self.mapping)

    def __getstate__(self):
        """Return this class state dictionary."""
        state = super(ConfigBlock,self).__getstate__()
        state['raw'] = self.raw
        return state

    def __setstate__(self, state):
        super(ConfigBlock,self).__setstate__(state)
        self.raw = state['raw']

    @savefile
    def save_yaml(self, filename):
        """Save class to disk."""
        import yaml
        if self.is_mpi_root():
            with open(filename,'w') as file:
                yaml.dump(self.raw,file,default_flow_style=None)
