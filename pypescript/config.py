"""Definition of :class:`ConfigBlock`."""

import logging

from .block import DataBlock, BlockMapping
from .lib import block
from . import syntax
from .syntax import Decoder


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
            Else, a dictionary as for initalise a :class:`DataBlock` instance.
            If ``None``, ignored.

        string : string, default=None
            String to be parsed and update ``self`` internal dictionary.

        parser : callable, default=parse_yaml
            Parser which turns a string into a dictionary.
        """
        if isinstance(data,block.DataBlock):
            block.DataBlock.__init__(self,data=data)
            return

        if isinstance(data,str) and data.endswith(syntax.file_extension):
            new = self.load(data)
            super(ConfigBlock,self).__init__(data=new.data,mapping=new.mapping)
            return

        decoder = Decoder(data=data,string=string,parser=parser)
        # filter those entries which match the (section,name) format
        data = {key:value for key,value in decoder.items() if isinstance(value,dict)}
        block.DataBlock.__init__(self,data=data,mapping=BlockMapping(decoder.mapping))
        self.raw = decoder.raw
