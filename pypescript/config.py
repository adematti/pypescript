"""Definition of :class:`ConfigBlock`."""

import logging

from .block import DataBlock
from .lib import block
from .syntax import Decoder


class ConfigError(Exception):

    pass


class ConfigBlock(DataBlock):
    """
    This class handles the pipeline configurations.
    Extends :class:`DataBlock` with an initialisation from a file.
    """
    logger = logging.getLogger('ConfigBlock')

    def __init__(self, filename=None, string=None, parser=None):
        """
        Initialise :class:`ConfigBlock`.

        Parameters
        ----------
        filename : string, ConfigBlock, dict, default=None
            Path to configuration file.
            Else, :class:`ConfigBlock` instance to be (shallow) copied.
            Else, a dictionary as for initalise a :class:`DataBlock` instance.
            If ``None``, ignored.

        string : string, default=None
            String to be parsed and update ``self`` internal dictionary.

        parser : callable, default=parse_yaml
            Parser which turns a string into a dictionary.
        """
        if isinstance(filename,self.__class__):
            block.DataBlock.__init__(self,data=filename)
            return

        decoder = Decoder(filename=filename,string=string,parser=parser)
        # filter those entries which match the (section,name) format
        data = {key:value for key,value in decoder.items() if isinstance(value,dict)}
        block.DataBlock.__init__(self,data=data,mapping=decoder.mapping)
