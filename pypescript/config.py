"""Definition of :class:`ConfigBlock`."""

import logging
import yaml

from .block import DataBlock


def parse_yaml(string):
    """Parse string in the *yaml* format."""
    config = yaml.safe_load(string)
    data = dict(config)
    return data


class ConfigError(Exception):

    pass


class ConfigBlock(DataBlock):
    """
    This class handles the pipeline configurations.
    Extends :class:`DataBlock` with an initialisation from a file.
    """
    logger = logging.getLogger('ConfigBlock')

    def __init__(self, filename=None, string=None, parser=parse_yaml):
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
        data = {}
        if isinstance(filename,self.__class__):
            self.update(filename)
            return

        if isinstance(filename,str):
            self.filename = filename
            with open(filename,'r') as file:
                if string is None: string = ''
                string += file.read()
        elif filename is not None:
            data = dict(filename)

        if string is not None and parser is not None:
            data.update(parser(string))

        super(ConfigBlock,self).__init__(data=data,add_sections=[])

    def __getstate__(self):
        """Return this class state dictionary."""
        state = super(ConfigBlock,self).__getstate__()
        state['filename'] = self.filename
        return state
