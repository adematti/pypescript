"""Definition of :class:`ConfigBlock`."""

import logging
import yaml
import re

from .block import DataBlock
from .lib import block

def parse_yaml(string):
    """Parse string in the *yaml* format."""
    # https://stackoverflow.com/questions/30458977/yaml-loads-5e-6-as-string-and-not-a-number
    loader = yaml.SafeLoader
    loader.add_implicit_resolver(u'tag:yaml.org,2002:float',
                                re.compile(u'''^(?:
                                 [-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+]?[0-9]+)?
                                |[-+]?(?:[0-9][0-9_]*)(?:[eE][-+]?[0-9]+)
                                |\\.[0-9_]+(?:[eE][-+][0-9]+)?
                                |[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*
                                |[-+]?\\.(?:inf|Inf|INF)
                                |\\.(?:nan|NaN|NAN))$''', re.X),
                                list(u'-+0123456789.'))
    config = yaml.load(string,Loader=loader)
    data = dict(config)
    return data


class ConfigError(Exception):

    pass


def search_in_dict(data, key, *keys):
    if len(keys) == 0:
        return data[key]
    return search_in_dict(data[key], *keys)


def load_recursive(filename=None, string=None, parser=parse_yaml):
    """Load config file recursively, replacing 'configblock_load' entries by the dictionaries of the corresponding config files."""
    data = {}

    if isinstance(filename,str):
        with open(filename,'r') as file:
            if string is None: string = ''
            string += file.read()
    elif filename is not None:
        data = dict(filename)

    if string is not None and parser is not None:
        data.update(parser(string))

    def callback(di):
        for key,value in list(di.items()):
            if key == 'configblock_copy':
                if not isinstance(value,(tuple,list)):
                    value = [value]
                del di[key]
                tmp = search_in_dict(data,value)
                for key,value in tmp.items():
                    di.setdefault(key,value)
            elif isinstance(value,dict):
                callback(value)

    callback(data)

    def callback(di):
        for key,value in list(di.items()):
            if key == 'configblock_load':
                if not isinstance(value,(tuple,list)):
                    value = [value]
                del di[key]
                for fn in value:
                    tmp = load_recursive(filename=fn,parser=parser)
                    for key,value in tmp.items():
                        di.setdefault(key,value)
            elif isinstance(value,dict):
                callback(value)

    callback(data)

    return data


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
        if isinstance(filename,self.__class__):
            block.DataBlock.__init__(self,data=filename)
            return

        data = load_recursive(filename=filename,string=string,parser=parser)
        block.DataBlock.__init__(self,data=data)
