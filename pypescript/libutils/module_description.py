import os
import logging
from collections import UserDict

import yaml

from .syntax_description import Decoder, YamlLoader


class ModuleDescription(UserDict):

    """This class handles module descriptions."""

    _file_extension = '.yaml'

    logger = logging.getLogger('ModuleDescription')

    def __init__(self, data=None, string=None, parser=None, **kwargs):
        """
        Initialise :class:`ModuleDescription`.

        Parameters
        ----------
        data : string, ModuleDescription, dict, default=None
            Path to description file.
            Else, :class:`ModuleDescription` instance to be (shallow) copied.
            Else, a dictionary.
            If ``None``, ignored.

        string : string, default=None
            String to be parsed and update ``self`` internal dictionary.

        parser : callable
            Parser which turns a string into a dictionary.
        """
        if isinstance(data,ModuleDescription):
            self.__dict__.update(data)
            return

        decoder = Decoder(data=data,string=string,parser=parser,**kwargs)
        # filter those entries which match the (section,name) format
        self.data = decoder.data
        self.raw = decoder.raw

    @classmethod
    def load(cls, filename, **kwargs):
        with open(filename,'r') as file:
            descriptions = list(yaml.load_all(file,Loader=YamlLoader))
        if len(descriptions) > 1:
            return [ModuleDescription(d,filename=filename,**kwargs) for d in descriptions]
        return ModuleDescription(descriptions[0],filename=filename,**kwargs)

    @classmethod
    def isinstance(cls, filename):
        """Check wether the file ``description_file`` containing ``description`` is a **pypescript** module description file."""
        if not filename.endswith(cls._file_extension):
            return False
        with open(filename,'r') as file:
            data = list(yaml.load_all(file,Loader=YamlLoader))
            if not all(isinstance(d,dict) and 'name' in d for d in data):
                return False
        basename = os.path.basename(filename[:-len(cls._file_extension)])
        for fn in os.listdir(os.path.dirname(filename)):
            if fn.startswith(basename) and not fn.endswith(cls._file_extension):
                return True
        return False

    @classmethod
    def filename_from_module(cls, module):
        return os.path.join(os.path.dirname(module.__file__), module.__name__.split('.')[-1] + cls._file_extension)

    @classmethod
    def from_module(cls, module):
        filename = cls.filename_from_module(module)
        if os.path.isfile(filename):
            return cls.load(filename)
