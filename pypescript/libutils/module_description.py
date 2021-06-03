import os
from collections import UserDict

import yaml


class ModuleDescription(UserDict):

    _file_extension = '.yaml'

    @classmethod
    def load(cls, filename):
        with open(filename,'r') as file:
            data = yaml.load(file,Loader=yaml.SafeLoader)
        if isinstance(data,list):
            return [cls(d) for d in data]
        return cls(data)

    def save(self, filename):
        with open(filename,'w') as file:
            yaml.dump(self.data,file,default_flow_style=None)

    @classmethod
    def isinstance(cls, filename):
        """Check wether the file ``description_file`` containing ``description`` is a **pypescript** module description file."""
        if not filename.endswith(cls._file_extension):
            return False
        with open(filename,'r') as file:
            data = yaml.load(file,Loader=yaml.SafeLoader)
            if not isinstance(data,list):
                data = [data]
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
