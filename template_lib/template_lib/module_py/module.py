from pypescript import BaseModule

"""
To implement a Python module, either:

- create a class (possibly subclassing BaseModule), implementing methods :meth:`~PyModule.setup`, :meth:`~PyModule.execute`, :meth:`~PyModule.cleanup`
which can access and modify attributes :attr:`~PyModule.name`, :attr:`~PyModule.config_block`, :attr:`~PyModule.data_block`.
- implement the three functions :func:`setup`, :func:`execute`, :func:`cleanup` with arguments `name`, `config_block`, `data_block`.
"""


class PyModule(BaseModule):

    def setup(self):
        """Set up module (called at the beginning)."""
        self.name
        self.config_block
        self.data_block
        return 0

    def execute(self):
        """Execute module, i.e. do calculation (called at each iteration)."""
        self.name
        self.config_block
        self.data_block
        return 0

    def cleanup(self):
        """Clean up, i.e. free variables if needed (called at the end)."""
        self.name
        self.config_block
        self.data_block
        return 0


class AnyModule(object):

    def setup(self):
        """Set up module (called at the beginning)."""
        self.name
        self.config_block
        self.data_block
        return 0

    def execute(self):
        """Execute module, i.e. do calculation (called at each iteration)."""
        self.name
        self.config_block
        self.data_block
        return 0

    def cleanup(self):
        """Clean up, i.e. free variables if needed (called at the end)."""
        self.name
        self.config_block
        self.data_block
        return 0


def setup(name, config_block, data_block):
    return 0


def execute(name, config_block, data_block):
    return 0


def cleanup(name, config_block, data_block):
    return 0
