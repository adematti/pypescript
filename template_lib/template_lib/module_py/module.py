from pypescript import BaseModule

"""
To implement a Python module, either:

- subclass BaseModule, implementing methods :meth:`~PyModule.setup`, :meth:`~PyModule.execute`, :meth:`~PyModule.cleanup`
which can access and modify attributes :attr:`~PyModule.name`, :attr:`~PyModule.config_block`, :attr:`~PyModule.data_block`.
- implement the three functions :func:`setup`, :func:`execute`, :func:`cleanup` with arguments `name`, `config_block`, `data_block`.
"""


class PyModule(BaseModule):

    def setup(self):
        """Set up module (called at the beginning)."""
        return 0

    def execute(self):
        """Execute module, i.e. do calculation (called at each iteration)."""
        return 0

    def cleanup(self):
        """Clean up, i.e. free variables if needed (called at the end)."""
        return 0



def setup(name, config_block, data_block):
    return 0


def execute(name, config_block, data_block):
    return 0


def cleanup(name, config_block, data_block):
    return 0
