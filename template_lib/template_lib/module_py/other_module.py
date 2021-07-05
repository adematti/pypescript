"""
If a file called other_module.py implements no function setup, execute, cleanup, but a class OtherModule (Pascal case version of the file name),
it will be loaded even if ``module_class`` is not specified.
"""

class OtherModule(object):

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
