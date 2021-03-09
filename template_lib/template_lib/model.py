import logging

import numpy as np
from pypescript import BaseModule

from template_lib import section_names


class FlatModel(BaseModule):

    logger = logging.getLogger('FlatModel')

    def setup(self):
        self.size = self.data_block[section_names.data,'y'].size
        return 0

    def execute(self):
        a = self.data_block[section_names.parameters,'a']
        self.data_block[section_names.model,'y'] = np.full(self.size,a,dtype='f8')
        return 0

    def cleanup(self):
        return 0


class AffineModel(BaseModule):

    logger = logging.getLogger('AffineModel')

    def setup(self):
        self.size = self.data_block.get(section_names.data,'y').size
        return 0

    def execute(self):
        a = self.data_block.get_float(section_names.parameters,'a')
        b = self.data_block.get_float(section_names.parameters,'b')
        self.data_block[section_names.model,'y'] = a + b*self.data_block[section_names.data,'x']
        return 0

    def cleanup(self):
        return 0
