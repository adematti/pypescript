import logging

import numpy as np

from pypescript import BasePipeline
from template_lib import section_names


class BaseLikelihood(BasePipeline):

    logger = logging.getLogger('BaseLikelihood')

    def setup(self):
        super(BaseLikelihood,self).setup()
        self.set_data()
        return 0

    def set_data(self):
        self.data = self.pipe_block[section_names.data,'y']
        self.data_block[section_names.data] = self.pipe_block[section_names.data]

    def set_model(self):
        self.model = self.data_block[section_names.model,'y'] = self.pipe_block[section_names.model,'y']

    def loglkl(self):
        return 0

    def execute(self):
        super(BaseLikelihood,self).execute()
        self.set_model()
        self.data_block[section_names.likelihood,'loglkl'] = self.loglkl()
        return 0


class GaussianLikelihood(BaseLikelihood):

    logger = logging.getLogger('GaussianLikelihood')

    def setup(self):
        super(GaussianLikelihood,self).setup()
        self.set_covariance()
        return 0

    def set_covariance(self):
        self.precision = np.diag(self.pipe_block[section_names.covariance,'invcov'])

    def loglkl(self):
        diff = self.model - self.data
        return -0.5*diff.T.dot(self.precision).dot(diff)


class SumLikelihood(BaseLikelihood):

    logger = logging.getLogger('SumLikelihood')

    def setup(self):
        BasePipeline.setup(self)
        return 0

    def execute(self):
        loglkl = 0
        for module in self:
            module.execute()
            loglkl += self.pipe_block[section_names.likelihood,'loglkl']
        self.data_block[section_names.likelihood,'loglkl'] = loglkl
        return 0


class JointGaussianLikelihood(GaussianLikelihood):

    logger = logging.getLogger('JointGaussianLikelihood')

    def __init__(self, *args, join=None, modules=None, **kwargs):
        join = join or []
        modules = modules or []
        super(JointGaussianLikelihood,self).__init__(*args,modules=join + modules,**kwargs)
        self.join = join + self._get_modules_from_filename(self.options.get_list('join',default=[]))
        self.after = [module for module in self.modules if module not in self.join]
        self.modules = self.join + self.after

    def setup(self):
        join = {}
        for module in self.join:
            module.setup()
            for key in self.pipe_block.keys(section=section_names.data):
                if key not in join: join[key] = []
                join[key].append(self.pipe_block[key])
        for key in join:
            self.data_block[key] = self.pipe_block[key] = np.concatenate(join[key])
        for module in self.after:
            module.setup()
        self.set_data()
        self.set_covariance()
        return 0

    def execute(self):
        join = {}
        for module in self.join:
            module.execute()
            for key in self.pipe_block.keys(section=section_names.model):
                if key not in join: join[key] = []
                join[key].append(self.pipe_block[key])
        for key in join:
            self.data_block[key] = self.pipe_block[key] = np.concatenate(join[key])
        for module in self.after:
            module.execute()
        self.set_model()
        self.data_block[section_names.likelihood,'loglkl'] = self.loglkl()
        return 0
