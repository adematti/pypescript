import logging

import numpy as np
from pypescript import BasePipeline

from template_lib import section_names


class BaseLikelihood(BasePipeline):

    logger = logging.getLogger('BaseLikelihood')

    def setup(self):
        super(BaseLikelihood,self).setup()
        self.set_data()

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


class GaussianLikelihood(BaseLikelihood):

    logger = logging.getLogger('GaussianLikelihood')

    def setup(self):
        super(GaussianLikelihood,self).setup()
        self.set_covariance()

    def set_covariance(self):
        self.precision = self.pipe_block[section_names.covariance,'invcov']

    def loglkl(self):
        diff = self.model - self.data
        return -0.5*diff.T.dot(self.precision).dot(diff)


class SumLikelihood(BaseLikelihood):

    logger = logging.getLogger('SumLikelihood')

    def setup(self):
        BasePipeline.setup(self)

    def execute(self):
        loglkl = 0
        self.pipe_block = self.data_block.copy()
        for todo in self.execute_todos:
            todo()
            loglkl += self.pipe_block[section_names.likelihood,'loglkl']
        self.data_block[section_names.likelihood,'loglkl'] = loglkl


class JointGaussianLikelihood(GaussianLikelihood):

    logger = logging.getLogger('JointGaussianLikelihood')

    def __init__(self, *args, join=None, **kwargs):
        super(JointGaussianLikelihood,self).__init__(*args, **kwargs)
        join = join or []
        join += self.options.get_list('join',[])
        self.join = []
        for module in join:
            self.join.append(self.add_module(module))

    def setup(self):
        join = {}
        self.pipe_block = self.data_block.copy()
        for module in self.join:
            module.set_data_block(self.pipe_block)
            module.setup()
            for key in self.pipe_block.keys(section=section_names.data):
                if key not in join: join[key] = []
                join[key].append(self.pipe_block[key])
        for key in join:
            self.data_block[key] = self.pipe_block[key] = np.concatenate(join[key])
        for todo in self.setup_todos:
            todo()
        self.set_data()
        self.set_covariance()

    def execute(self):
        join = {}
        self.pipe_block = self.data_block.copy()
        for module in self.join:
            module.set_data_block(self.pipe_block)
            module.execute()
            for key in self.pipe_block.keys(section=section_names.model):
                if key not in join: join[key] = []
                join[key].append(self.pipe_block[key])
        for key in join:
            self.data_block[key] = self.pipe_block[key] = np.concatenate(join[key])
        for todo in self.execute_todos:
            todo()
        self.set_model()
        self.data_block[section_names.likelihood,'loglkl'] = self.loglkl()

    def cleanup(self):
        self.pipe_block = self.data_block.copy()
        for module in self.join:
            module.set_data_block(self.pipe_block)
            module.cleanup()
        for todo in self.cleanup_todos:
            todo()
