import numpy as np

from template_lib import section_names


def setup(name, config_block, data_block):
    data_block[section_names.covariance,'invcov'] = 1./np.array(config_block[name,'yerr'])**2
    return 0

def execute(name, config_block, data_block):
    return 0

def cleanup(name, config_block, data_block):
    return 0
