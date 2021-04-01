import numpy as np

from template_lib import section_names


def setup(name, config_block, data_block):
    data_block[section_names.data,'y'] = np.array(config_block[name,'y'])
    data_block[section_names.data,'x'] = np.array(config_block.get(name,'x',np.arange(data_block[section_names.data,'y'].size)))
    return 0

def execute(name, config_block, data_block):
    return 0

def cleanup(name, config_block, data_block):
    return 0
