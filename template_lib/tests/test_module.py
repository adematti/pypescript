import os
import sys

import numpy as np

from pypescript import ConfigBlock, DataBlock, BaseModule
from pypescript.utils import setup_logging, MemoryMonitor

module_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

def test_py(name='test'):

    def basic_run(name,options):
        module = BaseModule.from_filename(name=name,options=options)
        module.setup()
        module.execute()
        module.cleanup()

    options = {}
    options['module_file'] = os.path.join(module_dir,'template_lib','module_py','module.py')
    options['module_class'] = 'PyModule'
    basic_run(name,options)

    options = {}
    options['module_file'] = os.path.join(module_dir,'template_lib','module_py','module.py')
    basic_run(name,options)

    sys.path.insert(0,os.path.dirname(module_dir))
    options = {}
    options['module_name'] = 'template_lib.module_py.module'
    basic_run(name,options)

    options = {}
    options['module_name'] = 'template_lib.module_py.module'
    options['module_class'] = 'PyModule'
    basic_run(name,options)

    options = {}
    options['module_name'] = 'template_lib.module_py.module'
    options['module_class'] = 'AnyModule'
    basic_run(name,options)


def test_extensions(name='test'):

    def basic_run(module):
        module.setup()
        for name in ['int','long','float','double']:
            assert (module.data_block['parameters',name] == 42)
            assert np.all(module.data_block['parameters','{}_array'.format(name)] == 42)
        assert (module.data_block['parameters','string'] == 'string')
        module.data_block['external','int_array'] = np.ones(200,dtype='i4')[:36]
        module.data_block['external','float_array'] = np.ones(200,dtype='f4')[:36]
        #module.data_block['internal','long_array'] = np.ones(200,dtype='i4')[:36]
        module.execute()
        for name in ['int','long','float','double']:
            assert np.all(module.data_block['parameters','{}_array'.format(name)] == 44)
        for name in ['int','float']:
            assert np.all(module.data_block['external','{}_array'.format(name)] == 2)
        module.cleanup()

    for lang in ['c','cpp','f90']:
        options = {}
        options['module_name'] = 'template_lib.module_{}.module'.format(lang)
        module = BaseModule.from_filename(name=name,options=options)
        #basic_run_dynamic(library)
        with MemoryMonitor() as mem:
            for i in range(1000):
                basic_run(module)


if __name__ == '__main__':

    setup_logging()
    test_py()
    test_extensions()
