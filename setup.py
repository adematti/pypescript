import os
import sys

base_dir = 'pypescript'
src_dir = os.path.join(base_dir,'block')

sys.path.insert(0,base_dir)
from libutils import setup, Extension

extension = Extension('pypescript.lib.block', sources=[os.path.join(src_dir,'blockmodule.c'),os.path.join(src_dir,'blockmapping.c')])

setup(name='pypescript',
    base_dir=base_dir,
    author='Arnaud de Mattia et al.',
    maintainer='Arnaud de Mattia',
    url='http://github.com/adematti/pypescript',
    description='Package to script analysis pipelines',
    license='GPLv3',
    packages=['pypescript','pypescript.libutils'],
    ext_modules=[extension],
    package_data={base_dir:['wrappers/*','block/*']},
    install_requires=['pyyaml','numpy','mpi4py'],
    extras_require={'extras':['pytest','psutil']},
    entry_points={'console_scripts': ['pypescript=pypescript.__main__:main','pypescript_section_names=pypescript.setuppype.generate_section_names:main']}
    )
