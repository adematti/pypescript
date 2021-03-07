from distutils.core import setup, Extension

module1 = Extension('block',sources=['blockmodule.c'])

setup(name='block',version='1.0',description='This is the block package',ext_modules=[module1])
