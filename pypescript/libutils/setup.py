"""Implementation of :func:`numpy.core.setup` to install **pypescript** libraries like a charm."""

import os
import sys
import shutil
import glob
import sysconfig
import pkg_resources

from setuptools import find_packages
from numpy.distutils.extension import Extension as _Extension
from numpy.distutils.extension import fortran_pyf_ext_re
from numpy.distutils.command.build_src import build_src as _build_src
from numpy.distutils.command.build_src import appendpath
from numpy.distutils.command.develop import develop as _develop
from numpy.distutils import log
from numpy.distutils.core import setup as _setup
from distutils.dep_util import newer_group

from .generate_section_names import SectionNames
from .generate_pymodule_csource import write_csource

from . import utils


def get_mpi4py_include():
    import mpi4py
    return mpi4py.get_include()


pypelib_wrappers = {'fortran':'fpypelib','c':'pypelib'}


def addfirst(li, *args):
    """Add elements in ``args`` on top of list of ``li`` and return list."""
    toret = list(args)
    for el in li:
        if el not in toret:
            toret.append(el)
    return toret


class Extension(_Extension):
    """
    Extend :class:`numpy.distutils.extension.Extension` to **pypescript** extensions (i.e. modules extending Python).

    Attributes
    ----------
    module_dir : string, default='.'
        Module directory.

    doc : string, default=None
        Module documentation, to fill ``m_doc`` entry of Python C API's ``PyModuleDef``.

    description_file : string, default None
        Module description file name.

    args : tuple
        Other arguments for :class:`numpy.distutils.extension.Extension`.

    kwargs : dict
        Other arguments for :class:`numpy.distutils.extension.Extension`.

    Note
    ----
    In :mod:`numpy.distutils`, **f2py** is called if Fortran source files are provided.
    Here, **f2py** is called if Fortran source files are provided AND a (possibly empty) list ``f2py_options`` is provided.
    """
    def __init__(self, *args, module_dir='.', doc=None, description_file=None, **kwargs):
        super(Extension,self).__init__(*args,**kwargs)
        self.module_dir = module_dir
        self.doc = doc
        self.description_file = description_file
        self.is_pypemodule = description_file is not None
        if self.is_pypemodule:
            if self.has_fortran_sources():
                self.extra_link_args = addfirst(self.extra_link_args,'-lmpifort',*('-l{}'.format(w) for w in pypelib_wrappers.values()))
            else:
                self.extra_link_args = addfirst(self.extra_link_args,'-lmpi','-l{}'.format(pypelib_wrappers['c']))
        self.use_f2py = self.has_fortran_sources() and kwargs.get('f2py_options',None) is not None
        for f90_flag in ['-ffree-line-length-none']:
            if f90_flag not in self.extra_f90_compile_args:
                self.extra_f90_compile_args.append(f90_flag)

    def has_fortran_sources(self):
        """Compile Fortran sources?"""
        return self.has_f2py_sources()

    def from_npextension(cls, ext):
        """
        Convert :class:`numpy.distutils.extension.Extension` ``ext`` to :class:`Extension`.

        Note
        ----
        For compatibility, as in :mod:`numpy.distutils`, **f2py** will be called if Fortran source files are provided.
        """
        new = cls.__new__(cls)
        new.__dict__.update(ext)
        new.is_pypemodule = False
        new.use_f2py = new.has_f2py_sources()


class build_src(_build_src):
    """Extend :class:`numpy.distutils.build_src.build_src` to generate the C file necessary to turn C/C++/Fortran sources into Python extension modules."""

    def build_extension_sources(self, ext):

        sources = list(ext.sources)

        log.info('building extension "%s" sources' % (ext.name))

        fullname = self.get_ext_fullname(ext.name)

        modpath = fullname.split('.')
        package = '.'.join(modpath[0:-1])

        if self.inplace:
            self.ext_target_dir = self.get_package_dir(package)

        # begin changes w.r.t. numpy version #
        if not isinstance(ext,Extension):
            ext = Extension.from_npextension(ext)
        if ext.is_pypemodule:
            # link previously built C pypescript wrappers
            build_temp = self.distribution.get_command_obj('build').build_temp
            ext.include_dirs = addfirst(ext.include_dirs,build_temp)
            if ext.has_fortran_sources():
                ext.depends += [appendpath(build_temp,'lib{}.a'.format(w)) for w in pypelib_wrappers.values()]
            else:
                ext.depends += [appendpath(build_temp,'lib{}.a'.format(pypelib_wrappers['c']))]
            ext.depends += ['-lmpi']
            sources = self.pymodule_csource(sources, ext)
        # end changes w.r.t. numpy version #
        sources = self.generate_sources(sources, ext)
        sources = self.template_sources(sources, ext)
        sources = self.swig_sources(sources, ext)
        # begin changes w.r.t. numpy version #
        if ext.use_f2py:
            sources = self.f2py_sources(sources, ext)
        sources = self.pyrex_sources(sources, ext)
        # end changes w.r.t. numpy version #

        sources, py_files = self.filter_py_files(sources)

        if package not in self.py_modules_dict:
            self.py_modules_dict[package] = []
        modules = []
        for f in py_files:
            module = os.path.splitext(os.path.basename(f))[0]
            modules.append((package, module, f))
        self.py_modules_dict[package] += modules

        sources, h_files = self.filter_h_files(sources)

        if h_files:
            log.info('%s - nothing done with h_files = %s',
                     package, h_files)
        #for f in h_files:
        #    self.distribution.headers.append((package,f))

        ext.sources = sources

    def pymodule_csource(self, sources, extension):
        """Write C source file to compile C/C++/Fortran files as a Python extension."""
        ext_name = extension.name.split('.')[-1]
        if self.inplace:
            target_dir = extension.module_dir
        else:
            target_dir = appendpath(self.build_src, extension.module_dir)
        target_file = os.path.join(target_dir, ext_name + 'module.c')
        if newer_group([extension.description_file], target_file):
            write_csource(filename=target_file,module_name=ext_name,doc=extension.doc)
        extension.depends += [target_file]
        return sources + [target_file]


# do not override NumpyDistribution as numpy.distutils.setup calls distutils.setup() with NumpyDistribution

class setup(object):

    """Class that extends the :func:`numpy.distutils.core.setup` function to setup a **pypescript** library."""

    def __init__(self,
                name='pypescript_lib',
                base_dir='.',
                sections=None,
                version=None,
                author=None,
                maintainer=None,
                url=None,
                description='pypescript library',
                long_description=None,
                license=None,
                packages=None,
                py_modules=None,
                ext_modules=None,
                pype_module_names=None,
                install_requires=None,
                data_files=None,
                libraries=None,
                **kwargs):
        """
        Initialise :class:`setup` and call :func:`numpy.distutils.core.setup` to install the **pypescript** library.
        Most arguments are similar to those of :func:`numpy.distutils.core.setup`.
        Only supplementary arguments are:

        Parameters
        ----------
        base_dir : string, default='.'
            Root of the directory tree to explore.

        sections : string, list, default='.'
            Section name *yaml* file, or list of sections (strings).

        pype_module_names : string, default=None
            Name of file containing a list of modules (w.r.t. ``base_dir``) to install.
            See :func:`utils.read_path_list`.
            If not ``None``, all modules in ``base_dir`` are considered.
        """
        self.base_dir = base_dir
        self.section_dir = os.path.join('build','sections')
        if sections is not None:
            if isinstance(sections,str):
                self.sections = SectionNames.load(sections)
            else:
                self.sections = SectionNames(sections)
        else:
            self.sections = SectionNames.load(os.path.join(self.base_dir,'section_names.yaml'))
        self.section_fns = []
        self.section_pyfn = os.path.join(self.section_dir,'section_names.py')
        self.section_fns.append(self.section_pyfn)
        self.section_fns.append(os.path.join(self.section_dir,'section_names.h'))
        self.section_fns.append(os.path.join(self.section_dir,'section_names.fi'))
        #os.environ['CC'] = os.environ.get('MPICC','mpicc')
        #os.environ['CXX'] = os.environ.get('MPICCX','mpicxx')
        #os.environ['F90'] = os.environ.get('MPIF90','mpif90')
        #os.environ['LDSHARED'] = os.environ.get('LDSHARED','mpicc')

        if packages is None:
            packages = find_packages(self.base_dir)
            packages = ['.'.join([os.path.basename(self.base_dir),package]) for package in packages]
            #exit()

        if version is None:
            sys.path.insert(0,name)
            from _version import __version__
            version = __version__

        py_modules = list(py_modules or [])
        ext_modules = list(ext_modules or [])
        data_files = list(data_files or [])
        libraries = list(libraries or [])
        install_requires = list(install_requires or [])
        pype_module_names = pype_module_names or (None,)*2

        if isinstance(pype_module_names,str):
            include_pype_module_names, exclude_pype_module_names = utils.read_path_list(pype_module_names)
        else:
            include_pype_module_names, exclude_pype_module_names = pype_module_names

        self.set_pype_modules(include_pype_module_names=include_pype_module_names,exclude_pype_module_names=exclude_pype_module_names)
        py_modules += self.pype_modules
        pype_ext_modules = self.pype_ext_modules
        #data_files += [(os.path.relpath(os.path.dirname(file),start=self.base_dir),[file]) for file in self.description_files]
        data_files += [(os.path.relpath(os.path.dirname(file),start='.'),[file]) for file in self.description_files]
        install_requires += self.pype_module_requires

        ext_modules += pype_ext_modules
        install_requires = list(set(install_requires)) # remove duplicates

        if pype_ext_modules:
            for section_fn in self.section_fns:
                if not hasattr(self.sections,'filename') or newer_group([self.sections.filename],section_fn):
                    self.sections.save(section_fn)
            # if Python extensions, need to compile **pypescript** wrappers
            pypelib_libraries = [(pypelib_wrappers['c'],{'sources':glob.glob(os.path.join(self.pypelib_wrappers_dir,'*.c')),
                                                        'include_dirs':[get_mpi4py_include(),sysconfig.get_path('include'),sysconfig.get_config_var('CONFINCLUDEDIR'),
                                                                    self.section_dir,self.pypelib_block_dir]})]
                                                        # Python include needed by pip, why?
            if self.has_fortran_sources:
                pypelib_libraries += [(pypelib_wrappers['fortran'],{'sources':glob.glob(os.path.join(self.pypelib_wrappers_dir,'*.F90')),
                                                            'extra_f90_compile_args':['-ffree-line-length-none']})]
            libraries = addfirst(libraries,*pypelib_libraries)
        else:
            self.sections.save(self.section_pyfn)
        # to have section_names.py at the root directory
        data_files += [(self.base_dir,[self.section_pyfn])]

        # to ensure this is also the case
        class develop(_develop):

            def run(_self):
                shutil.copyfile(self.section_pyfn,os.path.join(self.base_dir,os.path.basename(self.section_pyfn)))
                _develop.run(_self)

        _setup(name=name,
              version=version,
              author=author,
              maintainer=maintainer,
              url=url,
              description=description,
              long_description=long_description,
              license=license,
              packages=packages,
              py_modules=py_modules,
              ext_modules=ext_modules,
              install_requires=install_requires,
              cmdclass={'build_src':build_src,'develop':develop},
              data_files=data_files,
              libraries=libraries,
              **kwargs)

    def set_pype_modules(self, include_pype_module_names=None, exclude_pype_module_names=None):
        """
        Set modules to install.
        Split modules into Python extensions (i.e. to be compiled) :attr:`pype_ext_modules` and standard Python modules :attr:`pype_modules`.
        It also gathers the requirements from each module as specified in their description file into the list :attr:`pype_module_requires`.
        """
        self.has_fortran_sources = False
        extensions,modules,description_files,requirements = set(),set(),set(),set()
        for module_dir,full_name,description_file,description in\
            utils.walk_pype_modules(base_dir=self.base_dir,
                                    include_pype_module_names=include_pype_module_names,
                                    exclude_pype_module_names=exclude_pype_module_names):
            description_files.add(description_file)
            #full_name = utils.module_full_name(description_file,=self.)
            full_name = utils.module_full_name(description_file,base_dir='.')
            sources = description.get('sources',[])
            ext_sources = []
            for source in sources:
                if source.endswith('.py'):
                    #modules.add(utils.module_full_name(os.path.join(module_dir,source),base_dir=self.base_dir))
                    modules.add(utils.module_full_name(os.path.join(module_dir,source),base_dir='.'))
                else:
                    ext_sources += source
            requirements |= set(description.get('requirements',[]))
            if 'compile' in description:
                if 'sources' in description['compile']:
                    ext_sources = description['compile']['sources']
                if ext_sources:
                    compile_kwargs = description['compile']
                    if not hasattr(self,'pypelib_wrappers_dir'):
                        self.pypelib_wrappers_dir = pkg_resources.resource_filename('pypescript','wrappers')
                        self.pypelib_block_dir = pkg_resources.resource_filename('pypescript','block')
                    compile_kwargs['sources'] = sum([glob.glob(os.path.abspath(os.path.join(module_dir,source))) for source in ext_sources],[])
                    compile_kwargs['include_dirs'] = addfirst(compile_kwargs.get('include_dirs',[]),
                                                    get_mpi4py_include(),sysconfig.get_path('include'),sysconfig.get_config_var('CONFINCLUDEDIR'),
                                                    self.pypelib_wrappers_dir,self.pypelib_block_dir,self.section_dir)
                    extension = Extension(full_name, module_dir=module_dir, doc=description.get('description',''),
                                            description_file=description_file, **compile_kwargs)
                    if extension.has_fortran_sources():
                        self.has_fortran_sources = True
                    extensions.add(extension)
                else:
                    modules.add(full_name)
            else:
                modules.add(full_name)
        self.pype_modules = list(modules)
        self.pype_ext_modules = list(extensions)
        self.description_files = list(description_files)
        self.pype_module_requires = list(requirements)
