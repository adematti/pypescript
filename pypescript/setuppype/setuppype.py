"""Implementation of :func:`numpy.core.setup` to install **pypescript** libraries like a charm."""

import os
import sys
import glob
import sysconfig
import pkg_resources

from setuptools import find_packages
from numpy.distutils.extension import Extension as _Extension
from numpy.distutils.extension import fortran_pyf_ext_re
from numpy.distutils.command.build_src import build_src as _build_src
from numpy.distutils.command.build_src import appendpath
from numpy.distutils import log
from numpy.distutils.core import setup as _setup
from distutils.dep_util import newer_group

from .generate_section_names import WriteSections
from .generate_pymodule_csource import write_csource

from . import utils


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
                self.extra_link_args = addfirst(self.extra_link_args,*('-l{}'.format(w) for w in pypelib_wrappers.values()))
            else:
                self.extra_link_args = addfirst(self.extra_link_args,'-l{}'.format(pypelib_wrappers['c']))
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


# do not override NumpyDistribution as numpy.distutils.etup calls distutils.setup() with NumpyDistribution

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
                include_pype_module_names=None,
                exclude_pype_module_names=None,
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

        include_pype_module_names : list, default=None
            List of module names (w.r.t. ``base_dir``) to install.
            If not ``None``, all modules in ``base_dir`` are considered.

        exclude_pype_module_names : list, default=None
            List of module names (w.r.t. ``base_dir``) to exclude.
            If ``None``, no module is excluded.
        """

        self.base_dir = base_dir
        self.requirements_fn = 'requirements.txt'
        self.section_dir = os.path.join('build','sections')
        if sections is not None:
            if isinstance(sections,str):
                sections = {'filename':sections}
            self.sections = sections
        else:
            self.sections = {'filename':os.path.join(self.base_dir,'section_names.yaml')}
        self.section_headers = {}
        self.section_headers['python'] = os.path.join(self.section_dir,'section_names.py')
        self.section_headers['c'] = os.path.join(self.section_dir,'section_names.h')
        self.section_headers['fortran'] = os.path.join(self.section_dir,'section_names_f90.fi')

        if packages is None:
            packages = find_packages(self.base_dir)

        if version is None:
            sys.path.insert(0,name)
            from _version import __version__
            version = __version__

        py_modules = py_modules or []
        ext_modules = ext_modules or []
        data_files = data_files or []
        libraries = libraries or []

        if install_requires is None:
            try:
                with open(self.requirements_fn,'r') as file:
                    install_requires = [name.strip() for name in file]
            except OSError:
                install_requires = []

        self.set_pype_modules(include_pype_module_names=include_pype_module_names,exclude_pype_module_names=exclude_pype_module_names)
        py_modules += self.pype_modules
        pype_ext_modules = self.pype_ext_modules
        install_requires += self.pype_module_requires

        ext_modules += pype_ext_modules
        install_requires = list(set(install_requires)) # remove duplicates

        def write_sections(langs=None):
            if langs is None:
                langs = self.section_headers.keys()
            ws = WriteSections(**self.sections)
            for lang in langs:
                header_file = self.section_headers[lang]
                if 'filename' not in self.sections or newer_group([self.sections['filename']],header_file):
                    ws(filenames={lang:header_file})

        if pype_ext_modules:
            # if Python extensions, need to compile **pypescript** wrappers
            write_sections()
            pypelib_libraries = [(pypelib_wrappers['c'],{'sources':glob.glob(os.path.join(self.pypelib_wrappers_dir,'*.c')),
                                                        'include_dirs':[self.section_dir,self.pypelib_block_dir,sysconfig.get_path('include')]})]
                                                        # Python include needed by pip, why?
            pypelib_libraries += [(pypelib_wrappers['fortran'],{'sources':glob.glob(os.path.join(self.pypelib_wrappers_dir,'*.F90')),
                                                        'extra_f90_compile_args':['-ffree-line-length-none']})]
            libraries = addfirst(libraries,*pypelib_libraries)
        else:
            write_sections(langs=['python'])
        data_files += [(name,[self.section_headers['python']])]

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
              cmdclass={'build_src':build_src},
              data_files=data_files,
              libraries=libraries,
              **kwargs)

    def set_pype_modules(self, include_pype_module_names=None, exclude_pype_module_names=None):
        """
        Set modules to install.
        Split modules into Python extensions (i.e. to be compiled) :attr:`pype_ext_modules` and standard Python modules :attr:`pype_modules`.
        It also gathers the requirements from each module as specified in their description file into the list :attr:`pype_module_requires`.
        """
        extensions,modules,requirements = [],[],set()
        for module_dir,full_name,description_file,description in\
            utils.walk_pype_modules(base_dir=self.base_dir,
                                    include_pype_module_names=include_pype_module_names,
                                    exclude_pype_module_names=exclude_pype_module_names):
            sources = description.get('sources',[])
            ext_sources = []
            for source in sources:
                if source.endswith('.py'):
                    modules.append(utils.get_module_full_name(os.path.join(module_dir,source),base_dir=base_dir))
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
                                                    self.pypelib_wrappers_dir,self.pypelib_block_dir,self.section_dir)
                    extension = Extension(full_name, module_dir=module_dir, doc=description.get('description',''),
                                            description_file=description_file, **compile_kwargs)
                    extensions.append(extension)
                else:
                    modules.append(name)
            else:
                modules.append(name)
        self.pype_modules = modules
        self.pype_ext_modules = extensions
        self.pype_module_requires = list(requirements)
