"""Definition of :class:`BaseModule` and :class:`BasePipeline`."""

import os
import sys
import logging
import importlib

from .block import BlockMapping, DataBlock, SectionBlock
from .config import ConfigBlock, ConfigError
from .libutils import ModuleDescription
from . import syntax, section_names
from . import utils


def _import_pygraphviz():
    try:
        import pygraphviz as pgv
    except ImportError as e:
        raise ImportError('Please install pygraphviz: see https://github.com/pygraphviz/pygraphviz/blob/master/INSTALL.txt') from e
    return pgv


def mimport(module_name, module_file=None, module_class=None, name=None, data_block=None, options={}):
    if name is None:
        name = module_name.split('.')[-1]
    options = options or {}
    options[syntax.module_name] = module_name
    options[syntax.module_file] = module_file
    options[syntax.module_class] = module_class
    return BaseModule.from_filename(name=name,data_block=data_block,options=options)


#_all_loaded_modules = {}

@utils.addclslogger
class BaseModule(object):
    """
    Base module class, which wraps pure Python modules or Python C extensions.
    Modules interact with the rest of the pipeline through the three methods :meth:`BaseModule.setup`, :meth:`BaseModule.execute`
    and :meth:`BaseModule.cleanup`.

    Attributes
    ----------
    name : string
        Module name, which is set by the pipeline configuration and is a unique module identifier.
        One can use the same module implementation with different module names in the same pipeline.

    config_block : DataBlock
        Structure containing configuration options.

    data_block : DataBlock
        Structure containing data exchanged between modules.
    """
    logger = logging.getLogger('BaseModule')
    _available_options = [syntax.module_base_dir,syntax.module_name,syntax.module_file,syntax.module_class,
                            syntax.datablock_set,syntax.datablock_mapping,syntax.datablock_duplicate]

    def __init__(self, name, options=None, config_block=None, data_block=None, description=None):
        """
        Initialise :class:`BaseModule`.

        Parameters
        ----------
        name : string
            Module name, which is set by the pipeline configuration and is a unique module identifier.
            One can use the same module implementation with different module names in the same pipeline.

        options : SectionBlock, dict, default=None
            Options for this module.

        config_block : DataBlock, dict, string, default=None
            Structure containing configuration options. If ``None``, creates one.

        data_block : DataBlock, default=None
            Structure containing data exchanged between modules. If ``None``, creates one.

        description : dict, default=None
            Dictionary containing module description.
        """
        self.name = name
        self.description = description
        self.log_info('Init module {}.'.format(self),rank=0)
        self.set_config_block(options=options,config_block=config_block)
        self.set_data_block(data_block=data_block)
        self._cache = {}

    def set_config_block(self, options=None, config_block=None):
        """
        Set :attr:`config_block` and :attr:`options`.

        Parameters
        ----------
        options : SectionBlock, dict, default=None
            Options for this module, which update those in ``config_block``.

        config_block : DataBlock, dict, string, default=None
            Structure containing configuration options, which will be updated with ``options``.
        """
        self.config_block = ConfigBlock(config_block)
        if options is not None:
            for name,value in options.items():
                self.config_block[self.name,name] = value
        self.options = SectionBlock(self.config_block,self.name)
        self._datablock_set = syntax.collapse_sections(self.options.get_dict(syntax.datablock_set,{}),sep=None)
        self._datablock_mapping = BlockMapping(syntax.collapse_sections(self.options.get_dict(syntax.datablock_mapping,{}),sep=syntax.section_sep),sep=syntax.section_sep)
        self._datablock_duplicate = BlockMapping(syntax.collapse_sections(self.options.get_dict(syntax.datablock_duplicate,{}),sep=syntax.section_sep),sep=syntax.section_sep)
        self.check_options()

    def check_options(self):
        """Check provided options are mentioned in description file (if exists), else raises ``ConfigError``."""
        if self.description is not None:
            available_options = self.description['options']
            for name,value in self.options.items():
                if name in self._available_options:
                    continue
                if name not in available_options:
                    raise ConfigError('Option {} for module [{}] is not listed as available options in description file'.format(name,self.name))
                types = available_options[name].get('type',None)
                if types is not None:
                    if not utils.is_of_type(value,types):
                        raise ConfigError('Option {} for module [{}] is not of correct type ({}, while allowed types are {})'.format(name,self.name,type(value),types))
                choices = available_options[name].get('choices',None)
                if choices is not None:
                    if value not in choices:
                        raise ConfigError('Option {} for module [{}] is not allowed ({}, while allowed choices are {})'.format(name,self.name,value,choices))
            for name,options in available_options.items():
                if 'default' in options:
                    self.options.setdefault(name,options['default'])

    def set_data_block(self, data_block=None):
        """
        Set :attr:`data_block`.

        Parameters
        ----------
        data_block : DataBlock, default=None
            :class:`DataBlock` instance used by the module to retrieve and store items.
            If ``None``, creates one.
        """
        self.data_block = DataBlock(data_block)
        self.data_block.set_mapping(self._datablock_mapping)

    @property
    def mpicomm(self):
        return self.data_block[section_names.mpi,'comm']

    def setup(self):
        """Set up module (called at the beginning)."""
        raise NotImplementedError

    def execute(self):
        """Execute module, i.e. do calculation (called at each iteration)."""
        raise NotImplementedError

    def cleanup(self):
        """Clean up, i.e. free variables if needed (called at the end)."""
        raise NotImplementedError

    def __getattribute__(self, name):
        """
        Extends builtin :meth:`__getattribute__` to complement exceptions occuring in :meth:`setup`,
        :meth:`execute` and :meth:`cleanup` with module class and local name, for easy debugging.
        """
        if name in [syntax.setup_function,syntax.execute_function,syntax.cleanup_function]:
            fun = object.__getattribute__(self,name)

            def wrapper(*args,**kwargs):

                for key,value in self._datablock_set.items():
                    self.data_block[key] = value

                try:
                    fun(*args,**kwargs)
                except Exception as exc:
                    raise RuntimeError('Exception in function {} of {} [{}].'.format(name,self.__class__.__name__,self.name)) from exc

                for keyg,keyl in self._datablock_duplicate.items():
                    if keyl in self.data_block:
                        self.data_block[keyg] = self.data_block[keyl]

            return wrapper

        return object.__getattribute__(self,name)

    def __str__(self):
        """String as module class name + module local name (in this pipeline)."""
        return '{} [{}]'.format(self.__class__.__name__,self.name)

    @classmethod
    def from_filename(cls, name='module', options=None, config_block=None, data_block=None):
        """
        Create :class:`BaseModule`-type module from either module name or module file.
        The imported module can contain the following functions:

        - setup(name, config_block, data_block)
        - execute(name, config_block, data_block)
        - cleanup(name, config_block, data_block)

        Or a class with the following methods:

        - setup(self)
        - execute(self)
        - cleanup(self)

        which can use attributes :attr:`name`, :attr:`config_block` and :attr:`data_block`.

        Parameters
        ----------
        name : string
            Module name, which is set by the pipeline configuration and is a unique module identifier.

        options : SectionBlock, dict, default=None
            Options for this module.
            It should contain an entry 'module_file' OR (exclusive) 'module_name' (w.r.t. 'base_dir', defaulting to '.').
            It may contain an entry 'module_class' containing a class name if the module consists in a class.

        config_block : DataBlock, dict, string, default=None
            Structure containing configuration options.

        data_block : DataBlock, default=None
            Structure containing data exchanged between modules. If ``None``, creates one.
        """
        #if name in _all_loaded_modules:
        #    raise SyntaxError('You should NOT use the same module name in different pipelines. Create a new module, and use configblock_duplicate if useful!')
        options = options or {}
        base_dir = options.get(syntax.module_base_dir,'.')
        module_name = options.get(syntax.module_name,None)
        module_file = options.get(syntax.module_file,None)
        module_class = options.get(syntax.module_class,None)
        if module_file is None and module_name is None:
            raise ImportError('Failed importing module [{}]. You must provide a module file or a module name'.format(name))

        if module_file is not None:
            if module_name is not None:
                raise ImportError('Failed importing module [{}]. Both module file and module name are provided'.format(name))
            filename = os.path.join(base_dir,module_file)
            cls.log_info('Importing module {} [{}].'.format(filename,name),rank=0)
            basename = os.path.basename(filename)
            #base_module_name = os.path.splitext(basename)[0]
            spec = importlib.util.spec_from_file_location(base_module_name,filename)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            cls.log_info('Importing module {} [{}].'.format(module_name,name),rank=0)
            module = importlib.import_module(module_name)
            #base_module_name = module_name.split('.')[-1]
        base_module_name = module.__name__.split('.')[-1]
        description_file = ModuleDescription.filename_from_module(module)
        description = ModuleDescription.from_module(module)
        if description is not None:
            cls.log_info('Found description file {}.'.format(description_file),rank=0)
            multiple_descriptions = isinstance(description,list)
        else:
            cls.log_info('No description file provided at {}.'.format(description_file),rank=0)
            multiple_descriptions = False

        steps = [syntax.setup_function,syntax.execute_function,syntax.cleanup_function]

        def get_func_name(step):
            return options.get('{}_function'.format(step),step)

        import inspect
        all_cls = inspect.getmembers(module,inspect.isclass)
        all_name_cls = [c[0] for c in all_cls]

        if module_class is None:
            if description and not multiple_descriptions:
                name_cls = description['name']
            else:
                if len(all_cls) == 1:
                    name_cls = all_name_cls[0]
                else:
                    name_cls = utils.snake_to_pascal_case(base_module_name)
            if all(hasattr(module,get_func_name(step)) for step in steps):
                if description and multiple_descriptions:
                    raise ImportError('Description file {} describes multiple modules while there is only one in {}'.format(description_file,base_module_name))
                new_cls = type(name_cls,(BaseModule,),{'__init__':BaseModule.__init__, '__doc__':BaseModule.__doc__})

                def _make_func(module,step):
                    mod_func = getattr(module,get_func_name(step))

                    def func(self):
                        return mod_func(name,self.config_block,self.data_block)

                    return func

                for step in steps:
                    setattr(new_cls,step,_make_func(module,step))
                return new_cls(name,options=options,config_block=config_block,data_block=data_block,description=description)
                #_all_loaded_modules[name] = toret
                #return toret
            else:
                cls.log_info('No {} functions found in module [{}], trying to load class {}.'.format(steps,name,name_cls),rank=0)
                module_class = name_cls

        if module_class is not None:
            if module_class in all_name_cls:
                if multiple_descriptions:
                    found = False
                    for desc in description:
                        if desc['name'] == module_class:
                            description = desc
                            found = True
                            break
                    if not found:
                        cls.log_info('No description found for {} in description file {}.'.format(module_class,description_file),rank=0)
                mod_cls = getattr(module,module_class)
                if issubclass(mod_cls,cls):
                    toret = mod_cls(name,options=options,config_block=config_block,data_block=data_block)
                else:
                    new_cls = type(mod_cls.__name__,(BaseModule,mod_cls),{'__init__':BaseModule.__init__, '__doc__':mod_cls.__doc__})
                    for step in steps:
                        setattr(new_cls,step,getattr(mod_cls,get_func_name(step)))
                    toret = new_cls(name,options=options,config_block=config_block,data_block=data_block,description=description)
                #_all_loaded_modules[name] = toret
                return toret
            raise ValueError('Class {} does not exist in {} [{}]'.format(module_class,base_module_name,name))

    @classmethod
    def plot_inheritance_graph(cls, filename, exclude=None):
        """
        Plot inheritance graph to ``filename``.

        Parameters
        ----------
        filename : string
            Where to save graph (in ``ps`` format).

        exclude : list
            List of module (base name) to exclude from the graph.
        """
        exclude = exclude or []
        pgv = _import_pygraphviz()
        graph = pgv.AGraph(strict=True,directed=True)

        def norm_name(cls):
            return cls.__name__

        def callback(curcls,prevcls):
            if norm_name(curcls) in exclude:
                return
            graph.add_node(norm_name(curcls),color='lightskyblue',style='filled',group='inheritance',shape='box')
            graph.add_edge(norm_name(curcls),norm_name(prevcls),color='lightskyblue',style='bold',arrowhead='none')
            for newcls in curcls.__subclasses__():
                callback(newcls,curcls)

        for newcls in cls.__subclasses__():
            callback(newcls,cls)

        graph.layout('dot')
        cls.log_info('Saving graph to {}.'.format(filename),rank=0)
        utils.mkdir(os.path.dirname(filename))
        graph.draw(filename)
