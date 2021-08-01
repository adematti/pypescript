"""Definition of :class:`BaseModule`."""

import os
import sys
import logging
import importlib

from .block import BlockMapping, DataBlock, SectionBlock
from .config import ConfigBlock, ConfigError
from .libutils import ModuleDescription, syntax_description
from . import syntax, section_names
from . import utils


def _import_pygraphviz():
    try:
        import pygraphviz as pgv
    except ImportError as e:
        raise ImportError('Please install pygraphviz: see https://github.com/pygraphviz/pygraphviz/blob/master/INSTALL.txt') from e
    return pgv


def mimport(module_name, module_file=None, module_class=None, name=None, data_block=None, options=None):
    """
    Convenient function to load *pypescript* module.

    Parameters
    ----------
    module_name : string, default=None
        *Python* module name.

    module_file : string, default=None
        Module file, used if ``module_name`` not provided.

    module_class : string, default=None
        Module class to load from *Python* module.
        Unnessary if only one class in the module.

    name : string, default=None
        Local (i.e. bound to the pipeline) module name.

    data_block : DataBlock, default=None
        Structure containing data exchanged between modules. If ``None``, creates one.

    options : SectionBlock, dict, default=None
        Options for this module.
    """
    if name is None:
        if module_name is not None:
            name = module_name.split('.')[-1]
        else:
            name = os.path.splitext(module_file)[0].split(os.sep)[-1]
    options = options or {}
    options[syntax.module_name] = module_name
    options[syntax.module_file] = module_file
    options[syntax.module_class] = module_class
    return BaseModule.from_filename(name=name,data_block=data_block,options=options)


class MetaModule(type):

    """Meta class to replace :meth:`setup`, :meth:`execute` and :meth:`cleanup` module methods."""

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        cls.set_functions({name: getattr(cls,name) for name in [syntax.setup_function,syntax.execute_function,syntax.cleanup_function]})
        return cls

    def set_functions(cls, functions):
        """
        Wrap input ``functions`` and add corresponding methods to class ``cls``.
        Specifically:

        - before ``functions`` calls, fills in :attr:`BaseModule.data_block` with values specified in :attr:`BaseModule._datablock_set`
        - after ``functions`` calls, duplicate entries of :attr:`BaseModule.data_block` with key pairs specified in :attr:`BaseModule._datablock_duplicate`
        - set module :attr:`BaseModule._state`
        - exceptions occuring in ``functions`` calls are complemented with module class and local name, for easy debugging

        Parameters
        ----------
        functions : dict
            Dictionary of function name: callable.
        """
        def make_wrapper(step, fun):

            def wrapper(self):
                for key,value in self._datablock_set.items():
                    self.data_block[key] = value

                try:
                    fun(self)
                except Exception as exc:
                    raise RuntimeError('Exception in function {} of {} [{}].'.format(step,self.__class__.__name__,self.name)) from exc

                for keyg,keyl in self._datablock_duplicate.items():
                    if keyl in self.data_block:
                        self.data_block[keyg] = self.data_block[keyl]

                self._state = step

            return wrapper

        for step,fun in functions.items():
            setattr(cls,step,make_wrapper(step,fun))


@utils.addclslogger
class BaseModule(object,metaclass=MetaModule):
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

    description : ModuleDescription
        Module description.
    """
    logger = logging.getLogger('BaseModule')
    _available_options = [syntax.module_base_dir,syntax.module_name,syntax.module_file,syntax.module_class,
                            syntax.datablock_set,syntax.datablock_mapping,syntax.datablock_duplicate]

    def __init__(self, name, options=None, config_block=None, data_block=None, description=None, pipeline=None):
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

        description : string, ModuleDescription, dict, default=None
            Module description.

        pipeline : BasePipeline
            Pipeline instance for which this module was created.
        """
        self.name = name
        self.description = ModuleDescription(description)
        self.log_info('Init module {}.'.format(self),rank=0)
        self.set_config_block(options=options,config_block=config_block)
        self.set_data_block(data_block=data_block)
        self._cache = {}
        self._pipeline = pipeline
        self._state = syntax.cleanup_function # start with cleanup, (nothing allocated)

    def set_config_block(self, options=None, config_block=None):
        """
        Set :attr:`config_block` and :attr:`options`.
        Also sets:

        - :attr:`_datablock_set`, dictionary of (key, value) to set into :attr:`data_block`
        - :attr:`_datablock_mapping`, :class:`BlockMapping` instance that maps :attr:`data_block` entries to others
        - :attr:'_datablock_duplicate', :class:`BlockMapping` instance used to duplicate :attr:`data_block` entries

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
        self._datablock_set = {syntax.split_sections(key):value for key,value in syntax.collapse_sections(self.options.get_dict(syntax.datablock_set,{}),maxdepth=2).items()}
        self._datablock_mapping = BlockMapping(syntax.collapse_sections(self.options.get_dict(syntax.datablock_mapping,{})))
        self._datablock_duplicate = BlockMapping(syntax.collapse_sections(self.options.get_dict(syntax.datablock_duplicate,{})))
        self.check_options()

    def check_options(self):
        """Check provided options are mentioned in description file (if exists), else raises ``ConfigError``."""
        if self.description is not None:
            available_options = self.description.get('options',None)
            if available_options is None: return
            others = syntax_description.others in available_options
            for name,value in self.options.items():
                if name in self._available_options:
                    continue
                if name not in available_options:
                    if others:
                        continue
                    else:
                        raise ConfigError('Option "{}" for module [{}] is not listed as available options in description file'.format(name,self.name))
                types = available_options[name].get('type',None)
                if types is not None and value is not None:
                    if not utils.is_of_type(value,types):
                        raise ConfigError('Option "{}" for module [{}] is not of correct type ({}, while allowed types are {})'.format(name,self.name,type(value),types))
                choices = available_options[name].get('choices',None)
                if choices is not None:
                    if value not in choices:
                        raise ConfigError('Option "{}" for module [{}] is not allowed ({}, while allowed choices are {})'.format(name,self.name,value,choices))
            for name,options in available_options.items():
                if name == syntax_description.others: continue
                if 'default' in options:
                    self.options.setdefault(name,options['default'])
                if name not in self.options:
                    raise ConfigError('Option "{}" for module [{}] is requested'.format(name,self.name))

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
        """Return current MPI communicator."""
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

    def __str__(self):
        """String as module class name + module local name (in this pipeline)."""
        return '{} [{}]'.format(self.__class__.__name__,self.name)

    def fetch_module(self, name=''):
        """Fetch module/pipeline given (dot-separated) name."""
        names = name.split('.')
        module = self
        if names[0] == syntax.main: # start from root
            while module._pipeline is not None:
                module = module._pipeline
            assert module.name == syntax.main, module.name
            names = names[1:] # we are in main, skip it
        for name in names:
            if not hasattr(module,'modules'):
                raise ValueError('{} is not a pipeline'.format(module.name))
            module = module.modules[name]
        return module

    @classmethod
    def from_filename(cls, name='module', options=None, **kwargs):
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

        kwargs : dict
            Arguments for :meth:`BaseModule.__init__`.
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
            base_module_name = os.path.splitext(basename)[0]
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

        # first look at the classes defined in the module
        import inspect
        all_cls = inspect.getmembers(module,inspect.isclass)
        # to select classes that are indeed defined (not imported) in the provided module
        all_name_cls = [c[0] for c in all_cls if c[1].__module__ == module.__name__]

        # if not class specified in options
        if module_class is None:
            # if description file provided, set class name to "name" value
            if description and not multiple_descriptions:
                name_cls = description['name']
            else:
                # if only one class defined in module
                if len(all_name_cls) == 1:
                    name_cls = all_name_cls[0]
                else: # defining class name as Pascal case of module name
                    name_cls = utils.snake_to_pascal_case(base_module_name)
            if all(hasattr(module,get_func_name(step)) for step in steps): # all functions given in module: create on-the-fly class
                if description and multiple_descriptions:
                    raise ImportError('Description file {} describes multiple modules while there is only one in {}'.format(description_file,base_module_name))
                new_cls = MetaModule(name_cls,(BaseModule,),{'__init__':BaseModule.__init__, '__doc__':BaseModule.__doc__})

                def _make_func(module,step):
                    mod_func = getattr(module,get_func_name(step))

                    def func(self):
                        return mod_func(name,self.config_block,self.data_block)

                    return func

                new_cls.set_functions({step:_make_func(module,step) for step in steps})

                return new_cls(name,options=options,description=description,**kwargs)
                #_all_loaded_modules[name] = toret
                #return toret
            else:
                cls.log_debug('No {} functions found in module [{}], trying to load class {}.'.format(steps,name,name_cls),rank=0)
                module_class = name_cls

        # try to load class
        if module_class is not None:
            mod_cls = getattr(module,module_class,None)
            if mod_cls is None:
                raise ValueError('Class {} does not exist in {} [{}]'.format(module_class,base_module_name,name))
            if multiple_descriptions: # get the corresponding description by name
                found = False
                for desc in description:
                    if desc['name'] == module_class:
                        description = desc
                        found = True
                        break
                if not found:
                    cls.log_info('No description found for {} in description file {}.'.format(module_class,description_file),rank=0)
                    description = None
            mod_cls = getattr(module,module_class)
            if issubclass(mod_cls,cls): # if subclass of cls, do not create new class
                return mod_cls(name,options=options,description=description,**kwargs)
            # create BaseModule subclass
            new_cls = MetaModule(mod_cls.__name__,(BaseModule,mod_cls),{'__init__':BaseModule.__init__, '__doc__':mod_cls.__doc__})
            new_cls.set_functions({step:getattr(mod_cls,get_func_name(step)) for step in steps})
            return new_cls(name,options=options,description=description,**kwargs)
            #_all_loaded_modules[name] = toret

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
