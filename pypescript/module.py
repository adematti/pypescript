"""Definition of :class:`BaseModule` and :class:`BasePipeline`."""

import os
import sys
import logging
import importlib

from . import utils
from .block import BlockMapping, DataBlock, SectionBlock
from . import syntax, section_names
from .config import ConfigBlock


def _import_pygraphviz():
    try:
        import pygraphviz as pgv
    except ImportError as e:
        raise ImportError('Please install pygraphviz: see https://github.com/pygraphviz/pygraphviz/blob/master/INSTALL.txt') from e
    return pgv


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

    def __init__(self, name, options=None, config_block=None, data_block=None):
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
        """
        self.name = name
        self.log_info('Init module {}.'.format(self),rank=0)
        self.set_config_block(options=options,config_block=config_block)
        self.set_data_block(data_block=data_block)

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
                    if keyg in self.data_block:
                        self.data_block[keyl] = self.data_block[keyg]

            return wrapper

        return object.__getattribute__(self,name)

    def __str__(self):
        """String as module class name + module local name (in this pipeline)."""
        return '{} [{}]'.format(self.__class__.__name__,self.name)

    @classmethod
    def from_filename(cls, name, options=None, config_block=None, data_block=None):
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
        module_file = options.get(syntax.module_file,None)
        module_name = options.get(syntax.module_name,None)
        module_class = options.get(syntax.module_class,None)
        if module_file is None and module_name is None:
            raise ImportError('Failed importing module [{}]. You must provide a module file or a module name'.format(name))

        if module_file is not None:
            if module_name is not None:
                raise ImportError('Failed importing module [{}]. Both module file and module name are provided'.format(name))
            filename = os.path.join(base_dir,module_file)
            cls.log_info('Importing module {} [{}].'.format(filename,name),rank=0)
            basename = os.path.basename(filename)
            name_mod = os.path.splitext(basename)[0]
            spec = importlib.util.spec_from_file_location(name_mod,filename)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            cls.log_info('Importing module {} [{}].'.format(module_name,name),rank=0)
            module = importlib.import_module(module_name)
            name_mod = syntax.split_sections(module_name)[-1]

        steps = [syntax.setup_function,syntax.execute_function,syntax.cleanup_function]

        def get_func_name(step):
            return options.get('{}_function'.format(step),step)

        if module_class is None:
            name_cls = utils.snake_to_pascal_case(name_mod)
            if all(hasattr(module,get_func_name(step)) for step in steps):
                new_cls = type(name_cls,(BaseModule,),{'__init__':BaseModule.__init__, '__doc__':BaseModule.__doc__})

                def _make_func(module,step):
                    mod_func = getattr(module,get_func_name(step))

                    def func(self):
                        return mod_func(name,self.config_block,self.data_block)

                    return func

                for step in steps:
                    setattr(new_cls,step,_make_func(module,step))
                return new_cls(name,options=options,config_block=config_block,data_block=data_block)
                #_all_loaded_modules[name] = toret
                #return toret
            else:
                cls.log_info('No {} functions found in module [{}], trying to load class {}.'.format(steps,name,name_cls),rank=0)
                module_class = name_cls

        if module_class is not None:
            if hasattr(module,module_class):
                mod_cls = getattr(module,module_class)
                if issubclass(mod_cls,cls):
                    toret = mod_cls(name,options=options,config_block=config_block,data_block=data_block)
                else:
                    new_cls = type(mod_cls.__name__,(BaseModule,mod_cls),{'__init__':BaseModule.__init__, '__doc__':mod_cls.__doc__})
                    for step in steps:
                        setattr(new_cls,step,getattr(mod_cls,get_func_name(step)))
                    toret = new_cls(name,options=options,config_block=config_block,data_block=data_block)
                #_all_loaded_modules[name] = toret
                return toret
            else:
                raise ValueError('Class {} does not exist in {} [{}]'.format(module_class,name_mod,name))


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
