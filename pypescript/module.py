"""Definition of :class:`BaseModule` and :class:`BasePipeline`."""

import os
import sys
import logging
import importlib
import ctypes

from . import utils
from .utils import BaseClass
from .block import BlockMapping, DataBlock, SectionBlock
from . import section_names
from .config import ConfigBlock
from .parameter import ParamBlock


def _import_pygraphviz():
    try:
        import pygraphviz as pgv
    except ImportError as e:
        raise ImportError('Please install pygraphviz: see https://github.com/pygraphviz/pygraphviz/blob/master/INSTALL.txt') from e
    return pgv


class BaseModule(BaseClass):
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
    _reserved_option_names = ['module_name','module_class','common_parameters','specific_parameters','datablock_mapping','datablock_copy']
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
        self.logger.info('Init module {}.'.format(self))
        self.set_config_block(options=options,config_block=config_block)
        self.set_parameters()
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
        for param in self.parameters:
            self.data_block[section_names.parameters,param.name] = param.value # no mapping here
        self.data_block.set_mapping(self._mapping)

    def set_parameters(self):
        """
        Set :attr:`parameters`.
        """
        self.parameters = ParamBlock(self.options.get_string('common_parameters',None))
        mapping = {}
        specific = ParamBlock(self.options.get_string('specific_parameters',None))
        for param in specific:
            param.add_suffix(self.name)
            mapping[section_names.parameters,param] = (section_names.parameters,param.name)
        self.parameters.update(specific)
        self._mapping = BlockMapping(self.options.get_dict('datablock_mapping',None),sep='.')
        self._mapping.update(mapping)
        self._copy = BlockMapping(self.options.get_dict('datablock_copy',None),sep='.')

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
        if name in ['setup','execute','cleanup']:
            fun = super(BaseModule,self).__getattribute__(name)

            def wrapper(*args,**kwargs):
                try:
                    fun(*args,**kwargs)
                except Exception as exc:
                    raise RuntimeError('Exception in function {} of {} [{}].'.format(name,self.__class__.__name__,self.name)) from exc

                if name != 'cleanup':
                    for keyg,keyl in self._copy.items():
                        self.data_block[keyl] = self.data_block[keyg]

            return wrapper

        return super(BaseModule,self).__getattribute__(name)

    def __repr__(self):
        """Representation as module class name + module local name (in this pipeline)."""
        return '{} [{}]'.format(self.__class__.__name__,self.name)

    @classmethod
    def from_filename(cls, name, options=None, config_block=None, data_block=None):
        """
        Create :class:`BaseModule`-type module from either module name or module file.

        Parameters
        ----------
        name : string
            Module name, which is set by the pipeline configuration and is a unique module identifier.

        options : SectionBlock, dict, default=None
            Options for this module.
            It should contain an entry 'module_file' OR (exclusive) 'module_name' (w.r.t. 'base_dir', defaulting to '.').
            It may contain an entry 'module_class' containing a class name if the module consists in a class that extends :class:`BaseModule`.

        config_block : DataBlock, dict, string, default=None
            Structure containing configuration options.

        data_block : DataBlock, default=None
            Structure containing data exchanged between modules. If ``None``, creates one.
        """
        options = options or {}
        base_dir = options.get('base_dir','.')
        module_file = options.get('module_file',None)
        module_name = options.get('module_name',None)
        module_class = options.get('module_class',None)
        if module_file is None and module_name is None:
            raise ImportError('Failed importing module [{}]. You must provide a module file or a module name!'.format(name))

        if module_file is not None:
            if module_name is not None:
                raise ImportError('Failed importing module [{}]. Both module file and module name are provided!'.format(name))
            filename = os.path.join(base_dir,module_file)
            cls.logger.info('Importing library {} for module [{}].'.format(filename,name))
            basename = os.path.basename(filename)
            impname = os.path.splitext(basename)[0]
            spec = importlib.util.spec_from_file_location(impname,filename)
            library = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(library)
        else:
            cls.logger.info('Importing module {} for module [{}].'.format(module_name,name))
            library = importlib.import_module(module_name)
            impname = module_name.split('.')[-1]

        if module_class is not None:
            if hasattr(library,module_class):
                lib_cls = getattr(library,module_class)
                if issubclass(lib_cls,BaseModule):
                    return lib_cls(name,options=options,config_block=config_block,data_block=data_block)

        clsname = utils.snake_to_pascal_case(impname)
        lib_cls = type(clsname,(BaseModule,),{'__init__':BaseModule.__init__, '__doc__':BaseModule.__doc__})

        steps = ['setup','execute','cleanup']

        def _make_func(library,step):
            fname = options.get('{}_function'.format(step),step)
            flib = getattr(library,fname,None)

            def func(self):
                return flib(name,self.config_block,self.data_block)

            return func

        for step in steps:
            setattr(lib_cls,step,_make_func(library,step))

        return lib_cls(name,options=options,config_block=config_block,data_block=data_block)


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
        cls.logger.info('Saving graph to {}.'.format(filename))
        utils.mkdir(os.path.dirname(filename))
        graph.draw(filename)


class BasePipeline(BaseModule):
    """
    Extend :class:`BaseModule` to load, set up, execute, and clean up several modules.

    Attributes
    ----------
    modules : list
        List of modules.
    """
    _reserved_option_names = BaseModule._reserved_option_names + ['modules']
    logger = logging.getLogger('BasePipeline')

    def __init__(self, name='main', options=None, config_block=None, data_block=None, modules=None):
        """
        Initalise :class:`BasePipeline`.

        Parameters
        ----------
        name : string, default='main'
            See :class:`BaseModule` documentation.
            Defaults to 'main', the root of the full pipeline tree.

        options : SectionBlock, dict, default=None
            Options for this module.
            It should contain an entry 'modules' listing module names to load (defaults to empty list).

        config_block : DataBlock, dict, string, default=None
            Structure containing configuration options.

        data_block : DataBlock, default=None
            Structure containing data exchanged between modules. If ``None``, creates one.

        modules : list, default=None
            List of modules, which will be completed by those in 'modules' entry of options.
        """
        self.modules = modules or []
        super(BasePipeline,self).__init__(name,options=options,config_block=config_block,data_block=data_block)
        # modules will automatically inherit config_block, pipe_block, no need to reset set_config_block() and set_data_block()
        self.modules += self._get_modules_from_filename(self.options.get_list('modules',default=[]))
        self.set_parameters()

    def set_config_block(self, options=None, config_block=None):
        """
        Set :attr:`config_block` and :attr:`options`.
        It merges :attr:`config_block` of each module of the pipeline and update them with the merged version.

        Parameters
        ----------
        options : SectionBlock, dict, default=None
            Options for this module, which update those in ``config_block``.

        config_block : DataBlock, dict, string, default=None
            Structure containing configuration options, which will be updated with ``options``.
        """
        super(BasePipeline,self).set_config_block(options=options,config_block=config_block)
        for module in self:
            self.config_block.update(module.config_block)
        for module in self:
            module.set_config_block(config_block=self.config_block)
        self.options = SectionBlock(self.config_block,self.name)

    def set_data_block(self, data_block=None):
        """
        Set :attr:`data_block` and :attr:`pipe_block`, a local shallow copy of :attr:`data_block`.
        It updates :attr:`data_block` of each module of the pipeline with :attr:`pipe_block`.
        Hence, changes to :attr:`pipe_block` do not propage in the higher level part of the full pipeline.

        Parameters
        ----------
        data_block : DataBlock, default=None
            :class:`DataBlock` instance used by the module to retrieve and store items.
            If ``None``, creates one.
        """
        super(BasePipeline,self).set_data_block(data_block=data_block)
        self.pipe_block = self.data_block.copy() # shallow copy
        for module in self:
            module.set_data_block(self.pipe_block)

    def set_parameters(self):
        super(BasePipeline,self).set_parameters()
        for module in self:
            self.parameters.update(module.parameters)
            module.parameters = self.parameters

    def _get_modules_from_filename(self, names):
        """Convenient method to load modules for module names."""
        modules = []
        for name in names:
            module = BaseModule.from_filename(name=name,options=SectionBlock(self.config_block,name),config_block=self.config_block,data_block=self.pipe_block)
            modules.append(module)
        return modules

    def __iter__(self):
        """Iterate over :attr:`modules`."""
        yield from self.modules

    def setup(self):
        """Set up :attr:`modules`."""
        for module in self:
            module.setup()
        return 0

    def execute(self):
        """Execute :attr:`modules`."""
        for module in self:
            module.execute()
        return 0

    def execute_parameter_values(self, **kwargs):
        for name,value in kwargs.items():
            self.pipe_block[section_names.parameters,name] = value
        self.execute()

    def cleanup(self):
        """Clean up :attr:`modules`."""
        for module in self:
            module.cleanup()
        del self.pipe_block
        return 0

    def plot_pipeline_graph(self, filename):
        """Plot pipeline as a graph to ``filename``."""
        pgv = _import_pygraphviz()
        graph = pgv.AGraph(strict=True,directed=True)

        def norm_name(module):
            return '{}\\n[{}]'.format(module.__class__.__name__,module.name)

        def callback(module,prevmodule):
            graph.add_node(norm_name(module),color='lightskyblue',style='filled',group='pipeline',shape='box')
            graph.add_edge(norm_name(module),norm_name(prevmodule),color='lightskyblue',style='bold',arrowhead='none')
            if isinstance(module,BasePipeline):
                for newmodule in module.modules:
                    callback(newmodule,module)

        for module in self.modules:
            callback(module,self)

        graph.layout('dot')
        self.logger.info('Saving graph to {}.'.format(filename))
        utils.mkdir(os.path.dirname(filename))
        graph.draw(filename)
