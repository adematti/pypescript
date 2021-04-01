"""Definition of :class:`BaseModule` and :class:`BasePipeline`."""

import os
import sys
import logging
import importlib
import ctypes

from . import utils
from .block import BlockMapping, DataBlock, SectionBlock
from . import section_names
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
    _reserved_option_names = ['module_name','module_file','module_class','datablock_mapping','datablock_copy']
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
        self._datablock_mapping = BlockMapping(self.options.get_dict('datablock_mapping',None),sep='.')
        self._datablock_copy = BlockMapping(self.options.get_dict('datablock_copy',None),sep='.')

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
        if name in ['setup','execute','cleanup']:
            fun = object.__getattribute__(self,name)

            def wrapper(*args,**kwargs):
                try:
                    fun(*args,**kwargs)
                except Exception as exc:
                    raise RuntimeError('Exception in function {} of {} [{}].'.format(name,self.__class__.__name__,self.name)) from exc

                for keyg,keyl in self._datablock_copy.items():
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
        #    raise SyntaxError('You should NOT use the same module name in different pipelines. Create a new module, and use configblock_copy if useful!')
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
            cls.log_info('Importing module {} [{}].'.format(filename,name),rank=0)
            basename = os.path.basename(filename)
            name_mod = os.path.splitext(basename)[0]
            spec = importlib.util.spec_from_file_location(name_mod,filename)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            cls.log_info('Importing module {} [{}].'.format(module_name,name),rank=0)
            module = importlib.import_module(module_name)
            name_mod = module_name.split('.')[-1]

        steps = ['setup','execute','cleanup']

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
                cls.log_info('No setup, execute and cleanup functions found in module [{}], trying to load class {}.'.format(name,name_cls),rank=0)
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


class ModuleTodo(object):

    def __init__(self, pipeline, module, funcnames=None):
        self.pipeline = pipeline
        self.module = module
        if isinstance(funcnames,str):
            funcnames = [funcnames]
        self.funcnames = funcnames or []
        self.func = [getattr(module,funcname) for funcname in self.funcnames]

    def __repr__(self):
        return 'ModuleToDo(pipeline=[{}],module=[{}],funcnames={})'.format(self.pipeline.name,self.module.name,self.funcnames)

    def __call__(self):
        self.module.set_data_block(self.pipeline.pipe_block)
        for func in self.func: func()


class BasePipeline(BaseModule):
    """
    Extend :class:`BaseModule` to load, set up, execute, and clean up several modules.

    Attributes
    ----------
    modules : list
        List of modules.
    """
    _reserved_option_names = BaseModule._reserved_option_names + ['modules','setup','execute','cleanup']
    logger = logging.getLogger('BasePipeline')

    def __init__(self, name='main', options=None, config_block=None, data_block=None, modules=None, setup=None, execute=None, cleanup=None):
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
            List of modules, which will be completed by those in 'setup', 'execute' and 'cleanup' entries of options.
        """
        modules = modules or []
        setup_todos = setup or []
        execute_todos = execute or []
        cleanup_todos = cleanup or []
        self.modules = []
        self._datablock_bcast = []
        super(BasePipeline,self).__init__(name,options=options,config_block=config_block,data_block=data_block)
        # modules will automatically inherit config_block, pipe_block, no need to reset set_config_block() and set_data_block()
        modules += self.options.get_list('modules',default=[])
        setup_todos += self.options.get_list('setup',default=[])
        execute_todos += self.options.get_list('execute',default=[])
        cleanup_todos += self.options.get_list('cleanup',default=[])
        self.pipe_block = self.data_block.copy()
        self.set_todos(modules=modules,setup_todos=setup_todos,execute_todos=execute_todos,cleanup_todos=cleanup_todos)
        self._iter = self.options.get('iter',None)
        self.nprocs_per_task = self.options.get('nprocs_per_task',1)
        if self._iter is not None and not isinstance(self._iter,list):
            self._iter = eval(self._iter)
        for block in ['configblock_iter','datablock_iter','datablock_iter_key']:
            block_iter = {}
            for key,value in self.options.get_dict(block,{}).items():
                key = utils.split_section_name(key)
                if len(key) != 2:
                    raise ValueError('Incorrect {} key: {}.'.format(block,key))
                if value is None:
                    value = lambda i: i
                #elif isinstance(value,list):
                #    if not len(value) == len(self._iter):
                #        raise ValueError('{} {} list must be of the same length as iter = {:d}.'.format(block,key,len(self._iter)))
                #    value = lambda i: value[i]
                else:
                    value = eval(value)
                    if not callable(value):
                        raise TypeError('Incorrect {} value: {}.'.format(block,value))
                        #value = lambda i: value[i]
                block_iter[key] = value
            setattr(self,'_{}'.format(block),block_iter)

        if self._iter is not None:
            for key in self._datablock_iter_key:
                tmp = []
                for i in self._iter:
                    value = self._datablock_iter_key[key](i)
                    value = utils.split_section_name(value)
                    if value in self._datablock_bcast:
                        raise ValueError('DataBlock key {} must appear only once for all iterations.'.format(value))
                    tmp.append(value)
                self._datablock_iter_key[key] = tmp
                self._datablock_bcast += tmp
        self.set_config_block(config_block=self.config_block)

    def set_config_block(self, options=None, config_block=None):
        #super(BasePipeline,self).set_config_block(options=options,config_block=config_block)
        self.config_block = ConfigBlock(config_block)
        if options is not None:
            for name,value in options.items():
                self.config_block[self.name,name] = value
        self.options = SectionBlock(self.config_block,self.name)
        self._datablock_mapping = BlockMapping(self.options.get_dict('datablock_mapping',None),sep='.')
        datablock_copy = self.options.get('datablock_copy',None)
        if datablock_copy is not None:
            if isinstance(datablock_copy,list):
                datablock_copy = {key:key for key in datablock_copy}
            datablock_copy = {key:value if value is not None else key for key,value in datablock_copy.items()}
        self._datablock_copy = BlockMapping(datablock_copy,sep='.')
        for key in self._datablock_bcast:
            self._datablock_copy[key] = key
        for module in self.modules:
            self.config_block.update(module.config_block)
        for module in self.modules:
            module.set_config_block(config_block=self.config_block)

    def set_todos(self, modules=None, setup_todos=None, execute_todos=None, cleanup_todos=None):
        self.modules = [self.get_module_from_name(module) if isinstance(module,str) else module for module in modules]
        setup_todos = setup_todos or []
        execute_todos = execute_todos or []
        cleanup_todos = cleanup_todos or []
        modules_todo = {}
        self.setup_todos = []
        self.execute_todos = []
        self.cleanup_todos = []

        for step,todos in zip(['setup','execute','cleanup'],[setup_todos,execute_todos,cleanup_todos]):
            for itodo,module_todo in enumerate(todos):
                if isinstance(module_todo,dict):
                    for module,todo in module_todo.items():
                        break
                elif isinstance(module_todo,str):
                    split = module_todo.split(':')
                    if len(split) == 1:
                        split = (split[0],step)
                    module,todo = split
                else:
                    module,todo = module_todo
                if isinstance(module,str):
                    module_names = [module.name for module in self.modules]
                    if module in module_names:
                        module = self.modules[module_names.index(module)]
                    else:
                        module = self.get_module_from_name(module)
                        self.modules.append(module)
                if module.name not in modules_todo:
                    modules_todo[module.name] = []
                last_step = modules_todo[module.name][-1] if modules_todo[module.name] else None
                funcnames = []
                if todo == 'setup':
                    if last_step != 'cleanup': funcnames.append('cleanup')
                    funcnames.append(todo)
                elif todo == 'execute':
                    if last_step not in ['setup','execute']: funcnames.append('setup')
                    funcnames.append(todo)
                elif todo == 'cleanup':
                    if last_step not in ['setup','execute']: funcnames.append('setup')
                    funcnames.append(todo)
                modules_todo[module.name] += funcnames
                self_todos = getattr(self,'{}_todos'.format(step))
                self_todos += [ModuleTodo(self,module,funcnames=funcnames)]
        module_names = [module.name for module in self.modules]

        for module_name,todo in modules_todo.items():
            if todo[-1] != 'cleanup':
                module = self.modules[module_names.index(module_name)]
                self.cleanup_todos += [ModuleTodo(self,module,funcnames='cleanup')]
        for module in self.modules:
            if module.name not in modules_todo:
                self.setup_todos += [ModuleTodo(self,module,funcnames='setup')]
                self.execute_todos += [ModuleTodo(self,module,funcnames='execute')]
                self.cleanup_todos += [ModuleTodo(self,module,funcnames='cleanup')]

    def get_module_from_name(self, name):
        options = SectionBlock(self.config_block,name)
        #if not any(key in options for key in ['module_name','module_file','module_class']):
        #    options['module_name'] = 'pypescript.module'
        #    options['module_class'] = 'BasePipeline'
        return BaseModule.from_filename(name=name,options=options,config_block=self.config_block,data_block=self.pipe_block)

    def setup(self):
        """Set up :attr:`modules`."""
        self.pipe_block = self.data_block.copy()
        for todo in self.setup_todos:
            todo()

    @staticmethod
    def mpi_distribute(data_block, dests, mpicomm=None):
        for key,value in data_block.items():
            if hasattr(value,'mpi_distribute'):
                data_block[key] = value.mpi_distribute(dests=dests,mpicomm=mpicomm)
        return data_block

    def execute(self):
        """Execute :attr:`modules`."""
        pipe_block = self.pipe_block = self.data_block.copy()
        if self._iter is None:
            for todo in self.execute_todos:
                todo()
        else:
            key_to_ranks = {key:None for key in self._datablock_bcast}

            with utils.TaskManager(nprocs_per_task=self.nprocs_per_task,mpicomm=self.mpicomm) as tm:

                data_block = BasePipeline.mpi_distribute(self.data_block.copy(),dests=tm.self_worker_ranks,mpicomm=tm.mpicomm)

                for itask,task in tm.iterate(list(enumerate(self._iter))):
                    self.pipe_block = data_block.copy()
                    self.pipe_block['mpi','comm'] = tm.mpicomm
                    for key,value in self._configblock_iter.items():
                        self.config_block[key] = task if value is None else value(task)
                    for key,value in self._datablock_iter.items():
                        self.pipe_block[key] = task if value is None else value(task)
                    for todo in self.execute_todos:
                        todo()
                    for keyg,keyl in self._datablock_iter_key.items():
                        key = keyl[itask]
                        pipe_block[key] = self.pipe_block[keyg]
                        #if tm.mpicomm.rank == 0:
                        #    key_to_ranks[key] = tm.basecomm.rank
                        #key_to_ranks[key] = tm.mpicomm.allgather(tm.basecomm.rank)
                        key_to_ranks[key] = tm.basecomm.rank

                tm.basecomm.Barrier()
                for key in self._datablock_bcast:
                    ranks = tm.basecomm.allgather(key_to_ranks[key])
                    ranks = [r for r in ranks if r is not None]
                    if not ranks:
                        raise RuntimeError('(section, name) = {} has not been added to pipe_block'.format(key))
                    #elif len(ranks) > 1:
                    #    raise RuntimeError('(section, name) = {} has been used {} times'.format(key,len(ranks)))
                    #key_to_ranks[key] = ranks[0]
                    key_to_ranks[key] = ranks

                #tm.basecomm.Barrier()
                for key in self._datablock_bcast:
                    cls = None
                    if tm.basecomm.rank in key_to_ranks[key]:
                        toranks = tm.other_ranks
                        value = pipe_block[key]
                        if hasattr(value,'mpi_collect'):
                            cls = value.__class__
                            if tm.basecomm.rank == key_to_ranks[key][0]:
                                for rank in toranks:
                                    tm.basecomm.send(cls,dest=rank,tag=41)
                        elif tm.basecomm.rank == key_to_ranks[key][0]:
                            for rank in toranks:
                                tm.basecomm.send(None,dest=rank,tag=41)
                                tm.basecomm.send(pipe_block[key],dest=rank,tag=42)
                    else:
                        cls = tm.basecomm.recv(source=key_to_ranks[key][0],tag=41)
                        if cls is None:
                            pipe_block[key] = tm.basecomm.recv(source=key_to_ranks[key][0],tag=42)
                    if cls is not None:
                        pipe_block[key] = cls.mpi_collect(pipe_block.get(*key,None),sources=key_to_ranks[key],mpicomm=tm.basecomm)

                self.pipe_block = pipe_block

    def cleanup(self):
        """Clean up :attr:`modules`."""
        self.pipe_block = self.data_block.copy()
        for todo in self.cleanup_todos:
            todo()

    def __getattribute__(self, name):
        """
        Extends builtin :meth:`__getattribute__` to complement exceptions occuring in :meth:`setup`,
        :meth:`execute` and :meth:`cleanup` with module class and local name, for easy debugging.
        """
        if name in ['setup','execute','cleanup']:
            fun = object.__getattribute__(self,name)

            def wrapper(*args,**kwargs):
                try:
                    fun(*args,**kwargs)
                except Exception as exc:
                    raise RuntimeError('Exception in function {} of {} [{}].'.format(name,self.__class__.__name__,self.name)) from exc

                for keyg,keyl in self._datablock_copy.items():
                    if keyg in self.pipe_block:
                        self.data_block[keyl] = self.pipe_block[keyg]

            return wrapper

        return object.__getattribute__(self,name)


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
        self.log_info('Saving graph to {}.'.format(filename),rank=0)
        utils.mkdir(os.path.dirname(filename))
        graph.draw(filename)
