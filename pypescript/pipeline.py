"""Definition of :class:`BasePipeline` and subclasses."""

import os
import logging
import subprocess

import numpy as np

from .module import BaseModule, MetaModule, _import_pygraphviz
from . import syntax
from . import utils
from .block import BlockMapping, DataBlock, SectionBlock
from .config import ConfigBlock, ConfigError


class ModuleTodo(object):
    """
    Helper class to run module :meth:`BaseModule.setup`, :meth:`BaseModule.execute` and :meth:`BaseModule.cleanup`,
    based on each module's :attr:`BaseModule._state` and following decision tree:

    - if run 'setup': if state is 'setup' or 'execute', run 'cleanup' first
    - if run 'execute': if state is 'cleanup', run 'setup' first
    """
    _decision_tree = {'setup':{'setup':['cleanup','setup'],'execute':['cleanup','setup'],'cleanup':['setup']},
                      'execute':{'setup':['execute'],'execute':['execute'],'cleanup':['setup','execute']},
                      'cleanup':{'setup':['cleanup'],'execute':['cleanup'],'cleanup':[]}}

    def __init__(self, pipeline, module, step):
        """
        Instantiate :class:`ModuleTodo`.

        Parameters
        ----------
        pipeline : BasePipeline
            Pipeline instance that will call :class:`ModuleTodo` instance.

        module : BaseModule
            Module to run.

        step : string
            ``module`` method to call.
        """
        self.pipeline = pipeline
        self.module = module
        self.step = step

    def __repr__(self):
        return 'ModuleTodo(pipeline=[{}],module=[{}],steps={})'.format(self.pipeline.name,self.module.name,self.todo())

    def set_data_block(self):
        """Set module :attr:`BaseModule.data_block` to :attr:`BasePipeline.pipe_block`."""
        self.module.set_data_block(self.pipeline.pipe_block)

    def todo(self):
        """Return list of steps to run."""
        return self._decision_tree[self.step][self.module._state]

    def __call__(self):
        """Run module: set :attr:`BaseModule.data_block` and call module methods."""
        todo = self.todo()
        if todo:
            self.set_data_block()
            for step in todo:
                getattr(self.module,step)()


class MetaPipeline(MetaModule):

    """Meta class to replace :meth:`setup`, :meth:`execute` and :meth:`cleanup` pipeline methods."""

    def set_functions(cls, functions):
        """
        Wrap input ``functions`` and add corresponding methods to class ``cls``.
        Specifically:

        - before ``functions`` calls, fills in :attr:`BasePipeline.data_block` with values specified in :attr:`BasePipeline._datablock_set`
        - after ``functions`` calls, copy entries of :attr:`BasePipeline.pipe_block` into :attr:`BasePipeline.data_block`
          with key pairs specified in :attr:`BasePipeline._datablock_duplicate`
        - set pipeline :attr:`BasePipeline._state`
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
                        #print('db',self.name,keyg,keyl,id(self.data_block[keyl]))
                        self.data_block[keyg] = self.data_block[keyl]
                    elif keyl in self.pipe_block: # because not necessarily present at each step...
                        #print('pb',self.name,keyg,keyl,id(self.pipe_block[keyl]))
                        self.data_block[keyg] = self.pipe_block[keyl]

                self._state = step

            return wrapper

        for step,fun in functions.items():
            setattr(cls,step,make_wrapper(step,fun))


class BasePipeline(BaseModule,metaclass=MetaPipeline):
    """
    Extend :class:`BaseModule` to load, set up, execute, and clean up several modules.

    Attributes
    ----------
    modules : list
        List of modules.
    """
    logger = logging.getLogger('BasePipeline')
    _available_options = BaseModule._available_options + [syntax.modules,syntax.setup,syntax.execute,syntax.cleanup]

    def __init__(self, name=syntax.main, options=None, config_block=None, data_block=None, description=None, pipeline=None, modules=None, setup=None, execute=None, cleanup=None):
        """
        Initalize :class:`BasePipeline`.

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

        description : string, ModuleDescription, dict, default=None
            Module description.

        pipeline : BasePipeline
            Pipeline instance for which this (sub-)pipeline was created.

        modules : list, default=None
            List of modules, which will be completed by those in 'setup', 'execute' and 'cleanup' entries of options.

        setup : list, default=None
            List of 'module.method' (``method`` being one of ('setup', 'execute', 'cleanup')) strings.
            If ``method`` not specified, defaults to :meth:`BaseModule.setup`.

        execute : list, default=None
            List of 'module.method' (``method`` being one of ('setup', 'execute', 'cleanup')) strings.
            If ``method`` not specified, defaults to :meth:`BaseModule.execute`.

        cleanup : list, default=None
            List of 'module:method' (``method`` being one of ('setup', 'execute', 'cleanup')) strings.
            If ``method`` not specified, defaults to :meth:`BaseModule.cleanup`.
        """
        modules = modules or []
        setup_todos = setup or []
        execute_todos = execute or []
        cleanup_todos = cleanup or []
        self.modules = {} # because set_config_block will be called by __init__
        #self._datablock_bcast = []
        super(BasePipeline,self).__init__(name,options=options,config_block=config_block,data_block=data_block,description=description,pipeline=pipeline)
        # modules will automatically inherit config_block, pipe_block, no need to reset set_config_block() and set_data_block()
        modules += self.options.get_list(syntax.modules,default=[])
        setup_todos += self.options.get_list(syntax.setup,default=[])
        execute_todos += self.options.get_list(syntax.execute,default=[])
        cleanup_todos += self.options.get_list(syntax.cleanup,default=[])
        self.pipe_block = self.data_block.copy()
        self.set_todos(modules=modules,setup_todos=setup_todos,execute_todos=execute_todos,cleanup_todos=cleanup_todos)
        self.set_config_block(config_block=self.config_block)

    def set_config_block(self, options=None, config_block=None):
        """
        Set :attr:`config_block` and :attr:`options`.
        :attr:`config_block` is updated by that of all :attr:`modules`, then the resulting
        block is set in all :attr:`modules`.
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
        #super(BasePipeline,self).set_config_block(options=options,config_block=config_block)
        self.config_block = ConfigBlock(config_block)
        if options is not None:
            for name,value in options.items():
                self.config_block[self.name,name] = value
        self.options = SectionBlock(self.config_block,self.name)
        self._datablock_set = {syntax.split_sections(key):value for key,value in syntax.collapse_sections(self.options.get_dict(syntax.datablock_set,{}),maxdepth=2).items()}
        self._datablock_mapping = BlockMapping(syntax.collapse_sections(self.options.get_dict(syntax.datablock_mapping,{}),sep=syntax.section_sep),sep=syntax.section_sep)
        datablock_duplicate = self.options.get(syntax.datablock_duplicate,None)
        if datablock_duplicate is not None:
            if isinstance(datablock_duplicate,list):
                datablock_duplicate = {key:key for key in datablock_duplicate}
            else:
                datablock_duplicate = syntax.collapse_sections(datablock_duplicate,sep=syntax.section_sep)
            datablock_duplicate = {key:value if value is not None else key for key,value in datablock_duplicate.items()}
        self._datablock_duplicate = BlockMapping(datablock_duplicate,sep=syntax.section_sep)
        #for key in self._datablock_bcast:
        #    self._datablock_duplicate[key] = key
        for module in self.modules.values():
            self.config_block.update(module.config_block)
        for module in self.modules.values():
            module.set_config_block(config_block=self.config_block)

    def set_todos(self, modules=None, setup_todos=None, execute_todos=None, cleanup_todos=None):
        """Prepare :class:`ModuleTodo` instances for setup, execute, and cleanup."""
        setup_todos = setup_todos or []
        execute_todos = execute_todos or []
        cleanup_todos = cleanup_todos or []
        modules_todo = []

        for step,todos in zip([syntax.setup_function,syntax.execute_function,syntax.cleanup_function],[setup_todos,execute_todos,cleanup_todos]):
            self_todos = []
            setattr(self,'{}_todos'.format(step),self_todos)
            for itodo,module_todo in enumerate(todos):
                split = syntax.split_sections(module_todo,sep=syntax.module_function_sep)
                if len(split) == 1:
                    split = (split[0],step)
                module,todo = split
                module = self.add_module(module)
                if module.name not in modules_todo:
                    modules_todo.append(module.name)
                self_todos.append(ModuleTodo(self,module,step=todo))

        for name in modules_todo:
            self.cleanup_todos.append(ModuleTodo(self,self.modules[name],step=syntax.cleanup_function)) # just to make sure cleanup is run

        for module in modules:
            module = self.add_module(module)
            if module.name not in modules_todo:
                self.setup_todos.append(ModuleTodo(self,module,step=syntax.setup_function))
                self.execute_todos.append(ModuleTodo(self,module,step=syntax.execute_function))
                self.cleanup_todos.append(ModuleTodo(self,module,step=syntax.cleanup_function)) # just to make sure cleanup is run

    def add_module(self, module):
        if isinstance(module,str):
            if module.startswith(syntax.module_reference): # reference to module
                module = self.fetch_module(module[1:])
                if module.name in self.modules and module is not self.modules[module.name]:
                    raise ConfigError('Cannot reference a module with same name as an already loaded module'.format(module.name))
            else:
                # first search in loaded modules
                if module in self.modules.keys():
                    module = self.modules[module]
                else: # load it
                    module = self.get_module_from_name(module)
        if module._pipeline is None: module._pipeline = self
        self.modules[module.name] = module
        self.config_block.update(module.config_block)
        for mod in self.modules.values():
            mod.set_config_block(config_block=self.config_block)
        return module

    def get_module_from_name(self, name):
        """Return :class:`BaseModule` instance corresponding to module (pipeline) name."""
        options = SectionBlock(self.config_block,name)
        return BaseModule.from_filename(name=name,options=options,config_block=self.config_block,data_block=self.pipe_block,pipeline=self)

    def setup(self):
        """Set up :attr:`modules`, fed with :attr:`pipe_block`, a copy of :attr:`data_block`."""
        self.pipe_block = self.data_block.copy()
        for todo in self.setup_todos:
            todo()

    def execute(self):
        """Execute :attr:`modules`, fed with :attr:`pipe_block`, a copy of :attr:`data_block`."""
        self.pipe_block = self.data_block.copy()
        for todo in self.execute_todos:
            todo()

    def cleanup(self):
        """Clean up :attr:`modules`, fed with :attr:`pipe_block`, a copy of :attr:`data_block`."""
        self.pipe_block = self.data_block.copy()
        for todo in self.cleanup_todos:
            todo()

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
                for newmodule in module.modules.values():
                    callback(newmodule,module)

        for module in self.modules.values():
            callback(module,self)

        graph.layout('dot')
        self.log_info('Saving graph to {}.'.format(filename),rank=0)
        utils.mkdir(os.path.dirname(filename))
        graph.draw(filename)


class StreamPipeline(BasePipeline):
    """Extend :class:`BasePipeline` to load, set up, execute, and clean up several modules without copying :attr:`data_block`."""

    def setup(self):
        """Set up :attr:`modules`."""
        self.pipe_block = self.data_block
        for todo in self.setup_todos:
            todo()

    def execute(self):
        """Execute :attr:`modules`."""
        self.pipe_block = self.data_block
        for todo in self.execute_todos:
            todo()

    def cleanup(self):
        """Clean up :attr:`modules`."""
        self.pipe_block = self.data_block
        for todo in self.cleanup_todos:
            todo()


def _make_callable_from_array(array):

    def func(i):
        return array[i]

    return func


class MPIPipeline(BasePipeline):
    """
    Extend :class:`BasePipeline` to execute several modules in parallel with MPI.

    Attributes
    ----------
    nprocs_per_task : int
        Number of processes for each task.

    _iter : list, iterator
        Tasks to iterate on in the :meth:`execute` step.

    _configblock_iter : dict
        Mapping of :attr:`config_block` entry to callable giving value for each iteration.

    _datablock_iter : dict
        Mapping of :attr:`data_block` entry to callable giving value for each iteration.

    _datablock_key_iter : dict
        Mapping of :attr:`data_block` entry, to list of :attr:`data_block` keys,
        pointing to the :attr:`data_block` entry the where to store result for all iterations.
    """
    logger = logging.getLogger('MPIPipeline')
    _available_options = BasePipeline._available_options + [syntax.iter,syntax.nprocs_per_task,syntax.configblock_iter,syntax.datablock_iter,syntax.datablock_key_iter]

    def set_iter(self):
        self._iter = self.options.get(syntax.iter,None)
        if self._iter is not None and np.ndim(self._iter) == 0:
            # most certainly the number of iterations
            self._iter = range(self._iter)
        self.nprocs_per_task = self.options.get_int(syntax.nprocs_per_task,1)
        for block_name in ['configblock_iter','datablock_iter','datablock_key_iter']:
            block_keyword = getattr(syntax,block_name)
            block_iter = {}
            for key,value in syntax.collapse_sections(self.options.get_dict(block_keyword,{}),sep=None).items():
                if len(key) != 2:
                    raise ConfigError('Incorrect {} key: {}.'.format(block_keyword,key))
                if isinstance(value,(list,tuple)):
                    if self._iter is None:
                        self._iter = range(len(value))
                    if not len(value) == len(self._iter):
                        raise ConfigError('{} {} list must be of the same length as iter = {:d}.'.format(block_keyword,key,len(self._iter)))
                    value = _make_callable_from_array(value)
                elif not callable(value):
                    raise TypeError('Incorrect {} value: {}.'.format(block_keyword,value))
                block_iter[key] = value
            setattr(self,'_{}'.format(block_name),block_iter)

        self._datablock_bcast = []
        if self._iter is not None:
            for key in self._datablock_key_iter:
                tmp = []
                for i in self._iter:
                    value = self._datablock_key_iter[key](i)
                    value = syntax.split_sections(value,default_section=key[0])
                    if value in self._datablock_bcast:
                        raise ConfigError('DataBlock key {} must appear only once for all iterations.'.format(value))
                    tmp.append(value)
                self._datablock_key_iter[key] = tmp
                self._datablock_bcast += tmp
            for key in self._datablock_bcast:
                self._datablock_duplicate[key] = key

    def setup(self):
        """Set up :attr:`modules`, fed with :attr:`pipe_block`, a copy of :attr:`data_block`."""
        self.set_iter()
        super(MPIPipeline,self).setup()

    def execute(self):
        """Execute :attr:`modules`, fed with :attr:`pipe_block`, a copy of :attr:`data_block`, for all iterations."""
        self.run_iter(self.execute_todos)

    def run_iter(self, todos):
        """Run list of :class:`ModuleTodo` for all iterations."""
        pipe_block = self.pipe_block = self.data_block.copy()
        if self._iter is None:
            for todo in todos:
                todo()
        else:
            key_to_ranks = {key:None for key in self._datablock_bcast}
            #for key,value in self._configblock_iter.items():
            #    for task in self._iter:
            #        print(key,value(task))

            with utils.TaskManager(nprocs_per_task=self.nprocs_per_task,mpicomm=self.mpicomm) as tm:

                data_block = self.data_block.copy().mpi_distribute(dests=tm.self_worker_ranks,mpicomm=tm.mpicomm)

                for itask,task in tm.iterate(list(enumerate(self._iter))):
                    self.pipe_block = data_block.copy()
                    #self.pipe_block['mpi','comm'] = tm.mpicomm
                    for key,value in self._configblock_iter.items():
                        self.config_block[key] = value(task)
                    for key,value in self._datablock_iter.items():
                        self.pipe_block[key] = value(task)
                    for todo in todos:
                        todo()
                    for keyg,keyl in self._datablock_key_iter.items():
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
                                tm.basecomm.send(value,dest=rank,tag=42)
                    else:
                        cls = tm.basecomm.recv(source=key_to_ranks[key][0],tag=41)
                        if cls is None:
                            pipe_block[key] = tm.basecomm.recv(source=key_to_ranks[key][0],tag=42)
                    if cls is not None:
                        pipe_block[key] = cls.mpi_collect(pipe_block.get(*key,None),sources=key_to_ranks[key],mpicomm=tm.basecomm)

                self.pipe_block = pipe_block



class BatchError(Exception):

    """Exception raised when issue with batch job."""



class BatchPipeline(MPIPipeline):
    """
    Extend :class:`MPIPipeline` to execute a subpipeline with a batch job.

    Attributes
    ----------
    job_dir : string
        Directory path where to save config files, :class:`DataBlock` instance and possibly batch submission script for each job.

    mpiexec : string
        Name of MPI executable. Used when :attr:`job_template` not provided.

    job_template : string
        Template for job submission scripts.
        Should contain patterns ``{command}`` for command, and :attr:`job_options` keys.

    job_submit : string
        Command to submit job.

    job_options : dict
        Options for job.
    """
    logger = logging.getLogger('BatchPipeline')
    _available_options = MPIPipeline._available_options + [syntax.mpiexec,syntax.hpc_job_dir,syntax.hpc_job_submit,syntax.hpc_job_template,syntax.hpc_job_options]

    def __init__(self, *args, **kwargs):
        super(BatchPipeline,self).__init__(*args,**kwargs)
        setup_modules = [todo.module for todo in self.setup_todos]
        for todo in self.execute_todos:
            if todo.module in setup_modules:
                raise ConfigError('{} requires module [{}] to run entirely (setup, execute) in the pipeline execute step.'.format(self.__class__.__name__,todo.module.name))

    def setup(self):
        """Set up :attr:`modules`, fed with :attr:`pipe_block`, a copy of :attr:`data_block`."""
        self.job_dir = self.options.get_string(syntax.hpc_job_dir,'job_dir')
        self.mpiexec = self.options.get_string(syntax.mpiexec,'mpiexec')
        template_fn = self.options.get_string(syntax.hpc_job_template,None)
        if template_fn is not None:
            with open(template_fn,'r') as file:
                self.job_template = file.read()
            self.job_submit = self.options.get_string(syntax.hpc_job_submit,'sbatch')
            self.job_options = self.options.get_dict(syntax.hpc_job_options,{})
        else:
            self.job_template = None
        super(BatchPipeline,self).setup()

    def find_file_task(self, filetype, itask=None):
        """
        Return file name for task number ``itask`` corresponding to ``filetype``:
        - config_block: :attr:`iconfig_block` (:attr:`config_block` for this task)
        - data_block: :class:`ipipe_block` (:attr:`pipe_block` for this task)
        - save_data_block: :class:`pipe_block` output by execution of subpipeline
        - job: job submission script
        """
        if filetype == 'config_block':
            base, ext = filetype, 'yaml'
        elif filetype in ['data_block','save_data_block']:
            base, ext = filetype, 'npy'
        elif filetype == 'job':
            base, ext = 'script', 'job'
        else:
            raise ValueError('Unknown file type: {}'.format(filetype))
        if itask is None:
            basename = '{}.{}'.format(base,ext)
        else:
            basename = '{}_{}.{}'.format(base,itask,ext)
        return os.path.join(self.job_dir,basename)

    @property
    def is_datablock_saved(self):
        """Save :class:`DataBlock` instance only if items to be propagated in :attr:`data_block`."""
        return len(self._datablock_duplicate)

    def execute_task(self, itask=0):
        """Execute single task number ``itask``: either using the command line, or by executing a job script (if ``job_template`` is provided)."""
        config_block_fn = self.find_file_task('config_block',itask=itask)
        data_block_fn = self.find_file_task('data_block',itask=itask)
        self.iconfig_block.save_yaml(config_block_fn)
        self.ipipe_block.save(data_block_fn)
        command = 'pypescript {} --data-block-fn {}'.format(config_block_fn,data_block_fn)
        if self.is_datablock_saved:
            command = '{} --save-data-block-fn {}'.format(command,self.find_file_task('save_data_block',itask=itask))
        if self.mpiexec is not None and self.nprocs_per_task > 1:
            command = '{} -n {:d} {}'.format(self.mpiexec,self.nprocs_per_task,command)
        if self.job_template is not None:
            template = self.job_template.format(command=command,**self.job_options)
            template_fn = self.find_file_task('job',itask=itask)
            with open(template_fn,'w') as file:
                file.write(template)
            command = '{} {}'.format(self.job_submit,template_fn)
        self.log_info('Running {}'.format(command),rank=0)
        output = subprocess.run(command, capture_output=True, shell=True).stdout
        output = output.decode('utf-8')
        self.log_info('Output is:\n{}'.format(output),rank=0)

    def load_task(self, itask=0):
        """
        Load subpipeline output :class:`DataBlock` instance for task number ``itask`` from disk.
        If not asked to be saved, return ``None``.
        Else, if :class:`DataBlock` does not exist on disk, raise a :class:`BatchError`.
        """
        data_block = None
        if self.is_datablock_saved:
            try:
                data_block = DataBlock.load(self.find_file_task('save_data_block',itask=itask))
            except FileNotFoundError as exc:
                raise BatchError('Task {} has not completed.'.format(itask)) from exc
        return data_block

    def execute(self):
        """Execute subpipeline for each task, either using the command line, or by executing a job script (if ``job_template`` is provided)."""
        self.iconfig_block = self.config_block.copy()
        options = {}
        options[syntax.execute] = [syntax.join_sections((todo.module.name,todo.step),sep=syntax.module_function_sep) for todo in self.execute_todos]
        options[syntax.datablock_set] = {syntax.join_sections(key):value for key,value in self._datablock_set.items()}
        duplicate = {}
        for key in set(self._datablock_duplicate) | set(self._datablock_key_iter) - set(self._datablock_bcast):
            duplicate[syntax.join_sections(key)] = syntax.join_sections(key)
        options[syntax.datablock_duplicate] = duplicate
        self.iconfig_block.raw[syntax.main] = options
        self.ipipe_block = self.data_block.copy()

        iter = self._iter
        if self._iter is None: iter = [None]

        for task in iter:

            for key,value in self._configblock_iter.items():
                self.iconfig_block.raw[key] = value(task)
            for key,value in self._datablock_iter.items():
                self.ipipe_block[key] = value(task)

            self.execute_task(task)

        self.pipe_block = pipe_block = self.data_block.copy()
        for itask,task in enumerate(iter):
            pipe_block = self.load_task(task)
            for keyg,keyl in self._datablock_key_iter.items():
                self.pipe_block[keyl[itask]] = pipe_block[keyg]
        for key in set(self._datablock_duplicate) - set(self._datablock_bcast): # bcast treated above
            self.pipe_block[key] = pipe_block[key]
