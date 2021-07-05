import os
import yaml
import pytest

from pypescript import BaseModule, BasePipeline, ConfigBlock, SectionBlock
from pypescript.utils import setup_logging, MemoryMonitor
from template_lib.model import FlatModel
from template_lib.likelihood import BaseLikelihood, JointGaussianLikelihood
from template_lib import section_names


base_dir = os.path.dirname(os.path.realpath(__file__))
demo_dir = os.path.join(base_dir,'demos')


def test_section_names():
    assert 'mpi' in vars(section_names) # set up at installation


def test_demo1():

    os.chdir(base_dir)
    config_fn = os.path.join(demo_dir,'demo1.yaml')
    graph_fn = os.path.join(demo_dir,'pipe1.ps')

    pipeline = BasePipeline(config_block=config_fn)
    #pipeline.plot_pipeline_graph(graph_fn)
    pipeline.setup()
    pipeline.data_block[section_names.parameters,'a'] = 0.
    pipeline.execute()
    loglkl = pipeline.pipe_block[section_names.likelihood,'loglkl']
    pipeline.data_block[section_names.parameters,'a'] = 4.
    pipeline.execute()
    assert pipeline.pipe_block[section_names.likelihood,'loglkl'] != loglkl
    pipeline.cleanup()

    graph_fn = os.path.join(demo_dir,'inheritance.ps')
    #BaseModule.plot_inheritance_graph(graph_fn,exclude=['FlatModel'])


def test_demo2():

    config_fn = os.path.join(demo_dir,'demo2.yaml')
    graph_fn = os.path.join(demo_dir,'pipe2.ps')

    pipeline = BasePipeline(config_block=config_fn)
    #pipeline.plot_pipeline_graph(graph_fn)
    pipeline.setup()
    pipeline.data_block[section_names.parameters,'a'] = 0.
    pipeline.execute()
    loglkl = pipeline.pipe_block[section_names.likelihood,'loglkl']
    pipeline.data_block[section_names.parameters,'a'] = 4.
    pipeline.execute()
    assert pipeline.pipe_block[section_names.likelihood,'loglkl'] != loglkl
    pipeline.cleanup()


def test_demo3():

    config_fn = os.path.join(demo_dir,'demo3.yaml')
    config_fnb = os.path.join(demo_dir,'demo3b.yaml')
    config = ConfigBlock(config_fn)
    with open(config_fnb,'w') as file:
        yaml.dump(config.data, file)

    for config_fn in [config_fn,config_fnb][:1]:

        graph_fn = os.path.join(demo_dir,'pipe3.ps')

        pipeline = BasePipeline(config_block=config_fn)
        #pipeline.plot_pipeline_graph(graph_fn)
        pipeline.setup()
        pipeline.data_block[section_names.parameters,'a'] = 0.
        pipeline.execute()
        loglkl = pipeline.pipe_block[section_names.likelihood,'loglkl']
        pipeline.data_block[section_names.parameters,'a'] = 4.
        pipeline.execute()
        assert pipeline.pipe_block[section_names.likelihood,'loglkl'] != loglkl
        pipeline.cleanup()


def test_demo3b():

    config_fn = os.path.join(demo_dir,'demo3.yaml')
    graph_fn = os.path.join(demo_dir,'pipe3b.ps')

    config_block = ConfigBlock(config_fn)
    data1 = BaseModule.from_filename(name='data1',options=SectionBlock(config_block,'data1'))
    model1 = FlatModel(name='model1')
    data2 = BaseModule.from_filename(name='data2',options=SectionBlock(config_block,'data2'))
    model2 = BaseModule.from_filename(name='model2',options=SectionBlock(config_block,'model2'))
    cov = BaseModule.from_filename(name='cov',options=SectionBlock(config_block,'cov'))
    like1 = BaseLikelihood(name='like1',modules=[data1,model1])
    like2 = BaseLikelihood(name='like2',modules=[data2,model2])
    like = JointGaussianLikelihood(name='like',join=[like1,like2],modules=[cov])
    pipeline = BasePipeline(modules=[like])
    #pipeline.plot_pipeline_graph(graph_fn)
    pipeline.setup()
    pipeline.data_block[section_names.parameters,'a'] = 0.
    pipeline.execute()
    loglkl = pipeline.pipe_block[section_names.likelihood,'loglkl']
    pipeline.data_block[section_names.parameters,'a'] = 4.
    pipeline.execute()
    assert pipeline.pipe_block[section_names.likelihood,'loglkl'] != loglkl
    pipeline.cleanup()


def test_demo4():

    config_fn = os.path.join(demo_dir,'demo4.yaml')
    pipeline = BasePipeline(config_block=config_fn)
    pipeline.setup()
    assert pipeline.pipe_block[section_names.data,'ysave'].size == 3


def test_demo5():
    config_fn = os.path.join(demo_dir,'demo5.yaml')
    pipeline = BasePipeline(config_block=config_fn)
    pipeline.setup()
    pipeline.execute()
    pipeline.cleanup()



if __name__ == '__main__':

    setup_logging()
    test_section_names()
    with MemoryMonitor() as mem:
        for i in range(10):
            test_demo1()
            test_demo2()
            test_demo3()
            test_demo3b()
            test_demo4()
            test_demo5()
