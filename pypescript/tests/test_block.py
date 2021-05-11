import numpy as np
import pytest

from pypescript.block import BlockMapping, DataBlock, SectionBlock
from pypescript.config import ConfigBlock
from pypescript.utils import setup_logging, MemoryMonitor
from pypescript import syntax


def test_mapping():

    mapping = BlockMapping()
    assert repr(mapping) == 'BlockMapping({})'
    assert str(mapping) == '{}'
    mapping_ = {('parameters','a'):('parameters','b'),('section_a',):('section_b',)}
    str_ = str(mapping_)
    mapping = BlockMapping(mapping_)
    assert str(mapping) == str_
    mapping_str = {'parameters.a':'parameters.b','section_a':'section_b'}
    mapping = BlockMapping(mapping_str,sep='.')
    assert str(mapping) == str_
    assert str(BlockMapping.from_state(mapping.__getstate__())) == str_
    assert set(mapping.keys()) == set(mapping_.keys())
    assert set(mapping.items()) == set(mapping_.items())
    mapping['section_a_copy'] = mapping['section_a']
    with pytest.raises(TypeError):
        mapping['section_a_copy'] = 'section_a','b'
    del mapping['section_a']
    mapping['section_a'] = mapping['section_a_copy']
    del mapping['section_a_copy']
    mapping['parameters','b'] = 'parameters','b'
    with pytest.raises(TypeError):
        mapping['parameters','b'] = 'b'
    del mapping['parameters','b']
    assert str(mapping) == str_
    mapping.update(mapping)
    assert str(mapping) == str_
    mapping.update({('parameters','b'):('parameters','c')})
    assert 'section_a' in mapping
    assert ('parameters','b') in mapping
    mapping_copy = mapping.copy()
    del mapping_copy['parameters','b']
    assert ('parameters','b') not in mapping_copy
    assert ('parameters','b') in mapping
    mapping.clear()
    assert str(mapping) == '{}'
    with pytest.raises(AttributeError):
        mapping.data = {'section_c':'section_d'}


def test_block():

    block = DataBlock(add_sections=[])
    del block['mpi']
    assert str(block.data) == '{}'
    block = DataBlock(None,add_sections=[])
    del block['mpi']
    assert str(block.data) == '{}'
    d = {'section_a':{'name_a':{'answer':42}},'section_b':{'name_b':2,'name_c':4}}
    block = DataBlock(d,add_sections=[])
    del block['mpi']
    assert str(block.data) == str(d)
    assert str(DataBlock.from_state(block.__getstate__()).data) == str(d)
    assert block.keys() == [('section_a','name_a'),('section_b','name_b'),('section_b','name_c')]
    assert block.keys('section_a') == block.keys(section='section_a') == [('section_a','name_a')]
    with pytest.raises(TypeError):
        block.get()
    assert block.get('section_a') == block.get(section='section_a') == d['section_a']
    assert block.get('section_a','name_a') == block.get(section='section_a',name='name_a') == d['section_a']['name_a']
    assert block.get('section_test','name_test',42) == block.get('section_test',default=42) == 42
    assert len(block) == 2
    block['section_a','name_b'] = 2
    block.set('section_c',{'name_c':4})
    block.set('section_a','name_b',8)
    with pytest.raises(TypeError):
        block['section_a'] = 2
    del block['section_c']
    del block['section_a','name_b']
    assert 'section_a' in block
    assert block.has('section_a')
    assert ('section_a','name_a') in block
    assert block.has('section_a','name_a')
    assert block.items() == [(('section_a', 'name_a'), {'answer':42}), (('section_b', 'name_b'), 2), (('section_b', 'name_c'), 4)]
    block_copy = block.copy()
    block_copy = block.copy(nocopy=['section_b'])

    block['section_a']['name_b'] = 5
    assert block_copy['section_a'] == {'name_a':{'answer':42}}
    assert ('section_a','name_b') not in block_copy
    assert block.has('section_a','name_b')
    #block['section_b']['name_c'] = 5
    block['section_b'] = {'name_d':5}
    assert block_copy['section_b'] == block['section_b']
    block['section_a','name_a'] = 1 #np.ones(10000)
    block.set_mapping({'section_c':'section_a'})
    assert np.all(block['section_c','name_a'] == block['section_a','name_a'])
    block.set_mapping(BlockMapping({}))

    with pytest.raises(KeyError):
        block['section_c','name_a']
    block.set_mapping(BlockMapping({'section_a.name_b':'section_a.name_a'},sep='.'))
    assert np.all(block['section_a','name_b'] == block['section_a','name_a'])
    block.update(block_copy)
    assert block['section_a'] == {'name_a':{'answer':42}}
    block.update({'section_c':{'answer':42}})
    assert ('section_c','answer') in block
    #block['section_a','name_b']
    #assert np.all(block['section_a','name_b'] == block['section_a','name_a'])
    block.clear(section='section_a')
    assert block['section_a'] == {}
    block.clear()
    assert block.data == {}
    with pytest.raises(AttributeError):
        block.data = {'section_a':{'name_a':2}}
    # Test cyclic garbage collection, not worse than python dict itself

    block = DataBlock()
    block['block','name_a'] = block
    block['block','name_b'] = block

    #block = {}
    #block['block','name_a'] = block
    #test = {}
    #test['a'] = np.ones(10000)
    #test['b'] = test


def test_sections():
    d = {'section_a':{'name_a':{'answer':42}},'section_b':{'name_b':2}}
    block = DataBlock(d,add_sections=[])
    section = SectionBlock(block,'section_a')


def test_config():
    config = ConfigBlock('config.yaml')
    assert config.data == {'hello': {'answer': {'to': 42, 'the': 44}, 'world': 42, 'answer2': 44,
    'localpath': 'myglobalpath', syntax.module_name: 'hello'}, 'testdict': {'a':{'b':{'c': 42}}}}
    config2 = ConfigBlock(config)
    assert id(config2.data) == id(config.data)


if __name__ == '__main__':

    setup_logging()

    with MemoryMonitor() as mem:
        for i in range(5000):
            test_mapping()
            test_block()
            test_sections()

    test_config()
