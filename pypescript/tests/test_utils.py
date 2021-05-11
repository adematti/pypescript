import numpy as np

from pypescript.utils import setup_logging, BaseClass
from pypescript import syntax
from pypescript.syntax import Decoder


def test_base_class():
    self = BaseClass()
    self.log_info("I'm the BaseClass.")


def test_syntax():
    expanded, collapsed = {'a':{'b':{'c':2},'d':4}}, {'a.b.c': 2, 'a.d': 4}
    assert syntax.collapse_sections(expanded) == collapsed
    assert syntax.expand_sections(collapsed) == expanded
    collapsed = {'a.b': {'c': 2}, 'a.d': 4}
    assert syntax.collapse_sections(expanded,maxdepth=2) == collapsed
    assert syntax.expand_sections(collapsed) == expanded

    """
    di = {'hello': {'world': 42, 'localpath': '${path}', '$module_name': 'hello'}, 'testdict.a.b.c': 42,
    'path': 'myglobalpath', 'mynumber': 42, 'mylambda': '$(lambda i: i + ${mynumber})'}
    decoded = Decoder(di)

    la = decoded.pop('mylambda')
    assert decoded.data == {'hello': {'world': 42, 'localpath': 'myglobalpath', syntax.module_name: 'hello'},
    'testdict': {'a':{'b':{'c': 42}}}, 'path': 'myglobalpath', 'mynumber': 42}
    assert la(4) == 46
    """
    decoded = Decoder('config.yaml')
    la = decoded.pop('mylambda')
    assert decoded.data == {'hello': {'answer': {'to': 42, 'the': 44}, 'world': 42, 'answer2': 44,
    'localpath': 'myglobalpath', syntax.module_name: 'hello'}, 'testdict': {'a':{'b':{'c': 42}}},
    'path': 'myglobalpath', 'mynumber': 42, 'world':{}}
    assert decoded.mapping == {('world','answer3'):('hello','answer2')}
    assert la(2) == (88,'hello')


if __name__ == '__main__':

    setup_logging()
    test_base_class()
    test_syntax()
