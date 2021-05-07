import pytest
import numpy as np

from pypescript.utils import setup_logging, BaseClass
from pypescript import syntax


def test_base_class():
    self = BaseClass()
    self.log_info("I'm the BaseClass.")


def test_syntax():
    assert syntax.decode_keyword('module_name') is None
    assert syntax.decode_keyword('$module_name') == syntax.module_name
    assert syntax.decode_replace('${section.name}') == (None, ('section','name'))
    #assert syntax.decode_replace('${abcd.$module_name}') == (None, ('section',syntax.module_name))
    assert syntax.decode_replace('${/path/to/other/config/file.yaml:section.name}') == ('/path/to/other/config/file.yaml', ('section','name'))
    with pytest.raises(syntax.PypescriptKeywordError):
        syntax.decode_keyword('$modulename')
    with pytest.raises(syntax.PypescriptParserError):
        syntax.decode_replace('${/path/to/other/config:file.yaml:section.name}')
    assert np.all(syntax.decode_eval('$(np.ones(4))',globals=globals()) == np.ones(4))
    print(globals())


if __name__ == '__main__':

    setup_logging()
    test_base_class()
    test_syntax()
