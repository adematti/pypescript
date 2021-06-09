from pypescript.libutils import generate_rst_doc_table, ModuleDescription


def module_description():
    doc = {'name': 'module', 'version': '1.0.0', 'date': '03/01/2021', 'author': 'Arnaud de Mattia', 'maintainer': 'Arnaud de Mattia',
    'description': 'Template C module', 'url': '', 'licence': '', 'cite': [], 'bibtex': [], 'requirements': [],
    'compile': {'sources': ['module.c','module2.c'], 'include_dirs': [], 'library_dirs': [], 'libraries': [], 'extra_compile_args': [], 'extra_link_args': []},
    'long_description': 'Explain here in finer details what your module is about',
    'options': {'question': 'Answer to the Ultimate Question of Life, the Universe, and Everything', 'answer':"e'1./3.'"}, 'inputs': [], 'outputs': []}
    assert ModuleDescription(doc) == {'name': 'module', 'version': '1.0.0', 'date': '03/01/2021', 'author': 'Arnaud de Mattia', 'maintainer': 'Arnaud de Mattia', 'description': 'Template C module', 'url': '', 'licence': '', 'cite': [], 'bibtex': [], 'requirements': [], 'compile': {'sources': ['module.c', 'module2.c'], 'include_dirs': [], 'library_dirs': [], 'libraries': [], 'extra_compile_args': [], 'extra_link_args': []}, 'long_description': 'Explain here in finer details what your module is about', 'options': {'question': 'Answer to the Ultimate Question of Life, the Universe, and Everything', 'answer': 0.3333333333333333}, 'inputs': [], 'outputs': []}


def test_rst_doc():
    doc = {'name': 'module', 'version': '1.0.0', 'date': '03/01/2021', 'author': 'Arnaud de Mattia', 'maintainer': 'Arnaud de Mattia',
    'description': 'Template C module', 'url': '', 'licence': '', 'cite': [], 'bibtex': [], 'requirements': [],
    'compile': {'sources': ['module.c','module2.c'], 'include_dirs': [], 'library_dirs': [], 'libraries': [], 'extra_compile_args': [], 'extra_link_args': []},
    'long_description': 'Explain here in finer details what your module is about',
    'options': {'question': 'Answer to the Ultimate Question of Life, the Universe, and Everything', 'answer':"e'1./3.'"}, 'inputs': [], 'outputs': []}
    print(generate_rst_doc_table(doc))


if __name__ == '__main__':

    module_description()
    test_rst_doc()
