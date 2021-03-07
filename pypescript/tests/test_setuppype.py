from pypescript.setuppype.generate_pype_module_doc import generate_doc_rst_table


def test_rst_doc():
    doc = {'name': 'module', 'version': '1.0.0', 'date': '03/01/2021', 'author': 'Arnaud de Mattia', 'maintainer': 'Arnaud de Mattia',
    'description': 'Template C module', 'url': '', 'licence': '', 'cite': [], 'bibtex': [], 'requirements': [],
    'compile': {'sources': ['module.c','module2.c'], 'include_dirs': [], 'library_dirs': [], 'libraries': [], 'extra_compile_args': [], 'extra_link_args': []},
    'long_description': 'Explain here in finer details what your module is about',
    'parameters': {'answer': 'Answer to the Ultimate Question of Life, the Universe, and Everything'}, 'inputs': [], 'outputs': []}
    print(generate_doc_rst_table(doc))


if __name__ == '__main__':
    test_rst_doc()
