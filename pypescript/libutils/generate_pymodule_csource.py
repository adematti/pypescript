"""Generate C source files to wrapp C/C++/Fortran code into a Python extension."""

from . import utils

template = """// This file has been generated by the Python script generate_cpymodule.py. Do not edit it.
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <numpy/arrayobject.h>
#include "pypelib.h"
"""

for fun in ['setup','execute','cleanup']:
    template += """PyObject * ##__fun##_wrapper(PyObject *self, PyObject *args) {
      int toret = 0;
      PyObject *name = NULL, *config_block = NULL, *data_block = NULL;
      if (!PyArg_ParseTuple(args, "OOO", &name, &config_block, &data_block)) goto except;
      const char * name_str = PyUnicode_AsUTF8(name);
      toret = ##__fun##(name_str, (DataBlock *) config_block, (DataBlock *) data_block);
      //if (toret != 0) _PyErr_FormatFromCause(PyExc_RuntimeError,"Exception (signal %d) in function ##__fun## of ##__module_name## [%S].", toret, name);
      if ((toret != 0) && (!PyErr_Occurred()))
        PyErr_Format(PyExc_RuntimeError,"Exception (signal %d) in function ##__fun## of ##__module_name## [%S].", toret, name);
      goto finally;
    except:
      toret = -1;
    finally:
      if (toret == 0) Py_RETURN_NONE;
      return NULL;
    }\n\n""".replace('##__fun##',fun)

template += """static PyMethodDef pypemethods[] = {
  {"setup", setup_wrapper, METH_VARARGS, "Setup module."},
  {"execute", execute_wrapper, METH_VARARGS, "Execute module."},
  {"cleanup", cleanup_wrapper, METH_VARARGS, "Cleanup module."},
  {NULL, NULL, 0, NULL}        /* Sentinel */
};

static PyModuleDef pypemodule = {
  PyModuleDef_HEAD_INIT,
  .m_name = "##__module_name##",
  .m_doc = "##__doc##",
  .m_size = -1,
  .m_methods = pypemethods,
};

PyMODINIT_FUNC
PyInit_##__module_name##(void)
{
  import_array();
  if (import_datablock()<0) return NULL;
  return PyModule_Create(&pypemodule);
}
"""

def write_csource(filename, module_name, doc=''):
    """
    Write C source file to turn C/C++/Fortran code into a Python extension.

    Parameters
    ----------
    filename : string
        Where to write the C file.

    module_name : string
        Module base name.

    doc : string, default=''
        Short module documentation.
    """
    content = template.replace('##__module_name##',module_name).replace('##__doc##',doc)
    utils.mkdir(filename)
    with open(filename,'w') as file:
        file.write(content)
