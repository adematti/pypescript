#define PY_SSIZE_T_CLEAN
#include <Python.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#include "pypelib.h"

#define GENERATE_GET_SCALAR(__name,__type,__conversion)\
  int DataBlock_get_##__name##_default(DataBlock *data_block, const char * section, const char * name, __type * value, __type default_value)\
  {\
    if (DataBlock_has_value(data_block, section, name) != 1) {\
      *value = default_value;\
      return 1;\
    };\
    PyObject * py_value = DataBlock_get_py_value(data_block, section, name, NULL);\
    if (py_value == NULL) return -1;\
    *value = (__type) __conversion;\
    Py_XDECREF(py_value);\
    if (PyErr_Occurred()) return -1;\
    return 0;\
  }\
  int DataBlock_get_##__name(DataBlock *data_block, const char * section, const char * name, __type * value)\
  {\
    PyObject * py_value = DataBlock_get_py_value(data_block, section, name, NULL);\
    if (py_value == NULL) return -1;\
    *value = (__type) __conversion;\
    Py_XDECREF(py_value);\
    if (PyErr_Occurred()) return -1;\
    return 0;\
  }\

#define GENERATE_SET_SCALAR(__name,__type,__conversion)\
  int DataBlock_set_##__name(DataBlock *data_block, const char * section, const char * name, __type value)\
  {\
    PyObject * py_value = __conversion;\
    int toret = DataBlock_set_py_value(data_block, section, name, py_value);\
    Py_XDECREF(py_value);\
    return toret;\
  }\

#define GENERATE_GET_ARRAY(__name,__type,__nptype)\
  int DataBlock_get_##__name##_array(DataBlock *data_block, const char * section, const char * name, __type ** value, int * ndim, size_t ** shape)\
  {\
    int toret = 0;\
    PyObject * py_value = NULL;\
    PyArrayObject * np_array = NULL;\
    py_value = DataBlock_get_py_value(data_block, section, name, NULL);\
    if (py_value == NULL) return -1;\
    np_array = (PyArrayObject *) PyArray_FROM_OTF(py_value, __nptype, NPY_ARRAY_INOUT_ARRAY2 | NPY_ARRAY_C_CONTIGUOUS);\
    if (np_array == NULL) goto except;\
    if (PyArray_CHKFLAGS(np_array, NPY_ARRAY_WRITEBACKIFCOPY)) {\
      PyArray_ResolveWritebackIfCopy(np_array);\
      if (DataBlock_set_py_value(data_block, section, name, (PyObject *) np_array) != 0) goto except;\
    }\
    *ndim = PyArray_NDIM(np_array);\
    *shape = (size_t *) PyArray_SHAPE(np_array);\
    *value = (__type *) PyArray_DATA(np_array);\
    goto finally;\
  except:\
    toret = -1;\
  finally:\
    Py_XDECREF(py_value);\
    Py_XDECREF(np_array);\
    return toret;\
  }\


#define GENERATE_SET_ARRAY(__name,__type,__nptype)\
  int DataBlock_set_##__name##_array(DataBlock *data_block, const char * section, const char * name, __type * value, int ndim, size_t * shape)\
  {\
    PyObject * py_value = NULL;\
    py_value = PyArray_SimpleNewFromData(ndim, (npy_intp *) shape, __nptype, (void *) value);\
    PyArray_ENABLEFLAGS((PyArrayObject*) py_value, NPY_ARRAY_OWNDATA);\
    if (py_value == NULL) return -1;\
    int toret = DataBlock_set_py_value(data_block, section, name, py_value);\
    Py_XDECREF(py_value);\
    return toret;\
  }\


__attribute__((constructor)) void init(void) {
  Py_Initialize();
  import_array();
  import_datablock();
}

// DataBlock stuffs

void clear_errors(void) {
  PyErr_Clear();
}

int DataBlock_has_value(DataBlock *data_block, const char * section, const char * name)
{
  PyObject *py_section = NULL, *py_name = NULL;
  int toret = 0;
  py_section = PyUnicode_FromString(section);
  if (py_section == NULL) goto except;
  py_name = PyUnicode_FromString(name);
  if (py_name == NULL) goto except;
  toret = PyDataBlock_HasValue(data_block, py_section, py_name) == 1;
  goto finally;
except:
  toret = 0;
finally:
  Py_XDECREF(py_section);
  Py_XDECREF(py_name);
  return toret;
}

int DataBlock_del_value(DataBlock *data_block, const char * section, const char * name)
{
  PyObject *py_section = NULL, *py_name = NULL;
  int toret = 0;
  py_section = PyUnicode_FromString(section);
  if (py_section == NULL) goto except;
  py_name = PyUnicode_FromString(name);
  if (py_name == NULL) goto except;
  toret = PyDataBlock_DelValue(data_block, py_section, py_name);
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(py_section);
  Py_XDECREF(py_name);
  return toret;
}

int DataBlock_set_py_value(DataBlock *data_block, const char * section, const char * name, PyObject * py_value)
{
  PyObject *py_section = NULL, *py_name = NULL;
  int toret = 0;
  py_section = PyUnicode_FromString(section);
  if (py_section == NULL) goto except;
  py_name = PyUnicode_FromString(name);
  if (py_name == NULL) goto except;

  if (PyDataBlock_SetValue(data_block, py_section, py_name, py_value) != 0) goto except;
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(py_section);
  Py_XDECREF(py_name);
  return toret;
}

PyObject * DataBlock_get_py_value(DataBlock *data_block, const char * section, const char * name, PyObject * default_value)
{
  PyObject *py_section = NULL, *py_name = NULL, *py_value = NULL;
  py_section = PyUnicode_FromString(section);
  if (py_section == NULL) goto except;
  py_name = PyUnicode_FromString(name);
  if (py_name == NULL) goto except;
  py_value = PyDataBlock_GetValue(data_block, py_section, py_name, default_value);
  if (py_value == NULL) goto except;
  goto finally;
except:
  py_value = NULL;
finally:
  Py_XDECREF(py_section);
  Py_XDECREF(py_name);
  return py_value;
}

int DataBlock_duplicate_value(DataBlock *data_block, const char * section1, const char * name1, const char * section2, const char * name2)
{
  PyObject *py_value = DataBlock_get_py_value(data_block, section1, name1, NULL);
  if (py_value == NULL) return 0;
  int toret = DataBlock_set_py_value(data_block, section2, name2,  py_value);
  Py_DECREF(py_value);
  return toret;
}

int DataBlock_move_value(DataBlock *data_block, const char * section1, const char * name1, const char * section2, const char * name2)
{
  PyObject *py_value = DataBlock_get_py_value(data_block, section1, name1, NULL);
  if (py_value == NULL) return 0;
  DataBlock_del_value(data_block, section1, name1);
  int toret = DataBlock_set_py_value(data_block, section2, name2, py_value);
  Py_DECREF(py_value);
  return toret;
}

// Scalar getters

GENERATE_GET_SCALAR(capsule,void *,PyCapsule_GetPointer(py_value, NULL))

GENERATE_GET_SCALAR(int,int,PyLong_AsLong(py_value))

GENERATE_GET_SCALAR(long,long,PyLong_AsLong(py_value))

GENERATE_GET_SCALAR(float,float,PyFloat_AsDouble(py_value))

GENERATE_GET_SCALAR(double,double,PyFloat_AsDouble(py_value))

GENERATE_GET_SCALAR(string,char *,PyUnicode_AsUTF8(py_value)) // The caller is not responsible for deallocating the buffer

// Scalar setters

GENERATE_SET_SCALAR(capsule,void *,PyCapsule_New(value, NULL, NULL))

GENERATE_SET_SCALAR(int,int,PyLong_FromLong((long) value))

GENERATE_SET_SCALAR(long,long,PyLong_FromLong((long) value))

GENERATE_SET_SCALAR(float,float,PyFloat_FromDouble((double) value))

GENERATE_SET_SCALAR(double,double,PyFloat_FromDouble((double) value))

GENERATE_SET_SCALAR(string,char *,PyUnicode_FromString((const char *) value)) // Creates a copy of char *

// Array getters

GENERATE_GET_ARRAY(int,int,NPY_INT)

GENERATE_GET_ARRAY(long,long,NPY_LONG)

GENERATE_GET_ARRAY(float,float,NPY_FLOAT)

GENERATE_GET_ARRAY(double,double,NPY_DOUBLE)

// Array setters

GENERATE_SET_ARRAY(int,int,NPY_INT)

GENERATE_SET_ARRAY(long,long,NPY_LONG)

GENERATE_SET_ARRAY(float,float,NPY_FLOAT)

GENERATE_SET_ARRAY(double,double,NPY_DOUBLE)

GENERATE_SET_ARRAY(string,char *,NPY_STRING)
