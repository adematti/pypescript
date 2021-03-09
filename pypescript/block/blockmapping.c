#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include "blockmodule.h"

int PyBlockMapping_SetItem(PyBlockMapping *self, PyObject *key, PyObject *value)
{
  int toret = 0;
  if (PyTuple_Check(key) ^ PyTuple_Check(value)) {
    PyErr_SetString(PyExc_TypeError,"(key,value) pairs should be both tuple or scalars.");
    goto except;
  }
  if (PyTuple_Check(key) && (PyTuple_Size(key) != 2)) {
    PyErr_SetString(PyExc_TypeError,"If tuple, key should be of size 2 (section, name).");
    goto except;
  }
  if (PyTuple_Check(value) && (PyTuple_Size(value) != 2)) {
    PyErr_SetString(PyExc_TypeError,"If tuple, value should be of size 2 (section, name).");
    goto except;
  }
  toret = PyDict_SetItem((PyObject *) self->data, key, value);
  goto finally;
except:
  toret = -1;
finally:
  return toret;
}

int PyBlockMapping_DelItem(PyBlockMapping *self, PyObject *key)
{
  return PyDict_DelItem((PyObject *) self->data, key);
}

PyObject * PyBlockMapping_GetItem(PyBlockMapping *self, PyObject *key)
{
  PyObject *toret = PyDict_GetItem((PyObject *) self->data, key);
  Py_XINCREF(toret);
  return toret;
}

int PyBlockMapping_Contains(PyBlockMapping *self, PyObject *key) {
  return PyDict_Contains((PyObject *) self->data, key);
}

static int mapping_contains(PyObject *self, PyObject *key) {
  return PyBlockMapping_Contains((PyBlockMapping *) self, key);
}

int PyBlockMapping_ParseSectionName(PyBlockMapping *self, PyObject * section, PyObject * name, PyObject ** true_section, PyObject ** true_name)
{
  int toret = 1;
  PyObject *key = NULL, *value = NULL;
  key = Py_BuildValue("(OO)", section, name);
  if (key == NULL) goto except;
  if (PyBlockMapping_Contains(self, key) == 1) {
    value = PyDict_GetItem((PyObject *) self->data, key);
    if (!PyArg_ParseTuple(value, "OO", true_section, true_name)) goto except;
  }
  else if (PyBlockMapping_Contains(self, section) == 1) {
    *true_section = PyDict_GetItem((PyObject *) self->data, section);
    *true_name = name;
  }
  else {
    *true_section = section;
    *true_name = name;
  }
  goto finally;
except:
  toret = 0;
finally:
  Py_XDECREF(key);
  return toret;
}


static int mapping_assub(PyBlockMapping *self, PyObject *key, PyObject *value)
{
  if (value == NULL) return PyBlockMapping_DelItem(self, key);
  return PyBlockMapping_SetItem(self, key, value);
}

PyObject * PyBlockMapping_Keys(PyBlockMapping *self) {
  return PyDict_Keys((PyObject *) self->data);
}

PyObject * PyBlockMapping_Items(PyBlockMapping *self) {
  return PyDict_Items((PyObject *) self->data);
}

void PyBlockMapping_Clear(PyBlockMapping *self)
{
  PyDict_Clear((PyObject *) self->data);
}

static PyObject * mapping_clear(PyBlockMapping *self)
{
  PyBlockMapping_Clear(self);
  Py_RETURN_NONE;
}

static PyObject * mapping_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  PyBlockMapping *self = (PyBlockMapping *) type->tp_alloc(type, 0);
  if (self == NULL) goto except;

  self->data = (PyDictObject *) PyDict_New();
  if (self->data == NULL) goto except;
  goto finally;
except:
  Py_CLEAR(self->data);
  Py_XDECREF(self);
  self = NULL;
finally:
  return (PyObject *) self;
}

PyBlockMapping * PyBlockMapping_New(void)
{
  PyBlockMapping *toret = PyObject_GC_New(PyBlockMapping, &PyBlockMappingType);
  toret->data = (PyDictObject *) PyDict_New();
  if (toret->data == NULL) goto except;
  PyObject_GC_Track(toret);
  goto finally;
except:
  Py_CLEAR(toret->data);
  Py_XDECREF(toret);
  toret = NULL;
finally:
  return toret;
}

int PyBlockMapping_Update(PyBlockMapping *self, PyObject *other)
{
  int toret = 0;
  Py_ssize_t position = 0;
  PyObject *key, *item;
  Py_INCREF(other);

  if (PyBlockMapping_Check(other)) {
    PyObject * tmp = other;
    other = (PyObject *) ((PyBlockMapping *) tmp)->data;
    Py_INCREF(other);
    Py_DECREF(tmp);
  }
  //printf("I'm here4!!!\n");
  if (!PyDict_Check(other)) {
    PyErr_SetString(PyExc_TypeError,"Please provide a dictionary.");
    goto except;
  }
  //printf("I'm here5 %ld!!!\n",position);
  while (PyDict_Next(other, &position, &key, &item)) {
    //printf("Position %ld\n",position);
    if (PyBlockMapping_SetItem(self, key, item) != 0) goto except;
  }
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(other);
  return toret;
}

static PyObject * mapping_update(PyBlockMapping *self, PyObject *other)
{
  if (PyBlockMapping_Update(self, other) == 0) Py_RETURN_NONE;
  return NULL;
}

PyBlockMapping * PyBlockMapping_Copy(PyBlockMapping *self)
{
  PyBlockMapping *toret = PyBlockMapping_New();
  if (toret == NULL) goto finally;

  if (PyBlockMapping_Update(toret, (PyObject *) self) != 0) goto except;
  goto finally;
except:
  Py_XDECREF(toret);
  toret = NULL;
finally:
  return toret;
}

static PyObject * mapping_data_getter(PyDataBlock *self, void *closure) {
  PyObject * toret = (PyObject *) self->data;
  Py_XINCREF(toret);
  return toret;
}


Py_ssize_t PyBlockMapping_Size(PyBlockMapping *self)
{
  return PyDict_Size((PyObject *) self->data);
}


PyObject * PyBlockMapping_Repr(PyBlockMapping *self)
{
  return PyObject_Repr((PyObject *) self->data);
}

static int mapping_init(PyBlockMapping *self, PyObject *args, PyObject *kwds)
{
  int toret = 0;
  static char *kwlist[] = {"data", NULL};
  PyObject *data = NULL;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &data)) goto except;

  if ((data != NULL) & (data != Py_None)) {
    if (PyBlockMapping_Check(data)) {
      PyDictObject *data_ = ((PyBlockMapping *) data)->data;
      Py_INCREF(data_);
      Py_CLEAR(self->data);
      self->data = data_;
    }
    else if (PyBlockMapping_Update(self, data) != 0) goto except;
  }
  goto finally;
except:
  toret = -1;
finally:
  return toret;
}

// GC

static int mapping_traverse(PyBlockMapping *self, visitproc visit, void *arg)
{
  Py_VISIT(self->data);
  return 0;
}

static PyObject * mapping_tp_clear(PyBlockMapping *self)
{
  Py_CLEAR(self->data);
  Py_RETURN_NONE;
}

static void mapping_dealloc(PyBlockMapping *self)
{
  PyObject_GC_UnTrack(self);
  mapping_tp_clear(self);
  Py_TYPE(self)->tp_free((PyObject *) self);
}



//////////////////////// Python C API stuff ////////////////////////

static PyMemberDef PyBlockMapping_members[] = {
//  {"data", T_OBJECT_EX, offsetof(PyBlockMapping, data), 0, "data"},
  {NULL}  /* Sentinel */
};

static PyMethodDef PyBlockMapping_methods[] = {
  {"keys", (PyCFunction) PyBlockMapping_Keys, METH_NOARGS, "Return keys"},
  {"items", (PyCFunction) PyBlockMapping_Items, METH_NOARGS, "Return items"},
  {"update", (PyCFunction) mapping_update, METH_O, "Update BlockMapping"},
  {"copy", (PyCFunction) PyBlockMapping_Copy, METH_NOARGS, "Copy BlockMapping"},
  {"clear", (PyCFunction) mapping_clear, METH_NOARGS, "Clear BlockMapping"},
  {"__getitem__", (PyCFunction) PyBlockMapping_GetItem, METH_O | METH_COEXIST, "x.__getitem__(y) <==> x[y]"},
  {NULL}  /* Sentinel */
};

static PyMappingMethods PyBlockMapping_as_mapping = {
  (lenfunc)PyBlockMapping_Size, /*mp_length*/
  (binaryfunc)PyBlockMapping_GetItem, /*mp_subscript*/
  (objobjargproc)mapping_assub /*mp_ass_subscript*/
};

/* Hack to implement "key in dict" */
static PySequenceMethods PyBlockMapping_as_sequence = {
  0,                          /* sq_length */
  0,                          /* sq_concat */
  0,                          /* sq_repeat */
  0,                          /* sq_item */
  0,                          /* sq_slice */
  0,                          /* sq_ass_item */
  0,                          /* sq_ass_slice */
  mapping_contains,      /* sq_contains */
  0,                          /* sq_inplace_concat */
  0,                          /* sq_inplace_repeat */
};

static PyGetSetDef PyBlockMapping_properties[] = {
  {"data", (getter) mapping_data_getter, NULL, "Data dictionary", NULL},
  {NULL}
};


PyTypeObject PyBlockMappingType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "block.BlockMapping",
  .tp_doc = "BlockMapping object",
  .tp_basicsize = sizeof(PyBlockMapping),
  .tp_itemsize = 0,
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
  .tp_new = mapping_new,
  .tp_init = (initproc) mapping_init,
  .tp_dealloc = (destructor) mapping_dealloc,
  .tp_traverse = (traverseproc) mapping_traverse,
  .tp_clear = (inquiry) mapping_tp_clear,
  .tp_as_sequence = &PyBlockMapping_as_sequence,
  .tp_as_mapping = &PyBlockMapping_as_mapping,
  .tp_members = PyBlockMapping_members,
  .tp_methods = PyBlockMapping_methods,
  .tp_getset = PyBlockMapping_properties,
  .tp_repr = (reprfunc) PyBlockMapping_Repr,
};
