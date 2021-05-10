#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#define DATABLOCK_MODULE
#include "blockmodule.h"


PyObject * PyDataBlock_Sections(PyDataBlock *self)
{
  return PyDict_Keys((PyObject *) self->data);
}


int PyDataBlock_HasSection(PyDataBlock *self, PyObject *section)
{
  return PyDict_Contains((PyObject *) ((PyDataBlock *) self)->data, section);
}


PyObject * PyDataBlock_GetSection(PyDataBlock *self, PyObject *section, PyObject *default_value)
{
  PyObject *toret = NULL;
  toret = PyDict_GetItem((PyObject *) self->data, section);
  if (toret == NULL) {
    if (default_value == NULL) {
      PyErr_Format(PyExc_KeyError, "Section %S does not exist", section);
      goto except;
    }
    toret = default_value;
  }
  Py_INCREF(toret);
  goto finally;
except:
  Py_XDECREF(toret);
  toret = NULL;
finally:
  return toret;
}

PyObject * datablock_keys(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = {"section", NULL};
  PyObject *section = NULL, *section_data = NULL;
  PyObject *keys = NULL, *iter_sections = NULL, *sections = NULL, *iter_names = NULL, *name = NULL, *item = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|U", kwlist, &section))
    return NULL;

  if (section != NULL) {
    sections = Py_BuildValue("[O]",section); // raises error if NULL
    if (sections == NULL) goto except;
  }
  else {
    sections = PyDataBlock_Sections(self);
    if (sections == NULL) {
      PyErr_SetString(PyExc_TypeError,"Failed to get sections");
      goto except;
    }
  }

  keys = PyList_New(0);
  if (keys == NULL) {
    PyErr_SetString(PyExc_TypeError,"Failed to create list");
    goto except;
  }
  iter_sections = PyObject_GetIter(sections); // raises error if NULL
  if (iter_sections == NULL) goto except;
  while ((section = PyIter_Next(iter_sections))) {
    section_data = PyDataBlock_GetSection(self, section, NULL); // new reference
    if (section_data == NULL) {Py_XDECREF(section); goto except;};
    iter_names = PyObject_GetIter(section_data);
    if (iter_names == NULL) {Py_XDECREF(section_data); Py_XDECREF(section); goto except;};
    while ((name = PyIter_Next(iter_names))) {
      item = Py_BuildValue("(ON)", section, name); // raises error if NULL, N steals reference
      if (item == NULL) {Py_XDECREF(name); Py_XDECREF(iter_names); Py_XDECREF(section_data); Py_XDECREF(section); goto except;};
      if (PyList_Append(keys, item) != 0) {
        PyErr_SetString(PyExc_TypeError,"Failed to add item to list");
        Py_XDECREF(item); Py_XDECREF(iter_names); Py_XDECREF(section_data); Py_XDECREF(section);
        goto except;
      }
      Py_XDECREF(item);
      //Py_XDECREF(name);
    }
    Py_XDECREF(iter_names); Py_XDECREF(section_data); Py_XDECREF(section);
  }
  //assert(!PyErr_Occurred());
  goto finally;
except:
  //assert(PyErr_Occurred());
  Py_XDECREF(keys);
  keys = NULL;
finally:
  Py_XDECREF(sections);
  Py_XDECREF(iter_sections);
  return keys;
}

static PyObject * PyDataBlock_GetValue(PyDataBlock *self, PyObject *section, PyObject *name, PyObject *default_value)
{
  PyObject *toret = NULL, *true_section = NULL, *true_name = NULL, *item = NULL;
  if (!PyBlockMapping_ParseSectionName(self->mapping, section, name, &true_section, &true_name)) goto except;

  if ((default_value != NULL) && (PyDataBlock_HasSection(self, true_section) != 1)) {
    toret = default_value;
    Py_INCREF(toret);
    goto finally;
  }

  item = PyDataBlock_GetSection(self, true_section, NULL);
  if (item == NULL) goto except;
  toret = PyDict_GetItem(item, true_name);
  if (toret == NULL) {
    if (default_value == NULL) {
      PyErr_Format(PyExc_KeyError, "Name %S does not exist in section %S", name, section);
      goto except;
    }
    toret = default_value;
  }
  Py_INCREF(toret);
  goto finally;
except:
  Py_XDECREF(toret);
  toret = NULL;
finally:
  Py_XDECREF(item);
  return toret;
}

static int PyDataBlock_HasValue(PyDataBlock *self, PyObject *section, PyObject *name)
{
  int toret = 1;
  PyObject *true_section = NULL, *true_name = NULL, *item = NULL;
  if (!PyBlockMapping_ParseSectionName(self->mapping, section, name, &true_section, &true_name)) goto except;
  toret = PyDataBlock_HasSection(self, true_section);
  if (toret != 1) goto finally;
  item = PyDataBlock_GetSection(self, true_section, NULL);
  if (item == NULL) goto except;
  toret = PyDict_Contains(item, true_name);
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(item);
  return toret;
}


static PyObject* datablock_has(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  int contains = 0;
  static char *kwlist[] = {"section", "name", NULL};
  PyObject *section = NULL, *name = NULL;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &section, &name))
    return NULL;

  if (name == NULL) {
    contains = PyDataBlock_HasSection(self, section);
    goto finally;
  }
  contains = PyDataBlock_HasValue(self, section, name);
  goto finally;
finally:
  if (contains != 1) Py_RETURN_FALSE;
  Py_RETURN_TRUE;
}


static int datablock_contains(PyObject *self, PyObject *args)
{
  int toret = 1;
  PyObject *section = NULL, *name = NULL;

  if (PyTuple_Check(args)) {
    if (!PyArg_ParseTuple(args, "OO", &section, &name)) goto except;
    toret = PyDataBlock_HasValue((PyDataBlock *) self, section, name);
    goto finally;
  }
  toret = PyDataBlock_HasSection((PyDataBlock *) self, args);
  goto finally;
except:
  toret = -1;
finally:
  return toret;
}

PyObject * datablock_get(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  // Return new references
  PyObject *section = NULL, *name = NULL, *default_value = NULL;
  static char *kwlist[] = {"section", "name", "default", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|OO", kwlist, &section, &name, &default_value))
    return NULL;

  if (name == NULL) return PyDataBlock_GetSection(self, section, default_value);
  return PyDataBlock_GetValue(self, section, name, default_value);
}

PyObject * PyDataBlock_GetItem(PyDataBlock *self, PyObject *key)
{
  if (PyTuple_Check(key)) return datablock_get(self, key, NULL);
  return PyDataBlock_GetSection(self, key, NULL);
}

PyObject * datablock_items(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  PyObject *keys = NULL, *key, *value, *item, *iter_keys = NULL, *items = PyList_New(0);
  if (items == NULL) goto except;
  keys = datablock_keys(self, args, kwds);
  if (keys == NULL) goto except;
  iter_keys = PyObject_GetIter(keys);
  if (iter_keys == NULL) goto except;
  while ((key = PyIter_Next(iter_keys))) {
    value = PyDataBlock_GetItem(self, key);
    if (value == NULL) goto except;
    item = Py_BuildValue("(NN)", key, value); // N steals reference
    if (item == NULL) goto except;
    if (PyList_Append(items, item) != 0) {
      PyErr_SetString(PyExc_TypeError,"Failed to add item to list");
      goto except;
    }
    Py_XDECREF(item);
    //Py_XDECREF(value);
    //Py_XDECREF(key);
  }
  goto finally;
except:
  Py_XDECREF(items);
  items = NULL;
finally:
  Py_XDECREF(keys);
  Py_XDECREF(iter_keys);
  return items;
}

int PyDataBlock_SetSection(PyDataBlock *self, PyObject *section, PyObject *value)
{
  int toret = 0;
  PyObject *item = NULL;
  if (!PyDict_Check(value)) {
    PyErr_SetString(PyExc_TypeError, "Value must be a dictionary");
    goto except;
  }
  if (PyDataBlock_HasSection(self, section)) {
    item = PyDataBlock_GetSection(self, section, NULL);
    if (item == NULL) goto except;
    if (item != value) {
      PyDict_Clear(item);
      if (PyDict_Update(item,value) != 0) goto except;
    }
    goto finally;
  }
  item = PyDict_Copy(value);
  if (item == NULL) goto except;
  toret = PyDict_SetItem((PyObject *) self->data, section, item);
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(item);
  return toret;
}

static int PyDataBlock_SetValue(PyDataBlock *self, PyObject *section, PyObject *name, PyObject *value)
{
  int toret = 0;
  PyObject *true_section = NULL, *true_name = NULL, *item = NULL, *dict = NULL;
  if (!PyBlockMapping_ParseSectionName(self->mapping, section, name, &true_section, &true_name)) goto except;
  if (!PyDataBlock_HasSection(self, true_section)) {
    dict = PyDict_New();
    if (PyDataBlock_SetSection(self, true_section, dict) != 0) goto except;
  }
  item = PyDataBlock_GetSection(self, true_section, NULL);
  if (item == NULL) goto except;
  //printf("Ref set data 1 %d %d\n",Py_REFCNT(self),Py_REFCNT(value));
  toret = PyDict_SetItem(item, true_name, value);
  //printf("Ref set data 2 %d %d\n",Py_REFCNT(self),Py_REFCNT(value));
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(dict);
  Py_XDECREF(item);
  return toret;
}


PyObject * datablock_set(PyDataBlock *self, PyObject *args)
{
  // Does not steal reference
  int toret = 0;
  PyObject *section = NULL, *name = NULL, *value = NULL;
  if (!PyArg_ParseTuple(args,"OO|O", &section, &name, &value)) return NULL;
  if (value == NULL) {
    value = name;
    name = NULL;
  }
  if (name == NULL) {
    toret = PyDataBlock_SetSection(self, section, value);
    goto finally;
  }
  toret = PyDataBlock_SetValue(self, section, name, value);
  goto finally;
finally:
  if (toret == 0) Py_RETURN_NONE;
  return NULL;
}

void PyDataBlock_ClearAll(PyDataBlock *self)
{
  PyDict_Clear((PyObject *) self->data);
}


int PyDataBlock_ClearSection(PyDataBlock *self, PyObject *section)
{
  int toret = 0;
  PyObject *item = NULL;
  item = PyDataBlock_GetSection(self, section, NULL);
  PyDict_Clear((PyObject *) item);
  if (item == NULL) goto except;
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(item);
  return toret;
}

static PyObject * datablock_clear_section(PyDataBlock *self, PyObject *section)
{
  if (PyDataBlock_ClearSection(self, section) == 0) Py_RETURN_NONE;
  return NULL;
}


static PyObject * datablock_clear(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = {"section", NULL};
  PyObject *section = NULL;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|U", kwlist, &section))
    return NULL;

  if (section == NULL) {
    PyDataBlock_ClearAll(self);
    Py_RETURN_NONE;
  }
  return datablock_clear_section(self, section);
}


int PyDataBlock_DelSection(PyDataBlock *self, PyObject *section)
{
  return PyDict_DelItem((PyObject *) self->data, section);
}


static int PyDataBlock_DelValue(PyDataBlock *self, PyObject *section, PyObject *name)
{
  int toret = 0;
  PyObject *true_section = NULL, *true_name = NULL, *item = NULL;
  if (!PyBlockMapping_ParseSectionName(self->mapping, section, name, &true_section, &true_name)) goto except;
  item = PyDataBlock_GetSection(self, true_section, NULL);
  if (item == NULL) goto except;
  toret = PyDict_DelItem(item, true_name);
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(item);
  return toret;
}


int datablock_del(PyDataBlock *self, PyObject *args)
{

  PyObject *section = NULL, *name = NULL;
  if (!PyArg_ParseTuple(args,"O|O", &section, &name)) return -1;

  if (name == NULL) return PyDataBlock_DelSection(self, section);
  return PyDataBlock_DelValue(self, section, name);
}


static int datablock_assub(PyDataBlock *self, PyObject *key, PyObject *value)
{
  int toret = 0;
  if (value == NULL) {
    if (PyTuple_Check(key)) return datablock_del(self, key);
    return PyDataBlock_DelSection(self, key);
  }
  PyObject *section = NULL, *name = NULL;
  if (PyTuple_Check(key)) {
    if (!PyArg_ParseTuple(key, "OO", &section, &name)) goto except;
    toret = PyDataBlock_SetValue(self, section, name, value);
    goto finally;
  }
  toret = PyDataBlock_SetSection(self, key, value);
  goto finally;
except:
  toret = -1;
finally:
  return toret;
}

int PyDataBlock_Update(PyDataBlock *self, PyObject *other, PyObject *nocopy)
{
  int toret = 0;
  Py_ssize_t position = 0;
  PyObject *section, *item;
  Py_INCREF(other);

  if (PyDataBlock_Check(other)) {
    //if (PyDict_Update((PyObject *) self->data,(PyObject *) ((PyDataBlock *) other)->data) == 0) goto finally;
    //goto except;
    PyObject * tmp = other;
    other = (PyObject *) ((PyDataBlock *) tmp)->data;
    Py_INCREF(other);
    Py_DECREF(tmp);
  }

  if (!PyDict_Check(other)) {
    PyErr_SetString(PyExc_TypeError,"Please provide a dictionary.");
    goto except;
  }

  if (nocopy == NULL) {
    while (PyDict_Next(other, &position, &section, &item)) {
      //printf("Ref count 1 %d %d\n",Py_REFCNT(section),Py_REFCNT(item));
      if (PyDataBlock_SetSection(self, section, item) != 0) goto except;
      //printf("Ref count 2 %d %d\n",Py_REFCNT(section),Py_REFCNT(item));
    }
    goto finally;
  }
  if ((!PyTuple_Check(nocopy)) & (!PyList_Check(nocopy))) {
    PyErr_SetString(PyExc_TypeError,"Argument 'nocopy' should be a tuple or list.");
    goto except;
  }
  while (PyDict_Next(other, &position, &section, &item)) {
    int contains = PySequence_Contains(nocopy, section);
    //printf("Contains %d\n",contains);
    if (contains < 0) goto except;
    else if (contains) {
      if (PyDict_SetItem((PyObject *) self->data, section, item) != 0) goto except;
    }
    else if (PyDataBlock_SetSection(self, section, item) != 0) goto except;
  }
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(other);
  return toret;
}

static PyObject * datablock_update(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  static char *kwlist[] = {"other", "nocopy", NULL};
  PyObject *other = NULL, *nocopy = NULL;
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|O", kwlist, &other, &nocopy)) return NULL;
  if (PyDataBlock_Update(self, other, nocopy) == 0) Py_RETURN_NONE;
  return NULL;
}

static PyObject * datablock_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
  PyDataBlock *self = (PyDataBlock *) type->tp_alloc(type, 0);
  if (self == NULL) goto except;

  self->data = (PyDictObject *) PyDict_New();
  if (self->data == NULL) goto except;
  self->mapping = (PyBlockMapping *) PyBlockMapping_New();
  if (self->mapping == NULL) goto except;
  goto finally;
except:
  Py_CLEAR(self->data);
  Py_CLEAR(self->mapping);
  Py_XDECREF(self);
  self = NULL;
finally:
  return (PyObject *) self;
}

PyDataBlock * PyDataBlock_New(void)
{
  PyDataBlock *toret = PyObject_GC_New(PyDataBlock, &PyDataBlockType);
  toret->data = (PyDictObject *) PyDict_New();
  if (toret->data == NULL) goto except;
  toret->mapping = (PyBlockMapping *) PyDict_New();
  if (toret->mapping == NULL) goto except;
  //if (PyObject_GC_IsTracked(self))
  PyObject_GC_Track(toret);
  goto finally;
except:
  Py_CLEAR(toret->data);
  Py_CLEAR(toret->mapping);
  Py_XDECREF(toret);
  toret = NULL;
finally:
  return toret;
}

PyDataBlock * PyDataBlock_Copy(PyDataBlock *self, PyObject *nocopy)
{
  PyDataBlock *toret = NULL;

  toret = PyDataBlock_New();
  if (toret == NULL) goto except;

  if (PyDataBlock_Update(toret, (PyObject *) self, nocopy) != 0) goto except;

  Py_INCREF(self->mapping);
  Py_CLEAR(toret->mapping);
  toret->mapping = self->mapping;
  goto finally;
except:
  Py_XDECREF(toret);
  toret = NULL;
finally:
  return toret;
}

PyDataBlock * datablock_copy(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  PyObject *nocopy = NULL;
  static char *kwlist[] = {"nocopy", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|O", kwlist, &nocopy)) return NULL;
  return PyDataBlock_Copy(self, nocopy);
}


int PyDataBlock_SetMapping(PyDataBlock *self, PyObject *mapping)
{
  int toret = 0;
  PyBlockMapping *mapping_ = NULL;
  if (PyBlockMapping_Check(mapping)) {
    mapping_ = (PyBlockMapping *) mapping;
    Py_INCREF(mapping_);
    //mapping_ = PyBlockMapping_Copy(mapping);
  }
  else if (PyDict_Check(mapping)) {
    mapping_ = PyBlockMapping_New();
    if (mapping_ == NULL) goto except;
    if (PyBlockMapping_Update(mapping_,mapping) != 0) goto except;
  }
  else {
    PyErr_SetString(PyExc_TypeError,"Provided mapping must be either BlockMapping or dict.");
    goto except;
  }
  Py_CLEAR(self->mapping);
  self->mapping = mapping_;
  goto finally;
except:
  Py_XDECREF(mapping_);
  toret = -1;
finally:
  return toret;
}

static PyObject * datablock_set_mapping(PyDataBlock *self, PyObject *mapping)
{
  if (PyDataBlock_SetMapping(self, mapping) == 0) Py_RETURN_NONE;
  return NULL;
}


static PyObject * datablock_data_getter(PyDataBlock *self, void *closure) {
  PyObject * toret = (PyObject *) self->data;
  Py_XINCREF(toret);
  return toret;
}


static PyObject * datablock_mapping_getter(PyDataBlock *self, void *closure) {
  PyObject * toret = (PyObject *) self->mapping;
  Py_XINCREF(toret);
  return toret;
}


Py_ssize_t PyDataBlock_Size(PyDataBlock *self)
{
  return PyDict_Size((PyObject *) self->data);
}


PyObject * PyDataBlock_Repr(PyDataBlock *self)
{
  return PyUnicode_FromFormat("DataBlock(data=%S, mapping=%S)", (PyObject *) self->data, (PyObject *) self->mapping);
}

PyObject * PyDataBlock_Str(PyDataBlock *self)
{
  return PyUnicode_FromFormat("DataBlock(data=%S, mapping=%S)", (PyObject *) self->data, (PyObject *) self->mapping);
}


static int datablock_init(PyDataBlock *self, PyObject *args, PyObject *kwds)
{
  int toret = 0;
  static char *kwlist[] = {"data", "mapping", NULL};
  PyObject *data = NULL, *mapping = NULL;

  if (!PyArg_ParseTupleAndKeywords(args, kwds, "|OO", kwlist, &data, &mapping)) goto except;

  if ((data != NULL) & (data != Py_None)) {
    if (PyDataBlock_Check(data)) {
      PyDictObject *data_ = ((PyDataBlock *) data)->data;
      Py_INCREF(data_);
      Py_CLEAR(self->data);
      self->data = data_;
      PyBlockMapping *mapping_ = ((PyDataBlock *) data)->mapping;
      Py_INCREF(mapping_);
      Py_CLEAR(self->mapping);
      self->mapping = mapping_;
    }
    else if (PyDataBlock_Update(self, data, NULL) != 0) goto except;
  }
  if ((mapping != NULL) & (mapping != Py_None)) {
    if (PyDataBlock_SetMapping(self, mapping) != 0) goto except;
  }
  goto finally;
except:
  toret = -1;
finally:
  return toret;
}

// GC

static int datablock_traverse(PyDataBlock *self, visitproc visit, void *arg)
{
  Py_VISIT(self->data);
  Py_VISIT(self->mapping);
  return 0;
}

static int datablock_tp_clear(PyDataBlock *self)
{
  Py_CLEAR(self->data);
  Py_CLEAR(self->mapping);
  return 0;
}

static void datablock_dealloc(PyDataBlock *self)
{
  PyObject_GC_UnTrack(self);
  datablock_tp_clear(self);
  Py_TYPE(self)->tp_free((PyObject *) self);
}



//////////////////////// Python C API stuff ////////////////////////

static PyMemberDef PyDataBlock_members[] = {
//  {"data", T_OBJECT_EX, offsetof(PyDataBlock, data), 0, "data"},
  {NULL}  /* Sentinel */
};

static PyMethodDef PyDataBlock_methods[] = {
  {"sections", (PyCFunction) PyDataBlock_Sections, METH_NOARGS, "Return sections"},
  {"keys", (PyCFunction) datablock_keys, METH_VARARGS | METH_KEYWORDS, "Return keys"},
  {"items", (PyCFunction) datablock_items, METH_VARARGS | METH_KEYWORDS, "Return items"},
  {"get", (PyCFunction) datablock_get, METH_VARARGS | METH_KEYWORDS, "Return item"},
  {"has", (PyCFunction) datablock_has, METH_VARARGS | METH_KEYWORDS, "Has item"},
  {"set", (PyCFunction) datablock_set, METH_VARARGS, "Set item"},
  {"set_mapping", (PyCFunction) datablock_set_mapping, METH_O, "Set item"},
  {"update", (PyCFunction) datablock_update, METH_VARARGS | METH_KEYWORDS, "Update DataBlock"},
  {"copy", (PyCFunction) datablock_copy, METH_VARARGS | METH_KEYWORDS, "Copy DataBlock"},
  {"clear", (PyCFunction) datablock_clear, METH_VARARGS | METH_KEYWORDS, "Clear DataBlock"},
  {"__getitem__", (PyCFunction) PyDataBlock_GetItem, METH_O | METH_COEXIST, "x.__getitem__(y) <==> x[y]"},
  {NULL}  /* Sentinel */
};

static PyMappingMethods PyDataBlock_as_mapping = {
  (lenfunc)PyDataBlock_Size, /*mp_length*/
  (binaryfunc)PyDataBlock_GetItem, /*mp_subscript*/
  (objobjargproc)datablock_assub /*mp_ass_subscript*/
};

/* Hack to implement "key in dict" */
static PySequenceMethods PyDataBlock_as_sequence = {
  0,                          /* sq_length */
  0,                          /* sq_concat */
  0,                          /* sq_repeat */
  0,                          /* sq_item */
  0,                          /* sq_slice */
  0,                          /* sq_ass_item */
  0,                          /* sq_ass_slice */
  datablock_contains,         /* sq_contains */
  0,                          /* sq_inplace_concat */
  0,                          /* sq_inplace_repeat */
};


static PyGetSetDef PyDataBlock_properties[] = {
  {"data", (getter) datablock_data_getter, NULL, "Data dictionary", NULL},
  {"mapping", (getter) datablock_mapping_getter, NULL, "BlockMapping instance", NULL},
  {NULL}
};


PyTypeObject PyDataBlockType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "block.DataBlock",
  .tp_doc = "DataBlock object",
  .tp_basicsize = sizeof(PyDataBlock),
  .tp_itemsize = 0,
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
  .tp_new = datablock_new,
  .tp_init = (initproc) datablock_init,
  .tp_dealloc = (destructor) datablock_dealloc,
  .tp_traverse = (traverseproc) datablock_traverse,
  .tp_clear = (inquiry) datablock_tp_clear,
  .tp_as_sequence = &PyDataBlock_as_sequence,
  .tp_as_mapping = &PyDataBlock_as_mapping,
  .tp_members = PyDataBlock_members,
  .tp_methods = PyDataBlock_methods,
  .tp_getset = PyDataBlock_properties,
  .tp_repr = (reprfunc) PyDataBlock_Repr,
  .tp_str = (reprfunc) PyDataBlock_Str
};

/*
static PyTypeObject PyDataBlockType = {
  PyVarObject_HEAD_INIT(NULL, 0)
  .tp_name = "block.DataBlock",
  .tp_doc = "DataBlock object",
  .tp_basicsize = sizeof(PyDataBlock),
  .tp_itemsize = 0,
  .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
  .tp_new = datablock_new,
  .tp_init = (initproc) datablock_init,
  .tp_dealloc = (destructor) datablock_dealloc,
  .tp_as_sequence = &PyDataBlock_as_sequence,
  .tp_as_mapping = &PyDataBlock_as_mapping,
  .tp_members = PyDataBlock_members,
  .tp_methods = PyDataBlock_methods,
  .tp_getset = PyDataBlock_properties,
  .tp_repr = (reprfunc) PyDataBlock_Repr,
};
*/

static PyModuleDef blockmodule = {
  PyModuleDef_HEAD_INIT,
  .m_name = "block",
  .m_doc = "Block module that creates DataBlock.",
  .m_size = -1,
  .m_methods = NULL
};

PyMODINIT_FUNC
PyInit_block(void)
{
  PyObject *m;
  if (PyType_Ready(&PyDataBlockType) < 0) return NULL;
  if (PyType_Ready(&PyBlockMappingType) < 0) return NULL;

  m = PyModule_Create(&blockmodule);
  if (m == NULL) return NULL;

  Py_INCREF(&PyBlockMappingType);
  if (PyModule_AddObject(m, "BlockMapping", (PyObject *) &PyBlockMappingType) < 0) {
    Py_DECREF(&PyDataBlockType);
    Py_DECREF(m);
    return NULL;
  }

  Py_INCREF(&PyDataBlockType);
  if (PyModule_AddObject(m, "DataBlock", (PyObject *) &PyDataBlockType) < 0) {
    Py_DECREF(&PyBlockMappingType);
    Py_DECREF(&PyDataBlockType);
    Py_DECREF(m);
    return NULL;
  }

  static void *PyDataBlock_API[PyDataBlock_API_pointers];

  /* Initialize the C API pointer array */
  PyDataBlock_API[PyDataBlock_HasValue_NUM] = (void *) PyDataBlock_HasValue;
  PyDataBlock_API[PyDataBlock_DelValue_NUM] = (void *) PyDataBlock_DelValue;
  PyDataBlock_API[PyDataBlock_SetValue_NUM] = (void *) PyDataBlock_SetValue;
  PyDataBlock_API[PyDataBlock_GetValue_NUM] = (void *) PyDataBlock_GetValue;

  /* Create a Capsule containing the API pointer array's address */
  PyObject * c_api_object = PyCapsule_New((void *)PyDataBlock_API, "pypescript.lib.block._C_API", NULL);

  if (PyModule_AddObject(m, "_C_API", c_api_object) < 0) {
      Py_XDECREF(c_api_object);
      Py_DECREF(m);
      return NULL;
  }

  return m;
}
