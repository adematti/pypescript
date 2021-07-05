#ifndef _BLOCK_MODULE_
#define _BLOCK_MODULE_

#include <Python.h>
#include "blockmapping.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  PyObject_HEAD
  PyDictObject *data;
  PyBlockMapping *mapping;
} PyDataBlock;

extern PyTypeObject PyDataBlockType;

#define PyDataBlock_Check(op) PyObject_TypeCheck(op, &PyDataBlockType)

/* C API functions */
//int PyDataBlock_HasValue(PyDataBlock *self, PyObject *section, PyObject *name);
#define PyDataBlock_HasValue_NUM 0
#define PyDataBlock_HasValue_RETURN int
#define PyDataBlock_HasValue_PROTO (PyDataBlock *self, PyObject *section, PyObject *name)

//int PyDataBlock_DelValue(PyDataBlock *self, PyObject *section, PyObject *name);
#define PyDataBlock_DelValue_NUM 1
#define PyDataBlock_DelValue_RETURN int
#define PyDataBlock_DelValue_PROTO (PyDataBlock *self, PyObject *section, PyObject *name)

//int PyDataBlock_SetValue(PyDataBlock *self, PyObject *section, PyObject *name, PyObject *value);
#define PyDataBlock_SetValue_NUM 2
#define PyDataBlock_SetValue_RETURN int
#define PyDataBlock_SetValue_PROTO (PyDataBlock *self, PyObject *section, PyObject *name, PyObject *value)

//PyObject * PyDataBlock_GetValue(PyDataBlock *self, PyObject *section, PyObject *name, PyObject *default_value);
#define PyDataBlock_GetValue_NUM 3
#define PyDataBlock_GetValue_RETURN PyObject *
#define PyDataBlock_GetValue_PROTO (PyDataBlock *self, PyObject *section, PyObject *name, PyObject *default_value)

/* Total number of C API pointers */
#define PyDataBlock_API_pointers 4


#ifdef DATABLOCK_MODULE
// This section is used when compiling blockmodule.c

static PyDataBlock_HasValue_RETURN PyDataBlock_HasValue PyDataBlock_HasValue_PROTO;
static PyDataBlock_DelValue_RETURN PyDataBlock_DelValue PyDataBlock_DelValue_PROTO;
static PyDataBlock_SetValue_RETURN PyDataBlock_SetValue PyDataBlock_SetValue_PROTO;
static PyDataBlock_GetValue_RETURN PyDataBlock_GetValue PyDataBlock_GetValue_PROTO;

#else
// This section is used in modules that use blockmodule's API

static void **PyDataBlock_API;

#define PyDataBlock_HasValue \
 (*(PyDataBlock_HasValue_RETURN (*)PyDataBlock_HasValue_PROTO) PyDataBlock_API[PyDataBlock_HasValue_NUM])

#define PyDataBlock_DelValue \
 (*(PyDataBlock_DelValue_RETURN (*)PyDataBlock_DelValue_PROTO) PyDataBlock_API[PyDataBlock_DelValue_NUM])

#define PyDataBlock_SetValue \
 (*(PyDataBlock_SetValue_RETURN (*)PyDataBlock_SetValue_PROTO) PyDataBlock_API[PyDataBlock_SetValue_NUM])

#define PyDataBlock_GetValue \
 (*(PyDataBlock_GetValue_RETURN (*)PyDataBlock_GetValue_PROTO) PyDataBlock_API[PyDataBlock_GetValue_NUM])

// Return -1 on error, 0 on success.
// PyCapsule_Import will set an exception if there's an error.

static int
import_datablock(void)
{
    PyDataBlock_API = (void **) PyCapsule_Import("pypescript.lib.block._C_API", 0);
    return (PyDataBlock_API != NULL) ? 0 : -1;
}

#endif


#ifdef __cplusplus
}
#endif

#endif
