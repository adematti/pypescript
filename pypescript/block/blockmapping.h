#ifndef _BLOCK_MAPPING_
#define _BLOCK_MAPPING_

#include <Python.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  PyObject_HEAD
  PyDictObject *data;
} PyBlockMapping;

extern PyTypeObject PyBlockMappingType;

#define PyBlockMapping_Check(op) PyObject_TypeCheck(op, &PyBlockMappingType)

PyBlockMapping * PyBlockMapping_New(void);

PyBlockMapping * PyBlockMapping_Copy(PyBlockMapping *self);

int PyBlockMapping_Update(PyBlockMapping *self, PyObject *other);

int PyBlockMapping_ParseSectionName(PyBlockMapping *self, PyObject * section, PyObject * name, PyObject ** true_section, PyObject ** true_name);

#ifdef __cplusplus
}
#endif

#endif
