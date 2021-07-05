#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stdarg.h>
#include "pypelib.h"

#define STRINGIFY(A) #A

#define GENERATE_LOG(__name)\
  int log_##__name(const char * name, const char * format, ...)\
  {\
    va_list vargs;\
    va_start (vargs,format);\
    return log_msg(STRINGIFY(__name), name, format, vargs);\
  }\

/***********************************************************/
/* define logging function and logtypes for python.logging */
/* by H.Dickten 2014                                       */
/***********************************************************/

int log_msg(const char * type, const char * name, const char * format, va_list vargs)
{
  PyObject *logging = NULL, *logger = NULL, *py_name = NULL, *string = NULL;
  char * cstring;
  int toret = 0;

  if (vasprintf(&cstring, format, vargs) < 0) goto except;
  // import logging module on demand
  logging = PyImport_ImportModule("logging");
  if (logging == NULL) goto except; // raises error

  py_name = PyUnicode_FromString(name);
  if (py_name == NULL) goto except;

  logger = PyObject_CallMethod(logging, "getLogger", "O", py_name);
  if (logger == NULL) goto except; // raises error
  // build msg-string
  string = PyUnicode_FromString(cstring);
  if (string == NULL) goto except;

  if (PyObject_CallMethod(logger, type, "O", string) == NULL) goto except;  // raises error
  goto finally;
except:
  toret = -1;
finally:
  Py_XDECREF(string);
  Py_XDECREF(logger);
  Py_XDECREF(py_name);
  Py_XDECREF(logging);
  free(cstring);
  return toret;
}

// Loggers

GENERATE_LOG(info)

GENERATE_LOG(warning)

GENERATE_LOG(debug)

GENERATE_LOG(error)
