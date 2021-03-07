#ifndef _PYPE_LIB_
#define _PYPE_LIB_

#include "blockmodule.h"
#include "section_names.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef PyDataBlock DataBlock;

extern const char * MODULE_NAME;

extern int setup(const char * name, DataBlock *config_block, DataBlock *data_block);

extern int execute(const char * name, DataBlock *config_block, DataBlock *data_block);

extern int cleanup(const char * name, DataBlock *config_block, DataBlock *data_block);


void clear_errors(void);

int log_info(const char * name, const char * format, ...);

int log_warning(const char * name, const char * format, ...);

int log_debug(const char * name, const char * format, ...);

int log_error(const char * name, const char * format, ...);


// DataBlock stuffs
// Bool tests

int DataBlock_has_value(DataBlock *data_block, const char * section, const char * name);

int DataBlock_del_value(DataBlock *data_block, const char * section, const char * name);

PyObject * DataBlock_get_py_value(DataBlock *data_block, const char * section, const char * name, PyObject * default_value);

int DataBlock_set_py_value(DataBlock *data_block, const char * section, const char * name, PyObject * py_value);

int DataBlock_duplicate_value(DataBlock *data_block, const char * section1, const char * name1, const char * section2, const char * name2);

int DataBlock_move_value(DataBlock *data_block, const char * section1, const char * name1, const char * section2, const char * name2);

// Scalar getters

int DataBlock_get_capsule_default(DataBlock *data_block, const char * section, const char * name, void ** value, void * default_value);

int DataBlock_get_capsule(DataBlock *data_block, const char * section, const char * name, void ** value);

int DataBlock_get_int_default(DataBlock *data_block, const char * section, const char * name, int * value, int default_value);

int DataBlock_get_int(DataBlock *data_block, const char * section, const char * name, int * value);

int DataBlock_get_long_default(DataBlock *data_block, const char * section, const char * name, long * value, long default_value);

int DataBlock_get_long(DataBlock *data_block, const char * section, const char * name, long * value);

int DataBlock_get_float_default(DataBlock *data_block, const char * section, const char * name, float * value, float default_value);

int DataBlock_get_float(DataBlock *data_block, const char * section, const char * name, float * value);

int DataBlock_get_double_default(DataBlock *data_block, const char * section, const char * name, double * value, double default_value);

int DataBlock_get_double(DataBlock *data_block, const char * section, const char * name, double * value);

int DataBlock_get_string_default(DataBlock *data_block, const char * section, const char * name, char ** value, char * default_value);

int DataBlock_get_string(DataBlock *data_block, const char * section, const char * name, char ** value);

// Scalar setters

int DataBlock_set_capsule(DataBlock *data_block, const char * section, const char * name, void * value);

int DataBlock_set_int(DataBlock *data_block, const char * section, const char * name, int value);

int DataBlock_set_long(DataBlock *data_block, const char * section, const char * name, long value);

int DataBlock_set_float(DataBlock *data_block, const char * section, const char * name, float value);

int DataBlock_set_double(DataBlock *data_block, const char * section, const char * name, double value);

int DataBlock_set_string(DataBlock *data_block, const char * section, const char * name, char * value);

// Array getters

int DataBlock_get_int_array(DataBlock *data_block, const char * section, const char * name, int ** value, int * ndim, size_t ** shape);

int DataBlock_get_long_array(DataBlock *data_block, const char * section, const char * name, long ** value, int * ndim, size_t ** shape);

int DataBlock_get_float_array(DataBlock *data_block, const char * section, const char * name, float ** value, int * ndim, size_t ** shape);

int DataBlock_get_double_array(DataBlock *data_block, const char * section, const char * name, double ** value, int * ndim, size_t ** shape);

// Array setters

int DataBlock_set_int_array(DataBlock *data_block, const char * section, const char * name, int * value, int ndim, size_t * shape);

int DataBlock_set_long_array(DataBlock *data_block, const char * section, const char * name, long * value, int ndim, size_t * shape);

int DataBlock_set_float_array(DataBlock *data_block, const char * section, const char * name, float * value, int ndim, size_t * shape);

int DataBlock_set_double_array(DataBlock *data_block, const char * section, const char * name, double * value, int ndim, size_t * shape);


#ifdef __cplusplus
}
#endif

#endif
