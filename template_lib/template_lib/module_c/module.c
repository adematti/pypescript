#include "math.h"
#include "stdlib.h"
#include "stdio.h"
#include <mpi.h>
#include "pypelib.h"

// Here we define stuff only useful for the tests performed in the module
#define ANSWER 42
#define ASIZE 100
#define NDIM 1
#define GENERATE_ALL(__type)\
  int all##__type(__type * array, __type scalar, size_t size) {\
      for (size_t i=0;i<size;i++) {\
        if (array[i] != scalar) return 0;\
    }\
    return 1;\
  }\

GENERATE_ALL(int)
GENERATE_ALL(long)
GENERATE_ALL(float)
GENERATE_ALL(double)

//#define MODULE_NAME "CModule"
const char * MODULE_NAME = "CModule";

typedef struct {
  size_t n;
  float x;
} TestStruct;

int setup(const char * name, DataBlock *config_block, DataBlock *data_block) {
  // Set up module (called at the beginning)
  // In the following we are doing stupid things as an example
  // Return -1 if something wrong happens
  int status = 0;
  status = log_info(MODULE_NAME, "Setting up module [%s].", name);
  int answer = 0;
  long long_answer = 0;
  float float_answer = 0.;
  double double_answer = 0.;

  MPI_Comm comm;
  if (DataBlock_get_mpi_comm_default(data_block, MPI_SECTION, "comm", &comm, MPI_COMM_WORLD) < 0) goto except;
  int rank, size;
  char pname[MPI_MAX_PROCESSOR_NAME]; int len;
  MPI_Comm_size(comm, &size);
  MPI_Comm_rank(comm, &rank);
  MPI_Get_processor_name(pname, &len);
  pname[len] = 0;
  status = log_info(MODULE_NAME, "Hello, world! I am process %d of %d on %s.", rank, size, pname);

  if (DataBlock_get_int_default(config_block, name, "answer", &answer, ANSWER) < 0) goto except;
  if (answer != ANSWER) goto except;
  if (DataBlock_get_long_default(config_block, name, "answer", &long_answer, ANSWER) < 0) goto except;
  if (long_answer != ANSWER) goto except;
  if (DataBlock_get_float_default(config_block, name, "answer", &float_answer, ANSWER) < 0) goto except;
  if (float_answer != ANSWER) goto except;
  if (DataBlock_get_double_default(config_block, name, "answer", &double_answer, ANSWER) < 0) goto except;
  if (double_answer != ANSWER) goto except;
  if (DataBlock_set_int(data_block, PARAMETERS_SECTION, "int", ANSWER) != 0) goto except;
  if (DataBlock_set_long(data_block, PARAMETERS_SECTION, "long", ANSWER) != 0) goto except;
  if (DataBlock_set_float(data_block, PARAMETERS_SECTION, "float", ANSWER) != 0) goto except;
  if (DataBlock_set_double(data_block, PARAMETERS_SECTION, "double", ANSWER) != 0) goto except;

  // Note that DataBlock_set_string creates a copy of the passed value: you should free it if necessary, for example:
  char * string_scalar = (char *) malloc(sizeof(char)*10);
  sprintf(string_scalar,"string");
  if (DataBlock_set_string(data_block, PARAMETERS_SECTION, "string", string_scalar) != 0) goto except;
  free(string_scalar);
  int ndim = NDIM;
  size_t shape[1] = {ASIZE};
  int *int_array = (int *) malloc(sizeof(int)*ASIZE);
  for (size_t i=0;i<ASIZE;i++) int_array[i] = answer;
  long *long_array = (long *) malloc(sizeof(long)*ASIZE);
  for (size_t i=0;i<ASIZE;i++) long_array[i] = (long) answer;
  float *float_array = (float *) malloc(sizeof(float)*ASIZE);
  for (size_t i=0;i<ASIZE;i++) float_array[i] = (float) answer;
  double *double_array = (double *) malloc(sizeof(double)*ASIZE);
  for (size_t i=0;i<ASIZE;i++) double_array[i] = (double) answer;
  // DataBlock_set_xxx_array steals the reference, i.e. it takes full responsibility of the array it receives
  // Hence these arrays should not be freed
  // Hence NEVER do DataBlock_set_xxx_array with an array (and shape) coming from DataBlock_get_xxx_array:
  // as the array might be freed using the first pointer without the second knowing it
  // Rather use DataBlock_duplicate_value (or DataBlock_move_value) which will increase the reference counter appropriately
  if (DataBlock_set_int_array(data_block, PARAMETERS_SECTION, "int_array", int_array, ndim, shape) != 0) goto except;
  if (DataBlock_set_long_array(data_block, PARAMETERS_SECTION, "long_array", long_array, ndim, shape) != 0) goto except;
  if (DataBlock_set_float_array(data_block, PARAMETERS_SECTION, "float_array", float_array, ndim, shape) != 0) goto except;
  if (DataBlock_set_double_array(data_block, PARAMETERS_SECTION, "double_array", double_array, ndim, shape) != 0) goto except;

  TestStruct* s = (TestStruct*) malloc(sizeof(TestStruct));
  s->n = 42;
  s->x = 42.0;
  if (DataBlock_set_capsule(config_block, name, "capsule", (void *) s) != 0) goto except;
  goto finally;
except:
  status = -1;
finally:
  return status;
}


int execute(const char * name, DataBlock *config_block, DataBlock *data_block) {
  // Execute module, i.e. do calculation (called at each iteration)
  // In the following we are doing stupid things as an example
  // Return -1 if something wrong happens
  int status = 0;
  status = log_info(MODULE_NAME, "Executing module [%s].", name);
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "int")) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int") != 0) goto except;
  // Deleting an already-deleted value is silly
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int") == 0) goto except;
  // So let's suppress all previous errors (not be used with caution)
  clear_errors();
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "long")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "float")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "double")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "string")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "int_array")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "long_array")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "float_array")) goto except;
  if (!DataBlock_has_value(data_block, PARAMETERS_SECTION, "double_array")) goto except;
  if (DataBlock_move_value(data_block, PARAMETERS_SECTION, "long", PARAMETERS_SECTION, "long2") != 0) goto except;
  if (DataBlock_duplicate_value(data_block, PARAMETERS_SECTION, "int_array", PARAMETERS_SECTION, "int_array2") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "long2") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "float") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "double") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "string") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int_array") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int_array2") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "long_array") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "float_array") != 0) goto except;
  if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "double_array") != 0) goto except;
  if (setup(name, config_block, data_block) != 0) goto except;
  int ndim = NDIM;
  size_t *shape;
  int int_scalar, *int_array;
  long long_scalar, *long_array;
  float float_scalar, *float_array;
  double double_scalar, *double_array;
  char *string_scalar;
  int answer = ANSWER;
  if (DataBlock_get_int_default(config_block, name, "answer", &answer, answer) < 0) goto except;
  status = log_info(MODULE_NAME, "Answer is %d.", answer);
  // NULL as last argument: exception raised if value not in data_block
  if (DataBlock_get_int(data_block, PARAMETERS_SECTION, "int", &int_scalar) < 0) goto except;
  status = log_info(MODULE_NAME, "int is %d.", int_scalar);
  if (DataBlock_get_long(data_block, PARAMETERS_SECTION, "long", &long_scalar) < 0) goto except;
  status = log_info(MODULE_NAME, "long is %ld.", long_scalar);
  if (DataBlock_get_float(data_block, PARAMETERS_SECTION, "float", &float_scalar) < 0) goto except;
  status = log_info(MODULE_NAME, "float is %.3f.", float_scalar);
  if (DataBlock_get_double(data_block, PARAMETERS_SECTION, "double", &double_scalar) < 0) goto except;
  status = log_info(MODULE_NAME, "double is %.3f.", double_scalar);
  if (DataBlock_get_string(data_block, PARAMETERS_SECTION, "string", &string_scalar) < 0) goto except;
  status = log_info(MODULE_NAME, "string is %s.", string_scalar);
  if (DataBlock_get_int_array(data_block, PARAMETERS_SECTION, "int_array", &int_array, &ndim, &shape) < 0) goto except;
  if ((ndim != NDIM) || (shape[0] != ASIZE) || !allint(int_array,answer,ASIZE)) goto except;
  if (DataBlock_get_long_array(data_block, PARAMETERS_SECTION, "long_array", &long_array, &ndim, &shape) < 0) goto except;
  if ((ndim != NDIM) || (shape[0] != ASIZE) || !alllong(long_array,answer,ASIZE)) goto except;
  if (DataBlock_get_float_array(data_block, PARAMETERS_SECTION, "float_array", &float_array, &ndim, &shape) < 0) goto except;
  if ((ndim != NDIM) || (shape[0] != ASIZE) || !allfloat(float_array,(float) answer,ASIZE)) goto except;
  if (DataBlock_get_double_array(data_block, PARAMETERS_SECTION, "double_array", &double_array, &ndim, &shape) < 0) goto except;
  if ((ndim != NDIM) || (shape[0] != ASIZE) || !alldouble(double_array,(double) answer,ASIZE)) goto except;
  // In place operations, values in DataBlock updated automatically
  for (size_t i=0;i<shape[0];i++)
  {
    int_array[i] += 1;
    long_array[i] += 2;
    float_array[i] += 1;
    double_array[i] += 2;
  }
  int answer2 = answer + 1;
  // This will cast int to long, authorised (the inverse is not!); then int_array may not be alive anymore
  if (DataBlock_get_long_array(data_block, PARAMETERS_SECTION, "int_array", &long_array, &ndim, &shape) < 0) goto except;
  if ((ndim != NDIM) || (shape[0] != ASIZE) || !alllong(long_array,answer2,shape[0])) goto except;
  for (size_t i=0;i<shape[0];i++) long_array[i] += 1;
  if (DataBlock_get_double_array(data_block, PARAMETERS_SECTION, "float_array", &double_array, &ndim, &shape) < 0) goto except;
  if ((ndim != NDIM) || (shape[0] != ASIZE) || !alldouble(double_array,answer2,shape[0])) goto except;
  for (size_t i=0;i<shape[0];i++) double_array[i] += 1;
  // Now let's read in arrays defined in other modules
  if (DataBlock_get_int_array(data_block, "external", "int_array", &int_array, &ndim, &shape) < 0) goto except;
  status = log_info(MODULE_NAME, "External int array elements are [%d %d ...].", int_array[0], int_array[1]);
  status = log_info(MODULE_NAME, "External int array dimensions are %d, shape is (%d, ...).", ndim, shape[0]);
  if (DataBlock_get_float_array(data_block, "external", "float_array", &float_array, &ndim, &shape) < 0) goto except;
  status = log_info(MODULE_NAME, "External float array elements are [%.3f %.3f ...].", float_array[0], float_array[1]);
  status = log_info(MODULE_NAME, "External float array dimensions are %d, shape is (%d, ...).", ndim, shape[0]);
  for (size_t i=0;i<shape[0];i++) int_array[i] += 1;
  for (size_t i=0;i<shape[0];i++) float_array[i] += 1;
  TestStruct* s;
  if (DataBlock_get_capsule(config_block, name, "capsule", &s) != 0) goto except;
  if ((s->n != 42) || (s->x != 42.0)) goto except;
  goto finally;
except:
  status = -1;
finally:
  return status;
}

int cleanup(const char * name, DataBlock *config_block, DataBlock *data_block) {
  // Clean up, i.e. free variables if needed (called at the end)
  int status = 0;
  status = log_info(MODULE_NAME, "Cleaning up module [%s].", name);
  void* s;
  if (DataBlock_get_capsule(config_block, name, "capsule", &s) != 0) goto except;
  free(s);
  goto finally;
except:
  status = -1;
finally:
  return status;
}
