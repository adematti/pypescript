! Macros for DECLARATIONS and C-interface
#include "macros.fi"

! Here we define stuff only useful for the tests performed in the module, do not use them in your own!
#define ANSWER 42
#define ASIZE 100
#define NDIM 1
#define GENERATE_ALL(__name,__type) ; \
  function all/**/__name/**/(array, scalar, size) result(toret); \
    integer :: toret ; \
    __type, pointer, dimension(:) :: array ; \
    __type :: scalar ; \
    integer(kind=c_size_t) :: i, size ; \
    do i = 1, size, 1 ; \
      if (array(i) .ne. scalar) goto 1 ; \
    end do ; \
    goto 2 ; \
    1 toret = 0 ; \
    2 toret = 1 ; \
  end function all/**/__name/**/ ; \


module FModule

  use mpi
  use pypescript_types
  use pypescript_block
  implicit none
  character(len=*), parameter :: MODULE_NAME = "FModule"

  contains

  ! Here we define stuff only useful for the tests performed in the module
  GENERATE_ALL(int,integer(c_int))

  GENERATE_ALL(long,integer(c_long))

  GENERATE_ALL(float,real(c_float))

  GENERATE_ALL(double,real(c_double))

  function setup(name, config_block, data_block) result(status)
    ! Set up module (called at the beginning)
    ! In the following we are doing stupid things as an example
    ! Return -1 if something wrong happens
    DECLARATIONS
    character(len=100) :: msg
    integer, parameter :: ndim = NDIM
    integer(kind=c_int) :: answer
    integer(kind=c_long) :: long_answer
    real(kind=c_float) :: float_answer
    real(kind=c_double) :: double_answer
    integer(kind=c_size_t) :: shpe(1)
    character(len=20) :: string_scalar
    integer(kind=c_int), pointer, dimension(:) :: int_array
    integer(kind=c_long), pointer, dimension(:) :: long_array
    real(kind=c_float), pointer, dimension(:) :: float_array
    real(kind=c_double), pointer, dimension(:) :: double_array
    integer :: comm
    integer :: rank, size, nlen, ierr
    character (len=MPI_MAX_PROCESSOR_NAME) :: pname
    status = 0
    shpe = ASIZE
    answer = 0

    write(msg, '("Setting up module [",A,"].")') trim(name)
    status = log_info(MODULE_NAME, msg)

    if (DataBlock_get_mpi_comm_default(data_block, MPI_SECTION, "comm", comm, MPI_COMM_WORLD) .lt. 0) goto 1

    call MPI_Comm_rank(comm, rank, ierr)
    call MPI_Comm_size(comm, size, ierr)
    call MPI_Get_processor_name(pname, nlen, ierr)
    write(msg, '("Hello, World! I am process ",I2," of ",I2," on ",A,".")')  rank, size, pname(1:nlen)
    ! write(msg, '("Hello, World! I am process ",I2," of ",I2," on ")')  rank, size
    status = log_info(MODULE_NAME, msg)

    if (DataBlock_get_int_default(config_block, name, "answer", answer, ANSWER) .lt. 0) goto 1
    if (answer .ne. ANSWER) goto 1
    if (DataBlock_get_long_default(config_block, name, "answer", long_answer, int(ANSWER,kind=c_long)) .lt. 0) goto 1
    if (long_answer .ne. ANSWER) goto 1
    if (DataBlock_get_float_default(config_block, name, "answer", float_answer, real(ANSWER,kind=c_float)) .lt. 0) goto 1
    if (float_answer .ne. ANSWER) goto 1
    if (DataBlock_get_double_default(config_block, name, "answer", double_answer, real(ANSWER,kind=c_double)) .lt. 0) goto 1
    if (double_answer .ne. ANSWER) goto 1
    if (DataBlock_set_int(data_block, PARAMETERS_SECTION, "int", ANSWER) .ne. 0) goto 1
    if (DataBlock_set_long(data_block, PARAMETERS_SECTION, "long", int(ANSWER,kind=c_long)) .ne. 0) goto 1
    if (DataBlock_set_float(data_block, PARAMETERS_SECTION, "float", real(ANSWER,kind=c_float)) .ne. 0) goto 1
    if (DataBlock_set_double(data_block, PARAMETERS_SECTION, "double", real(ANSWER,kind=c_double)) .ne. 0) goto 1
    ! Note that DataBlock_set_string creates a copy of the passed value: you should free it if necessary, for example:
    write(string_scalar, *) "string"
    ! Write adds leading white space, remove it with 2:
    if (DataBlock_set_string(data_block, PARAMETERS_SECTION, "string", string_scalar(2:)) .ne. 0) goto 1
    allocate(int_array(ASIZE))
    allocate(long_array(ASIZE))
    allocate(float_array(ASIZE))
    allocate(double_array(ASIZE))
    int_array(:) = answer
    long_array(:) = answer
    float_array(:) = answer
    double_array(:) = answer
    ! DataBlock_set_xxx_array steals the reference, i.e. it takes full responsibility of the array it receives
    ! Hence these arrays should not be deallocated
    ! Hence NEVER do DataBlock_set_xxx_array with an array (and shape) coming from DataBlock_get_xxx_array:
    ! as the array might be freed using the first pointer without the second knowing it
    ! Rather use DataBlock_duplicate_value (or DataBlock_move_value) which will increase the reference counter appropriately
    if (DataBlock_set_int_array(data_block, PARAMETERS_SECTION, "int_array", int_array, ndim, shpe) .ne. 0) goto 1
    if (DataBlock_set_long_array(data_block, PARAMETERS_SECTION, "long_array", long_array, ndim, shpe) .ne. 0) goto 1
    if (DataBlock_set_float_array(data_block, PARAMETERS_SECTION, "float_array", float_array, ndim, shpe) .ne. 0) goto 1
    if (DataBlock_set_double_array(data_block, PARAMETERS_SECTION, "double_array", double_array, ndim, shpe) .ne. 0) goto 1
    goto 2

1   status = -1
2  end function setup

  function execute(name, config_block, data_block) result(status)
    ! Execute module, i.e. do calculation (called at each iteration)
    ! In the following we are doing stupid things as an example
    ! Return -1 if something wrong happens
    DECLARATIONS
    character(len=60) :: msg
    integer :: answer, answer2
    integer :: ndim
    integer(kind=c_int) :: int_scalar
    integer(kind=c_long) :: long_scalar
    real(kind=c_float) :: float_scalar
    real(kind=c_double) :: double_scalar
    character(len=40) :: string_scalar
    integer(kind=c_size_t), pointer, dimension(:) :: shpe
    integer(kind=c_size_t) :: i
    integer(kind=c_size_t), parameter :: asize = ASIZE
    integer(kind=c_int), pointer, dimension(:) :: int_array
    integer(kind=c_long), pointer, dimension(:) :: long_array
    real(kind=c_float), pointer, dimension(:) :: float_array
    real(kind=c_double), pointer, dimension(:) :: double_array
    status = 0
    ndim = 0
    answer = 0
    ! Execute module, i.e. do calculation (called at each iteration)
    ! In the following we are doing stupid things as an example
    ! Return -1 if something wrong happens
    write(msg, '("Executing module [",A,"].")') trim(name)
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "int") .ne. 1) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int") .ne. 0) goto 1
    ! Deleting an already-deleted value is silly
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int") .eq. 0) goto 1
    ! So let's suppress all previous errors (not be used with caution)
    call clear_errors()
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "long") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "float") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "double") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "string") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "int_array") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "long_array") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "float_array") .ne. 1) goto 1
    if (DataBlock_has_value(data_block, PARAMETERS_SECTION, "double_array") .ne. 1) goto 1
    if (DataBlock_move_value(data_block, PARAMETERS_SECTION, "long", PARAMETERS_SECTION, "long2") .ne. 0) goto 1
    if (DataBlock_duplicate_value(data_block, PARAMETERS_SECTION, "int_array", PARAMETERS_SECTION, "int_array2") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "long2") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "float") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "double") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "string") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int_array") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "int_array2") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "long_array") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "float_array") .ne. 0) goto 1
    if (DataBlock_del_value(data_block, PARAMETERS_SECTION, "double_array") .ne. 0) goto 1
    if (setup(name, config_block, data_block) .ne. 0) goto 1

    if (DataBlock_get_int_default(config_block, name, "answer", answer, ANSWER) .lt. 0) goto 1
    write(msg, '("Answer is ",I2,".")') answer
    status = log_info(MODULE_NAME, msg)
    ! NULL as last argument: exception raised if value not in data_block
    if (DataBlock_get_int(data_block, PARAMETERS_SECTION, "int", int_scalar) .lt. 0) goto 1
    write(msg, '("int is ",I2,".")') int_scalar
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_get_long(data_block, PARAMETERS_SECTION, "long", long_scalar) .lt. 0) goto 1
    write(msg, '("long is ",I2,".")') long_scalar
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_get_float(data_block, PARAMETERS_SECTION, "float", float_scalar) .lt. 0) goto 1
    write(msg, '("float is ",F6.3,".")') float_scalar
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_get_double(data_block, PARAMETERS_SECTION, "double", double_scalar) .lt. 0) goto 1
    write(msg, '("double is ",F6.3,".")') double_scalar
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_get_string(data_block, PARAMETERS_SECTION, "string", string_scalar) .lt. 0) goto 1
    write(msg, '("string is ",A,".")') trim(string_scalar)
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_get_int_array(data_block, PARAMETERS_SECTION, "int_array", int_array, ndim, shpe) .lt. 0) goto 1
    if ((ndim .ne. NDIM) .or. (shpe(1) .ne. asize) .or. (allint(int_array,answer,asize) .ne. 1)) goto 1
    if (DataBlock_get_long_array(data_block, PARAMETERS_SECTION, "long_array", long_array, ndim, shpe) .lt. 0) goto 1
    if ((ndim .ne. NDIM) .or. (shpe(1) .ne. asize) .or. (alllong(long_array,int(answer,kind=c_long),asize) .ne. 1)) goto 1
    if (DataBlock_get_float_array(data_block, PARAMETERS_SECTION, "float_array", float_array, ndim, shpe) .lt. 0) goto 1
    if ((ndim .ne. NDIM) .or. (shpe(1) .ne. asize) .or. (allfloat(float_array,real(answer,kind=c_float),asize) .ne. 1)) goto 1
    if (DataBlock_get_double_array(data_block, PARAMETERS_SECTION, "double_array", double_array, ndim, shpe) .lt. 0) goto 1
    if ((ndim .ne. NDIM) .or. (shpe(1) .ne. asize) .or. (alldouble(double_array,real(answer,kind=c_double),asize) .ne. 1)) goto 1
    ! In place operations, values in DataBlock updated automatically
    do i = 1, shpe(1), 1
      int_array(i) = int_array(i) + 1
      long_array(i) = long_array(i) + 2
      float_array(i) = float_array(i) + 1.0
      double_array(i) = double_array(i) + 2.0
    end do
    answer2 = answer + 1;
    ! This will cast int to long, authorised (the inverse is not!); then int_array may not be alive anymore
    if (DataBlock_get_long_array(data_block, PARAMETERS_SECTION, "int_array", long_array, ndim, shpe) .lt. 0) goto 1
    if ((ndim .ne. NDIM) .or. (shpe(1) .ne. asize) .or. (alllong(long_array,int(answer2,kind=c_long),shpe(1)) .ne. 1)) goto 1
    do i = 1, shpe(1), 1
      long_array(i) = long_array(i) + 1
    end do
    if (DataBlock_get_double_array(data_block, PARAMETERS_SECTION, "float_array", double_array, ndim, shpe) .lt. 0) goto 1
    if ((ndim .ne. NDIM) .or. (shpe(1) .ne. asize) .or. (alldouble(double_array,real(answer2,kind=c_double),shpe(1)) .ne. 1)) goto 1
    do i = 1, shpe(1), 1
      double_array(i) = double_array(i) + 1.0
    end do
    ! Now let's read in arrays defined in other modules
    if (DataBlock_get_int_array(data_block, "external", "int_array", int_array, ndim, shpe) .lt. 0) goto 1
    write(msg, '("External int array elements are [",I2,", ",I2,",...].")') int_array(1), int_array(2)
    status = log_info(MODULE_NAME, msg)
    write(msg, '("External int array dimensions are ",I2,", shape is (",I4,"...).")') ndim, shpe(1)
    status = log_info(MODULE_NAME, msg)
    if (DataBlock_get_float_array(data_block, "external", "float_array", float_array, ndim, shpe) .lt. 0) goto 1
    write(msg, '("External int array elements are [",F6.3,", ",F6.3,",...].")') float_array(1), float_array(2)
    status = log_info(MODULE_NAME, msg)
    write(msg, '("External int array dimensions are ",I2,", shape is (",I4,"...).")') ndim, shpe(1)
    status = log_info(MODULE_NAME, msg)
    do i = 1, shpe(1), 1
      int_array(i) = int_array(i) + 1
      float_array(i) = float_array(i) + 1.0
    end do
    goto 2

1   status = -1
2  end function execute


  function cleanup(name, config_block, data_block) result(status)
    ! Clean up, i.e. free variables if needed (called at the end)
    DECLARATIONS
    character(len=40) :: msg

    write(msg, '("Cleaning up module [",A,"].")') trim(name)
    status = log_info(MODULE_NAME, msg)

  end function cleanup

end module


! Let us generate the C interface, REQUIRED!

GENERATE_CINTERFACE(FModule)
