#define GENERATE_LOG_WRAPPER(__name) ; \
  function /**/__name/**/_wrapper(name, string) bind(C, name="__name") ; \
    use, intrinsic :: iso_c_binding ; \
    use pypescript_types ; \
    implicit none ; \
    integer(kind=DataBlock_status) :: /**/__name/**/_wrapper ; \
    character(kind=c_char), dimension(*) :: name ; \
    character(kind=c_char), dimension(*) :: string ; \
  end function /**/__name/**/_wrapper ; \

#define GENERATE_LOG(__name) ; \
  function __name(name, string) result(status) ; \
    integer(kind=DataBlock_status) :: status ; \
    character(len=*) :: name ; \
    character(len=*) :: string ; \
    status = /**/__name/**/_wrapper(trim(name)//C_NULL_CHAR, trim(string)//C_NULL_CHAR) ; \
  end function __name ; \

#define GENERATE_GET_SCALAR_DEFAULT_WRAPPER(__name,__type,__cname) ; \
  function DataBlock_get_/**/__name/**/_default_wrapper(data_block, section, name, value, default_value) bind(C, name=__cname) ; \
    use, intrinsic :: iso_c_binding ; \
    use pypescript_types ; \
    implicit none ; \
    integer(kind=DataBlock_status) :: DataBlock_get_/**/__name/**/_default_wrapper ; \
    integer(kind=DataBlock_type), value :: data_block ; \
    character(kind=c_char), dimension(*) :: section, name ; \
    __type :: value ; \
    __type, value :: default_value ; \
  end function DataBlock_get_/**/__name/**/_default_wrapper ; \

#define GENERATE_GET_SCALAR_WRAPPER(__name,__type,__cname) ; \
  function DataBlock_get_/**/__name/**/_wrapper(data_block, section, name, value) bind(C, name=__cname) ; \
    use, intrinsic :: iso_c_binding ; \
    use pypescript_types ; \
    implicit none ; \
    integer(kind=DataBlock_status) :: DataBlock_get_/**/__name/**/_wrapper ; \
    integer(kind=DataBlock_type), value :: data_block ; \
    character(kind=c_char), dimension(*) :: section, name ; \
    __type :: value ; \
  end function DataBlock_get_/**/__name/**/_wrapper ; \

#define GENERATE_GET_SCALAR_DEFAULT(__name,__type) ; \
  function DataBlock_get_/**/__name/**/_default(data_block, section, name, value, default_value) result(status) ; \
    integer(kind=DataBlock_status) :: status ; \
    integer(kind=DataBlock_type) :: data_block ; \
    character(len=*) :: section, name ; \
    __type :: value, default_value ; \
    status = DataBlock_get_/**/__name/**/_default_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, value, default_value) ; \
  end function DataBlock_get_/**/__name/**/_default ; \

#define GENERATE_GET_SCALAR(__name,__type) ; \
  function DataBlock_get_/**/__name/**/(data_block, section, name, value) result(status) ; \
    integer(kind=DataBlock_status) :: status ; \
    integer(kind=DataBlock_type) :: data_block ; \
    character(len=*) :: section, name ; \
    __type :: value ; \
    status = DataBlock_get_/**/__name/**/_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, value) ; \
  end function DataBlock_get_/**/__name/**/ ; \

#define GENERATE_SET_SCALAR_WRAPPER(__name,__type,__cname) ; \
  function DataBlock_set_/**/__name/**/_wrapper(data_block, section, name, value) bind(C, name=__cname) ; \
    use, intrinsic :: iso_c_binding ; \
    use pypescript_types ; \
    implicit none ; \
    integer(kind=DataBlock_status) :: DataBlock_set_/**/__name/**/_wrapper ; \
    integer(kind=DataBlock_type), value :: data_block ; \
    character(kind=c_char), dimension(*) :: section, name ; \
    __type, value :: value ; \
  end function DataBlock_set_/**/__name/**/_wrapper ; \

#define GENERATE_SET_SCALAR(__name,__type) ; \
  function DataBlock_set_/**/__name/**/(data_block, section, name, value) result(status) ; \
    integer(kind=DataBlock_status) :: status ; \
    integer(kind=DataBlock_type) :: data_block ; \
    character(len=*) :: section, name ; \
    __type :: value ; \
    status = DataBlock_set_/**/__name/**/_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, value) ; \
  end function DataBlock_set_/**/__name/**/ ; \

#define GENERATE_GET_ARRAY_WRAPPER(__name,__cname) ; \
  function DataBlock_get_/**/__name/**/_array_wrapper(data_block, section, name, value, ndim, shpe) bind(C, name=__cname) ; \
    use, intrinsic :: iso_c_binding ; \
    use pypescript_types ; \
    implicit none ; \
    integer(kind=DataBlock_status) :: DataBlock_get_/**/__name/**/_array_wrapper ; \
    integer(kind=DataBlock_type), value :: data_block ; \
    character(kind=c_char), dimension(*) :: section, name ; \
    integer(kind=c_int) :: ndim ; \
    type(c_ptr) :: value, shpe ; \
  end function DataBlock_get_/**/__name/**/_array_wrapper ; \

#define GENERATE_GET_ARRAY(__name,__type) ; \
  function DataBlock_get_/**/__name/**/_array(data_block, section, name, value, ndim, shpe) result(status) ; \
    integer(kind=DataBlock_status) :: status ; \
    integer(kind=DataBlock_type) :: data_block ; \
    character(len=*) :: section, name ; \
    __type, pointer, dimension(:) :: value ; \
    integer(kind=c_int) :: ndim ; \
    integer(kind=c_size_t), pointer, dimension(:) :: shpe ; \
    type(c_ptr) :: cvalue, cshpe ; \
    status = DataBlock_get_/**/__name/**/_array_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, cvalue, ndim, cshpe) ; \
    if (status == 0) then ; \
      call c_f_pointer(cshpe, shpe, [ndim]) ; \
      call c_f_pointer(cvalue, value, shpe) ; \
    endif ; \
  end function DataBlock_get_/**/__name/**/_array ; \

#define GENERATE_SET_ARRAY_WRAPPER(__name,__type,__cname) ; \
  function DataBlock_set_/**/__name/**/_array_wrapper(data_block, section, name, value, ndim, shpe) bind(C, name=__cname) ; \
    use, intrinsic :: iso_c_binding ; \
    use pypescript_types ; \
    implicit none ; \
    integer(kind=DataBlock_status) :: DataBlock_set_/**/__name/**/_array_wrapper ; \
    integer(kind=DataBlock_type), value :: data_block ; \
    character(kind=c_char), dimension(*) :: section, name ; \
    __type, dimension(*) :: value ; \
    integer(kind=c_int), value :: ndim ; \
    integer(kind=c_size_t), dimension(ndim) :: shpe ; \
  end function DataBlock_set_/**/__name/**/_array_wrapper ; \

#define GENERATE_SET_ARRAY(__name,__type) ; \
  function DataBlock_set_/**/__name/**/_array(data_block, section, name, value, ndim, shpe) result(status) ; \
    integer(kind=DataBlock_status) :: status ; \
    integer(kind=DataBlock_type) :: data_block ; \
    character(len=*) :: section, name ; \
    __type, dimension(*) :: value ; \
    integer(kind=c_int) :: ndim ; \
    integer(kind=c_size_t), dimension(ndim) :: shpe ; \
    status = DataBlock_set_/**/__name/**/_array_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, value, ndim, shpe) ; \
  end function DataBlock_set_/**/__name/**/_array ; \


module pypescript_types

  use, intrinsic :: iso_c_binding
  integer, parameter :: DataBlock_type = c_size_t
  integer, parameter :: DataBlock_status = c_int

end module pypescript_types


module pypescript_wrappers

  use, intrinsic :: iso_c_binding
  implicit none

  interface

    ! Loggers

    GENERATE_LOG_WRAPPER(log_info)

    ! DataBlock stuffs

    subroutine clear_errors_wrapper() bind(C, name="clear_errors")
      use, intrinsic :: iso_c_binding
      implicit none
    end subroutine

    function DataBlock_has_value_wrapper(data_block, section, name) bind(C, name="DataBlock_has_value")
      use, intrinsic :: iso_c_binding
      use pypescript_types
      implicit none
      integer(kind=DataBlock_status) :: DataBlock_has_value_wrapper
      integer(kind=DataBlock_type), value :: data_block
      character(kind=c_char), dimension(*) :: section, name
    end function DataBlock_has_value_wrapper

    function DataBlock_del_value_wrapper(data_block, section, name) bind(C, name="DataBlock_del_value")
      use, intrinsic :: iso_c_binding
      use pypescript_types
      implicit none
      integer(kind=DataBlock_status) :: DataBlock_del_value_wrapper
      integer(kind=DataBlock_type), value :: data_block
      character(kind=c_char), dimension(*) :: section, name
    end function DataBlock_del_value_wrapper

    function DataBlock_duplicate_value_wrapper(data_block, section1, name1, section2, name2) bind(C, name="DataBlock_duplicate_value")
      use, intrinsic :: iso_c_binding
      use pypescript_types
      implicit none
      integer(kind=DataBlock_status) :: DataBlock_duplicate_value_wrapper
      integer(kind=DataBlock_type), value :: data_block
      character(kind=c_char), dimension(*) :: section1, name1, section2, name2
    end function DataBlock_duplicate_value_wrapper

    function DataBlock_move_value_wrapper(data_block, section1, name1, section2, name2) bind(C, name="DataBlock_move_value")
      use, intrinsic :: iso_c_binding
      use pypescript_types
      implicit none
      integer(kind=DataBlock_status) :: DataBlock_move_value_wrapper
      integer(kind=DataBlock_type), value :: data_block
      character(kind=c_char), dimension(*) :: section1, name1, section2, name2
    end function DataBlock_move_value_wrapper

    ! Scalar getters
    GENERATE_GET_SCALAR_DEFAULT_WRAPPER(mpi_comm,integer(c_int),"DataBlock_get_mpi_comm_default")
    GENERATE_GET_SCALAR_WRAPPER(mpi_comm,integer(c_int),"DataBlock_get_mpi_comm")

    GENERATE_GET_SCALAR_DEFAULT_WRAPPER(int,integer(c_int),"DataBlock_get_int_default")
    GENERATE_GET_SCALAR_WRAPPER(int,integer(c_int),"DataBlock_get_int")

    GENERATE_GET_SCALAR_DEFAULT_WRAPPER(long,integer(c_long),"DataBlock_get_long_default")
    GENERATE_GET_SCALAR_WRAPPER(long,integer(c_long),"DataBlock_get_long")

    GENERATE_GET_SCALAR_DEFAULT_WRAPPER(float,real(c_float),"DataBlock_get_float_default")
    GENERATE_GET_SCALAR_WRAPPER(float,real(c_float),"DataBlock_get_float")

    GENERATE_GET_SCALAR_DEFAULT_WRAPPER(double,real(c_double),"DataBlock_get_double_default")
    GENERATE_GET_SCALAR_WRAPPER(double,real(c_double),"DataBlock_get_double")

    function DataBlock_get_string_default_wrapper(data_block, section, name, value, default_value) bind(C, name="DataBlock_get_string_default")
      use, intrinsic :: iso_c_binding
      use pypescript_types
      implicit none
      integer(kind=DataBlock_status) :: DataBlock_get_string_default_wrapper
      integer(kind=DataBlock_type), value :: data_block
      character(kind=c_char), dimension(*) :: section, name, default_value
      type(c_ptr) :: value
    end function DataBlock_get_string_default_wrapper

    GENERATE_GET_SCALAR_WRAPPER(string,type(c_ptr),"DataBlock_get_string")

    ! Scalar setters

    GENERATE_SET_SCALAR_WRAPPER(int,integer(c_int),"DataBlock_set_int")

    GENERATE_SET_SCALAR_WRAPPER(long,integer(c_long),"DataBlock_set_long")

    GENERATE_SET_SCALAR_WRAPPER(float,real(c_float),"DataBlock_set_float")

    GENERATE_SET_SCALAR_WRAPPER(double,real(c_double),"DataBlock_set_double")

    function DataBlock_set_string_wrapper(data_block, section, name, value) bind(C, name="DataBlock_set_string")
      use, intrinsic :: iso_c_binding
      use pypescript_types
      implicit none
      integer(kind=DataBlock_status) :: DataBlock_set_string_wrapper
      integer(kind=DataBlock_type), value :: data_block
      character(kind=c_char), dimension(*) :: section, name
      character(kind=c_char), dimension(*) :: value
    end function DataBlock_set_string_wrapper

    ! Array getters

    GENERATE_GET_ARRAY_WRAPPER(int,"DataBlock_get_int_array")

    GENERATE_GET_ARRAY_WRAPPER(long,"DataBlock_get_long_array")

    GENERATE_GET_ARRAY_WRAPPER(float,"DataBlock_get_float_array")

    GENERATE_GET_ARRAY_WRAPPER(double,"DataBlock_get_double_array")

    ! Array setters

    GENERATE_SET_ARRAY_WRAPPER(int,integer(c_int),"DataBlock_set_int_array")

    GENERATE_SET_ARRAY_WRAPPER(long,integer(c_long),"DataBlock_set_long_array")

    GENERATE_SET_ARRAY_WRAPPER(float,real(c_float),"DataBlock_set_float_array")

    GENERATE_SET_ARRAY_WRAPPER(double,real(c_double),"DataBlock_set_double_array")

    function wrap_strlen(str) bind(C, name='strlen')
      use iso_c_binding
      implicit none
      type(c_ptr), value :: str
      integer(c_size_t) :: wrap_strlen
    end function wrap_strlen

    subroutine wrap_free(p) bind(C, name='free')
      use iso_c_binding
      implicit none
      type(c_ptr), value :: p
    end subroutine wrap_free

  end interface

  contains

  function c_string_to_fortran(c_str, max_len) result(f_str)
    integer(kind=c_size_t) :: max_len
    character(max_len) :: f_str
    character, pointer, dimension(:) :: p_str
    type(c_ptr) :: c_str
    integer(kind=c_size_t) :: n, shpe(1)
    integer(kind=c_size_t) :: i

    !Initialize an empty string
    do i=1,max_len-1
        f_str(i:i+1) = " "
    enddo

    !Check for NULL pointer.  If so translate as blank
    if(.not. c_associated(c_str)) return

    !Otherwise, get string length and copy that many chars
    n = wrap_strlen(c_str)
    shpe(1) = n
    call c_f_pointer(c_str, p_str, shpe)
    do i=1,n
        f_str(i:i+1) = p_str(i)
    enddo

  end function

end module pypescript_wrappers


module pypescript_block

  use pypescript_wrappers
  use pypescript_types
  implicit none

  ! integer(kind=c_size_t), parameter :: DATABLOCK_MAX_STRING_LENGTH=256

  contains

  GENERATE_LOG(log_info)

  subroutine clear_errors()
    call clear_errors_wrapper()
  end subroutine

  function DataBlock_has_value(data_block, section, name) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section, name
    status = DataBlock_has_value_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR)
  end function DataBlock_has_value

  function DataBlock_del_value(data_block, section, name) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section, name
    status = DataBlock_del_value_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR)
  end function DataBlock_del_value

  function DataBlock_duplicate_value(data_block, section1, name1, section2, name2) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section1, name1, section2, name2
    status = DataBlock_duplicate_value_wrapper(data_block, trim(section1)//C_NULL_CHAR, trim(name1)//C_NULL_CHAR,&
    & trim(section2)//C_NULL_CHAR, trim(name2)//C_NULL_CHAR)
  end function DataBlock_duplicate_value

  function DataBlock_move_value(data_block, section1, name1, section2, name2) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section1, name1, section2, name2
    status = DataBlock_move_value_wrapper(data_block, trim(section1)//C_NULL_CHAR, trim(name1)//C_NULL_CHAR,&
    & trim(section2)//C_NULL_CHAR, trim(name2)//C_NULL_CHAR)
  end function DataBlock_move_value

  ! Scalar getters

  GENERATE_GET_SCALAR_DEFAULT(mpi_comm,integer(c_int))
  GENERATE_GET_SCALAR(mpi_comm,integer(c_int))

  GENERATE_GET_SCALAR_DEFAULT(int,integer(c_int))
  GENERATE_GET_SCALAR(int,integer(c_int))

  GENERATE_GET_SCALAR_DEFAULT(long,integer(c_long))
  GENERATE_GET_SCALAR(long,integer(c_long))

  GENERATE_GET_SCALAR_DEFAULT(float,real(c_float))
  GENERATE_GET_SCALAR(float,real(c_float))

  GENERATE_GET_SCALAR_DEFAULT(double,real(c_double))
  GENERATE_GET_SCALAR(double,real(c_double))

  function DataBlock_get_string_default(data_block, section, name, value, default_value) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section, name, value, default_value
    type(c_ptr) :: cvalue
    status = DataBlock_get_string_default_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, cvalue, default_value)
    if (status == 0) then
      value = c_string_to_fortran(cvalue, wrap_strlen(cvalue))
    else if (status == 1) then
      value = default_value
    end if
  end function DataBlock_get_string_default

  function DataBlock_get_string(data_block, section, name, value) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section, name, value
    type(c_ptr) :: cvalue
    status = DataBlock_get_string_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, cvalue)
    if (status == 0) value = c_string_to_fortran(cvalue, wrap_strlen(cvalue))
  end function DataBlock_get_string

  function DataBlock_set_string(data_block, section, name, value) result(status)
    integer(kind=DataBlock_status) :: status
    integer(kind=DataBlock_type) :: data_block
    character(len=*) :: section, name, value
    status = DataBlock_set_string_wrapper(data_block, trim(section)//C_NULL_CHAR, trim(name)//C_NULL_CHAR, trim(value)//C_NULL_CHAR)
  end function DataBlock_set_string

  ! Scalar setters

  GENERATE_SET_SCALAR(int,integer(c_int))

  GENERATE_SET_SCALAR(long,integer(c_long))

  GENERATE_SET_SCALAR(float,real(c_float))

  GENERATE_SET_SCALAR(double,real(c_double))

  ! Array getters

  GENERATE_GET_ARRAY(int,integer(c_int))

  GENERATE_GET_ARRAY(long,integer(c_long))

  GENERATE_GET_ARRAY(float,real(c_float))

  GENERATE_GET_ARRAY(double,real(c_double))

  ! Array setters

  GENERATE_SET_ARRAY(int,integer(c_int))

  GENERATE_SET_ARRAY(long,integer(c_long))

  GENERATE_SET_ARRAY(float,real(c_float))

  GENERATE_SET_ARRAY(double,real(c_double))

end module pypescript_block
