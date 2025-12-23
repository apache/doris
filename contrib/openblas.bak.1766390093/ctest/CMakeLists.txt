include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

if (NOT NOFORTRAN)
enable_language(Fortran)
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DADD${BU} -DCBLAS")
if (BINARY32 AND CMAKE_C_PLATFORM_ID MATCHES "MinGW" AND CMAKE_Fortran_COMPILER_VERSION VERSION_GREATER 14.1)
       list(REMOVE_ITEM ${CMAKE_Fortran_FLAGS} -O3 -O2 -O1 -Os)
       set (CMAKE_Fortran_FLAGS_RELEASE "" CACHE STRING "" FORCE)
endif()
if (CMAKE_Fortran_COMPILER_ID STREQUAL GNU)
	set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -fno-tree-vectorize")
endif()

if(WIN32)
FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.ps1
"$ErrorActionPreference = \"Stop\"\n"
"Get-Content $args[1] | & $args[0]\n"
)
set(test_helper powershell -ExecutionPolicy Bypass "${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.ps1")
else()
FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.sh
"$1 < $2\n"
)
set(test_helper sh "${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.sh")
endif()

foreach(float_type ${FLOAT_TYPES})
  string(SUBSTRING ${float_type} 0 1 float_char_upper)
  string(TOLOWER ${float_char_upper} float_char)
  if (${float_char} STREQUAL "b")
   continue()
  endif()
  #level1
if (NOT NOFORTRAN)
  add_executable(x${float_char}cblat1
    c_${float_char}blat1.f
    c_${float_char}blas1.c)
else()
  add_executable(x${float_char}cblat1
    c_${float_char}blat1c.c
    c_${float_char}blas1.c)
endif()
  target_link_libraries(x${float_char}cblat1 ${OpenBLAS_LIBNAME})
  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD" OR ${CMAKE_SYSTEM_NAME} MATCHES "QNX")
      target_link_libraries(x${float_char}cblat1 m)
  endif()
  add_test(NAME "x${float_char}cblat1"
    COMMAND $<TARGET_FILE:x${float_char}cblat1>)

  #level2
if (NOT NOFORTRAN)
  add_executable(x${float_char}cblat2
    c_${float_char}blat2.f
    c_${float_char}blas2.c
    c_${float_char}2chke.c
    auxiliary.c
    c_xerbla.c
    constant.c)
else()
  add_executable(x${float_char}cblat2
    c_${float_char}blat2c.c
    c_${float_char}blas2.c
    c_${float_char}2chke.c
    auxiliary.c
    c_xerbla.c
    constant.c)
endif()
  target_link_libraries(x${float_char}cblat2 ${OpenBLAS_LIBNAME})
  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD" OR ${CMAKE_SYSTEM_NAME} MATCHES "QNX")
    target_link_libraries(x${float_char}cblat2 m)
  endif()
  add_test(NAME "x${float_char}cblat2"
    COMMAND ${test_helper} $<TARGET_FILE:x${float_char}cblat2> "${PROJECT_SOURCE_DIR}/ctest/${float_char}in2")

  #level3
if (NOT NOFORTRAN)
  add_executable(x${float_char}cblat3
    c_${float_char}blat3.f
    c_${float_char}blas3.c
    c_${float_char}3chke.c
    auxiliary.c
    c_xerbla.c
    constant.c)
  if (USE_GEMM3M)
  if ((${float_char} STREQUAL "c") OR (${float_char} STREQUAL "z"))
  add_executable(x${float_char}cblat3_3m
    c_${float_char}blat3_3m.f
    c_${float_char}blas3_3m.c
    c_${float_char}3chke_3m.c
    auxiliary.c
    c_xerbla.c
    constant.c)
  endif()
  endif()
else()
  add_executable(x${float_char}cblat3
    c_${float_char}blat3c.c
    c_${float_char}blas3.c
    c_${float_char}3chke.c
    auxiliary.c
    c_xerbla.c
    constant.c)
  if (USE_GEMM3M)
  if ((${float_char} STREQUAL "c") OR (${float_char} STREQUAL "z"))
  add_executable(x${float_char}cblat3_3m
    c_${float_char}blat3c_3m.c
    c_${float_char}blas3_3m.c
    c_${float_char}3chke_3m.c
    auxiliary.c
    c_xerbla.c
    constant.c)
  endif()
  endif()
endif()
  target_link_libraries(x${float_char}cblat3 ${OpenBLAS_LIBNAME})
  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD" OR ${CMAKE_SYSTEM_NAME} MATCHES "QNX")
    target_link_libraries(x${float_char}cblat3 m)
  endif()
  if (USE_GEMM3M)
  if ((${float_char} STREQUAL "c") OR (${float_char} STREQUAL "z"))
  target_link_libraries(x${float_char}cblat3_3m ${OpenBLAS_LIBNAME})
  if(${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD" OR ${CMAKE_SYSTEM_NAME} MATCHES "QNX")
    target_link_libraries(x${float_char}cblat3_3m m)
  endif()
  endif()
  endif()
  add_test(NAME "x${float_char}cblat3"
    COMMAND ${test_helper} $<TARGET_FILE:x${float_char}cblat3> "${PROJECT_SOURCE_DIR}/ctest/${float_char}in3")
    if (USE_GEMM3M)
    if ((${float_char} STREQUAL "c") OR (${float_char} STREQUAL "z"))
  add_test(NAME "x${float_char}cblat3_3m"
    COMMAND ${test_helper} $<TARGET_FILE:x${float_char}cblat3_3m> "${PROJECT_SOURCE_DIR}/ctest/${float_char}in3_3m")
    endif()
  endif()
endforeach()
