include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

if (MSVC AND "${CMAKE_C_COMPILER_ID}" MATCHES Clang)
  set(OpenBLAS_utest_src utest_main2.c)
else ()
  set(OpenBLAS_utest_src
    utest_main.c
    test_min.c
    test_amax.c
    test_ismin.c
    test_rotmg.c
    test_rot.c
    test_axpy.c
    test_dsdot.c
    test_dnrm2.c
    test_swap.c
    test_zscal.c
    test_amin.c
    test_axpby.c
    test_gemv.c
  )
endif ()


set(DIR_EXT test_extensions)
set(OpenBLAS_utest_ext_src
utest_main.c
${DIR_EXT}/common.c
${DIR_EXT}/xerbla.c
${DIR_EXT}/test_isamin.c 
${DIR_EXT}/test_idamin.c 
${DIR_EXT}/test_icamin.c 
${DIR_EXT}/test_izamin.c 
${DIR_EXT}/test_ssum.c 
${DIR_EXT}/test_dsum.c 
${DIR_EXT}/test_scsum.c 
${DIR_EXT}/test_dzsum.c
${DIR_EXT}/test_samin.c
${DIR_EXT}/test_damin.c
${DIR_EXT}/test_scamin.c
${DIR_EXT}/test_dzamin.c
${DIR_EXT}/test_scamax.c
${DIR_EXT}/test_dzamax.c
${DIR_EXT}/test_zrotg.c
${DIR_EXT}/test_crotg.c
${DIR_EXT}/test_drotmg.c
${DIR_EXT}/test_srotmg.c
${DIR_EXT}/test_zscal.c
${DIR_EXT}/test_cscal.c
${DIR_EXT}/test_domatcopy.c
${DIR_EXT}/test_somatcopy.c
${DIR_EXT}/test_zomatcopy.c
${DIR_EXT}/test_comatcopy.c
${DIR_EXT}/test_simatcopy.c
${DIR_EXT}/test_dimatcopy.c
${DIR_EXT}/test_cimatcopy.c
${DIR_EXT}/test_zimatcopy.c
${DIR_EXT}/test_sgeadd.c
${DIR_EXT}/test_dgeadd.c
${DIR_EXT}/test_cgeadd.c
${DIR_EXT}/test_zgeadd.c
${DIR_EXT}/test_saxpby.c
${DIR_EXT}/test_daxpby.c
${DIR_EXT}/test_caxpby.c
${DIR_EXT}/test_zaxpby.c
${DIR_EXT}/test_caxpyc.c
${DIR_EXT}/test_zaxpyc.c
${DIR_EXT}/test_cgemv_t.c
${DIR_EXT}/test_zgemv_t.c
${DIR_EXT}/test_cgemv_n.c
${DIR_EXT}/test_zgemv_n.c
${DIR_EXT}/test_crot.c
${DIR_EXT}/test_zrot.c
${DIR_EXT}/test_cgbmv.c
${DIR_EXT}/test_zgbmv.c
${DIR_EXT}/test_dgemmt.c
${DIR_EXT}/test_sgemmt.c
${DIR_EXT}/test_cgemmt.c
${DIR_EXT}/test_zgemmt.c
${DIR_EXT}/test_ztrmv.c
${DIR_EXT}/test_ctrmv.c
${DIR_EXT}/test_ztrsv.c
${DIR_EXT}/test_ctrsv.c
${DIR_EXT}/test_zgemm.c
${DIR_EXT}/test_cgemm.c
)

# crashing on travis cl with an error code suggesting resource not found
if (NOT MSVC)
set(OpenBLAS_utest_src
  ${OpenBLAS_utest_src}
  test_dotu.c
  )
endif ()

# known to hang with the native Windows and Android threads
# FIXME needs checking if this works on any of the other platforms
if (OS_CYGWIN_NT OR OS_LINUX)
if (NOT USE_OPENMP)
set(OpenBLAS_utest_src
  ${OpenBLAS_utest_src}
  test_fork.c
  )
endif()
set(OpenBLAS_utest_src
  ${OpenBLAS_utest_src}
  test_post_fork.c
  )
endif()

if (NOT NO_LAPACK)
set(OpenBLAS_utest_src
  ${OpenBLAS_utest_src}
  test_potrs.c
  )
set(OpenBLAS_utest_ext_src
  ${OpenBLAS_utest_ext_src}
  ${DIR_EXT}/test_cspmv.c
  ${DIR_EXT}/test_zspmv.c
  ${DIR_EXT}/test_csbmv.c
  ${DIR_EXT}/test_zsbmv.c
  )
if (NOT NO_CBLAS AND NOT NO_LAPACKE)
set(OpenBLAS_utest_src
  ${OpenBLAS_utest_src}
  test_kernel_regress.c
  )
endif()
endif()

set(OpenBLAS_utest_bin openblas_utest)
add_executable(${OpenBLAS_utest_bin} ${OpenBLAS_utest_src})

set(OpenBLAS_utest_ext_bin openblas_utest_ext)
add_executable(${OpenBLAS_utest_ext_bin} ${OpenBLAS_utest_ext_src})

target_link_libraries(${OpenBLAS_utest_bin} ${OpenBLAS_LIBNAME})
target_link_libraries(${OpenBLAS_utest_ext_bin} ${OpenBLAS_LIBNAME})

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux" OR ${CMAKE_SYSTEM_NAME} MATCHES "FreeBSD" OR ${CMAKE_SYSTEM_NAME} MATCHES "QNX"  )
target_link_libraries(${OpenBLAS_utest_bin} m)
endif()

if (${CMAKE_SYSTEM_NAME} STREQUAL "WindowsStore")
set_target_properties( ${OpenBLAS_utest_bin} PROPERTIES COMPILE_DEFINITIONS "_CRT_SECURE_NO_WARNINGS")
endif()

#Set output for utest
set_target_properties( ${OpenBLAS_utest_bin} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
foreach (OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
  string( TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG )
  set_target_properties( ${OpenBLAS_utest_bin} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_CURRENT_BINARY_DIR})
endforeach()

if (MSVC AND BUILD_SHARED_LIBS)
add_custom_command(TARGET ${OpenBLAS_utest_bin}
          POST_BUILD
          COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_BINARY_DIR}/lib/${CMAKE_CFG_INTDIR}/${OpenBLAS_LIBNAME}.dll ${CMAKE_CURRENT_BINARY_DIR}/.
)
endif()

add_test(${OpenBLAS_utest_bin} ${CMAKE_CURRENT_BINARY_DIR}/${OpenBLAS_utest_bin})
add_test(${OpenBLAS_utest_ext_bin} ${CMAKE_CURRENT_BINARY_DIR}/${OpenBLAS_utest_bin})
