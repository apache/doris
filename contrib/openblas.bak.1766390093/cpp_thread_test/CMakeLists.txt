include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

enable_language(CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -DADD${BU} -DCBLAS")

if (USE_OPENMP)
if (CPP_THREAD_SAFETY_TEST)
  message(STATUS building thread safety test)
  add_executable(dgemm_thread_safety dgemm_thread_safety.cpp) 
  target_link_libraries(dgemm_thread_safety ${OpenBLAS_LIBNAME})
  add_test( dgemm_thread_safety ${CMAKE_CURRENT_BINARY_DIR}/dgemm_thread_safety)
endif()


if (CPP_THREAD_SAFETY_TEST OR CPP_THREAD_SAFETY_GEMV)
  add_executable(dgemv_thread_safety dgemv_thread_safety.cpp) 
  target_link_libraries(dgemv_thread_safety ${OpenBLAS_LIBNAME})
  add_test(dgemv_thread_safety ${CMAKE_CURRENT_BINARY_DIR}/dgemv_thread_safety)
endif()

endif()
