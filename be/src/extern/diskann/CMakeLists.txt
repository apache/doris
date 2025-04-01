# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT license.

# Parameters:
#
# BOOST_ROOT:
#   Specify root of the Boost library if Boost cannot be auto-detected. 
#
# DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS:
#   This is a work-in-progress feature, not completed yet. The core DiskANN library will be split into
#   build-related and search-related functionality. In build-related functionality, when using tcmalloc,
#   it's possible to release memory that's free but reserved by tcmalloc. Setting this to true enables
#   such behavior.
#   Contact for this feature: gopalrs.
cmake_minimum_required(VERSION 3.15)
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/extern")
project(diskann)
set(CMAKE_STANDARD 17)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_COMPILE_WARNING_AS_ERROR OFF)
add_compile_options(-Wno-error=attributes)
add_compile_options(-Wno-deprecated-copy)
add_compile_options(-Wno-reorder)
add_compile_options(-Wno-unused-but-set-variable)
add_compile_options(-Wno-error=unused-variable)


message(STATUS "CMAKE_C_FLAGS in be: ${CMAKE_C_FLAGS}")
message(STATUS "CMAKE_CXX_FLAGS in be: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_PREFIX_PATH in be: ${CMAKE_PREFIX_PATH}")
message(STATUS "LD_LIBRARY_PATH in be: $ENV{LD_LIBRARY_PATH}")
message(STATUS "CMAKE_MODULE_PATH in be: $ENV")


set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")



include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(/home/users/clz/baidu/third-party/doris-diskann/build)

if(NOT PYBIND)
    set(DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS ON)
endif()
# It's necessary to include tcmalloc headers only if calling into MallocExtension interface.
# For using tcmalloc in DiskANN tools, it's enough to just link with tcmalloc.
if (DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS)
    include_directories(${PROJECT_SOURCE_DIR}/gperftools/src)
endif()

#OpenMP
find_package(OpenMP)
message(" OpenMP1 ${OPENMP_FOUND} ${CMAKE_CXX_COMPILER}")
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
else()
    message(FATAL_ERROR "No OpenMP support")
endif()

# DiskANN core uses header-only libraries. Only DiskANN tools need program_options which has a linker library,
# but its size is small. Reduce number of dependent DLLs by linking statically.

# find_package(Boost COMPONENTS program_options)
# message("${Boost_PROGRAM_OPTIONS_INCLUDE_DIRS},${Boost_PROGRAM_OPTIONS_LIBRARIES}")
# if (NOT Boost_FOUND)
#     message(FATAL_ERROR "Couldn't find Boost dependency")
# endif()
include_directories(/home/users/clz/baidu/third-party/palo/thirdparty/installed/include/)
link_directories(/home/users/clz/baidu/third-party/palo/thirdparty/installed/lib64)

#MKL Config
# expected path for manual intel mkl installs
set(POSSIBLE_OMP_PATHS "/opt/intel/oneapi/compiler/latest/linux/compiler/lib/intel64_lin/libiomp5.so;/usr/lib/x86_64-linux-gnu/libiomp5.so;/opt/intel/lib/intel64_lin/libiomp5.so")
foreach(POSSIBLE_OMP_PATH ${POSSIBLE_OMP_PATHS})
    if (EXISTS ${POSSIBLE_OMP_PATH})
        get_filename_component(OMP_PATH ${POSSIBLE_OMP_PATH} DIRECTORY)
    endif()
endforeach()

if(NOT OMP_PATH)
    message(FATAL_ERROR "Could not find Intel OMP in standard locations; use -DOMP_PATH to specify the install location for your environment")
endif()
link_directories(${OMP_PATH})


set(POSSIBLE_MKL_LIB_PATHS "/opt/intel/oneapi/mkl/latest/lib/intel64/libmkl_core.so;/usr/lib/x86_64-linux-gnu/libmkl_core.so;/opt/intel/mkl/lib/intel64/libmkl_core.so")
foreach(POSSIBLE_MKL_LIB_PATH ${POSSIBLE_MKL_LIB_PATHS})
    if (EXISTS ${POSSIBLE_MKL_LIB_PATH})
        get_filename_component(MKL_PATH ${POSSIBLE_MKL_LIB_PATH} DIRECTORY)
    endif()
endforeach()

set(POSSIBLE_MKL_INCLUDE_PATHS "/opt/intel/oneapi/mkl/latest/include;/usr/include/mkl;/opt/intel/mkl/include/;")
foreach(POSSIBLE_MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATHS})
    if (EXISTS ${POSSIBLE_MKL_INCLUDE_PATH})
        set(MKL_INCLUDE_PATH ${POSSIBLE_MKL_INCLUDE_PATH})
    endif()
endforeach()
if(NOT MKL_PATH)
    message(FATAL_ERROR "Could not find Intel MKL in standard locations; use -DMKL_PATH to specify the install location for your environment")
elseif(NOT MKL_INCLUDE_PATH)
    message(FATAL_ERROR "Could not find Intel MKL in standard locations; use -DMKL_INCLUDE_PATH to specify the install location for headers for your environment")
endif()
if (EXISTS ${MKL_PATH}/libmkl_def.so.2)
    set(MKL_DEF_SO ${MKL_PATH}/libmkl_def.so.2)
elseif(EXISTS ${MKL_PATH}/libmkl_def.so)
    set(MKL_DEF_SO ${MKL_PATH}/libmkl_def.so)
else()
    message(FATAL_ERROR "Despite finding MKL, libmkl_def.so was not found in expected locations.")
endif()
link_directories(${MKL_PATH})
message("mkl ${MKL_PATH}")
include_directories(${MKL_INCLUDE_PATH})

# compile flags and link libraries
add_compile_options(-m64 -Wl,--no-as-needed)
# if (NOT PYBIND)
#     link_libraries(mkl_intel_ilp64 mkl_intel_thread mkl_core iomp5 pthread m dl)
# else()
    # static linking for python so as to minimize customer dependency issues
    link_libraries(
            ${MKL_PATH}/libmkl_intel_ilp64.a
            ${MKL_PATH}/libmkl_intel_thread.a
            ${MKL_PATH}/libmkl_core.a
            ${MKL_DEF_SO}
            /opt/intel/lib/intel64_lin/libiomp5.a
            pthread
            m
            dl
    )
# endif()
add_definitions(-DMKL_ILP64)

# Section for tcmalloc. The DiskANN tools are always linked to tcmalloc. 
if(NOT PYBIND)
    set(DISKANN_TOOLS_TCMALLOC_LINK_OPTIONS "-ltcmalloc")
endif()

if (DISKANN_RELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS)
    add_definitions(-DRELEASE_UNUSED_TCMALLOC_MEMORY_AT_CHECKPOINTS)
endif()

set(DISKANN_ASYNC_LIB aio)


#Main compiler/linker settings 
set(ENV{TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD} 500000000000)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx2 -mfma -msse2 -ftree-vectorize -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -fopenmp -fopenmp-simd -funroll-loops -Wfatal-errors -DUSE_AVX2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -DNDEBUG")
if (NOT PYBIND)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -Ofast")
    if (NOT PORTABLE)
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native -mtune=native")
    endif()
else()
    # -Ofast is not supported in a python extension module
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -fPIC")
endif()




set(CMAKE_COMPILE_WARNING_AS_ERROR OFF)

file(GLOB CPP_SOURCES src/*.cpp)
add_library(${PROJECT_NAME} ${CPP_SOURCES})
add_library(${PROJECT_NAME}_s STATIC ${CPP_SOURCES})



