# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Make sure compile check in doris will not break compilation of faiss and openblas
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

set(BUILD_WITHOUT_LAPACK OFF CACHE BOOL "Disable LAPACK support in OpenBLAS")
set(NO_SHARED TRUE CACHE BOOL "Disable shared library in OpenBLAS")
set(C_LAPACK TRUE CACHE BOOL "Enable C interface for LAPACK in OpenBLAS")
set(USE_OPENMP TRUE CACHE BOOL "Enable OpenMP in OpenBLAS")
set(NOFORTRAN ON CACHE BOOL "Disable Fortran in OpenBLAS")
set(BUILD_STATIC_LIBS ON CACHE BOOL "Build static libraries in OpenBLAS")
set(BUILD_TESTING OFF CACHE BOOL "Build shared libraries in OpenBLAS")
set(BUILD_RELAPACK ON CACHE BOOL "Build relapack in OpenBLAS")
set(BUILD_BENCHMARKS OFF CACHE BOOL "Build benchmarks in OpenBLAS")
set(NO_LAPACK OFF CACHE BOOL "Disable LAPACK in OpenBLAS")
set(NO_CBLAS ON CACHE BOOL "Disable CBLAS in OpenBLAS")
set(NO_AVX512 ON CACHE BOOL "Disable AVX512 in OpenBLAS")

# Allow overriding DYNAMIC_ARCH from build.sh via ENABLE_DYNAMIC_ARCH variable
if (DEFINED ENABLE_DYNAMIC_ARCH)
    if (NOT CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
        set(DYNAMIC_ARCH "${ENABLE_DYNAMIC_ARCH}" CACHE BOOL "Enable runtime CPU detection in OpenBLAS")
    endif ()
else()
    if (NOT CMAKE_SYSTEM_NAME MATCHES "Darwin" AND NOT CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
        set(DYNAMIC_ARCH ON CACHE BOOL "Enable runtime CPU detection in OpenBLAS")
    endif ()
endif ()

# On macOS, CMake's FindOpenMP auto-detection often fails because:
# - Apple Clang doesn't support -fopenmp natively
# - Even Homebrew LLVM's clang may fail FindOpenMP's try_compile if libomp
#   isn't in the default search path (macOS -isysroot restricts library search)
# We must:
# 1. Provide OpenMP cache variable hints (so FindOpenMP knows which flags to use)
# 2. Add -L<lib_dir> to CMAKE_C/CXX_FLAGS (so try_compile's linker can find libomp)
#    NOTE: CMAKE_C_FLAGS IS propagated to check_c_source_compiles/try_compile,
#    but CMAKE_EXE_LINKER_FLAGS is NOT for source-file try_compile.
if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
    # Try to find libomp from the compiler's own lib directory (Homebrew LLVM bundles it)
    get_filename_component(_COMPILER_DIR "${CMAKE_C_COMPILER}" DIRECTORY)
    get_filename_component(_COMPILER_PREFIX "${_COMPILER_DIR}" DIRECTORY)
    if (EXISTS "${_COMPILER_PREFIX}/lib/libomp.dylib")
        set(_OMP_LIB_DIR "${_COMPILER_PREFIX}/lib")
        set(_OMP_INC_DIR "${_COMPILER_PREFIX}/include")
        set(_OMP_LIB "${_OMP_LIB_DIR}/libomp.dylib")
        set(_OMP_FLAG "-fopenmp")
    else()
        # Fallback: try Homebrew's standalone libomp formula
        execute_process(COMMAND brew --prefix libomp
            OUTPUT_VARIABLE _LIBOMP_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
        if (_LIBOMP_PREFIX AND EXISTS "${_LIBOMP_PREFIX}/lib/libomp.dylib")
            set(_OMP_LIB_DIR "${_LIBOMP_PREFIX}/lib")
            set(_OMP_INC_DIR "${_LIBOMP_PREFIX}/include")
            set(_OMP_LIB "${_OMP_LIB_DIR}/libomp.dylib")
            set(_OMP_FLAG "-Xclang -fopenmp")
        endif()
    endif()
    if (DEFINED _OMP_LIB)
        # Add lib/include paths to CMAKE_C/CXX_FLAGS so they are propagated to
        # FindOpenMP's internal try_compile (check_c_source_compiles).
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I${_OMP_INC_DIR} -L${_OMP_LIB_DIR}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${_OMP_INC_DIR} -L${_OMP_LIB_DIR}")

        # Set OpenMP cache hints so FindOpenMP knows exactly which flags/libs to use
        set(OpenMP_C_FLAGS "${_OMP_FLAG}" CACHE STRING "C compiler flags for OpenMP" FORCE)
        set(OpenMP_CXX_FLAGS "${_OMP_FLAG}" CACHE STRING "CXX compiler flags for OpenMP" FORCE)
        set(OpenMP_C_LIB_NAMES "omp" CACHE STRING "C OpenMP lib names" FORCE)
        set(OpenMP_CXX_LIB_NAMES "omp" CACHE STRING "CXX OpenMP lib names" FORCE)
        set(OpenMP_omp_LIBRARY "${_OMP_LIB}" CACHE FILEPATH "Path to libomp" FORCE)
    endif()
endif()

# EXCLUDE_FROM_ALL so that binary in openblas is not installed.
add_subdirectory(${PROJECT_SOURCE_DIR}/../contrib/openblas ${PROJECT_BINARY_DIR}/openblas EXCLUDE_FROM_ALL)

set(OPENBLAS_LIBRARY "${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.a" CACHE PATH "Path to OpenBLAS build directory")
set(FAISS_ENABLE_MKL OFF CACHE BOOL "Disable MKL support in FAISS")
set(FAISS_ENABLE_GPU OFF CACHE BOOL "Disable GPU support in FAISS")
set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "Disable Python support in FAISS")
set(FAISS_ENABLE_EXTRAS OFF CACHE BOOL "Disable FAISS extras")
set(BUILD_TESTING OFF CACHE BOOL "Disable FAISS testing")
if (DEFINED USE_AVX2 AND NOT USE_AVX2)
    set(FAISS_OPT_LEVEL "generic" CACHE STRING "FAISS CPU dispatch level" FORCE)
    set(NO_AVX2 ON CACHE BOOL "Disable AVX2 support in OpenBLAS" FORCE)
    set(NO_AVX ON CACHE BOOL "Disable AVX support in OpenBLAS" FORCE)
else()
    set(FAISS_OPT_LEVEL "avx2" CACHE STRING "FAISS CPU dispatch level" FORCE)
    set(NO_AVX OFF CACHE BOOL "Disable AVX support in OpenBLAS" FORCE)
    set(NO_AVX2 OFF CACHE BOOL "Disable AVX2 support in OpenBLAS" FORCE)
endif()

# EXCLUDE_FROM_ALL so that binary in faiss is not installed.
add_subdirectory(${PROJECT_SOURCE_DIR}/../contrib/faiss ${PROJECT_BINARY_DIR}/faiss EXCLUDE_FROM_ALL)