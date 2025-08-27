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

# EXCLUDE_FROM_ALL so that binary in openblas is not installed.
add_subdirectory(${PROJECT_SOURCE_DIR}/../contrib/openblas ${PROJECT_BINARY_DIR}/openblas EXCLUDE_FROM_ALL)

set(OPENBLAS_LIBRARY "${PROJECT_BINARY_DIR}/openblas/lib/libopenblas.a" CACHE PATH "Path to OpenBLAS build directory")
set(FAISS_ENABLE_MKL OFF CACHE BOOL "Disable MKL support in FAISS")
set(FAISS_ENABLE_GPU OFF CACHE BOOL "Disable GPU support in FAISS")
set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "Disable Python support in FAISS")
set(FAISS_ENABLE_EXTRAS OFF CACHE BOOL "Disable FAISS extras")
set(BUILD_TESTING OFF CACHE BOOL "Disable FAISS testing")

# EXCLUDE_FROM_ALL so that binary in faiss is not installed.
add_subdirectory(${PROJECT_SOURCE_DIR}/../contrib/faiss ${PROJECT_BINARY_DIR}/faiss EXCLUDE_FROM_ALL)