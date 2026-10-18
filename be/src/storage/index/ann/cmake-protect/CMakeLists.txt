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

# Make sure compile check in doris will not break compilation of faiss
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")

# Set FAISS_ENABLE_GPU OFF before including FAISS to prevent CUDA language detection
set(FAISS_ENABLE_GPU OFF CACHE BOOL "Enable GPU support in FAISS")
set(FAISS_ENABLE_MKL OFF CACHE BOOL "Disable MKL support in FAISS")
set(FAISS_ENABLE_PYTHON OFF CACHE BOOL "Disable Python support in FAISS")
set(FAISS_ENABLE_EXTRAS OFF CACHE BOOL "Disable FAISS extras")
set(BUILD_TESTING OFF CACHE BOOL "Disable FAISS testing")
if (DEFINED USE_AVX2 AND NOT USE_AVX2)
    set(FAISS_OPT_LEVEL "generic" CACHE STRING "FAISS CPU dispatch level" FORCE)
else()
    set(FAISS_OPT_LEVEL "avx2" CACHE STRING "FAISS CPU dispatch level" FORCE)
endif()

# EXCLUDE_FROM_ALL so that binary in faiss is not installed.
# faiss links the prebuilt OpenBLAS through the openblas target in cmake/thirdparty.cmake.
add_subdirectory(${PROJECT_SOURCE_DIR}/../contrib/faiss ${PROJECT_BINARY_DIR}/faiss EXCLUDE_FROM_ALL)
