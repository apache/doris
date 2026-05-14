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

# xsimd config shim – provides xsimd target for Arrow
# Compute the xsimd include dir relative to this config file's location
# This file is at thirdparty/cmake/shims/xsimd/xsimdConfig.cmake
# xsimd source is at thirdparty/src/xsimd-13.0.0/include
get_filename_component(_xsimd_shim_dir "${CMAKE_CURRENT_LIST_DIR}" ABSOLUTE)
get_filename_component(_tp_cmake_shims "${_xsimd_shim_dir}/.." ABSOLUTE)
get_filename_component(_tp_cmake "${_tp_cmake_shims}/.." ABSOLUTE)
get_filename_component(_tp_root "${_tp_cmake}/.." ABSOLUTE)
set(_xsimd_inc "${_tp_root}/src/xsimd-13.0.0/include")

if(NOT TARGET xsimd)
    add_library(xsimd INTERFACE IMPORTED)
    target_include_directories(xsimd INTERFACE "${_xsimd_inc}")
endif()
set(xsimd_FOUND TRUE)
set(${CMAKE_FIND_PACKAGE_NAME}_FOUND TRUE)
set(xsimd_VERSION "13.0.0")
