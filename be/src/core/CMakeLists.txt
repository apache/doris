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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/core")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/core")

file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS *.cpp)
add_library(Core STATIC ${SRC_FILES})

pch_reuse(Core)
# wide_integer_from_double.cpp defines DORIS_WIDE_INTEGER_FROM_DOUBLE_IMPL_TU
# before including wide_integer_impl.h to compile the from-double bodies exactly
# once. The PCH already contains that header (via storage/olap_common.h) in
# declaration-only mode, and its pre-spent include guard would leave the explicit
# instantiations below without definitions. Opt this single TU out of the PCH.
set_source_files_properties(${CMAKE_CURRENT_SOURCE_DIR}/wide_integer_from_double.cpp
                            PROPERTIES SKIP_PRECOMPILE_HEADERS ON)
