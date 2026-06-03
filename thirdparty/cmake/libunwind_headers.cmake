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

# libunwind_headers: copy libunwind headers from old/include
set(UNWIND_OLD_DIR "${TP_SOURCE_DIR}/../old/include")
set(UNWIND_NESTED_DIR "${CMAKE_CURRENT_BINARY_DIR}/libunwind_headers/include")
file(MAKE_DIRECTORY ${UNWIND_NESTED_DIR})

# Copy libunwind.h and related headers
foreach(_h libunwind.h libunwind-common.h libunwind-dynamic.h libunwind-x86_64.h libunwind-coredump.h unwind.h libunwind-ptrace.h)
    if(EXISTS "${UNWIND_OLD_DIR}/${_h}")
        file(COPY "${UNWIND_OLD_DIR}/${_h}" DESTINATION ${UNWIND_NESTED_DIR})
    endif()
endforeach()

add_library(libunwind_headers INTERFACE)
target_include_directories(libunwind_headers SYSTEM INTERFACE ${UNWIND_NESTED_DIR})
