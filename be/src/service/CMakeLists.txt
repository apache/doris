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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/service")

add_library(Service
    backend_options.cpp
    backend_service.cpp
    brpc_service.cpp
    http_service.cpp
    internal_service.cpp
)

add_executable(palo_be
    doris_main.cpp
)

# This permits libraries loaded by dlopen to link to the symbols in the program.
# set_target_properties(palo_be PROPERTIES LINK_FLAGS -pthread)

target_link_libraries(palo_be
    ${DORIS_LINK_LIBS}
)

install(DIRECTORY DESTINATION ${OUTPUT_DIR}/lib/)

install(TARGETS palo_be
    DESTINATION ${OUTPUT_DIR}/lib/)
