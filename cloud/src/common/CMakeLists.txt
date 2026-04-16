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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/common")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/common")

set(COMMON_FILES
    configbase.cpp
    util.cpp
    logging.cpp
    bvars.cpp
    encryption_util.cpp
    metric.cpp
    kms.cpp
    network_util.cpp
)

if (USE_JEMALLOC)
    set(COMMON_FILES ${COMMON_FILES}
        ${BUILD_DIR}/../../be/src/runtime/memory/jemalloc_hook.cpp
    )
endif()

add_library(Common STATIC
    ${COMMON_FILES}
    )
