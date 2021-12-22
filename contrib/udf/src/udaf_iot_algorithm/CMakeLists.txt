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

# Include udf
include_directories(thirdparty/include)

# Set all libraries
add_library(udf STATIC IMPORTED)
set_target_properties(udf PROPERTIES IMPORTED_LOCATION thirdparty/lib/libDorisUdf.a)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/udaf_iot_algorithm")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/udaf_iot_algorithm")

add_library(udaf_iot_algorithm SHARED udaf_iot_algorithm.cpp)
target_link_libraries(udaf_iot_algorithm
        udf
        -static-libstdc++
        -static-libgcc
        )