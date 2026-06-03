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

# pugixml — hand-written add_library (no add_subdirectory)
set(PUGIXML_SRC_DIR ${TP_SOURCE_DIR}/pugixml-1.15)

add_library(pugixml STATIC
    ${PUGIXML_SRC_DIR}/src/pugixml.cpp
)

target_include_directories(pugixml SYSTEM PUBLIC
    ${PUGIXML_SRC_DIR}/src
)

target_compile_options(pugixml PRIVATE -fPIC -w)

set_target_properties(pugixml PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
