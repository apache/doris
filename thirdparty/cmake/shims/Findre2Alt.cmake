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

# Find module shim for re2Alt
if(TARGET re2)
    get_target_property(_re2_inc re2 INTERFACE_INCLUDE_DIRECTORIES)
    list(GET _re2_inc 0 RE2_INCLUDE_DIR)
    set(RE2_LIB re2)
    set(re2Alt_FOUND TRUE)
    set(re2_FOUND TRUE)

    include(FindPackageHandleStandardArgs)
    find_package_handle_standard_args(re2Alt DEFAULT_MSG RE2_LIB RE2_INCLUDE_DIR)

    # re2::re2 is already created as ALIAS in re2.cmake
endif()
