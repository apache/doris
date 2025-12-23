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

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX17_FLAGS} ${WARN_FLAGS}")

INCLUDE(CheckCXXSourceCompiles)

CHECK_CXX_SOURCE_COMPILES("
    #include <cstdint>
    int main(int, char*[]) { }"
  ORC_CXX_HAS_CSTDINT
)

CHECK_CXX_SOURCE_COMPILES("
    #include <thread>
    int main(void) {
      thread_local int s;
      return s;
    }"
  ORC_CXX_HAS_THREAD_LOCAL
)
