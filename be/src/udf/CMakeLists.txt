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
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/udf")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/udf")

# Build this library twice. Once to be linked into the main Doris. This version
# can have dependencies on our other libs. The second version is shipped as part
# of the UDF sdk, which can't use other libs.
add_library(Udf udf.cpp)
add_library(DorisUdf udf.cpp)
set_target_properties(DorisUdf PROPERTIES COMPILE_FLAGS "-DDORIS_UDF_SDK_BUILD")

# We can't use the normal link list since we want to pick up libDorisUdf (the external
# library) rather than the internal libUdf.
set (UDF_TEST_LINK_LIBS
  -Wl,--start-group
  Common
  GlobalFlags
  DorisUdf
  Runtime
  Util
  -Wl,--end-group
# Below are all external dependencies.  They should some after the doris libs.
  ${Boost_LIBRARIES}
  glogstatic
  gflagsstatic
  -lboost_date_time
  gtest)
  
set_target_properties(DorisUdf PROPERTIES PUBLIC_HEADER "udf.h;uda_test_harness.h")
INSTALL(TARGETS DorisUdf
        ARCHIVE DESTINATION ${OUTPUT_DIR}/udf
        LIBRARY DESTINATION ${OUTPUT_DIR}/udf/lib
        PUBLIC_HEADER DESTINATION ${OUTPUT_DIR}/udf/include)

#ADD_BE_TEST(udf_test)
#ADD_BE_TEST(uda_test)
