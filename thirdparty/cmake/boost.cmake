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

# boost — pure CMake build (no b2/bootstrap)
# Only date_time (5 .cpp) and system (1 .cpp) need compilation.
# Everything else is header-only.

set(BOOST_SRC ${TP_SOURCE_DIR}/boost_1_81_0)

# Header-only interface for all of Boost
add_library(boost_headers INTERFACE)
target_include_directories(boost_headers INTERFACE ${BOOST_SRC})

# boost_date_time
# Only gregorian_types.cpp and posix_time_types.cpp are needed.
# greg_weekday.cpp, greg_month.cpp, date_generators.cpp redefine functions
# already inline in headers since Boost 1.81 — skip them.
add_library(boost_date_time_static STATIC
    ${BOOST_SRC}/libs/date_time/src/gregorian/gregorian_types.cpp
    ${BOOST_SRC}/libs/date_time/src/posix_time/posix_time_types.cpp
)
target_include_directories(boost_date_time_static PUBLIC ${BOOST_SRC})
target_compile_definitions(boost_date_time_static PRIVATE BOOST_DATE_TIME_SOURCE)
target_compile_options(boost_date_time_static PRIVATE -fPIC -w)
set_target_properties(boost_date_time_static PROPERTIES OUTPUT_NAME boost_date_time)

# boost_system (1 source file)
add_library(boost_system_static STATIC
    ${BOOST_SRC}/libs/system/src/error_code.cpp
)
target_include_directories(boost_system_static PUBLIC ${BOOST_SRC})
target_compile_options(boost_system_static PRIVATE -fPIC -w)
set_target_properties(boost_system_static PROPERTIES OUTPUT_NAME boost_system)

# Public ALIAS targets
add_library(boost_date_time ALIAS boost_date_time_static)
add_library(boost_system ALIAS boost_system_static)

# boost_container — pmr (monotonic_buffer_resource etc.) needs compilation
add_library(boost_container_static STATIC
    ${BOOST_SRC}/libs/container/src/pool_resource.cpp
    ${BOOST_SRC}/libs/container/src/monotonic_buffer_resource.cpp
    ${BOOST_SRC}/libs/container/src/synchronized_pool_resource.cpp
    ${BOOST_SRC}/libs/container/src/unsynchronized_pool_resource.cpp
    ${BOOST_SRC}/libs/container/src/global_resource.cpp
)
target_include_directories(boost_container_static PUBLIC ${BOOST_SRC})
target_compile_options(boost_container_static PRIVATE -fPIC -w)
set_target_properties(boost_container_static PROPERTIES OUTPUT_NAME boost_container)
add_library(boost_container ALIAS boost_container_static)

# Create IMPORTED targets for Arrow / other downstream compatibility
if(NOT TARGET Boost::headers)
    add_library(Boost::headers INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::headers INTERFACE boost_headers)
endif()
if(NOT TARGET Boost::date_time)
    add_library(Boost::date_time INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::date_time INTERFACE boost_date_time_static)
endif()
if(NOT TARGET Boost::container)
    add_library(Boost::container INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::container INTERFACE boost_container_static)
endif()
if(NOT TARGET Boost::system)
    add_library(Boost::system INTERFACE IMPORTED GLOBAL)
    target_link_libraries(Boost::system INTERFACE boost_system_static)
endif()
