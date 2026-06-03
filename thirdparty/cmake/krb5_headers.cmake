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

# krb5_headers: expose krb5 headers from source tree
# Doris code uses #include <krb5.h> directly.
set(KRB5_SRC ${TP_SOURCE_DIR}/krb5-1.19)
set(KRB5_NESTED_DIR "${CMAKE_CURRENT_BINARY_DIR}/krb5_headers/include")
file(MAKE_DIRECTORY ${KRB5_NESTED_DIR})

# Copy source headers available at configure time
foreach(_h krb5.h profile.h)
    if(EXISTS "${KRB5_SRC}/src/include/${_h}")
        file(COPY "${KRB5_SRC}/src/include/${_h}" DESTINATION ${KRB5_NESTED_DIR})
    endif()
endforeach()
# com_err.h
if(EXISTS "${KRB5_SRC}/src/util/et/com_err.h")
    file(COPY "${KRB5_SRC}/src/util/et/com_err.h" DESTINATION ${KRB5_NESTED_DIR})
endif()
# gssapi.h top-level
if(EXISTS "${KRB5_SRC}/src/lib/gssapi/generic/gssapi.h")
    file(COPY "${KRB5_SRC}/src/lib/gssapi/generic/gssapi.h" DESTINATION ${KRB5_NESTED_DIR})
endif()
# gssapi/ subdirectory
file(MAKE_DIRECTORY "${KRB5_NESTED_DIR}/gssapi")
file(GLOB _GSSAPI_HEADERS "${KRB5_SRC}/src/lib/gssapi/generic/*.h")
if(_GSSAPI_HEADERS)
    file(COPY ${_GSSAPI_HEADERS} DESTINATION "${KRB5_NESTED_DIR}/gssapi")
endif()
# krb5/ subdirectory
if(EXISTS "${KRB5_SRC}/src/include/krb5")
    file(COPY "${KRB5_SRC}/src/include/krb5" DESTINATION ${KRB5_NESTED_DIR})
endif()

# Also include krb5 build output directory — krb5/krb5.h is generated there
set(KRB5_BUILD_INCLUDE "${CMAKE_CURRENT_BINARY_DIR}/krb5/include")
file(MAKE_DIRECTORY ${KRB5_BUILD_INCLUDE})

# Use a non-INTERFACE library wrapper so we can add build dependencies.
# krb5/krb5.h is generated during krb5 build, so consumers must wait.
add_library(krb5_headers STATIC "${CMAKE_CURRENT_BINARY_DIR}/krb5_headers_dummy.cpp")
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/krb5_headers_dummy.cpp" "// dummy\n")
target_include_directories(krb5_headers SYSTEM PUBLIC
    ${KRB5_NESTED_DIR}
    ${KRB5_BUILD_INCLUDE}
)
if(TARGET krb5_generate_headers)
    add_dependencies(krb5_headers krb5_generate_headers)
elseif(TARGET krb5_builder)
    add_dependencies(krb5_headers krb5_builder)
endif()
