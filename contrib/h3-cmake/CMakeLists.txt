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

set(H3_SOURCE_DIR "${CONTRIB_PATH}/h3")
set(H3_BINARY_DIR "${PROJECT_BINARY_DIR}/h3")

file(STRINGS "${H3_SOURCE_DIR}/VERSION" H3_VERSION LIMIT_COUNT 1)
string(REPLACE "." ";" H3_VERSION_PARTS "${H3_VERSION}")
list(GET H3_VERSION_PARTS 0 H3_VERSION_MAJOR)
list(GET H3_VERSION_PARTS 1 H3_VERSION_MINOR)
list(GET H3_VERSION_PARTS 2 H3_VERSION_PATCH)

set(H3_SOURCES
    ${H3_SOURCE_DIR}/src/h3lib/lib/algos.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/area.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/baseCells.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/bbox.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/cellsToMultiPoly.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/directedEdge.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/faceijk.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/h3Assert.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/h3Index.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/iterators.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/latLng.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/linkedGeo.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/localij.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/mathExtensions.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/polygon.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/polyfill.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/vec2d.c
    ${H3_SOURCE_DIR}/src/h3lib/lib/vertex.c)

file(MAKE_DIRECTORY "${H3_BINARY_DIR}/include")
configure_file(
    ${H3_SOURCE_DIR}/src/h3lib/include/h3api.h.in
    ${H3_BINARY_DIR}/include/h3api.h)

add_library(h3 STATIC ${H3_SOURCES})
set_target_properties(h3 PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_compile_features(h3 PUBLIC c_std_99)
# Keep upstream warnings visible without imposing Doris warning policy.
target_compile_options(h3 PRIVATE -Wno-error)
# Upstream uses empty initializers as a C extension before C23.
if(CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_options(h3 PRIVATE -Wno-c23-extensions)
endif()
target_compile_definitions(h3 PRIVATE BUILDING_H3=1)
target_include_directories(
    h3
    SYSTEM PUBLIC
        $<BUILD_INTERFACE:${H3_SOURCE_DIR}/src/h3lib/include>
        $<BUILD_INTERFACE:${H3_BINARY_DIR}/include>)
target_link_libraries(h3 PUBLIC m)
