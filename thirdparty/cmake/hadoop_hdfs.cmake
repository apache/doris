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

# hadoop_hdfs - JNI based, build from source using CMake
# The Doris hadoop source requires custom cmake functions normally defined
# in HadoopCommon.cmake. We inject stub definitions via CMAKE_PROJECT_INCLUDE_BEFORE.
set(HADOOP_SRC ${TP_SOURCE_DIR}/doris-thirdparty-hadoop-3.3.6.6-for-doris)
set(HADOOP_BUILD_DIR ${CMAKE_CURRENT_BINARY_DIR}/hadoop_hdfs)
set(_HDFS_SRC_DIR ${HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs)

set(HADOOP_HDFS_A "${HADOOP_BUILD_DIR}/libhdfs.a")

include(ProcessorCount)
ProcessorCount(NPROC)

# Find JNI include dirs
find_package(JNI QUIET)
set(_JNI_CFLAGS "")
if(JNI_INCLUDE_DIRS)
    foreach(_dir ${JNI_INCLUDE_DIRS})
        string(APPEND _JNI_CFLAGS " -I${_dir}")
    endforeach()
elseif(DEFINED ENV{JAVA_HOME})
    set(_JNI_CFLAGS "-I$ENV{JAVA_HOME}/include -I$ENV{JAVA_HOME}/include/linux")
endif()

# We still need to write the CMake stubs at configure time so they exist before the command runs.
file(MAKE_DIRECTORY ${HADOOP_BUILD_DIR})
file(WRITE "${HADOOP_BUILD_DIR}/config.h" "/* auto-generated config.h stub */\n")
file(WRITE "${HADOOP_BUILD_DIR}/HadoopInit.cmake" [=[
# Stub definitions for hadoop cmake functions (replacing HadoopCommon.cmake)
function(hadoop_add_dual_library LIB_NAME)
  add_library(${LIB_NAME} STATIC ${ARGN})
endfunction()
function(hadoop_target_link_dual_libraries LIB_NAME)
  target_link_libraries(${LIB_NAME} ${ARGN})
endfunction()
function(hadoop_dual_output_directory LIB_NAME)
endfunction()
function(build_libhdfs_test)
endfunction()
function(link_libhdfs_test)
endfunction()
function(add_libhdfs_test)
endfunction()
# OS_DIR: relative to CMAKE_CURRENT_SOURCE_DIR (which is the libhdfs dir)
set(OS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/os/posix)
set(OS_LINK_LIBRARIES "")
set(LIB_DL dl)
set(OUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
]=])

# Forward parent CMAKE_C_FLAGS so MSAN/ASAN/etc. flags reach libhdfs sources.
# Without this, the sub-cmake gets only -fPIC + JNI includes, and libhdfs is
# silently uninstrumented even under MSAN build (defeats the boundary patch in
# thirdparty/patches/doris-thirdparty-hadoop-3.3.6.6-for-doris-msan.patch).
add_custom_command(
    OUTPUT ${HADOOP_HDFS_A}
    COMMAND ${CMAKE_COMMAND}
        -DCMAKE_INSTALL_PREFIX=${HADOOP_BUILD_DIR}
        "-DCMAKE_PROJECT_INCLUDE_BEFORE=${HADOOP_BUILD_DIR}/HadoopInit.cmake"
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
        "-DCMAKE_C_FLAGS=-fPIC -I${HADOOP_BUILD_DIR} ${_JNI_CFLAGS} ${CMAKE_C_FLAGS}"
        -DJVM_ARCH_DATA_MODEL=64
        -DGENERATED_JAVAH=${_HDFS_SRC_DIR}/include
        -Wno-dev
        ${_HDFS_SRC_DIR}
    COMMAND ${CMAKE_COMMAND} --build . -j${NPROC}
    WORKING_DIRECTORY ${HADOOP_BUILD_DIR}
    COMMENT "Building hadoop_hdfs from source..."
)

add_custom_target(hadoop_hdfs_builder DEPENDS ${HADOOP_HDFS_A})

if(NOT TARGET hadoop_hdfs)
add_library(hadoop_hdfs STATIC IMPORTED GLOBAL)
if(EXISTS "${HADOOP_BUILD_DIR}/libhdfs.a")
    set_target_properties(hadoop_hdfs PROPERTIES IMPORTED_LOCATION "${HADOOP_BUILD_DIR}/libhdfs.a")
elseif(EXISTS "${HADOOP_BUILD_DIR}/lib/libhdfs.a")
    set_target_properties(hadoop_hdfs PROPERTIES IMPORTED_LOCATION "${HADOOP_BUILD_DIR}/lib/libhdfs.a")
elseif(EXISTS "${HADOOP_BUILD_DIR}/lib64/libhdfs.a")
    set_target_properties(hadoop_hdfs PROPERTIES IMPORTED_LOCATION "${HADOOP_BUILD_DIR}/lib64/libhdfs.a")
else()
    message(WARNING "[contrib] hadoop_hdfs library not found, using placeholder")
    set_target_properties(hadoop_hdfs PROPERTIES IMPORTED_LOCATION "${HADOOP_BUILD_DIR}/libhdfs.a")
endif()
set_target_properties(hadoop_hdfs PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${_HDFS_SRC_DIR}/include"
)
add_dependencies(hadoop_hdfs hadoop_hdfs_builder)
endif()
