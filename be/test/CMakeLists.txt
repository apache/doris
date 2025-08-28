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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/test")

# where to put generated libraries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/test")

file(GLOB_RECURSE UT_FILES CONFIGURE_DEPENDS *.cpp)

if(NOT DEFINED DORIS_WITH_LZO)
    list(REMOVE_ITEM UT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/exec/plain_text_line_reader_lzop_test.cpp)
endif()

if (OS_MACOSX)
    list(REMOVE_ITEM UT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/util/system_metrics_test.cpp)
endif()

list(REMOVE_ITEM UT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/tools/benchmark_tool.cpp)

# todo: need fix those ut
list(REMOVE_ITEM UT_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/agent/heartbeat_server_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/common/config_validator_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/http/metrics_action_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/binary_dict_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/binary_plain_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/binary_prefix_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/bitshuffle_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/column_reader_writer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/frame_of_reference_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/plain_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/rowset/segment_v2/rle_page_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime/decimal_value_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/decompress_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util/url_coding_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/io/fs/remote_file_system_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/olap/remote_rowset_gc_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime/jsonb_value_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runtime/large_int_value_test.cpp
)

message(STATUS "Disable the metrics collection for orc")
add_compile_definitions(ENABLE_METRICS=0)

list(APPEND UT_FILES
    ${CONTRIB_PATH}/apache-orc/c++/test/MemoryInputStream.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/MemoryOutputStream.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestAttributes.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestBlockBuffer.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestBufferedOutputStream.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestBloomFilter.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestByteRle.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestByteRLEEncoder.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestColumnPrinter.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestColumnReader.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestColumnStatistics.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestCompression.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestDecompression.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestDecimal.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestDictionaryEncoding.cc
    #${CONTRIB_PATH}/apache-orc/c++/test/TestDriver.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestInt128.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestMurmur3.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestPredicateLeaf.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestPredicatePushdown.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestReader.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestRleDecoder.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestRleEncoder.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestRLEV2Util.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestSargsApplier.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestSearchArgument.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestStripeIndexStatistics.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestTimestampStatistics.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestTimezone.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestType.cc
    ${CONTRIB_PATH}/apache-orc/c++/test/TestWriter.cc
)

include_directories(
  ${CMAKE_SOURCE_DIR}/../contrib/apache-orc/c++/src
  ${CMAKE_BINARY_DIR}/apache-orc/c++/src
  ${CMAKE_BINARY_DIR}/apache-orc/c++/include
)

add_executable(doris_be_test ${UT_FILES})

target_link_libraries(doris_be_test ${TEST_LINK_LIBS})
set_target_properties(doris_be_test PROPERTIES COMPILE_FLAGS "-fno-access-control")

if (OS_MACOSX AND ARCH_ARM)
    find_program(DSYMUTIL NAMES dsymutil)
    message(STATUS "dsymutil found: ${DSYMUTIL}")
    find_program(LLVM_STRIP NAMES llvm-strip)
    message(STATUS "llvm-strip found: ${LLVM_STRIP}")
    add_custom_command(TARGET doris_be_test POST_BUILD
        COMMAND ${DSYMUTIL} $<TARGET_FILE:doris_be_test>
        COMMAND ${LLVM_STRIP} --strip-all $<TARGET_FILE:doris_be_test>
    )
endif()
