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

# paimon_cpp — hand-written add_library calls (no add_subdirectory)
#
# Builds paimon-cpp from source without delegating to its own CMake build
# system, to keep the build fully integrated with Doris's thirdparty tree.
#
# Strategy:
#   - roaring_bitmap  : build from paimon's bundled single-file version
#                       (uses roaring.hh, incompatible with Doris's CRoaring)
#   - xxhash          : build from paimon's bundled single-file version
#                       (same API as Doris's xxhash, but kept separate to
#                        avoid potential name-mangling differences)
#   - fmt_paimon      : ALIAS to Doris's existing fmt target
#   - tbb_paimon      : ExternalProject (oneTBB, not present in Doris TP)
#   - orc::orc        : ExternalProject (paimon uses a patched ORC v2.1.1,
#                       different from orc-1.9.0 in Doris TP)
#   - arrow / parquet : ALIAS to Doris's arrow_static / parquet_static
#   - glog            : ALIAS to Doris's glog target
#   - all paimon libs : hand-written STATIC add_library calls

if(NOT (ENABLE_PAIMON_CPP AND EXISTS "${TP_SOURCE_DIR}/paimon-cpp"))
    return()
endif()

include(ExternalProject)

set(PAIMON_SRC_DIR  "${TP_SOURCE_DIR}/paimon-cpp")
set(PAIMON_THIRD_PARTY_DIR "${PAIMON_SRC_DIR}/third_party")
# Binary dir for ExternalProjects built on behalf of paimon
set(PAIMON_EP_DIR   "${CMAKE_CURRENT_BINARY_DIR}/paimon_ep")

# ============================================================================
# Common ExternalProject flags (mirror what paimon's ThirdpartyToolchain uses)
# ============================================================================
set(_paimon_ep_cxx_flags "${CMAKE_CXX_FLAGS} -fPIC -Wno-error -Wno-sign-compare -Wno-ignored-attributes -D_GLIBCXX_USE_CXX11_ABI=1")
set(_paimon_ep_c_flags   "${CMAKE_C_FLAGS} -fPIC")

set(_paimon_ep_common_cmake_args
    "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_STATIC_LIBS=ON
    -DBUILD_TESTING=OFF
    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
    "-DCMAKE_CXX_FLAGS=${_paimon_ep_cxx_flags}"
    "-DCMAKE_C_FLAGS=${_paimon_ep_c_flags}"
    -DCMAKE_INSTALL_LIBDIR=lib
)

# ============================================================================
# roaring_bitmap — paimon's bundled single-file amalgamation
# Exposes roaring.h / roaring.hh at include root.
# ============================================================================
if(NOT TARGET roaring_bitmap)
    add_library(roaring_bitmap STATIC
        ${PAIMON_THIRD_PARTY_DIR}/roaring_bitmap/roaring.cpp
    )
    target_include_directories(roaring_bitmap SYSTEM PUBLIC
        ${PAIMON_THIRD_PARTY_DIR}/roaring_bitmap
    )
    target_compile_options(roaring_bitmap PRIVATE -fPIC -w -Wno-missing-field-initializers)
    set_target_properties(roaring_bitmap PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

if(NOT TARGET roaring_bitmap_paimon)
    add_library(roaring_bitmap_paimon ALIAS roaring_bitmap)
endif()

# ============================================================================
# xxhash_paimon — paimon's bundled single-file xxhash
# Same API as Doris's xxhash; kept separate so include dirs stay clean.
# ============================================================================
if(NOT TARGET xxhash_paimon)
    add_library(xxhash_paimon STATIC
        ${PAIMON_THIRD_PARTY_DIR}/xxhash/xxhash.c
    )
    target_include_directories(xxhash_paimon SYSTEM PUBLIC
        ${PAIMON_THIRD_PARTY_DIR}/xxhash
    )
    target_compile_options(xxhash_paimon PRIVATE -fPIC -w)
    set_target_properties(xxhash_paimon PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

# ============================================================================
# fmt_paimon — reuse Doris's existing fmt target
# ============================================================================
if(NOT TARGET fmt_paimon)
    add_library(fmt_paimon ALIAS fmt)
endif()

# ============================================================================
# tbb_paimon — oneTBB v2021.13.0 (not present in Doris TP)
# ============================================================================
set(_tbb_prefix "${PAIMON_EP_DIR}/tbb_ep-install")
set(_tbb_include_dir "${_tbb_prefix}/include")
set(_tbb_static_lib  "${_tbb_prefix}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}tbb${CMAKE_STATIC_LIBRARY_SUFFIX}")

file(MAKE_DIRECTORY "${_tbb_include_dir}")

set(_tbb_cxx_flags "${_paimon_ep_cxx_flags} -Wno-error -Wno-documentation")
set(_tbb_c_flags   "${_paimon_ep_c_flags}")

ExternalProject_Add(tbb_ep
    URL "https://github.com/uxlfoundation/oneTBB/archive/refs/tags/v2021.13.0.tar.gz"
    URL_HASH "SHA256=3ad5dd08954b39d113dc5b3f8a8dc6dc1fd5250032b7c491eb07aed5c94133e1"
    PREFIX "${PAIMON_EP_DIR}/tbb_ep-prefix"
    INSTALL_DIR "${_tbb_prefix}"
    CMAKE_ARGS
        ${_paimon_ep_common_cmake_args}
        "-DCMAKE_INSTALL_PREFIX=${_tbb_prefix}"
        "-DCMAKE_CXX_FLAGS=${_tbb_cxx_flags}"
        "-DCMAKE_C_FLAGS=${_tbb_c_flags}"
        -DTBB_TEST=OFF
    BUILD_BYPRODUCTS "${_tbb_static_lib}"
)

if(NOT TARGET tbb_paimon)
    add_library(tbb_paimon STATIC IMPORTED GLOBAL)
    set_target_properties(tbb_paimon PROPERTIES
        IMPORTED_LOCATION "${_tbb_static_lib}"
        INTERFACE_INCLUDE_DIRECTORIES "${_tbb_include_dir}"
    )
    add_dependencies(tbb_paimon tbb_ep)
endif()

# ============================================================================
# orc::orc — paimon uses ORC v2.1.1 with a custom patch (orc.diff)
# ============================================================================
set(_orc_prefix  "${PAIMON_EP_DIR}/orc_ep-install")
set(_orc_include "${_orc_prefix}/include")
set(_orc_static  "${_orc_prefix}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}orc${CMAKE_STATIC_LIBRARY_SUFFIX}")

file(MAKE_DIRECTORY "${_orc_include}")

# Gather dependency roots for ORC's CMake find-package calls.
# Use concrete (non-alias) targets so get_target_property works reliably.
get_target_property(_orc_snappy_inc  snappy       INTERFACE_INCLUDE_DIRECTORIES)
get_filename_component(_orc_snappy_root "${_orc_snappy_inc}" DIRECTORY)

get_target_property(_orc_lz4_inc  lz4_static  INTERFACE_INCLUDE_DIRECTORIES)
get_filename_component(_orc_lz4_root "${_orc_lz4_inc}" DIRECTORY)

get_target_property(_orc_zstd_inc  libzstd_static  INTERFACE_INCLUDE_DIRECTORIES)
get_filename_component(_orc_zstd_root "${_orc_zstd_inc}" DIRECTORY)

get_target_property(_orc_zlib_inc  zlibstatic  INTERFACE_INCLUDE_DIRECTORIES)
get_filename_component(_orc_zlib_root "${_orc_zlib_inc}" DIRECTORY)

get_target_property(_orc_proto_inc  libprotobuf  INTERFACE_INCLUDE_DIRECTORIES)
get_filename_component(_orc_proto_root "${_orc_proto_inc}" DIRECTORY)

set(_orc_cxx_flags "${_paimon_ep_cxx_flags} -Wno-error")
set(_orc_c_flags   "${_paimon_ep_c_flags}   -Wno-error")

ExternalProject_Add(orc_ep
    URL "https://github.com/apache/orc/archive/refs/tags/v2.1.1.tar.gz"
    URL_HASH "SHA256=1f8eef537814fdcd003de13e49c6edb35427b45eb40bafd3355f775d99a0ff99"
    PREFIX "${PAIMON_EP_DIR}/orc_ep-prefix"
    INSTALL_DIR "${_orc_prefix}"
    PATCH_COMMAND
        ${CMAKE_COMMAND} -E chdir <SOURCE_DIR> bash -c
        "[ -f .patched ] && echo 'ORC patch already applied, skip...' || patch -s -N -p1 -i '${PAIMON_SRC_DIR}/cmake_modules/orc.diff' && touch .patched"
    CMAKE_ARGS
        ${_paimon_ep_common_cmake_args}
        "-DCMAKE_INSTALL_PREFIX=${_orc_prefix}"
        "-DCMAKE_CXX_FLAGS=${_orc_cxx_flags}"
        "-DCMAKE_C_FLAGS=${_orc_c_flags}"
        "-DSNAPPY_HOME=${_orc_snappy_root}"
        "-DLZ4_HOME=${_orc_lz4_root}"
        "-DZSTD_HOME=${_orc_zstd_root}"
        "-DZLIB_HOME=${_orc_zlib_root}"
        "-DPROTOBUF_HOME=${_orc_proto_root}"
        "-DProtobuf_ROOT=${_orc_proto_root}"
        -DBUILD_JAVA=OFF
        -DBUILD_CPP_TESTS=OFF
        -DBUILD_TOOLS=OFF
        -DBUILD_CPP_ENABLE_METRICS=ON
    UPDATE_DISCONNECTED 1
    BUILD_BYPRODUCTS "${_orc_static}"
    DEPENDS libzstd_static snappy lz4_static zlibstatic libprotobuf
)

if(NOT TARGET orc::orc)
    add_library(orc::orc STATIC IMPORTED GLOBAL)
    set_target_properties(orc::orc PROPERTIES
        IMPORTED_LOCATION "${_orc_static}"
        INTERFACE_INCLUDE_DIRECTORIES "${_orc_include}"
    )
    target_link_libraries(orc::orc INTERFACE libzstd_static snappy lz4_static zlibstatic libprotobuf)
    add_dependencies(orc::orc orc_ep)
endif()

# ============================================================================
# Arrow / Parquet — alias to Doris's existing targets
# ============================================================================
# Paimon references "arrow", "parquet", "arrow_dataset", "arrow_acero"
# without the _static suffix. Create shim INTERFACE targets.

if(NOT TARGET arrow)
    add_library(arrow INTERFACE IMPORTED GLOBAL)
    target_link_libraries(arrow INTERFACE arrow_static)
endif()
if(NOT TARGET parquet)
    add_library(parquet INTERFACE IMPORTED GLOBAL)
    target_link_libraries(parquet INTERFACE parquet_static)
endif()
if(NOT TARGET arrow_dataset)
    add_library(arrow_dataset INTERFACE IMPORTED GLOBAL)
    target_link_libraries(arrow_dataset INTERFACE arrow_dataset_static)
endif()
if(NOT TARGET arrow_acero)
    add_library(arrow_acero INTERFACE IMPORTED GLOBAL)
    target_link_libraries(arrow_acero INTERFACE arrow_acero_static)
endif()

# Arrow include dirs (needed by paimon sources)
get_target_property(_arrow_incdirs arrow_static INTERFACE_INCLUDE_DIRECTORIES)
if(NOT _arrow_incdirs OR _arrow_incdirs MATCHES "NOTFOUND")
    set(_arrow_inc_list
        "${TP_SOURCE_DIR}/arrow-apache-arrow-17.0.0/cpp/src"
        "${CMAKE_CURRENT_BINARY_DIR}/arrow/src"
    )
else()
    set(_arrow_inc_list "${_arrow_incdirs}")
endif()

# ============================================================================
# glog — alias to Doris's target (paimon uses glog directly)
# ============================================================================
# glog already defined in Doris TP; no alias needed.

# ============================================================================
# RapidJSON — alias to Doris's rapidjson (paimon uses RapidJSON target name)
# ============================================================================
if(NOT TARGET RapidJSON)
    add_library(RapidJSON INTERFACE IMPORTED GLOBAL)
    if(TARGET rapidjson)
        target_link_libraries(RapidJSON INTERFACE rapidjson)
    else()
        target_include_directories(RapidJSON INTERFACE
            "${TP_SOURCE_DIR}/rapidjson-232389d4f1012dddec4ef84861face2d2ba85709/include"
        )
    endif()
endif()

# ============================================================================
# Shared include directories for all paimon compilation units
# ============================================================================
set(_paimon_include_dirs
    # paimon public API
    "${PAIMON_SRC_DIR}/include"
    # paimon private sources (src/paimon is the root for #include "paimon/...")
    "${PAIMON_SRC_DIR}/src"
    # paimon's bundled roaring (roaring.h / roaring.hh)
    "${PAIMON_THIRD_PARTY_DIR}/roaring_bitmap"
    # paimon's bundled xxhash (xxhash.h)
    "${PAIMON_THIRD_PARTY_DIR}/xxhash"
    # Arrow / Parquet headers
    ${_arrow_inc_list}
    # ORC headers (populated after orc_ep builds)
    "${_orc_include}"
    # TBB headers (populated after tbb_ep builds)
    "${_tbb_include_dir}"
)

set(_paimon_compile_defs
    SNAPPY_CODEC_AVAILABLE
    ZSTD_CODEC_AVAILABLE
    RAPIDJSON_HAS_STDSTRING
    PAIMON_ENABLE_ORC
    GLOG_USE_GLOG_EXPORT
    _GLIBCXX_USE_CXX11_ABI=1
)

# Helper macro: create a paimon static library with consistent settings
# Usage:
#   _add_paimon_lib(<name> SOURCES file1.cpp ... [DEPS dep1 ...])
macro(_add_paimon_lib _name)
    cmake_parse_arguments(_apl "" "" "SOURCES;DEPS" ${ARGN})

    add_library(${_name}_static STATIC ${_apl_SOURCES})

    target_include_directories(${_name}_static SYSTEM PUBLIC
        ${_paimon_include_dirs}
    )
    target_compile_definitions(${_name}_static PUBLIC
        ${_paimon_compile_defs}
    )
    target_compile_options(${_name}_static PRIVATE
        -fPIC -w
    )
    set_target_properties(${_name}_static PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        CXX_STANDARD 17
        CXX_STANDARD_REQUIRED ON
    )

    if(_apl_DEPS)
        target_link_libraries(${_name}_static PRIVATE ${_apl_DEPS})
    endif()

    # Ensure ExternalProject artifacts exist before compiling
    add_dependencies(${_name}_static tbb_ep orc_ep)

    add_library(${_name} ALIAS ${_name}_static)
endmacro()

# ============================================================================
# paimon — core library (paimon_common + paimon_core sources)
# ============================================================================
_add_paimon_lib(paimon
    SOURCES
    # --- common ---
    ${PAIMON_SRC_DIR}/src/paimon/common/compression/block_compression_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/compression/block_compressor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/compression/block_decompressor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/abstract_binary_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/binary_array.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/binary_array_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/binary_row.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/binary_row_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/binary_section.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/binary_string.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/blob.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/blob_descriptor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/blob_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/columnar/columnar_array.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/columnar/columnar_map.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/columnar/columnar_row.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/decimal.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/internal_row.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/record_batch.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/serializer/binary_row_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/data/timestamp.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/defs.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/executor/executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/factories/singleton.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/factories/io_hook.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/factories/factory_creator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bitmap/bitmap_index_result.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/file_indexer_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/file_index_format.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/file_index_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/file_index_result.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/format/column_stats.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/format/file_format_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/fs/file_system.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/fs/resolving_file_system.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/fs/file_system_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_config.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/complete_index_score_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/bitmap_vector_search_global_index_result.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/bitmap_global_index_result.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/global_index_result.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/global_indexer_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/buffered_input_stream.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/byte_array_input_stream.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/data_input_stream.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/data_output_stream.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/memory_segment_output_stream.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/offset_input_stream.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/cache/cache.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/cache/cache_key.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/io/cache/cache_manager.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/logging/logging.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/bytes.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/memory_pool.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/memory_segment.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/memory_segment_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/memory_slice.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/memory_slice_input.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/memory/memory_slice_output.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/metrics/metrics_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/options/memory_size.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/options/time_duration.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/and.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/compound_predicate.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/equal.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/greater_or_equal.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/greater_than.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/in.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/is_not_null.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/is_null.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/leaf_predicate.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/less_or_equal.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/less_than.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/literal_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/literal.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/not_equal.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/not_in.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/or.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/predicate_builder.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/predicate/predicate_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/concat_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/predicate_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/prefetch_file_batch_reader_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/reader_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/complete_row_kind_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/reader/data_evolution_file_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/block_handle.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/block_footer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/block_iterator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/block_trailer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/block_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/block_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/sst_file_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/sst/sst_file_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/types/data_field.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/types/data_type.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/types/data_type_json_parser.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/types/row_kind.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/types/row_type.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/arrow/mem_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/binary_row_partition_computer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/bit_set.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/bloom_filter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/bloom_filter64.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/bucket_id_calculator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/crc32c.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/decimal_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/delta_varint_compressor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/path_util.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/range.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/read_ahead_cache.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/byte_range_combiner.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/roaring_bitmap32.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/roaring_bitmap64.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/status.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/utils/string_utils.cpp
    # --- core ---
    ${PAIMON_SRC_DIR}/src/paimon/core/append/append_only_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/binary_to_string_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/boolean_to_decimal_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/boolean_to_numeric_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/boolean_to_string_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/casted_row.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/cast_executor_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/date_to_string_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/date_to_timestamp_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/decimal_to_decimal_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/decimal_to_numeric_primitive_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/numeric_primitive_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/numeric_primitive_to_decimal_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/numeric_primitive_to_timestamp_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/numeric_to_boolean_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/numeric_to_string_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/string_to_binary_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/string_to_boolean_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/string_to_date_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/string_to_decimal_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/string_to_numeric_primitive_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/string_to_timestamp_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/timestamp_to_date_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/timestamp_to_numeric_primitive_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/timestamp_to_string_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/timestamp_to_timestamp_cast_executor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/casting/casting_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/catalog/catalog.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/catalog/file_system_catalog.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/catalog/identifier.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/core_options.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/deletionvectors/deletion_vector.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/global_index/global_index_evaluator_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/global_index/global_index_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/global_index/global_index_scan_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/global_index/row_range_global_index_scanner_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/global_index/global_index_write_task.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/index/index_file_handler.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/index/global_index_meta.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/index/index_file_meta_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/meta_to_arrow_array_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/async_key_value_producer_and_consumer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_meta_09_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_meta_10_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_meta_12_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_meta_first_row_id_legacy_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_meta.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_meta_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_path_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/data_file_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/field_mapping_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/complete_row_tracking_fields_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/file_index_evaluator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/key_value_data_file_record_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/key_value_data_file_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/key_value_in_memory_record_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/key_value_meta_projection_consumer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/key_value_projection_consumer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/key_value_projection_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/io/rolling_blob_file_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/file_kind.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/file_source.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/index_manifest_entry_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/index_manifest_file.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_entry.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_entry_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_entry_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_file.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_file_meta.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_file_meta_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/manifest_list.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/partition_entry.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/manifest/index_manifest_file_handler.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/aggregate/aggregate_merge_function.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/aggregate/field_sum_agg.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/interval_partition.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/loser_tree.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/partial_update_merge_function.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/sort_merge_reader_with_loser_tree.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/compact/sort_merge_reader_with_min_heap.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/mergetree/merge_tree_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/migrate/file_meta_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/data_evolution_file_store_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/data_evolution_split_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/abstract_file_store_write.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/abstract_split_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/append_only_file_store_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/append_only_file_store_write.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/commit_context.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/expire_snapshots.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/file_store_commit.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/file_store_commit_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/file_store_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/file_store_write.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/internal_read_context.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/key_value_file_store_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/key_value_file_store_write.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/manifest_file_merger.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/merge_file_split_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/orphan_files_cleaner.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/orphan_files_cleaner_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/raw_file_split_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/read_context.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/scan_context.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/operation/write_context.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/postpone/postpone_bucket_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/schema/arrow_schema_validator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/schema/schema_manager.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/schema/schema_validation.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/schema/table_schema.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/snapshot.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/stats/simple_stats_collector.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/stats/simple_stats_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/stats/simple_stats.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/stats/simple_stats_evolution.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/sink/commit_message.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/sink/commit_message_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/sink/commit_message_serializer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/append_only_table_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/split.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/data_split_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/data_table_batch_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/data_table_stream_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/fallback_table_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/key_value_table_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/merge_tree_split_generator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/data_evolution_split_generator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/plan_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/snapshot/snapshot_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/startup_mode.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/table_read.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/table_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/table/source/data_evolution_batch_scan.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/tag/tag.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/field_mapping.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/fields_comparator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/file_store_path_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/file_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/manifest_meta_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/partition_path_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/primary_key_table_utils.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/snapshot_manager.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/special_field_ids.cpp
    ${PAIMON_SRC_DIR}/src/paimon/core/utils/tag_manager.cpp
    DEPS
        arrow glog fmt tbb_paimon roaring_bitmap xxhash_paimon RapidJSON
)

# ============================================================================
# paimon_local_file_system
# ============================================================================
_add_paimon_lib(paimon_local_file_system
    SOURCES
    ${PAIMON_SRC_DIR}/src/paimon/fs/local/local_file.cpp
    ${PAIMON_SRC_DIR}/src/paimon/fs/local/local_file_system.cpp
    ${PAIMON_SRC_DIR}/src/paimon/fs/local/local_file_system_factory.cpp
    DEPS paimon fmt
)

# ============================================================================
# paimon_file_index
# ============================================================================
_add_paimon_lib(paimon_file_index
    SOURCES
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bitmap/bitmap_file_index_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bitmap/bitmap_file_index.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bitmap/bitmap_file_index_meta.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bitmap/bitmap_file_index_meta_v1.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bitmap/bitmap_file_index_meta_v2.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bsi/bit_slice_index_roaring_bitmap.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bloomfilter/bloom_filter_file_index.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bloomfilter/bloom_filter_file_index_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/file_index/bloomfilter/fast_hash.cpp
    DEPS paimon arrow fmt xxhash_paimon
)

# ============================================================================
# paimon_global_index
# ============================================================================
_add_paimon_lib(paimon_global_index
    SOURCES
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/bitmap/bitmap_global_index.cpp
    ${PAIMON_SRC_DIR}/src/paimon/common/global_index/bitmap/bitmap_global_index_factory.cpp
    DEPS paimon paimon_file_index arrow fmt
)

# ============================================================================
# paimon_blob_file_format
# ============================================================================
_add_paimon_lib(paimon_blob_file_format
    SOURCES
    ${PAIMON_SRC_DIR}/src/paimon/format/blob/blob_file_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/blob/blob_file_format_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/blob/blob_format_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/blob/blob_stats_extractor.cpp
    DEPS paimon arrow fmt
)

# ============================================================================
# paimon_parquet_file_format
# ============================================================================
_add_paimon_lib(paimon_parquet_file_format
    SOURCES
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_field_id_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/predicate_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/file_reader_wrapper.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_timestamp_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_file_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_file_format_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_format_writer.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_input_stream_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_output_stream_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_schema_util.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_stats_extractor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/parquet/parquet_writer_builder.cpp
    DEPS paimon parquet arrow fmt
)

# ============================================================================
# paimon_orc_file_format
# ============================================================================
_add_paimon_lib(paimon_orc_file_format
    SOURCES
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/predicate_converter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_reader_wrapper.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/read_range_generator.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_file_format_factory.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_file_batch_reader.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_input_stream_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_output_stream_impl.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_adapter.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_stats_extractor.cpp
    ${PAIMON_SRC_DIR}/src/paimon/format/orc/orc_format_writer.cpp
    DEPS paimon orc::orc arrow fmt tbb_paimon
)
