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

# brpc — hand-written add_library (no add_subdirectory)
set(_BRPC_SRC ${TP_SOURCE_DIR}/brpc-1.4.0)
set(_BRPC_PROTO_DIR "${_BRPC_SRC}/src")
set(_BRPC_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/brpc")
file(MAKE_DIRECTORY ${_BRPC_GEN_DIR}/brpc/policy)

# Generate butil/config.h (logging.h:25 includes it). Upstream brpc CMakeLists
# writes this into src/butil/, but we keep src/ pristine and write into the
# build-time gen dir, which is added to the include path below.
file(MAKE_DIRECTORY ${_BRPC_GEN_DIR}/butil)
# config.h.in begins with `#ifdef BRPC_WITH_GLOG / #undef BRPC_WITH_GLOG` and
# then `#cmakedefine BRPC_WITH_GLOG @WITH_GLOG_VAL@`. The header is included by
# logging.h on every compile, so even though we pass -DBRPC_WITH_GLOG=1 on the
# command line, butil/config.h unconditionally undefs it and then redefines
# only if the cmake variable BRPC_WITH_GLOG (not WITH_GLOG_VAL) is truthy.
# Set BOTH the indicator and the value var, otherwise the macro disappears,
# and server.cpp's `#if !BRPC_WITH_GLOG` block (which references VLogService)
# is compiled — leaving an undefined vtable at link time.
set(BRPC_WITH_GLOG 1)
set(WITH_GLOG_VAL "1")
configure_file(
    ${_BRPC_SRC}/config.h.in
    ${_BRPC_GEN_DIR}/butil/config.h
    @ONLY
)

# ============================================================================
# Collect include dirs for dependencies
# ============================================================================
get_target_property(_gflags_incs gflags_static INTERFACE_INCLUDE_DIRECTORIES)
list(GET _gflags_incs 0 _gflags_inc)

get_target_property(_glog_incs glog INTERFACE_INCLUDE_DIRECTORIES)
list(GET _glog_incs 0 _glog_inc)

get_target_property(_leveldb_incs leveldb INTERFACE_INCLUDE_DIRECTORIES)
list(GET _leveldb_incs 0 _leveldb_inc)

get_target_property(_snappy_incs snappy INTERFACE_INCLUDE_DIRECTORIES)
list(GET _snappy_incs 0 _snappy_inc)

# ============================================================================
# Proto compilation: .proto → .pb.cc / .pb.h
# ============================================================================
tp_protobuf_generate(_BRPC_PROTO_CC_FILES _BRPC_PROTO_H_FILES
    PROTO_PATH ${_BRPC_PROTO_DIR}
    OUT_DIR    ${_BRPC_GEN_DIR}
    PROTOS
        idl_options.proto
        brpc/rtmp.proto
        brpc/rpc_dump.proto
        brpc/get_favicon.proto
        brpc/span.proto
        brpc/builtin_service.proto
        brpc/get_js.proto
        brpc/errno.proto
        brpc/nshead_meta.proto
        brpc/options.proto
        brpc/policy/baidu_rpc_meta.proto
        brpc/policy/hulu_pbrpc_meta.proto
        brpc/policy/public_pbrpc_meta.proto
        brpc/policy/sofa_pbrpc_meta.proto
        brpc/policy/mongo.proto
        brpc/trackme.proto
        brpc/streaming_rpc_meta.proto
        brpc/proto_base.proto
)

# ============================================================================
# BUTIL sources — exact explicit list from brpc's CMakeLists.txt (Linux)
# ============================================================================
set(BUTIL_SOURCES
    ${_BRPC_SRC}/src/butil/third_party/dmg_fp/g_fmt.cc
    ${_BRPC_SRC}/src/butil/third_party/dmg_fp/dtoa_wrapper.cc
    ${_BRPC_SRC}/src/butil/third_party/dynamic_annotations/dynamic_annotations.c
    ${_BRPC_SRC}/src/butil/third_party/icu/icu_utf.cc
    ${_BRPC_SRC}/src/butil/third_party/superfasthash/superfasthash.c
    ${_BRPC_SRC}/src/butil/third_party/modp_b64/modp_b64.cc
    ${_BRPC_SRC}/src/butil/third_party/symbolize/demangle.cc
    ${_BRPC_SRC}/src/butil/third_party/symbolize/symbolize.cc
    ${_BRPC_SRC}/src/butil/third_party/snappy/snappy-sinksource.cc
    ${_BRPC_SRC}/src/butil/third_party/snappy/snappy-stubs-internal.cc
    ${_BRPC_SRC}/src/butil/third_party/snappy/snappy.cc
    ${_BRPC_SRC}/src/butil/third_party/murmurhash3/murmurhash3.cpp
    ${_BRPC_SRC}/src/butil/arena.cpp
    ${_BRPC_SRC}/src/butil/at_exit.cc
    ${_BRPC_SRC}/src/butil/atomicops_internals_x86_gcc.cc
    ${_BRPC_SRC}/src/butil/base64.cc
    ${_BRPC_SRC}/src/butil/big_endian.cc
    ${_BRPC_SRC}/src/butil/cpu.cc
    ${_BRPC_SRC}/src/butil/debug/alias.cc
    ${_BRPC_SRC}/src/butil/debug/asan_invalid_access.cc
    ${_BRPC_SRC}/src/butil/debug/crash_logging.cc
    ${_BRPC_SRC}/src/butil/debug/debugger.cc
    ${_BRPC_SRC}/src/butil/debug/debugger_posix.cc
    ${_BRPC_SRC}/src/butil/debug/dump_without_crashing.cc
    ${_BRPC_SRC}/src/butil/debug/proc_maps_linux.cc
    ${_BRPC_SRC}/src/butil/debug/stack_trace.cc
    ${_BRPC_SRC}/src/butil/debug/stack_trace_posix.cc
    ${_BRPC_SRC}/src/butil/environment.cc
    ${_BRPC_SRC}/src/butil/files/file.cc
    ${_BRPC_SRC}/src/butil/files/file_posix.cc
    ${_BRPC_SRC}/src/butil/files/file_enumerator.cc
    ${_BRPC_SRC}/src/butil/files/file_enumerator_posix.cc
    ${_BRPC_SRC}/src/butil/files/file_path.cc
    ${_BRPC_SRC}/src/butil/files/file_path_constants.cc
    ${_BRPC_SRC}/src/butil/files/memory_mapped_file.cc
    ${_BRPC_SRC}/src/butil/files/memory_mapped_file_posix.cc
    ${_BRPC_SRC}/src/butil/files/scoped_file.cc
    ${_BRPC_SRC}/src/butil/files/scoped_temp_dir.cc
    ${_BRPC_SRC}/src/butil/file_util.cc
    ${_BRPC_SRC}/src/butil/file_util_posix.cc
    ${_BRPC_SRC}/src/butil/guid.cc
    ${_BRPC_SRC}/src/butil/guid_posix.cc
    ${_BRPC_SRC}/src/butil/hash.cc
    ${_BRPC_SRC}/src/butil/lazy_instance.cc
    ${_BRPC_SRC}/src/butil/location.cc
    ${_BRPC_SRC}/src/butil/memory/aligned_memory.cc
    ${_BRPC_SRC}/src/butil/memory/ref_counted.cc
    ${_BRPC_SRC}/src/butil/memory/ref_counted_memory.cc
    ${_BRPC_SRC}/src/butil/memory/singleton.cc
    ${_BRPC_SRC}/src/butil/memory/weak_ptr.cc
    ${_BRPC_SRC}/src/butil/posix/file_descriptor_shuffle.cc
    ${_BRPC_SRC}/src/butil/posix/global_descriptors.cc
    ${_BRPC_SRC}/src/butil/process_util.cc
    ${_BRPC_SRC}/src/butil/rand_util.cc
    ${_BRPC_SRC}/src/butil/rand_util_posix.cc
    ${_BRPC_SRC}/src/butil/fast_rand.cpp
    ${_BRPC_SRC}/src/butil/safe_strerror_posix.cc
    ${_BRPC_SRC}/src/butil/sha1_portable.cc
    ${_BRPC_SRC}/src/butil/strings/latin1_string_conversions.cc
    ${_BRPC_SRC}/src/butil/strings/nullable_string16.cc
    ${_BRPC_SRC}/src/butil/strings/safe_sprintf.cc
    ${_BRPC_SRC}/src/butil/strings/string16.cc
    ${_BRPC_SRC}/src/butil/strings/string_number_conversions.cc
    ${_BRPC_SRC}/src/butil/strings/string_split.cc
    ${_BRPC_SRC}/src/butil/strings/string_piece.cc
    ${_BRPC_SRC}/src/butil/strings/string_util.cc
    ${_BRPC_SRC}/src/butil/strings/string_util_constants.cc
    ${_BRPC_SRC}/src/butil/strings/stringprintf.cc
    ${_BRPC_SRC}/src/butil/strings/utf_offset_string_conversions.cc
    ${_BRPC_SRC}/src/butil/strings/utf_string_conversion_utils.cc
    ${_BRPC_SRC}/src/butil/strings/utf_string_conversions.cc
    ${_BRPC_SRC}/src/butil/synchronization/cancellation_flag.cc
    ${_BRPC_SRC}/src/butil/synchronization/condition_variable_posix.cc
    ${_BRPC_SRC}/src/butil/synchronization/waitable_event_posix.cc
    ${_BRPC_SRC}/src/butil/threading/non_thread_safe_impl.cc
    ${_BRPC_SRC}/src/butil/threading/platform_thread_posix.cc
    ${_BRPC_SRC}/src/butil/threading/simple_thread.cc
    ${_BRPC_SRC}/src/butil/threading/thread_checker_impl.cc
    ${_BRPC_SRC}/src/butil/threading/thread_collision_warner.cc
    ${_BRPC_SRC}/src/butil/threading/thread_id_name_manager.cc
    ${_BRPC_SRC}/src/butil/threading/thread_local_posix.cc
    ${_BRPC_SRC}/src/butil/threading/thread_local_storage.cc
    ${_BRPC_SRC}/src/butil/threading/thread_local_storage_posix.cc
    ${_BRPC_SRC}/src/butil/threading/thread_restrictions.cc
    ${_BRPC_SRC}/src/butil/threading/watchdog.cc
    ${_BRPC_SRC}/src/butil/time/clock.cc
    ${_BRPC_SRC}/src/butil/time/default_clock.cc
    ${_BRPC_SRC}/src/butil/time/default_tick_clock.cc
    ${_BRPC_SRC}/src/butil/time/tick_clock.cc
    ${_BRPC_SRC}/src/butil/time/time.cc
    ${_BRPC_SRC}/src/butil/time/time_posix.cc
    ${_BRPC_SRC}/src/butil/version.cc
    ${_BRPC_SRC}/src/butil/logging.cc
    ${_BRPC_SRC}/src/butil/class_name.cpp
    ${_BRPC_SRC}/src/butil/errno.cpp
    ${_BRPC_SRC}/src/butil/find_cstr.cpp
    ${_BRPC_SRC}/src/butil/status.cpp
    ${_BRPC_SRC}/src/butil/string_printf.cpp
    ${_BRPC_SRC}/src/butil/thread_local.cpp
    ${_BRPC_SRC}/src/butil/unix_socket.cpp
    ${_BRPC_SRC}/src/butil/endpoint.cpp
    ${_BRPC_SRC}/src/butil/fd_utility.cpp
    ${_BRPC_SRC}/src/butil/files/temp_file.cpp
    ${_BRPC_SRC}/src/butil/files/file_watcher.cpp
    ${_BRPC_SRC}/src/butil/time.cpp
    ${_BRPC_SRC}/src/butil/zero_copy_stream_as_streambuf.cpp
    ${_BRPC_SRC}/src/butil/crc32c.cc
    ${_BRPC_SRC}/src/butil/containers/case_ignored_flat_map.cpp
    ${_BRPC_SRC}/src/butil/iobuf.cpp
    ${_BRPC_SRC}/src/butil/binary_printer.cpp
    ${_BRPC_SRC}/src/butil/recordio.cc
    ${_BRPC_SRC}/src/butil/popen.cpp
    # Linux-specific
    ${_BRPC_SRC}/src/butil/file_util_linux.cc
    ${_BRPC_SRC}/src/butil/threading/platform_thread_linux.cc
    ${_BRPC_SRC}/src/butil/strings/sys_string_conversions_posix.cc
)

# ============================================================================
# GLOB remaining source dirs (bvar, bthread, json2pb, mcpack2pb, brpc)
# ============================================================================
file(GLOB_RECURSE _BVAR_SRCS    "${_BRPC_SRC}/src/bvar/*.cpp")
file(GLOB_RECURSE _BTHREAD_SRCS "${_BRPC_SRC}/src/bthread/*.cpp")
file(GLOB_RECURSE _JSON2PB_SRCS "${_BRPC_SRC}/src/json2pb/*.cpp")
file(GLOB_RECURSE _BRPC_SRCS    "${_BRPC_SRC}/src/brpc/*.cpp")

set(_MCPACK2PB_SRCS
    ${_BRPC_SRC}/src/mcpack2pb/field_type.cpp
    ${_BRPC_SRC}/src/mcpack2pb/mcpack2pb.cpp
    ${_BRPC_SRC}/src/mcpack2pb/parser.cpp
    ${_BRPC_SRC}/src/mcpack2pb/serializer.cpp
)

# Exclude macOS-only event dispatcher
list(FILTER _BRPC_SRCS EXCLUDE REGEX "event_dispatcher_kqueue\\.cpp$")
list(FILTER _BRPC_SRCS EXCLUDE REGEX "event_dispatcher_epoll\\.cpp$")
# Exclude thrift (we do not enable WITH_THRIFT)
list(FILTER _BRPC_SRCS EXCLUDE REGEX "thrift_message\\.cpp$")
list(FILTER _BRPC_SRCS EXCLUDE REGEX "thrift_service\\.cpp$")
list(FILTER _BRPC_SRCS EXCLUDE REGEX "thrift_protocol\\.cpp$")

# ============================================================================
# add_library: brpc-static
# ============================================================================
add_library(brpc-static STATIC
    ${BUTIL_SOURCES}
    ${_BVAR_SRCS}
    ${_BTHREAD_SRCS}
    ${_JSON2PB_SRCS}
    ${_MCPACK2PB_SRCS}
    ${_BRPC_SRCS}
    ${_BRPC_PROTO_CC_FILES}
)

# Consumers need brpc headers and generated .pb.h files
target_include_directories(brpc-static SYSTEM PUBLIC
    ${_BRPC_SRC}/src
    ${_BRPC_GEN_DIR}
)

# Private: dependency headers required to compile brpc itself
target_include_directories(brpc-static PRIVATE
    ${_gflags_inc}
    ${_glog_inc}
    ${CMAKE_CURRENT_BINARY_DIR}/glog
    ${TP_SOURCE_DIR}/protobuf-21.11/src
    ${_leveldb_inc}
    ${_snappy_inc}
    ${TP_SOURCE_DIR}/boost_1_81_0
    ${CMAKE_CURRENT_BINARY_DIR}/openssl/include
)

target_compile_options(brpc-static PRIVATE -fPIC -w -O2 -pipe -fstrict-aliasing
    -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer
    -msse4 -msse4.2
)

target_compile_definitions(brpc-static PRIVATE
    BRPC_WITH_GLOG=1
    BRPC_WITH_RDMA=0
    GFLAGS_NS=google
    BTHREAD_USE_FAST_PTHREAD_MUTEX
    __const__=__unused__
    _GNU_SOURCE
    USE_SYMBOLIZE
    NO_TCMALLOC
    __STDC_FORMAT_MACROS
    __STDC_LIMIT_MACROS
    __STDC_CONSTANT_MACROS
    "BRPC_REVISION=\"unknown\""
    __STRICT_ANSI__
    NDEBUG
)

find_package(Threads REQUIRED)
target_link_libraries(brpc-static PRIVATE
    gflags_static
    glog
    libprotobuf
    libprotoc
    leveldb
    snappy
    ssl
    crypto
    z
    Threads::Threads
    dl
    rt
)

set_target_properties(brpc-static PROPERTIES
    OUTPUT_NAME brpc
    POSITION_INDEPENDENT_CODE ON
)

add_library(brpc ALIAS brpc-static)
