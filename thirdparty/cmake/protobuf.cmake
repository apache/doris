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

# protobuf — hand-written add_library (no add_subdirectory)
set(_PB_SRC "${TP_SOURCE_DIR}/protobuf-21.11")
set(_PB_DIR "${_PB_SRC}/src/google/protobuf")

# ============================================================================
# Source files — copied verbatim from upstream cmake/libprotobuf-lite.cmake,
# cmake/libprotobuf.cmake, cmake/libprotoc.cmake
# ============================================================================

set(libprotobuf_lite_files
    ${_PB_DIR}/any_lite.cc
    ${_PB_DIR}/arena.cc
    ${_PB_DIR}/arenastring.cc
    ${_PB_DIR}/arenaz_sampler.cc
    ${_PB_DIR}/extension_set.cc
    ${_PB_DIR}/generated_enum_util.cc
    ${_PB_DIR}/generated_message_tctable_lite.cc
    ${_PB_DIR}/generated_message_util.cc
    ${_PB_DIR}/implicit_weak_message.cc
    ${_PB_DIR}/inlined_string_field.cc
    ${_PB_DIR}/io/coded_stream.cc
    ${_PB_DIR}/io/io_win32.cc
    ${_PB_DIR}/io/strtod.cc
    ${_PB_DIR}/io/zero_copy_stream.cc
    ${_PB_DIR}/io/zero_copy_stream_impl.cc
    ${_PB_DIR}/io/zero_copy_stream_impl_lite.cc
    ${_PB_DIR}/map.cc
    ${_PB_DIR}/message_lite.cc
    ${_PB_DIR}/parse_context.cc
    ${_PB_DIR}/repeated_field.cc
    ${_PB_DIR}/repeated_ptr_field.cc
    ${_PB_DIR}/stubs/bytestream.cc
    ${_PB_DIR}/stubs/common.cc
    ${_PB_DIR}/stubs/int128.cc
    ${_PB_DIR}/stubs/status.cc
    ${_PB_DIR}/stubs/statusor.cc
    ${_PB_DIR}/stubs/stringpiece.cc
    ${_PB_DIR}/stubs/stringprintf.cc
    ${_PB_DIR}/stubs/structurally_valid.cc
    ${_PB_DIR}/stubs/strutil.cc
    ${_PB_DIR}/stubs/time.cc
    ${_PB_DIR}/wire_format_lite.cc
)

set(libprotobuf_files
    ${_PB_DIR}/any.cc
    ${_PB_DIR}/any.pb.cc
    ${_PB_DIR}/api.pb.cc
    ${_PB_DIR}/compiler/importer.cc
    ${_PB_DIR}/compiler/parser.cc
    ${_PB_DIR}/descriptor.cc
    ${_PB_DIR}/descriptor.pb.cc
    ${_PB_DIR}/descriptor_database.cc
    ${_PB_DIR}/duration.pb.cc
    ${_PB_DIR}/dynamic_message.cc
    ${_PB_DIR}/empty.pb.cc
    ${_PB_DIR}/extension_set_heavy.cc
    ${_PB_DIR}/field_mask.pb.cc
    ${_PB_DIR}/generated_message_bases.cc
    ${_PB_DIR}/generated_message_reflection.cc
    ${_PB_DIR}/generated_message_tctable_full.cc
    ${_PB_DIR}/io/gzip_stream.cc
    ${_PB_DIR}/io/printer.cc
    ${_PB_DIR}/io/tokenizer.cc
    ${_PB_DIR}/map_field.cc
    ${_PB_DIR}/message.cc
    ${_PB_DIR}/reflection_ops.cc
    ${_PB_DIR}/service.cc
    ${_PB_DIR}/source_context.pb.cc
    ${_PB_DIR}/struct.pb.cc
    ${_PB_DIR}/stubs/substitute.cc
    ${_PB_DIR}/text_format.cc
    ${_PB_DIR}/timestamp.pb.cc
    ${_PB_DIR}/type.pb.cc
    ${_PB_DIR}/unknown_field_set.cc
    ${_PB_DIR}/util/delimited_message_util.cc
    ${_PB_DIR}/util/field_comparator.cc
    ${_PB_DIR}/util/field_mask_util.cc
    ${_PB_DIR}/util/internal/datapiece.cc
    ${_PB_DIR}/util/internal/default_value_objectwriter.cc
    ${_PB_DIR}/util/internal/error_listener.cc
    ${_PB_DIR}/util/internal/field_mask_utility.cc
    ${_PB_DIR}/util/internal/json_escaping.cc
    ${_PB_DIR}/util/internal/json_objectwriter.cc
    ${_PB_DIR}/util/internal/json_stream_parser.cc
    ${_PB_DIR}/util/internal/object_writer.cc
    ${_PB_DIR}/util/internal/proto_writer.cc
    ${_PB_DIR}/util/internal/protostream_objectsource.cc
    ${_PB_DIR}/util/internal/protostream_objectwriter.cc
    ${_PB_DIR}/util/internal/type_info.cc
    ${_PB_DIR}/util/internal/utility.cc
    ${_PB_DIR}/util/json_util.cc
    ${_PB_DIR}/util/message_differencer.cc
    ${_PB_DIR}/util/time_util.cc
    ${_PB_DIR}/util/type_resolver_util.cc
    ${_PB_DIR}/wire_format.cc
    ${_PB_DIR}/wrappers.pb.cc
)

set(libprotoc_files
    ${_PB_DIR}/compiler/code_generator.cc
    ${_PB_DIR}/compiler/command_line_interface.cc
    ${_PB_DIR}/compiler/cpp/enum.cc
    ${_PB_DIR}/compiler/cpp/enum_field.cc
    ${_PB_DIR}/compiler/cpp/extension.cc
    ${_PB_DIR}/compiler/cpp/field.cc
    ${_PB_DIR}/compiler/cpp/file.cc
    ${_PB_DIR}/compiler/cpp/generator.cc
    ${_PB_DIR}/compiler/cpp/helpers.cc
    ${_PB_DIR}/compiler/cpp/map_field.cc
    ${_PB_DIR}/compiler/cpp/message.cc
    ${_PB_DIR}/compiler/cpp/message_field.cc
    ${_PB_DIR}/compiler/cpp/padding_optimizer.cc
    ${_PB_DIR}/compiler/cpp/parse_function_generator.cc
    ${_PB_DIR}/compiler/cpp/primitive_field.cc
    ${_PB_DIR}/compiler/cpp/service.cc
    ${_PB_DIR}/compiler/cpp/string_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_doc_comment.cc
    ${_PB_DIR}/compiler/csharp/csharp_enum.cc
    ${_PB_DIR}/compiler/csharp/csharp_enum_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_field_base.cc
    ${_PB_DIR}/compiler/csharp/csharp_generator.cc
    ${_PB_DIR}/compiler/csharp/csharp_helpers.cc
    ${_PB_DIR}/compiler/csharp/csharp_map_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_message.cc
    ${_PB_DIR}/compiler/csharp/csharp_message_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_primitive_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_reflection_class.cc
    ${_PB_DIR}/compiler/csharp/csharp_repeated_enum_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_repeated_message_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_repeated_primitive_field.cc
    ${_PB_DIR}/compiler/csharp/csharp_source_generator_base.cc
    ${_PB_DIR}/compiler/csharp/csharp_wrapper_field.cc
    ${_PB_DIR}/compiler/java/context.cc
    ${_PB_DIR}/compiler/java/doc_comment.cc
    ${_PB_DIR}/compiler/java/enum.cc
    ${_PB_DIR}/compiler/java/enum_field.cc
    ${_PB_DIR}/compiler/java/enum_field_lite.cc
    ${_PB_DIR}/compiler/java/enum_lite.cc
    ${_PB_DIR}/compiler/java/extension.cc
    ${_PB_DIR}/compiler/java/extension_lite.cc
    ${_PB_DIR}/compiler/java/field.cc
    ${_PB_DIR}/compiler/java/file.cc
    ${_PB_DIR}/compiler/java/generator.cc
    ${_PB_DIR}/compiler/java/generator_factory.cc
    ${_PB_DIR}/compiler/java/helpers.cc
    ${_PB_DIR}/compiler/java/kotlin_generator.cc
    ${_PB_DIR}/compiler/java/map_field.cc
    ${_PB_DIR}/compiler/java/map_field_lite.cc
    ${_PB_DIR}/compiler/java/message.cc
    ${_PB_DIR}/compiler/java/message_builder.cc
    ${_PB_DIR}/compiler/java/message_builder_lite.cc
    ${_PB_DIR}/compiler/java/message_field.cc
    ${_PB_DIR}/compiler/java/message_field_lite.cc
    ${_PB_DIR}/compiler/java/message_lite.cc
    ${_PB_DIR}/compiler/java/name_resolver.cc
    ${_PB_DIR}/compiler/java/primitive_field.cc
    ${_PB_DIR}/compiler/java/primitive_field_lite.cc
    ${_PB_DIR}/compiler/java/service.cc
    ${_PB_DIR}/compiler/java/shared_code_generator.cc
    ${_PB_DIR}/compiler/java/string_field.cc
    ${_PB_DIR}/compiler/java/string_field_lite.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_enum.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_enum_field.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_extension.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_field.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_file.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_generator.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_helpers.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_map_field.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_message.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_message_field.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_oneof.cc
    ${_PB_DIR}/compiler/objectivec/objectivec_primitive_field.cc
    ${_PB_DIR}/compiler/php/php_generator.cc
    ${_PB_DIR}/compiler/plugin.cc
    ${_PB_DIR}/compiler/plugin.pb.cc
    ${_PB_DIR}/compiler/python/generator.cc
    ${_PB_DIR}/compiler/python/helpers.cc
    ${_PB_DIR}/compiler/python/pyi_generator.cc
    ${_PB_DIR}/compiler/ruby/ruby_generator.cc
    ${_PB_DIR}/compiler/subprocess.cc
    ${_PB_DIR}/compiler/zip_writer.cc
)

# ============================================================================
# libprotobuf-lite
# ============================================================================
add_library(libprotobuf-lite STATIC ${libprotobuf_lite_files})
target_include_directories(libprotobuf-lite PUBLIC ${_PB_SRC}/src)
target_compile_definitions(libprotobuf-lite PRIVATE GOOGLE_PROTOBUF_CMAKE_BUILD HAVE_ZLIB)
target_link_libraries(libprotobuf-lite PRIVATE Threads::Threads)
set_target_properties(libprotobuf-lite PROPERTIES OUTPUT_NAME protobuf-lite)
add_library(protobuf::libprotobuf-lite ALIAS libprotobuf-lite)

# ============================================================================
# libprotobuf (includes lite sources)
# ============================================================================
add_library(libprotobuf STATIC ${libprotobuf_lite_files} ${libprotobuf_files})
target_include_directories(libprotobuf PUBLIC ${_PB_SRC}/src)
target_compile_definitions(libprotobuf PRIVATE GOOGLE_PROTOBUF_CMAKE_BUILD HAVE_ZLIB)
target_link_libraries(libprotobuf PRIVATE Threads::Threads libz)
set_target_properties(libprotobuf PROPERTIES OUTPUT_NAME protobuf)
add_library(protobuf::libprotobuf ALIAS libprotobuf)

# ============================================================================
# libprotoc
# ============================================================================
add_library(libprotoc STATIC ${libprotoc_files})
target_include_directories(libprotoc PUBLIC ${_PB_SRC}/src)
target_compile_definitions(libprotoc PRIVATE GOOGLE_PROTOBUF_CMAKE_BUILD HAVE_ZLIB)
target_link_libraries(libprotoc PRIVATE libprotobuf)
set_target_properties(libprotoc PROPERTIES OUTPUT_NAME protoc)
add_library(protobuf::libprotoc ALIAS libprotoc)

# ============================================================================
# protoc executable (needed by grpc & brpc for proto compilation)
# ============================================================================
set(_PREBUILT_PROTOC "$ENV{DORIS_THIRDPARTY}/_gensrc_tools/protoc_build/protoc")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "MSAN" AND EXISTS "${_PREBUILT_PROTOC}")
    # MSAN: use pre-built protoc (uninstrumented) to avoid false positives during code gen
    message(STATUS "MSAN: using pre-built protoc from ${_PREBUILT_PROTOC}")
    add_executable(protoc IMPORTED GLOBAL)
    set_target_properties(protoc PROPERTIES IMPORTED_LOCATION "${_PREBUILT_PROTOC}")
else()
    add_executable(protoc ${_PB_DIR}/compiler/main.cc)
    target_link_libraries(protoc PRIVATE libprotoc libprotobuf)
    if(COMPILER_CLANG)
        target_link_options(protoc PRIVATE -static-libstdc++)
    endif()
    add_executable(protobuf::protoc ALIAS protoc)
endif()

# Pre-set Protobuf_FOUND variables for downstream find_package(Protobuf)
set(Protobuf_FOUND TRUE)
set(PROTOBUF_FOUND TRUE)
set(Protobuf_INCLUDE_DIRS "${_PB_SRC}/src")
set(PROTOBUF_INCLUDE_DIRS "${_PB_SRC}/src")
set(Protobuf_LIBRARIES libprotobuf)
set(PROTOBUF_LIBRARIES libprotobuf)
set(Protobuf_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
set(PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
