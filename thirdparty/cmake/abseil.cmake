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

# abseil-cpp — hand-written monolithic add_library (no add_subdirectory)
# Strategy: one STATIC library from all abseil sources, then INTERFACE aliases
# for every absl_* target name referenced by downstream consumers (protobuf, grpc, s2).
set(ABSEIL_SRC_DIR ${TP_SOURCE_DIR}/abseil-cpp-20250512.1)

# Collect all non-test source files
file(GLOB_RECURSE ABSEIL_SRCS "${ABSEIL_SRC_DIR}/absl/*.cc")
list(FILTER ABSEIL_SRCS EXCLUDE REGEX "test")
list(FILTER ABSEIL_SRCS EXCLUDE REGEX "mock")
list(FILTER ABSEIL_SRCS EXCLUDE REGEX "benchmark")
list(FILTER ABSEIL_SRCS EXCLUDE REGEX "matcher")
list(FILTER ABSEIL_SRCS EXCLUDE REGEX "_gentables\\.cc$")

add_library(abseil STATIC ${ABSEIL_SRCS})

target_include_directories(abseil SYSTEM PUBLIC
    ${ABSEIL_SRC_DIR}
)

target_compile_options(abseil PRIVATE -fPIC -w)
target_compile_features(abseil PUBLIC cxx_std_17)

find_package(Threads REQUIRED)
target_link_libraries(abseil PUBLIC Threads::Threads)

set_target_properties(abseil PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

# Create INTERFACE IMPORTED aliases for every absl_* target name
# referenced by downstream consumers (protobuf, grpc, s2, arrow, etc.)
set(ABSL_COMPONENT_NAMES
    absl_base absl_city absl_civil_time absl_cord absl_cord_internal
    absl_cordz_functions absl_cordz_handle absl_cordz_info absl_cordz_sample_token
    absl_crc_cord_state absl_crc_cpu_detect absl_crc_internal absl_crc32c
    absl_debugging_internal absl_decode_rust_punycode absl_demangle_internal
    absl_demangle_rust absl_die_if_null absl_examine_stack absl_exponential_biased
    absl_failure_signal_handler absl_flags_commandlineflag
    absl_flags_commandlineflag_internal absl_flags_config absl_flags_internal
    absl_flags_marshalling absl_flags_parse absl_flags_private_handle_accessor
    absl_flags_program_name absl_flags_reflection absl_flags_usage
    absl_flags_usage_internal absl_graphcycles_internal absl_hash
    absl_hashtablez_sampler absl_int128 absl_kernel_timeout_internal
    absl_leak_check absl_log_flags absl_log_globals absl_log_initialize
    absl_log_internal_check_op absl_log_internal_conditions
    absl_log_internal_fnmatch absl_log_internal_format
    absl_log_internal_globals absl_log_internal_log_sink_set
    absl_log_internal_message absl_log_internal_nullguard
    absl_log_internal_proto absl_log_internal_structured_proto
    absl_log_severity absl_log_sink absl_low_level_hash absl_malloc_internal
    absl_periodic_sampler absl_poison absl_random_distributions
    absl_random_internal_distribution_test_util absl_random_internal_entropy_pool
    absl_random_internal_platform absl_random_internal_randen
    absl_random_internal_randen_hwaes absl_random_internal_randen_hwaes_impl
    absl_random_internal_randen_slow absl_random_internal_seed_material
    absl_random_seed_gen_exception absl_random_seed_sequences absl_raw_hash_set
    absl_raw_logging_internal absl_scoped_set_env absl_spinlock_wait
    absl_stacktrace absl_status absl_statusor absl_strerror
    absl_str_format_internal absl_strings absl_strings_internal absl_string_view
    absl_symbolize absl_synchronization absl_throw_delegate absl_time
    absl_time_zone absl_tracing_internal absl_utf8_for_code_point
    absl_vlog_config_internal
    absl_profile
)

# Also create aliases for header-only components that don't have their own .cc files
# but are referenced by downstream consumers via absl:: namespace
set(ABSL_HEADER_ONLY_NAMES
    btree cleanup core_headers cordz_update_scope config
    algorithm algorithm_container type_traits utility meta
    memory span optional variant any strings_internal
    numeric_representation flat_hash_map flat_hash_set
    fixed_array inlined_vector node_hash_map node_hash_set
    function_ref any_invocable bind_front
    absl_check absl_log log_entry log_streamer
    log_structured check
    cordz_update_tracker dynamic_annotations flags
    bad_any_cast bad_optional_access bad_variant_access
    compare container_common container_memory
    compressed_tuple container_internal counting_allocator
    layout raw_hash_map raw_hash_set_test
    endian bits bit_cast int128
    exponential_biased sample_recorder
    errno_saver fast_type_id
    has_ostream_operator pretty_function
    base_internal atomic_hook throw_delegate
    log_internal_check_impl log_internal_strip
    log_internal_append_truncated
    str_format string_view strings
    random_random random_bit_gen_ref random_internal_traits
    random_internal_mock_helpers
    time civil_time time_zone
    debugging_internal stacktrace symbolize
    synchronization graphcycles_internal
    kernel_timeout_internal
    status statusor cord
    hash city low_level_hash
    crc32c crc_cord_state crc_internal crc_cpu_detect
    flags_commandlineflag flags_config flags_internal
    flags_marshalling flags_parse flags_reflection
    flags_program_name flags_private_handle_accessor
    flags_usage flags_usage_internal
    leak_check malloc_internal
    raw_logging_internal log_severity
    spinlock_wait strerror
    tracing_internal periodic_sampler
    no_destructor null_mutex
    prefetch
)

foreach(_comp IN LISTS ABSL_COMPONENT_NAMES)
    if(NOT TARGET ${_comp})
        add_library(${_comp} INTERFACE IMPORTED GLOBAL)
        target_link_libraries(${_comp} INTERFACE abseil)
    endif()
endforeach()

# Also create absl:: namespace aliases used by grpc/protobuf
foreach(_comp IN LISTS ABSL_COMPONENT_NAMES)
    string(REGEX REPLACE "^absl_" "" _short "${_comp}")
    if(NOT TARGET absl::${_short})
        add_library(absl::${_short} INTERFACE IMPORTED GLOBAL)
        target_link_libraries(absl::${_short} INTERFACE abseil)
    endif()
endforeach()

# Header-only absl:: targets (no corresponding .cc files)
foreach(_comp IN LISTS ABSL_HEADER_ONLY_NAMES)
    if(NOT TARGET absl::${_comp})
        add_library(absl::${_comp} INTERFACE IMPORTED GLOBAL)
        target_link_libraries(absl::${_comp} INTERFACE abseil)
    endif()
endforeach()
