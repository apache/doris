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

# Rust integration for doris-ffi static library.
#
# Two modes:
#   1. Pre-built: If libdoris_ffi.a already exists (e.g., built by cargo-zigbuild
#      on the CI host), use it directly. No Rust toolchain needed inside the
#      build container.
#   2. Corrosion: If no pre-built .a found and cargo is available, build via
#      Corrosion (FetchContent). Used on developer machines.

# Check for pre-built .a (from zigbuild, manylinux2014, or manual build)
set(PREBUILT_RUST_PATHS
    "${SRC_DIR}/rust/doris-native/target/x86_64-unknown-linux-gnu/release/libdoris_ffi.a"
    "${SRC_DIR}/rust/doris-native/target/release/libdoris_ffi.a"
    "${CMAKE_BINARY_DIR}/libdoris_ffi.a"
)

set(RUST_LIB_PATH "")
foreach(p ${PREBUILT_RUST_PATHS})
    if (EXISTS "${p}")
        set(RUST_LIB_PATH "${p}")
        message(STATUS "Rust readers: using pre-built library at ${p}")
        break()
    endif()
endforeach()

if (RUST_LIB_PATH)
    # Mode 1: Pre-built .a found — no Corrosion needed
    add_library(doris_ffi_lib STATIC IMPORTED GLOBAL)
    set_target_properties(doris_ffi_lib PROPERTIES
        IMPORTED_LOCATION "${RUST_LIB_PATH}"
        IMPORTED_LINK_INTERFACE_LIBRARIES "m;dl;pthread"
    )
    message(STATUS "Rust readers enabled (pre-built)")
else()
    # Mode 2: Build via Corrosion (developer machines with cargo)
    find_program(CARGO_EXECUTABLE cargo)
    if (NOT CARGO_EXECUTABLE)
        message(WARNING "BUILD_RUST_READERS=ON but no pre-built libdoris_ffi.a and no cargo in PATH. Disabling.")
        set(BUILD_RUST_READERS OFF PARENT_SCOPE)
        return()
    endif()

    include(FetchContent)
    FetchContent_Declare(
        Corrosion
        GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
        GIT_TAG v0.5.1
    )
    FetchContent_MakeAvailable(Corrosion)

    set(RUST_MANIFEST_PATH "${SRC_DIR}/rust/doris-native/Cargo.toml")
    corrosion_import_crate(
        MANIFEST_PATH ${RUST_MANIFEST_PATH}
        CRATES doris-ffi
    )

    add_library(doris_ffi_lib STATIC IMPORTED GLOBAL)
    set_target_properties(doris_ffi_lib PROPERTIES
        IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/libdoris_ffi.a"
        IMPORTED_LINK_INTERFACE_LIBRARIES "m;dl;pthread"
    )
    add_dependencies(doris_ffi_lib cargo-build_doris_ffi)
    message(STATUS "Rust readers enabled (Corrosion)")
endif()
