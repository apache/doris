// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#ifdef BUILD_RUST_READERS

#include <arrow/c/abi.h>

#include <cstddef>
#include <cstdint>

namespace doris::lance_ffi {

// FFI status codes (must match Rust error.rs)
constexpr int32_t LANCE_FFI_OK = 0;
constexpr int32_t LANCE_FFI_EOF = 1;
constexpr int32_t LANCE_FFI_ERR_LANCE = -1;
constexpr int32_t LANCE_FFI_ERR_ARROW = -2;
constexpr int32_t LANCE_FFI_ERR_IO = -3;
constexpr int32_t LANCE_FFI_ERR_PANIC = -4;
constexpr int32_t LANCE_FFI_ERR_INVALID_ARG = -5;

} // namespace doris::lance_ffi

// Opaque handle to a Rust LanceReader.
using LanceReaderHandle = void*;

extern "C" {

/// Open a Lance dataset and create a reader.
int32_t lance_reader_open(const uint8_t* uri_ptr, size_t uri_len,
                          const uint8_t* const* column_names_ptr,
                          const size_t* column_names_len_ptr, size_t num_columns, size_t batch_size,
                          LanceReaderHandle* handle_out);

/// Read the next batch via Arrow C Data Interface.
/// Returns LANCE_FFI_OK with data, LANCE_FFI_EOF on end, negative on error.
int32_t lance_reader_next_batch(LanceReaderHandle handle, ArrowSchema* schema_out,
                                ArrowArray* array_out, bool* eof_out, int64_t* bytes_out);

/// Get the schema of the scan output.
int32_t lance_reader_get_schema(LanceReaderHandle handle, ArrowSchema* schema_out);

/// Close the reader and free resources. Safe to call with null handle.
void lance_reader_close(LanceReaderHandle handle);

/// Retrieve the last error message. Returns bytes written (excluding null terminator).
size_t lance_reader_last_error(uint8_t* buf, size_t buf_len);

/// Open a Lance dataset from a JSON config string.
/// Config JSON: {"uri":"...", "columns":[], "batch_size":N, "version":N, "storage_options":{}}
/// storage_options carries S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.)
int32_t lance_reader_open_json(const uint8_t* config_json_ptr, size_t config_json_len,
                               LanceReaderHandle* handle_out);

/// Phase 0 echo function for build verification.
int32_t rust_echo(int32_t x);

/// Create a test Lance dataset at the given path. For GTests only.
/// Dataset has 5 rows: id(INT32), name(UTF8), score(FLOAT64).
int32_t lance_test_create_dataset(const uint8_t* path_ptr, size_t path_len);

/// Create a multi-fragment test dataset. 3 fragments, 5 rows each = 15 total.
/// Columns: id(INT32), name(UTF8), value(FLOAT64).
int32_t lance_test_create_multi_fragment_dataset(const uint8_t* path_ptr, size_t path_len);

} // extern "C"

#endif // BUILD_RUST_READERS
