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

//! C FFI functions for the Lance reader.
//!
//! All functions are wrapped in `catch_unwind` to prevent Rust panics from
//! unwinding across the FFI boundary (which is undefined behavior).
//! Errors are stored in thread-local storage and retrieved via
//! `lance_reader_last_error`.
//!
//! Data is exchanged via the Arrow C Data Interface (ArrowSchema + ArrowArray),
//! which is version-stable and allows zero-copy transfer between Rust and C++.

use std::panic::AssertUnwindSafe;
use std::ptr;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use crate::error::{self, FFI_EOF, FFI_ERR_ARROW, FFI_ERR_INVALID_ARG, FFI_ERR_PANIC, FFI_OK};
use crate::lance_reader::{LanceReader, LanceReaderConfig};

/// Opaque handle to a LanceReader. Allocated on the heap, freed by `lance_reader_close`.
type LanceReaderHandle = *mut LanceReader;

/// Open a Lance dataset and create a reader handle.
///
/// # Arguments
/// * `uri_ptr` - Pointer to UTF-8 encoded dataset URI
/// * `uri_len` - Length of the URI in bytes
/// * `column_names_ptr` - Array of pointers to UTF-8 column name strings
/// * `column_names_len_ptr` - Array of lengths for each column name
/// * `num_columns` - Number of columns (0 = read all)
/// * `batch_size` - Maximum rows per batch
/// * `handle_out` - Output: pointer to the created reader handle
///
/// # Returns
/// FFI_OK on success, negative error code on failure.
#[no_mangle]
pub extern "C" fn lance_reader_open(
    uri_ptr: *const u8,
    uri_len: usize,
    column_names_ptr: *const *const u8,
    column_names_len_ptr: *const usize,
    num_columns: usize,
    batch_size: usize,
    handle_out: *mut LanceReaderHandle,
) -> i32 {
    error::clear_last_error();

    std::panic::catch_unwind(AssertUnwindSafe(|| {
        // Validate arguments
        if uri_ptr.is_null() || handle_out.is_null() {
            error::set_last_error("uri_ptr and handle_out must not be null".to_string());
            return FFI_ERR_INVALID_ARG;
        }

        let uri = unsafe {
            let slice = std::slice::from_raw_parts(uri_ptr, uri_len);
            match std::str::from_utf8(slice) {
                Ok(s) => s,
                Err(e) => {
                    error::set_last_error(format!("Invalid UTF-8 in URI: {}", e));
                    return FFI_ERR_INVALID_ARG;
                }
            }
        };

        let columns: Vec<String> =
            if num_columns > 0 && !column_names_ptr.is_null() && !column_names_len_ptr.is_null() {
                (0..num_columns)
                    .map(|i| unsafe {
                        let name_ptr = *column_names_ptr.add(i);
                        let name_len = *column_names_len_ptr.add(i);
                        let slice = std::slice::from_raw_parts(name_ptr, name_len);
                        String::from_utf8_lossy(slice).into_owned()
                    })
                    .collect()
            } else {
                Vec::new()
            };

        let batch_size = if batch_size == 0 { 4096 } else { batch_size };

        match LanceReader::open(uri, &columns, batch_size) {
            Ok(reader) => {
                let boxed = Box::new(reader);
                unsafe {
                    ptr::write(handle_out, Box::into_raw(boxed));
                }
                FFI_OK
            }
            Err(e) => {
                let code = e.status_code();
                error::set_last_error(e.to_string());
                code
            }
        }
    }))
    .unwrap_or_else(|panic| {
        let msg = format_panic(&panic);
        error::set_last_error(msg);
        FFI_ERR_PANIC
    })
}

/// Open a Lance dataset from a JSON config string.
///
/// The config JSON contains: uri, columns, batch_size, version, storage_options.
/// storage_options carries S3 credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, etc.)
///
/// Example config:
/// ```json
/// {
///   "uri": "s3://bucket/data.lance",
///   "columns": ["id", "name"],
///   "batch_size": 4096,
///   "version": 0,
///   "storage_options": {
///     "AWS_ACCESS_KEY_ID": "...",
///     "AWS_SECRET_ACCESS_KEY": "...",
///     "AWS_ENDPOINT": "...",
///     "AWS_REGION": "us-east-1"
///   }
/// }
/// ```
#[no_mangle]
pub extern "C" fn lance_reader_open_json(
    config_json_ptr: *const u8,
    config_json_len: usize,
    handle_out: *mut LanceReaderHandle,
) -> i32 {
    error::clear_last_error();

    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if config_json_ptr.is_null() || handle_out.is_null() {
            error::set_last_error("config_json_ptr and handle_out must not be null".to_string());
            return FFI_ERR_INVALID_ARG;
        }

        let json_str = unsafe {
            let slice = std::slice::from_raw_parts(config_json_ptr, config_json_len);
            match std::str::from_utf8(slice) {
                Ok(s) => s,
                Err(e) => {
                    error::set_last_error(format!("Invalid UTF-8 in config JSON: {}", e));
                    return FFI_ERR_INVALID_ARG;
                }
            }
        };

        let config: LanceReaderConfig = match serde_json::from_str(json_str) {
            Ok(c) => c,
            Err(e) => {
                error::set_last_error(format!("Invalid config JSON: {}", e));
                return FFI_ERR_INVALID_ARG;
            }
        };

        match LanceReader::open_with_config(&config) {
            Ok(reader) => {
                let boxed = Box::new(reader);
                unsafe {
                    ptr::write(handle_out, Box::into_raw(boxed));
                }
                FFI_OK
            }
            Err(e) => {
                let code = e.status_code();
                error::set_last_error(e.to_string());
                code
            }
        }
    }))
    .unwrap_or_else(|panic| {
        let msg = format_panic(&panic);
        error::set_last_error(msg);
        FFI_ERR_PANIC
    })
}

/// Read the next batch from the Lance reader.
///
/// # Arguments
/// * `handle` - Reader handle from `lance_reader_open`
/// * `schema_out` - Output: Arrow C schema (caller allocates, Rust fills)
/// * `array_out` - Output: Arrow C array (caller allocates, Rust fills)
/// * `eof_out` - Output: set to true when no more data
/// * `bytes_out` - Output: approximate byte size of the batch (for memory tracking)
///
/// # Returns
/// FFI_OK on success with data, FFI_EOF on end of stream, negative on error.
#[no_mangle]
pub extern "C" fn lance_reader_next_batch(
    handle: LanceReaderHandle,
    schema_out: *mut FFI_ArrowSchema,
    array_out: *mut FFI_ArrowArray,
    eof_out: *mut bool,
    bytes_out: *mut i64,
) -> i32 {
    error::clear_last_error();

    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() || schema_out.is_null() || array_out.is_null() || eof_out.is_null() {
            error::set_last_error("All output pointers must not be null".to_string());
            return FFI_ERR_INVALID_ARG;
        }

        let reader = unsafe { &mut *handle };

        match reader.next_batch() {
            Ok(Some(batch)) => {
                // Calculate approximate byte size for memory tracking
                let batch_bytes: i64 = batch
                    .columns()
                    .iter()
                    .map(|col| col.get_array_memory_size() as i64)
                    .sum();

                // Export via Arrow C Data Interface
                let struct_array: arrow::array::StructArray = batch.into();
                let data = arrow::ffi::to_ffi(&struct_array.into());
                match data {
                    Ok((ffi_array, ffi_schema)) => {
                        unsafe {
                            ptr::write(array_out, ffi_array);
                            ptr::write(schema_out, ffi_schema);
                            ptr::write(eof_out, false);
                            if !bytes_out.is_null() {
                                ptr::write(bytes_out, batch_bytes);
                            }
                        }
                        FFI_OK
                    }
                    Err(e) => {
                        error::set_last_error(format!("Arrow FFI export failed: {}", e));
                        FFI_ERR_ARROW
                    }
                }
            }
            Ok(None) => {
                unsafe {
                    ptr::write(eof_out, true);
                    if !bytes_out.is_null() {
                        ptr::write(bytes_out, 0);
                    }
                }
                FFI_EOF
            }
            Err(e) => {
                let code = e.status_code();
                error::set_last_error(e.to_string());
                code
            }
        }
    }))
    .unwrap_or_else(|panic| {
        let msg = format_panic(&panic);
        error::set_last_error(msg);
        FFI_ERR_PANIC
    })
}

/// Get the schema of the scan output.
///
/// # Arguments
/// * `handle` - Reader handle
/// * `schema_out` - Output: Arrow C schema
///
/// # Returns
/// FFI_OK on success, negative on error.
#[no_mangle]
pub extern "C" fn lance_reader_get_schema(
    handle: LanceReaderHandle,
    schema_out: *mut FFI_ArrowSchema,
) -> i32 {
    error::clear_last_error();

    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if handle.is_null() || schema_out.is_null() {
            error::set_last_error("handle and schema_out must not be null".to_string());
            return FFI_ERR_INVALID_ARG;
        }

        let reader = unsafe { &*handle };
        let schema = reader.schema();

        match arrow::ffi::FFI_ArrowSchema::try_from(schema.as_ref()) {
            Ok(ffi_schema) => {
                unsafe {
                    ptr::write(schema_out, ffi_schema);
                }
                FFI_OK
            }
            Err(e) => {
                error::set_last_error(format!("Schema export failed: {}", e));
                FFI_ERR_ARROW
            }
        }
    }))
    .unwrap_or_else(|panic| {
        let msg = format_panic(&panic);
        error::set_last_error(msg);
        FFI_ERR_PANIC
    })
}

/// Close the reader and free all resources.
///
/// Safe to call with a null handle (no-op).
/// After this call, the handle must not be used again.
#[no_mangle]
pub extern "C" fn lance_reader_close(handle: LanceReaderHandle) {
    if handle.is_null() {
        return;
    }
    // catch_unwind to prevent panic on drop from crossing FFI
    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(handle));
    }));
}

/// Retrieve the last error message.
///
/// Copies the error string into the provided buffer. Returns the number of
/// bytes written (excluding null terminator), or 0 if no error is stored.
///
/// # Arguments
/// * `buf` - Buffer to write the error message into
/// * `buf_len` - Size of the buffer
#[no_mangle]
pub extern "C" fn lance_reader_last_error(buf: *mut u8, buf_len: usize) -> usize {
    if buf.is_null() || buf_len == 0 {
        return 0;
    }
    let slice = unsafe { std::slice::from_raw_parts_mut(buf, buf_len) };
    error::get_last_error(slice)
}

/// Create a small test Lance dataset at the given path.
/// The dataset has 3 columns: id (INT32), name (UTF8), score (FLOAT64)
/// with 5 rows. Used by C++ GTests for end-to-end testing.
///
/// Returns FFI_OK on success, negative on error.
#[no_mangle]
pub extern "C" fn lance_test_create_dataset(path_ptr: *const u8, path_len: usize) -> i32 {
    error::clear_last_error();

    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if path_ptr.is_null() {
            error::set_last_error("path must not be null".to_string());
            return FFI_ERR_INVALID_ARG;
        }

        let path = unsafe {
            let slice = std::slice::from_raw_parts(path_ptr, path_len);
            match std::str::from_utf8(slice) {
                Ok(s) => s,
                Err(e) => {
                    error::set_last_error(format!("Invalid UTF-8 in path: {}", e));
                    return FFI_ERR_INVALID_ARG;
                }
            }
        };

        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                error::set_last_error(format!("Failed to create runtime: {}", e));
                return error::FFI_ERR_IO;
            }
        };

        let result = rt.block_on(async {
            use arrow::array::{Float64Array, Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use std::sync::Arc;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("score", DataType::Float64, false),
            ]));

            let batch = arrow::array::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(StringArray::from(vec![
                        "alice", "bob", "carol", "dave", "eve",
                    ])),
                    Arc::new(Float64Array::from(vec![90.5, 85.0, 92.3, 78.1, 88.7])),
                ],
            )
            .map_err(|e| crate::error::FfiError::Arrow(e.to_string()))?;

            let batches = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
            let write_result: Result<lance::Dataset, lance::Error> =
                lance::Dataset::write(batches, path, None::<lance::dataset::WriteParams>).await;
            write_result.map_err(|e| crate::error::FfiError::Lance(e.to_string()))?;

            Ok::<_, crate::error::FfiError>(())
        });

        match result {
            Ok(()) => FFI_OK,
            Err(e) => {
                let code = e.status_code();
                error::set_last_error(e.to_string());
                code
            }
        }
    }))
    .unwrap_or_else(|panic| {
        let msg = format_panic(&panic);
        error::set_last_error(msg);
        FFI_ERR_PANIC
    })
}

fn format_panic(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        format!("Rust panic: {}", s)
    } else if let Some(s) = panic.downcast_ref::<String>() {
        format!("Rust panic: {}", s)
    } else {
        "Rust panic: unknown".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Helper: create a small Lance dataset and return the URI.
    fn create_test_dataset(dir: &std::path::Path) -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
            ],
        )
        .unwrap();

        let uri = dir.join("ffi_test.lance").to_string_lossy().to_string();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let batches = arrow_array::RecordBatchIterator::new(vec![Ok(batch)], schema);
            lance::Dataset::write(batches, &uri, None::<lance::dataset::WriteParams>)
                .await
                .unwrap();
        });

        uri
    }

    #[test]
    fn test_ffi_open_null_uri() {
        let mut handle: LanceReaderHandle = ptr::null_mut();
        let rc = lance_reader_open(
            ptr::null(),
            0,
            ptr::null(),
            ptr::null(),
            0,
            1024,
            &mut handle,
        );
        assert_eq!(rc, FFI_ERR_INVALID_ARG);

        // Verify error message is set
        let mut buf = [0u8; 256];
        let len = lance_reader_last_error(buf.as_mut_ptr(), buf.len());
        assert!(len > 0);
        let msg = std::str::from_utf8(&buf[..len]).unwrap();
        assert!(msg.contains("null"));
    }

    #[test]
    fn test_ffi_open_null_handle_out() {
        let uri = b"/some/path";
        let rc = lance_reader_open(
            uri.as_ptr(),
            uri.len(),
            ptr::null(),
            ptr::null(),
            0,
            1024,
            ptr::null_mut(),
        );
        assert_eq!(rc, FFI_ERR_INVALID_ARG);
    }

    #[test]
    fn test_ffi_open_nonexistent_path() {
        let uri = b"/nonexistent/dataset.lance";
        let mut handle: LanceReaderHandle = ptr::null_mut();
        let rc = lance_reader_open(
            uri.as_ptr(),
            uri.len(),
            ptr::null(),
            ptr::null(),
            0,
            1024,
            &mut handle,
        );
        assert!(rc < 0, "Expected error, got {}", rc);
        assert!(handle.is_null());
    }

    #[test]
    fn test_ffi_close_null_handle() {
        // Should not crash
        lance_reader_close(ptr::null_mut());
    }

    #[test]
    fn test_ffi_last_error_null_buf() {
        let len = lance_reader_last_error(ptr::null_mut(), 0);
        assert_eq!(len, 0);
    }

    #[test]
    fn test_ffi_next_batch_null_handle() {
        let mut schema = FFI_ArrowSchema::empty();
        let mut array = FFI_ArrowArray::empty();
        let mut eof = false;
        let mut bytes: i64 = 0;

        let rc = lance_reader_next_batch(
            ptr::null_mut(),
            &mut schema,
            &mut array,
            &mut eof,
            &mut bytes,
        );
        assert_eq!(rc, FFI_ERR_INVALID_ARG);
    }

    #[test]
    fn test_ffi_get_schema_null_handle() {
        let mut schema = FFI_ArrowSchema::empty();
        let rc = lance_reader_get_schema(ptr::null_mut(), &mut schema);
        assert_eq!(rc, FFI_ERR_INVALID_ARG);
    }

    #[test]
    fn test_ffi_full_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        // Open
        let mut handle: LanceReaderHandle = ptr::null_mut();
        let rc = lance_reader_open(
            uri.as_ptr(),
            uri.len(),
            ptr::null(),
            ptr::null(),
            0,
            1024,
            &mut handle,
        );
        assert_eq!(rc, FFI_OK, "open failed");
        assert!(!handle.is_null());

        // Get schema
        let mut schema = FFI_ArrowSchema::empty();
        let rc = lance_reader_get_schema(handle, &mut schema);
        assert_eq!(rc, FFI_OK, "get_schema failed");

        // Import and verify schema
        let imported_schema = arrow::datatypes::Schema::try_from(&schema).unwrap();
        assert_eq!(imported_schema.fields().len(), 3);
        assert_eq!(imported_schema.field(0).name(), "id");
        assert_eq!(imported_schema.field(1).name(), "name");
        assert_eq!(imported_schema.field(2).name(), "value");

        // Read first batch
        let mut out_schema = FFI_ArrowSchema::empty();
        let mut out_array = FFI_ArrowArray::empty();
        let mut eof = false;
        let mut bytes: i64 = 0;

        let rc = lance_reader_next_batch(
            handle,
            &mut out_schema,
            &mut out_array,
            &mut eof,
            &mut bytes,
        );
        assert_eq!(rc, FFI_OK, "next_batch failed");
        assert!(!eof);
        assert!(bytes > 0, "bytes should be positive");

        // Import and verify batch data via Arrow C Data Interface
        let imported_array = unsafe { arrow::ffi::from_ffi(out_array, &out_schema) }.unwrap();
        let struct_array = arrow::array::StructArray::from(imported_array);
        let record_batch = arrow::array::RecordBatch::from(struct_array);
        assert_eq!(record_batch.num_rows(), 3);
        assert_eq!(record_batch.num_columns(), 3);

        let ids = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[10, 20, 30]);

        // Read next batch — should be EOF
        let mut out_schema2 = FFI_ArrowSchema::empty();
        let mut out_array2 = FFI_ArrowArray::empty();
        let mut eof2 = false;
        let mut bytes2: i64 = 0;
        let rc = lance_reader_next_batch(
            handle,
            &mut out_schema2,
            &mut out_array2,
            &mut eof2,
            &mut bytes2,
        );
        assert_eq!(rc, FFI_EOF);
        assert!(eof2);

        // Close
        lance_reader_close(handle);
    }

    #[test]
    fn test_ffi_open_with_column_projection() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        let col_name = b"name";
        let col_ptrs = [col_name.as_ptr()];
        let col_lens = [col_name.len()];

        let mut handle: LanceReaderHandle = ptr::null_mut();
        let rc = lance_reader_open(
            uri.as_ptr(),
            uri.len(),
            col_ptrs.as_ptr(),
            col_lens.as_ptr(),
            1,
            1024,
            &mut handle,
        );
        assert_eq!(rc, FFI_OK);
        assert!(!handle.is_null());

        // Verify schema has only 1 column
        let mut schema = FFI_ArrowSchema::empty();
        let rc = lance_reader_get_schema(handle, &mut schema);
        assert_eq!(rc, FFI_OK);

        let imported_schema = arrow::datatypes::Schema::try_from(&schema).unwrap();
        assert_eq!(imported_schema.fields().len(), 1);
        assert_eq!(imported_schema.field(0).name(), "name");

        lance_reader_close(handle);
    }

    #[test]
    fn test_ffi_echo() {
        assert_eq!(crate::rust_echo(42), 42);
        assert_eq!(crate::rust_echo(0), 0);
        assert_eq!(crate::rust_echo(-999), -999);
    }

    #[test]
    fn test_ffi_open_json_config() {
        let tmp = tempfile::tempdir().unwrap();
        let uri = create_test_dataset(tmp.path());

        let config = serde_json::json!({
            "uri": uri,
            "columns": ["name", "value"],
            "batch_size": 1024,
            "version": 0,
            "storage_options": {}
        });
        let config_str = config.to_string();

        let mut handle: LanceReaderHandle = ptr::null_mut();
        let rc = lance_reader_open_json(config_str.as_ptr(), config_str.len(), &mut handle);
        assert_eq!(rc, FFI_OK, "open_json failed: {}", {
            let mut buf = [0u8; 256];
            let len = lance_reader_last_error(buf.as_mut_ptr(), buf.len());
            std::str::from_utf8(&buf[..len]).unwrap_or("?").to_string()
        });
        assert!(!handle.is_null());

        // Verify schema has 2 projected columns
        let mut schema = FFI_ArrowSchema::empty();
        let rc = lance_reader_get_schema(handle, &mut schema);
        assert_eq!(rc, FFI_OK);
        let imported = arrow::datatypes::Schema::try_from(&schema).unwrap();
        assert_eq!(imported.fields().len(), 2);
        assert_eq!(imported.field(0).name(), "name");
        assert_eq!(imported.field(1).name(), "value");

        lance_reader_close(handle);
    }

    #[test]
    fn test_ffi_open_json_invalid() {
        let mut handle: LanceReaderHandle = ptr::null_mut();
        let bad_json = b"not valid json";
        let rc = lance_reader_open_json(bad_json.as_ptr(), bad_json.len(), &mut handle);
        assert_eq!(rc, FFI_ERR_INVALID_ARG);
    }
}

/// Create a multi-fragment test Lance dataset at the given path.
/// Writes 3 separate batches as 3 fragments, 5 rows each = 15 total rows.
/// Used by regression tests to verify fragment-level parallelism.
#[no_mangle]
pub extern "C" fn lance_test_create_multi_fragment_dataset(
    path_ptr: *const u8,
    path_len: usize,
) -> i32 {
    error::clear_last_error();

    std::panic::catch_unwind(AssertUnwindSafe(|| {
        if path_ptr.is_null() {
            error::set_last_error("path must not be null".to_string());
            return FFI_ERR_INVALID_ARG;
        }

        let path = unsafe {
            let slice = std::slice::from_raw_parts(path_ptr, path_len);
            match std::str::from_utf8(slice) {
                Ok(s) => s,
                Err(e) => {
                    error::set_last_error(format!("Invalid UTF-8: {}", e));
                    return FFI_ERR_INVALID_ARG;
                }
            }
        };

        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                error::set_last_error(format!("Runtime: {}", e));
                return error::FFI_ERR_IO;
            }
        };

        let result = rt.block_on(async {
            use arrow::array::{Float64Array, Int32Array, StringArray};
            use arrow::datatypes::{DataType, Field, Schema};
            use std::sync::Arc;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
            ]));

            // Fragment 1: ids 1-5
            let batch1 = arrow::array::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
                    Arc::new(StringArray::from(vec!["a1", "a2", "a3", "a4", "a5"])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
                ],
            )
            .map_err(|e| crate::error::FfiError::Arrow(e.to_string()))?;

            // Write first fragment (create dataset)
            let batches = arrow_array::RecordBatchIterator::new(vec![Ok(batch1)], schema.clone());
            let write_result: Result<lance::Dataset, lance::Error> =
                lance::Dataset::write(batches, path, None::<lance::dataset::WriteParams>).await;
            write_result.map_err(|e| crate::error::FfiError::Lance(e.to_string()))?;

            // Fragment 2: ids 6-10
            let batch2 = arrow::array::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])),
                    Arc::new(StringArray::from(vec!["b1", "b2", "b3", "b4", "b5"])),
                    Arc::new(Float64Array::from(vec![6.0, 7.0, 8.0, 9.0, 10.0])),
                ],
            )
            .map_err(|e| crate::error::FfiError::Arrow(e.to_string()))?;

            let batches = arrow_array::RecordBatchIterator::new(vec![Ok(batch2)], schema.clone());
            let mut params = lance::dataset::WriteParams::default();
            params.mode = lance::dataset::WriteMode::Append;
            let write_result: Result<lance::Dataset, lance::Error> =
                lance::Dataset::write(batches, path, Some(params)).await;
            write_result.map_err(|e| crate::error::FfiError::Lance(e.to_string()))?;

            // Fragment 3: ids 11-15
            let batch3 = arrow::array::RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![11, 12, 13, 14, 15])),
                    Arc::new(StringArray::from(vec!["c1", "c2", "c3", "c4", "c5"])),
                    Arc::new(Float64Array::from(vec![11.0, 12.0, 13.0, 14.0, 15.0])),
                ],
            )
            .map_err(|e| crate::error::FfiError::Arrow(e.to_string()))?;

            let batches = arrow_array::RecordBatchIterator::new(vec![Ok(batch3)], schema);
            let mut params = lance::dataset::WriteParams::default();
            params.mode = lance::dataset::WriteMode::Append;
            let write_result: Result<lance::Dataset, lance::Error> =
                lance::Dataset::write(batches, path, Some(params)).await;
            write_result.map_err(|e| crate::error::FfiError::Lance(e.to_string()))?;

            Ok::<_, crate::error::FfiError>(())
        });

        match result {
            Ok(()) => FFI_OK,
            Err(e) => {
                let code = e.status_code();
                error::set_last_error(e.to_string());
                code
            }
        }
    }))
    .unwrap_or_else(|panic| {
        let msg = format_panic(&panic);
        error::set_last_error(msg);
        FFI_ERR_PANIC
    })
}
