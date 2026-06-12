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

use std::cell::RefCell;
use std::fmt;

/// FFI status codes returned to C++.
pub const FFI_OK: i32 = 0;
pub const FFI_EOF: i32 = 1;
pub const FFI_ERR_LANCE: i32 = -1;
pub const FFI_ERR_ARROW: i32 = -2;
pub const FFI_ERR_IO: i32 = -3;
pub const FFI_ERR_PANIC: i32 = -4;
pub const FFI_ERR_INVALID_ARG: i32 = -5;

/// Errors that can occur at the FFI boundary.
#[derive(Debug)]
pub enum FfiError {
    Lance(String),
    Arrow(String),
    Io(String),
    InvalidArg(String),
}

impl fmt::Display for FfiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FfiError::Lance(msg) => write!(f, "Lance error: {}", msg),
            FfiError::Arrow(msg) => write!(f, "Arrow error: {}", msg),
            FfiError::Io(msg) => write!(f, "IO error: {}", msg),
            FfiError::InvalidArg(msg) => write!(f, "Invalid argument: {}", msg),
        }
    }
}

impl From<lance::Error> for FfiError {
    fn from(e: lance::Error) -> Self {
        FfiError::Lance(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for FfiError {
    fn from(e: arrow::error::ArrowError) -> Self {
        FfiError::Arrow(e.to_string())
    }
}

impl From<std::io::Error> for FfiError {
    fn from(e: std::io::Error) -> Self {
        FfiError::Io(e.to_string())
    }
}

impl FfiError {
    pub fn status_code(&self) -> i32 {
        match self {
            FfiError::Lance(_) => FFI_ERR_LANCE,
            FfiError::Arrow(_) => FFI_ERR_ARROW,
            FfiError::Io(_) => FFI_ERR_IO,
            FfiError::InvalidArg(_) => FFI_ERR_INVALID_ARG,
        }
    }
}

pub type FfiResult<T> = Result<T, FfiError>;

// Thread-local storage for the last error message.
// The C++ side retrieves this via lance_reader_last_error() only on error paths.
thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Store an error message in thread-local storage.
pub fn set_last_error(msg: String) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(msg);
    });
}

/// Clear the last error.
pub fn clear_last_error() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

/// Copy the last error message into the provided buffer.
/// Returns the number of bytes written (excluding null terminator),
/// or 0 if no error is stored.
pub fn get_last_error(buf: &mut [u8]) -> usize {
    LAST_ERROR.with(|e| {
        let borrowed = e.borrow();
        match borrowed.as_ref() {
            Some(msg) => {
                let bytes = msg.as_bytes();
                let copy_len = bytes.len().min(buf.len().saturating_sub(1));
                if copy_len > 0 {
                    buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
                }
                if buf.len() > copy_len {
                    buf[copy_len] = 0; // null terminator
                }
                copy_len
            }
            None => 0,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get_last_error() {
        clear_last_error();
        set_last_error("test error message".to_string());

        let mut buf = [0u8; 256];
        let len = get_last_error(&mut buf);
        assert_eq!(len, 18);
        assert_eq!(&buf[..len], b"test error message");
        assert_eq!(buf[len], 0); // null terminator
    }

    #[test]
    fn test_clear_last_error() {
        set_last_error("some error".to_string());
        clear_last_error();

        let mut buf = [0u8; 64];
        let len = get_last_error(&mut buf);
        assert_eq!(len, 0);
    }

    #[test]
    fn test_get_last_error_truncation() {
        set_last_error("a]long error message that exceeds buffer".to_string());

        let mut buf = [0u8; 10];
        let len = get_last_error(&mut buf);
        // Should copy at most buf_len - 1 = 9 bytes
        assert_eq!(len, 9);
        assert_eq!(&buf[..9], b"a]long er");
        assert_eq!(buf[9], 0); // null terminator
    }

    #[test]
    fn test_get_last_error_empty_buffer() {
        set_last_error("error".to_string());
        let mut buf = [0u8; 0];
        let len = get_last_error(&mut buf);
        assert_eq!(len, 0);
    }

    #[test]
    fn test_error_status_codes() {
        assert_eq!(FfiError::Lance("x".into()).status_code(), FFI_ERR_LANCE);
        assert_eq!(FfiError::Arrow("x".into()).status_code(), FFI_ERR_ARROW);
        assert_eq!(FfiError::Io("x".into()).status_code(), FFI_ERR_IO);
        assert_eq!(
            FfiError::InvalidArg("x".into()).status_code(),
            FFI_ERR_INVALID_ARG
        );
    }

    #[test]
    fn test_error_display() {
        let e = FfiError::Lance("dataset not found".into());
        assert_eq!(e.to_string(), "Lance error: dataset not found");

        let e = FfiError::Arrow("schema mismatch".into());
        assert_eq!(e.to_string(), "Arrow error: schema mismatch");
    }
}
