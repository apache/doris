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

// FFI functions necessarily work with raw pointers
#![allow(clippy::not_unsafe_ptr_arg_deref)]

pub mod error;
pub mod ffi;
pub mod lance_reader;

use std::panic::AssertUnwindSafe;

/// Trivial FFI round-trip function for build verification (Phase 0).
/// Returns the input value unchanged.
#[no_mangle]
pub extern "C" fn rust_echo(x: i32) -> i32 {
    std::panic::catch_unwind(AssertUnwindSafe(|| x)).unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_echo() {
        assert_eq!(rust_echo(42), 42);
        assert_eq!(rust_echo(0), 0);
        assert_eq!(rust_echo(-1), -1);
    }
}
