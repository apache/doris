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

#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

// Single auditable seam for "resize a decode buffer that is about to be fully
// overwritten". Collapses the scattered `assign(n, 0)` / `resize(n)` calls in the
// PFOR/zstd decode paths into one intent-revealing entry point and removes the
// redundant zero-fill on warm-reused buffers.
namespace snii {

// Resize a vector of trivially-copyable T to `n` without any zero-fill beyond what
// std::vector itself mandates.
//
// CONTRACT: the caller MUST fully overwrite [0, n) before reading any element. The
// PFOR (`pfor_decode`/`bitunpack`, including the w==0 memset and exception patch
// paths) and zstd (`ZSTD_decompress` + length check) decoders all satisfy this.
//
// For a warm-reused buffer whose current size() >= n this performs NO
// value-initialization (resize shrinks in place). For a cold/grown buffer
// std::vector still value-initializes the new tail -- that is unavoidable for
// std::vector and is intentional here. Do NOT use on any buffer that may be read
// before being fully overwritten.
template <class T>
inline void resize_uninitialized(std::vector<T>& v, std::size_t n) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "resize_uninitialized requires a trivially-copyable element type");
    v.resize(n);
}

// Allocator that default-initializes (instead of value-initializes) on the no-arg
// construct path: for trivial T this skips zeroing entirely. Use only for buffers
// that are fully overwritten before any read.
template <class T>
struct default_init_allocator : std::allocator<T> {
    template <class U>
    struct rebind {
        using other = default_init_allocator<U>;
    };
    using std::allocator<T>::allocator;

    template <class U, class... Args>
    void construct(U* p, Args&&... args) {
        if constexpr (sizeof...(Args) == 0) {
            ::new (static_cast<void*>(p)) U; // default-init: no zeroing for trivial U
        } else {
            ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
        }
    }
};

// A vector whose grow path default-initializes, so even a cold grow avoids the
// zero-fill that std::vector<T> would perform for trivial T.
template <class T>
using uninitialized_vector = std::vector<T, default_init_allocator<T>>;

template <class T>
inline void resize_uninitialized(uninitialized_vector<T>& v, std::size_t n) {
    v.resize(n);
}

} // namespace snii
