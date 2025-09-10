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
#include <atomic>
#include <memory>

namespace doris {
#ifndef USE_LIBCPP
template <typename T>
using atomic_shared_ptr = std::atomic<std::shared_ptr<T>>;
#else
// libcpp do not support atomic<std::shared_ptr<T>>
// so we implement a simple version of atomic_shared_ptr here
template <typename T>
class atomic_shared_ptr {
public:
    atomic_shared_ptr() noexcept : _ptr(nullptr) {}
    atomic_shared_ptr(std::shared_ptr<T> desired) noexcept : _ptr(desired) {}
    atomic_shared_ptr(const atomic_shared_ptr&) = delete;
    atomic_shared_ptr& operator=(const atomic_shared_ptr&) = delete;

    void store(std::shared_ptr<T> desired,
               std::memory_order order = std::memory_order_seq_cst) noexcept {
        std::atomic_store_explicit(&_ptr, desired, order);
    }

    std::shared_ptr<T> load(std::memory_order order = std::memory_order_seq_cst) const noexcept {
        return std::atomic_load_explicit(&_ptr, order);
    }

private:
    mutable std::shared_ptr<T> _ptr;
};
#endif
} // namespace doris