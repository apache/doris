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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/MultiVersion.h
// and modified by Doris

#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>

/** Allow to store and read-only usage of an object in several threads,
  *  and to atomically replace an object in another thread.
  * The replacement is atomic and reading threads can work with different versions of an object.
  *
  * Usage:
  *  MultiVersion<T> x;
  * - on data update:
  *  x.set(new value);
  * - on read-only usage:
  * {
  *     MultiVersion<T>::Version current_version = x.get();
  *     // use *current_version
  * }   // now we finish own current version; if the version is outdated and no one else is using it - it will be destroyed.
  *
  * All methods are thread-safe.
  */
template <typename T>
class MultiVersion {
public:
    /// Version of object for usage. shared_ptr manage lifetime of version.
    using Version = std::shared_ptr<const T>;

    /// Default initialization - by nullptr.
    MultiVersion() = default;

    explicit MultiVersion(std::unique_ptr<const T>&& value)
            : current_version(Version {std::move(value)}) {}

    // Use a shared_mutex (read-write lock) rather than the C++20
    // std::atomic<std::shared_ptr<T>> specialization or the deprecated free
    // atomic_load/atomic_store overloads — neither is portable across the
    // toolchains we build with (libc++ in LLVM 20 lacks the atomic<shared_ptr>
    // specialization; libstdc++ 12+ marks the free functions deprecated).
    // MultiVersion is only used on low-frequency paths (metrics scrape,
    // debug strings, symbol resolution), so the lock overhead is negligible.

    /// Obtain current version for read-only usage. Returns shared_ptr, that manages lifetime of version.
    Version get() const {
        std::shared_lock lock(mutex);
        return current_version;
    }

    /// Update an object with new version.
    void set(std::unique_ptr<const T>&& value) {
        std::unique_lock lock(mutex);
        current_version = Version {std::move(value)};
    }

private:
    mutable std::shared_mutex mutex;
    Version current_version;
};
