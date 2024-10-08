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

#include <parallel_hashmap/phmap.h> // IWYU pragma: export

#include "vec/common/allocator.h"

namespace doris::vectorized {

/// `Allocator_` implements several interfaces of `std::allocator`
/// which `phmap::flat_hash_map` will use.
template <typename T>
class Allocator_ : private Allocator<true, false, false, DefaultMemoryAllocator> {
public:
    using value_type = T;
    using pointer = T*;

    Allocator_() = default;

    template <typename T_>
    Allocator_(const Allocator_<T_>&) {};

    constexpr T* allocate(size_t n) { return static_cast<T*>(Allocator::alloc(n * sizeof(T))); }

    void deallocate(pointer p, size_t n) { Allocator::free(p, n * sizeof(T)); }

    friend bool operator==(const Allocator_&, const Allocator_&) { return true; }
};

template <typename K, typename V, typename Hash = phmap::Hash<K>, typename Eq = phmap::EqualTo<K>,
          typename Alloc = Allocator_<phmap::Pair<const K, V>>>
using flat_hash_map = phmap::flat_hash_map<K, V, Hash, Eq, Alloc>;

template <typename K, typename Hash = phmap::Hash<K>, typename Eq = phmap::EqualTo<K>,
          typename Alloc = Allocator_<K>>
using flat_hash_set = phmap::flat_hash_set<K, Hash, Eq, Alloc>;

} // namespace doris::vectorized
