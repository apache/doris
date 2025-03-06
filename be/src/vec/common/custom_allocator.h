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

#include "vec/common/allocator.h"
#include "vec/common/allocator_fwd.h"

template <class T, typename MemoryAllocator = Allocator<true>>
class CustomStdAllocator;

template <typename T>
using DorisVector = std::vector<T, CustomStdAllocator<T>>;

template <class Key, class T, class Compare = std::less<Key>,
          class Allocator = CustomStdAllocator<std::pair<const Key, T>>>
using DorisMap = std::map<Key, T, Compare, Allocator>;

// NOTE: Even CustomStdAllocator 's allocate/dallocate could modify memory tracker,but it's still stateless,
// because threadcontext owns the memtracker, not CustomStdAllocator.
template <class T, typename MemoryAllocator>
class CustomStdAllocator : private MemoryAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    CustomStdAllocator() noexcept = default;

    template <class U>
    struct rebind {
        typedef CustomStdAllocator<U> other;
    };

    template <class Up>
    CustomStdAllocator(const CustomStdAllocator<Up>&) noexcept {}

    T* allocate(size_t n) { return static_cast<T*>(MemoryAllocator::alloc(n * sizeof(T))); }

    void deallocate(T* ptr, size_t n) noexcept { MemoryAllocator::free((void*)ptr, n * sizeof(T)); }

    size_t max_size() const noexcept { return size_t(~0) / sizeof(T); }

    T* allocate(size_t n, const void*) { return allocate(n); }

    template <class Up, class... Args>
    void construct(Up* p, Args&&... args) {
        ::new ((void*)p) Up(std::forward<Args>(args)...);
    }

    void destroy(T* p) { p->~T(); }

    T* address(T& t) const noexcept { return std::addressof(t); }

    T* address(const T& t) const noexcept { return std::addressof(t); }
};

template <class T, class Up>
bool operator==(const CustomStdAllocator<T>&, const CustomStdAllocator<Up>&) {
    return true;
}

template <class T, class Up>
bool operator!=(const CustomStdAllocator<T>&, const CustomStdAllocator<Up>&) {
    return false;
}