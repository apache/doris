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
#include <cstdint>
#include <hll.hpp>
#include <memory>
#include <string_view>

#include "core/custom_allocator.h"
#include "core/string_ref.h"

namespace doris::datasketches_hll {

struct AllocationHeader;
void* allocate(size_t bytes);
void deallocate(void* ptr) noexcept;

namespace detail {
class AllocationOwner {
public:
    AllocationOwner() = default;
    ~AllocationOwner();
    AllocationOwner(const AllocationOwner&) = delete;
    AllocationOwner& operator=(const AllocationOwner&) = delete;
    void clear() noexcept;
    void release() noexcept;

private:
    friend void* datasketches_hll::allocate(size_t bytes);
    friend void datasketches_hll::deallocate(void* ptr) noexcept;
    AllocationHeader* _head = nullptr;
};
} // namespace detail

// DataSketches uses allocator.allocate() + placement new in its factories. When a constructor
// or a later merge throws, some allocations never reach an owning sketch. Declare this scope
// before local sketches, and destroy/reset any existing union before an exception leaves it.
// Normal destructors run first; the scope then frees only the allocations they did not release.
class AllocationScope {
public:
    AllocationScope();
    ~AllocationScope();

    AllocationScope(const AllocationScope&) = delete;
    AllocationScope& operator=(const AllocationScope&) = delete;

private:
    detail::AllocationOwner _owner;
    detail::AllocationOwner* _previous;
    int _exceptions;
};

// Stateless across copies and rebinds. The current call's scope records the allocation, so a
// union can copy registers from a sketch created in an earlier scope or on another thread.
template <typename T>
class SketchAllocator {
public:
    using value_type = T;
    SketchAllocator() = default;
    template <typename U>
    SketchAllocator(const SketchAllocator<U>&) noexcept {}

    T* allocate(size_t count) {
        static_assert(alignof(T) <= alignof(std::max_align_t));
        return static_cast<T*>(datasketches_hll::allocate(count * sizeof(T)));
    }
    void deallocate(T* ptr, size_t) noexcept { datasketches_hll::deallocate(ptr); }

    template <typename U>
    bool operator==(const SketchAllocator<U>&) const noexcept {
        return true;
    }
    template <typename U>
    bool operator!=(const SketchAllocator<U>&) const noexcept {
        return false;
    }
};

// Shared by the DataSketches HLL aggregate and scalar functions so that both track memory through
// Doris allocators and report broken input the same way.
using Alloc = SketchAllocator<uint8_t>;
using Sketch = datasketches::hll_sketch_alloc<Alloc>;

// The library can free its old gadget during LIST/SET promotion before installing the new
// pointer. If a subsequent allocation fails, that union is not safe to destroy. Keep its
// allocations under Doris ownership and discard the failed state without invoking that
// invalid library destructor. The public sketch format and estimates remain unchanged.
class Union {
public:
    explicit Union(uint8_t lg_k, const Alloc& allocator = Alloc());
    ~Union();
    Union(Union&&) noexcept;
    Union& operator=(Union&&) noexcept;
    Union(const Union&);
    Union& operator=(const Union&);

    void update(const Sketch& sketch);
    Sketch get_result(datasketches::target_hll_type type = datasketches::HLL_4) const;
    double get_estimate() const;
    double get_lower_bound(uint8_t std_devs) const;
    double get_upper_bound(uint8_t std_devs) const;
    uint8_t get_lg_config_k() const;
    static double get_rel_err(bool upper, bool unioned, uint8_t lg_k, uint8_t std_devs);

private:
    struct Impl;
    struct Deleter {
        void operator()(Impl* ptr) const noexcept;
    };
    std::unique_ptr<Impl, Deleter> _impl;
};

// Deserializes a serialized DataSketches HLL sketch. `action` names the caller in the error
// message, e.g. "add" or "read".
//
// Empty bytes are rejected before the library reads them. A doris::Exception keeps its code, so
// MEM_ALLOC_FAILED raised by the allocator is not reported as corrupted data; any other failure of
// the library means the bytes are not a valid sketch and becomes CORRUPTION.
Sketch deserialize(const StringRef& value, std::string_view action);

} // namespace doris::datasketches_hll
