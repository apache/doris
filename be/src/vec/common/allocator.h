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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/Allocator.h
// and modified by Doris

#pragma once

// TODO: Readable

#include <fmt/format.h>
#if defined(USE_JEMALLOC)
#include <jemalloc/jemalloc.h>
#endif // defined(USE_JEMALLOC)

#ifdef __APPLE__
#include <malloc/malloc.h>
#define GET_MALLOC_SIZE(ptr) malloc_size(ptr)
#else
#include <malloc.h>
#define GET_MALLOC_SIZE(ptr) malloc_usable_size(ptr)
#endif
#include <stdint.h>
#include <string.h>

#include "common/config.h"
#include "common/status.h"
#include "util/sse_util.hpp"

#ifdef NDEBUG
#define ALLOCATOR_ASLR 0
#else
#define ALLOCATOR_ASLR 1
#endif

#include <sys/mman.h>

#include <algorithm>
#include <cstdlib>
#include <string>

#include "common/compiler_util.h" // IWYU pragma: keep
#ifdef THREAD_SANITIZER
/// Thread sanitizer does not intercept mremap. The usage of mremap will lead to false positives.
#define DISABLE_MREMAP 1
#endif
#include "common/exception.h"
#include "vec/common/mremap.h"

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

#ifndef __THROW
#if __cplusplus
#define __THROW noexcept
#else
#define __THROW
#endif
#endif

namespace doris {
static constexpr size_t MMAP_MIN_ALIGNMENT = 4096;
static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

// The memory for __int128 should be aligned to 16 bytes.
// By the way, in 64-bit system, the address of a block returned by malloc or realloc in GNU systems
// is always a multiple of sixteen. (https://www.gnu.org/software/libc/manual/html_node/Aligned-Memory-Blocks.html)
static constexpr int ALLOCATOR_ALIGNMENT_16 = 16;

class MemTrackerLimiter;

class DefaultMemoryAllocator {
public:
    static void* malloc(size_t size) __THROW { return std::malloc(size); }

    static void* calloc(size_t n, size_t size) __THROW { return std::calloc(n, size); }

    static constexpr bool need_record_actual_size() { return false; }

    static int posix_memalign(void** ptr, size_t alignment, size_t size) __THROW {
        return ::posix_memalign(ptr, alignment, size);
    }

    static void* realloc(void* ptr, size_t size) __THROW { return std::realloc(ptr, size); }

    static void free(void* p) __THROW { std::free(p); }

    static void release_unused() {
#if defined(USE_JEMALLOC)
        jemallctl(fmt::format("arena.{}.purge", MALLCTL_ARENAS_ALL).c_str(), nullptr, nullptr,
                  nullptr, 0);
#endif // defined(USE_JEMALLOC)
    }
};

/** It would be better to put these Memory Allocators where they are used, such as in the orc memory pool and arrow memory pool.
  * But currently allocators use templates in .cpp instead of all in .h, so they can only be placed here.
  */
class ORCMemoryAllocator {
public:
    static void* malloc(size_t size) __THROW { return reinterpret_cast<char*>(std::malloc(size)); }

    static void* calloc(size_t n, size_t size) __THROW { return std::calloc(n, size); }

    static constexpr bool need_record_actual_size() { return true; }

    static size_t allocated_size(void* ptr) { return GET_MALLOC_SIZE(ptr); }

    static int posix_memalign(void** ptr, size_t alignment, size_t size) __THROW {
        return ::posix_memalign(ptr, alignment, size);
    }

    static void* realloc(void* ptr, size_t size) __THROW {
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }

    static void free(void* p) __THROW { std::free(p); }

    static void release_unused() {}
};

class RecordSizeMemoryAllocator {
public:
    static void* malloc(size_t size) __THROW {
        void* p = std::malloc(size);
        if (p) {
            std::lock_guard<std::mutex> lock(_mutex);
            _allocated_sizes[p] = size;
        }
        return p;
    }

    static void* calloc(size_t n, size_t size) __THROW {
        void* p = std::calloc(n, size);
        if (p) {
            std::lock_guard<std::mutex> lock(_mutex);
            _allocated_sizes[p] = n * size;
        }
        return p;
    }

    static constexpr bool need_record_actual_size() { return false; }

    static size_t allocated_size(void* ptr) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _allocated_sizes.find(ptr);
        if (it != _allocated_sizes.end()) {
            return it->second;
        }
        return 0;
    }

    static int posix_memalign(void** ptr, size_t alignment, size_t size) __THROW {
        int ret = ::posix_memalign(ptr, alignment, size);
        if (ret == 0 && *ptr) {
            std::lock_guard<std::mutex> lock(_mutex);
            _allocated_sizes[*ptr] = size;
        }
        return ret;
    }

    static void* realloc(void* ptr, size_t size) __THROW {
        std::lock_guard<std::mutex> lock(_mutex);

        auto it = _allocated_sizes.find(ptr);
        if (it != _allocated_sizes.end()) {
            _allocated_sizes.erase(it);
        }

        void* p = std::realloc(ptr, size);

        if (p) {
            _allocated_sizes[p] = size;
        }

        return p;
    }

    static void free(void* p) __THROW {
        if (p) {
            std::lock_guard<std::mutex> lock(_mutex);
            _allocated_sizes.erase(p);
            std::free(p);
        }
    }

    static void release_unused() {}

private:
    static std::unordered_map<void*, size_t> _allocated_sizes;
    static std::mutex _mutex;
};

/** Responsible for allocating / freeing memory. Used, for example, in PODArray, Arena.
  * Also used in hash tables.
  * The interface is different from std::allocator
  * - the presence of the method realloc, which for large chunks of memory uses mremap;
  * - passing the size into the `free` method;
  * - by the presence of the `alignment` argument;
  * - the possibility of zeroing memory (used in hash tables);
  * - random hint address for mmap
  * - mmap_threshold for using mmap less or more
  */
template <bool clear_memory_, bool mmap_populate, bool use_mmap, typename MemoryAllocator,
          bool check_and_tracking_memory>
class Allocator {
public:
    // Allocate memory range.
    void* alloc(size_t size, size_t alignment = 0);

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void* realloc(void* buf, size_t old_size, size_t new_size, size_t alignment = 0);

    // Free memory range.
    void free(void* buf, size_t size);

    void release_unused() { MemoryAllocator::release_unused(); }

    bool sys_memory_exceed(size_t size, std::string* err_msg) const;

    bool memory_tracker_exceed(size_t size, std::string* err_msg) const;

    static constexpr bool need_check_and_tracking_memory() { return check_and_tracking_memory; }

protected:
    static constexpr size_t get_stack_threshold() { return 0; }

    static constexpr bool clear_memory = clear_memory_;

private:
    void sys_memory_check(size_t size) const;
    void memory_tracker_check(size_t size) const;
    // If sys memory or tracker exceeds the limit, but there is no external catch bad_alloc,
    // alloc will continue to execute, so the consume memtracker is forced.
    void memory_check(size_t size) const;
    void alloc_fault_probability() const;

    // Increases consumption of this tracker by 'bytes'.
    // some special cases:
    // 1. objects that inherit Allocator will not be shared by multiple queries.
    //  non-compliant: page cache, ORC ByteBuffer.
    // 2. objects that inherit Allocator will only free memory allocated by themselves.
    //  non-compliant: phmap, the memory alloced by an object may be transferred to another object and then free.
    // 3. the memory tracker in TLS is the same during the construction of objects that inherit Allocator
    //  and during subsequent memory allocation.
    void consume_memory(size_t size) const;
    void release_memory(size_t size) const;
    void throw_bad_alloc(const std::string& err) const;
    void add_address_sanitizers(void* buf, size_t size) const;
    void remove_address_sanitizers(void* buf, size_t size) const;

    // Freshly mmapped pages are copy-on-write references to a global zero page.
    // On the first write, a page fault occurs, and an actual writable page is
    // allocated. If we are going to use this memory soon, such as when resizing
    // hash tables, it makes sense to pre-fault the pages by passing
    // MAP_POPULATE to mmap(). This takes some time, but should be faster
    // overall than having a hot loop interrupted by page faults.
    // It is only supported on Linux.
    static constexpr int mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS
#if defined(OS_LINUX)
                                      | (mmap_populate ? MAP_POPULATE : 0)
#endif
            ;
};

/** Allocator with optimization to place small memory ranges in automatic memory.
  */
template <typename Base, size_t N, size_t Alignment>
class AllocatorWithStackMemory : private Base {
private:
    alignas(Alignment) char stack_memory[N];

public:
    /// Do not use boost::noncopyable to avoid the warning about direct base
    /// being inaccessible due to ambiguity, when derived classes are also
    /// noncopiable (-Winaccessible-base).
    AllocatorWithStackMemory(const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory& operator=(const AllocatorWithStackMemory&) = delete;
    AllocatorWithStackMemory() = default;
    ~AllocatorWithStackMemory() = default;

    void* alloc(size_t size) {
        if (size <= N) {
            if constexpr (Base::clear_memory) memset(stack_memory, 0, N);
            return stack_memory;
        }

        return Base::alloc(size, Alignment);
    }

    void free(void* buf, size_t size) {
        if (size > N) Base::free(buf, size);
    }

    void* realloc(void* buf, size_t old_size, size_t new_size) {
        /// Was in stack_memory, will remain there.
        if (new_size <= N) return buf;

        /// Already was big enough to not fit in stack_memory.
        if (old_size > N) return Base::realloc(buf, old_size, new_size, Alignment);

        /// Was in stack memory, but now will not fit there.
        void* new_buf = Base::alloc(new_size, Alignment);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

    bool sys_memory_exceed(size_t size, std::string* err_msg) const {
        return Base::sys_memory_exceed(size, err_msg);
    }

    bool memory_tracker_exceed(size_t size, std::string* err_msg) const {
        return Base::memory_tracker_exceed(size, err_msg);
    }

    static constexpr bool need_check_and_tracking_memory() {
        return Base::need_check_and_tracking_memory();
    }

protected:
    static constexpr size_t get_stack_threshold() { return N; }
};
} // namespace doris