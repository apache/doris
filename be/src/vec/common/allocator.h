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

// TODO: Tracker
// TODO: Readable

#include <fmt/format.h>
#include <string.h>

#include <exception>

#include "common/status.h"

#ifdef NDEBUG
#define ALLOCATOR_ASLR 0
#else
#define ALLOCATOR_ASLR 1
#endif

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#include <malloc.h>
#endif

#include <sys/mman.h>

#include <algorithm>
#include <cstdlib>

#include "common/compiler_util.h"
#ifdef THREAD_SANITIZER
/// Thread sanitizer does not intercept mremap. The usage of mremap will lead to false positives.
#define DISABLE_MREMAP 1
#endif
#include "vec/common/allocator_fwd.h"
#include "vec/common/exception.h"
#include "vec/common/mremap.h"

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

/**
  * Many modern allocators (for example, tcmalloc) do not do a mremap for
  * realloc, even in case of large enough chunks of memory. Although this allows
  * you to increase performance and reduce memory consumption during realloc.
  * To fix this, we do mremap manually if the chunk of memory is large enough.
  * The threshold (64 MB) is chosen quite large, since changing the address
  * space is very slow, especially in the case of a large number of threads. We
  * expect that the set of operations mmap/something to do/mremap can only be
  * performed about 1000 times per second.
  *
  * P.S. This is also required, because tcmalloc can not allocate a chunk of
  * memory greater than 16 GB.
  */
#ifdef NDEBUG
static constexpr size_t MMAP_THRESHOLD = 64 * (1ULL << 20);
#else
/**
      * In debug build, use small mmap threshold to reproduce more memory
      * stomping bugs. Along with ASLR it will hopefully detect more issues than
      * ASan. The program may fail due to the limit on number of memory mappings.
      */
static constexpr size_t MMAP_THRESHOLD = 4096;
#endif

static constexpr size_t MMAP_MIN_ALIGNMENT = 4096;
static constexpr size_t MALLOC_MIN_ALIGNMENT = 8;

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
template <bool clear_memory_, bool mmap_populate>
class Allocator {
public:
    /// Allocate memory range.
    void* alloc(size_t size, size_t alignment = 0) {
        // CurrentMemoryTracker::alloc(size);
        return alloc_no_track(size, alignment);
    }

    /// Free memory range.
    void free(void* buf, size_t size) {
        free_no_track(buf, size);
        // CurrentMemoryTracker::free(size);
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void* realloc(void* buf, size_t old_size, size_t new_size, size_t alignment = 0) {
        if (old_size == new_size) {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        } else if (old_size < MMAP_THRESHOLD && new_size < MMAP_THRESHOLD &&
                   alignment <= MALLOC_MIN_ALIGNMENT) {
            /// Resize malloc'd memory region with no special alignment requirement.
            // CurrentMemoryTracker::realloc(old_size, new_size);

            void* new_buf = ::realloc(buf, new_size);
            if (nullptr == new_buf)
                doris::vectorized::throwFromErrno("Allocator: Cannot realloc from " +
                                                          std::to_string(old_size) + " to " +
                                                          std::to_string(new_size) + ".",
                                                  doris::TStatusCode::VEC_CANNOT_ALLOCATE_MEMORY);

            buf = new_buf;
            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char*>(buf) + old_size, 0, new_size - old_size);
        } else if (old_size >= MMAP_THRESHOLD && new_size >= MMAP_THRESHOLD) {
            /// Resize mmap'd memory region.
            // CurrentMemoryTracker::realloc(old_size, new_size);

            // On apple and freebsd self-implemented mremap used (common/mremap.h)
            buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE, PROT_READ | PROT_WRITE,
                                    mmap_flags, -1, 0);
            if (MAP_FAILED == buf)
                doris::vectorized::throwFromErrno("Allocator: Cannot mremap memory chunk from " +
                                                          std::to_string(old_size) + " to " +
                                                          std::to_string(new_size) + ".",
                                                  doris::TStatusCode::VEC_CANNOT_MREMAP);

            /// No need for zero-fill, because mmap guarantees it.
        } else if (new_size < MMAP_THRESHOLD) {
            /// Small allocs that requires a copy. Assume there's enough memory in system. Call CurrentMemoryTracker once.
            // CurrentMemoryTracker::realloc(old_size, new_size);

            void* new_buf = alloc_no_track(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            free_no_track(buf, old_size);
            buf = new_buf;
        } else {
            /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.

            void* new_buf = alloc(new_size, alignment);
            memcpy(new_buf, buf, std::min(old_size, new_size));
            free(buf, old_size);
            buf = new_buf;
        }

        return buf;
    }

protected:
    static constexpr size_t get_stack_threshold() { return 0; }

    static constexpr bool clear_memory = clear_memory_;

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

private:
    void* alloc_no_track(size_t size, size_t alignment) {
        void* buf;

        if (size >= MMAP_THRESHOLD) {
            if (alignment > MMAP_MIN_ALIGNMENT)
                throw doris::vectorized::Exception(
                        fmt::format(
                                "Too large alignment {}: more than page size when allocating {}.",
                                alignment, size),
                        doris::TStatusCode::VEC_BAD_ARGUMENTS);

            buf = mmap(get_mmap_hint(), size, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
            if (MAP_FAILED == buf)
                doris::vectorized::throwFromErrno(fmt::format("Allocator: Cannot mmap {}.", size),
                                                  doris::TStatusCode::VEC_CANNOT_ALLOCATE_MEMORY);

            /// No need for zero-fill, because mmap guarantees it.
        } else {
            if (alignment <= MALLOC_MIN_ALIGNMENT) {
                if constexpr (clear_memory)
                    buf = ::calloc(size, 1);
                else
                    buf = ::malloc(size);

                if (nullptr == buf)
                    doris::vectorized::throwFromErrno(
                            fmt::format("Allocator: Cannot malloc {}.", size),
                            doris::TStatusCode::VEC_CANNOT_ALLOCATE_MEMORY);
            } else {
                buf = nullptr;
                int res = posix_memalign(&buf, alignment, size);

                if (0 != res)
                    doris::vectorized::throwFromErrno(
                            fmt::format("Cannot allocate memory (posix_memalign) {}.", size),
                            doris::TStatusCode::VEC_CANNOT_ALLOCATE_MEMORY, res);

                if constexpr (clear_memory) memset(buf, 0, size);
            }
        }
        return buf;
    }

    void free_no_track(void* buf, size_t size) {
        if (size >= MMAP_THRESHOLD) {
            if (0 != munmap(buf, size))
                doris::vectorized::throwFromErrno(fmt::format("Allocator: Cannot munmap {}.", size),
                                                  doris::TStatusCode::VEC_CANNOT_MUNMAP);
        } else {
            ::free(buf);
        }
    }

#ifndef NDEBUG
    /// In debug builds, request mmap() at random addresses (a kind of ASLR), to
    /// reproduce more memory stomping bugs. Note that Linux doesn't do it by
    /// default. This may lead to worse TLB performance.
    void* get_mmap_hint() {
        // return reinterpret_cast<void *>(std::uniform_int_distribution<intptr_t>(0x100000000000UL, 0x700000000000UL)(thread_local_rng));
        return nullptr;
    }
#else
    void* get_mmap_hint() { return nullptr; }
#endif
};

/** When using AllocatorWithStackMemory, located on the stack,
  *  GCC 4.9 mistakenly assumes that we can call `free` from a pointer to the stack.
  * In fact, the combination of conditions inside AllocatorWithStackMemory does not allow this.
  */
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfree-nonheap-object"
#endif

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

protected:
    static constexpr size_t get_stack_threshold() { return N; }
};

#if !__clang__
#pragma GCC diagnostic pop
#endif
