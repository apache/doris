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
#include <stdint.h>
#include <string.h>

#include "common/config.h"
#include "common/status.h"
#include "runtime/memory/chunk.h"
#include "runtime/memory/chunk_allocator.h"
#include "util/sse_util.hpp"

#ifdef NDEBUG
#define ALLOCATOR_ASLR 0
#else
#define ALLOCATOR_ASLR 1
#endif

#if !defined(__APPLE__) && !defined(__FreeBSD__)
#else
#define _DARWIN_C_SOURCE
#endif

#include <sys/mman.h>

#include <algorithm>
#include <cstdlib>
#include <string>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
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

/**
 * Memory allocation between 4KB and 64MB will be through ChunkAllocator,
 * those less than 4KB will be through malloc (for example, tcmalloc),
 * and those greater than 64MB will be through MMAP.
 * In the actual test, chunkallocator allocates less than 4KB of memory slower than malloc,
 * and chunkallocator allocates more than 64MB of memory slower than MMAP,
 * but the 4KB threshold is an empirical value, which needs to be determined
 * by more detailed test later.
  */
static constexpr size_t CHUNK_THRESHOLD = 4096;
/**
  * In debug build, use small mmap threshold to reproduce more memory
  * stomping bugs. Along with ASLR it will hopefully detect more issues than
  * ASan. The program may fail due to the limit on number of memory mappings.
  */
static constexpr size_t MMAP_THRESHOLD_DEBUG = 4096; // delete immediately

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
    void sys_memory_check(size_t size) const;
    void memory_tracker_check(size_t size) const;
    // If sys memory or tracker exceeds the limit, but there is no external catch bad_alloc,
    // alloc will continue to execute, so the consume memtracker is forced.
    void memory_check(size_t size) const;
    // Increases consumption of this tracker by 'bytes'.
    void consume_memory(size_t size) const;
    void release_memory(size_t size) const;
    void throw_bad_alloc(const std::string& err) const;

    /// Allocate memory range.
    void* alloc(size_t size, size_t alignment = 0) {
        memory_check(size);
        void* buf;

#ifdef NDEBUG
        if (size >= doris::config::mmap_threshold) {
#else
        if (size >= MMAP_THRESHOLD_DEBUG) {
#endif
            if (alignment > MMAP_MIN_ALIGNMENT)
                throw doris::Exception(
                        doris::ErrorCode::INVALID_ARGUMENT,
                        "Too large alignment {}: more than page size when allocating {}.",
                        alignment, size);

            consume_memory(size);
            buf = mmap(nullptr, size, PROT_READ | PROT_WRITE, mmap_flags, -1, 0);
            if (MAP_FAILED == buf) {
                release_memory(size);
                throw_bad_alloc(fmt::format("Allocator: Cannot mmap {}.", size));
            }

            /// No need for zero-fill, because mmap guarantees it.
        } else if (!doris::config::disable_chunk_allocator_in_vec && size >= CHUNK_THRESHOLD) {
            doris::Chunk chunk;
            if (!doris::ChunkAllocator::instance()->allocate_align(size, &chunk)) {
                throw_bad_alloc(fmt::format("Allocator: Cannot allocate chunk {}.", size));
            }
            buf = chunk.data;
            if constexpr (clear_memory) memset(buf, 0, chunk.size);
        } else {
            if (alignment <= MALLOC_MIN_ALIGNMENT) {
                if constexpr (clear_memory)
                    buf = ::calloc(size, 1);
                else
                    buf = ::malloc(size);

                if (nullptr == buf) {
                    throw_bad_alloc(fmt::format("Allocator: Cannot malloc {}.", size));
                }
            } else {
                buf = nullptr;
                int res = posix_memalign(&buf, alignment, size);

                if (0 != res) {
                    throw_bad_alloc(
                            fmt::format("Cannot allocate memory (posix_memalign) {}.", size));
                }

                if constexpr (clear_memory) memset(buf, 0, size);
            }
        }
        return buf;
    }

    /// Free memory range.
    void free(void* buf, size_t size) {
#ifdef NDEBUG
        if (size >= doris::config::mmap_threshold) {
#else
        if (size >= MMAP_THRESHOLD_DEBUG) {
#endif
            if (0 != munmap(buf, size)) {
                throw_bad_alloc(fmt::format("Allocator: Cannot munmap {}.", size));
            } else {
                release_memory(size);
            }
        } else if (!doris::config::disable_chunk_allocator_in_vec && size >= CHUNK_THRESHOLD &&
                   ((size & (size - 1)) == 0)) {
            // Only power-of-two length are added to ChunkAllocator
            doris::ChunkAllocator::instance()->free((uint8_t*)buf, size);
        } else {
            ::free(buf);
        }
    }

    /** Enlarge memory range.
      * Data from old range is moved to the beginning of new range.
      * Address of memory range could change.
      */
    void* realloc(void* buf, size_t old_size, size_t new_size, size_t alignment = 0) {
        if (old_size == new_size) {
            /// nothing to do.
            /// BTW, it's not possible to change alignment while doing realloc.
        } else if (old_size < CHUNK_THRESHOLD && new_size < CHUNK_THRESHOLD &&
                   alignment <= MALLOC_MIN_ALIGNMENT) {
            memory_check(new_size);
            /// Resize malloc'd memory region with no special alignment requirement.
            void* new_buf = ::realloc(buf, new_size);
            if (nullptr == new_buf) {
                throw_bad_alloc(fmt::format("Allocator: Cannot realloc from {} to {}.", old_size,
                                            new_size));
            }

            buf = new_buf;
            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char*>(buf) + old_size, 0, new_size - old_size);
#ifdef NDEBUG
        } else if (old_size >= doris::config::mmap_threshold &&
                   new_size >= doris::config::mmap_threshold) {
#else
        } else if (old_size >= MMAP_THRESHOLD_DEBUG && new_size >= MMAP_THRESHOLD_DEBUG) {
#endif
            memory_check(new_size);
            /// Resize mmap'd memory region.
            consume_memory(new_size - old_size);
            // On apple and freebsd self-implemented mremap used (common/mremap.h)
            buf = clickhouse_mremap(buf, old_size, new_size, MREMAP_MAYMOVE, PROT_READ | PROT_WRITE,
                                    mmap_flags, -1, 0);
            if (MAP_FAILED == buf) {
                release_memory(new_size - old_size);
                throw_bad_alloc(fmt::format("Allocator: Cannot mremap memory chunk from {} to {}.",
                                            old_size, new_size));
            }

            /// No need for zero-fill, because mmap guarantees it.

            if constexpr (mmap_populate) {
                // MAP_POPULATE seems have no effect for mremap as for mmap,
                // Clear enlarged memory range explicitly to pre-fault the pages
                if (new_size > old_size)
                    memset(reinterpret_cast<char*>(buf) + old_size, 0, new_size - old_size);
            }
        } else {
            memory_check(new_size);
            // CHUNK_THRESHOLD <= old_size <= MMAP_THRESHOLD use system realloc is slow, use ChunkAllocator.
            // Big allocs that requires a copy.
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

protected:
    static constexpr size_t get_stack_threshold() { return N; }
};
