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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/PODArray.h
// and modified by Doris

#pragma once

#include <common/compiler_util.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <boost/core/noncopyable.hpp>
#include <cassert>
#include <cstddef>
#include <initializer_list>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "runtime/thread_context.h"
#include "vec/common/allocator.h" // IWYU pragma: keep
#include "vec/common/memcpy_small.h"

#ifndef NDEBUG
#include <sys/mman.h>
#endif

#include "vec/common/pod_array_fwd.h"

namespace doris::vectorized {
#include "common/compile_check_avoid_begin.h"
/** For zero argument, result is zero.
  * For arguments with most significand bit set, result is zero.
  * For other arguments, returns value, rounded up to power of two.
  */
inline size_t round_up_to_power_of_two_or_zero(size_t n) {
    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    ++n;

    return n;
}

/** A dynamic array for POD types.
  * Designed for a small number of large arrays (rather than a lot of small ones).
  * To be more precise - for use in ColumnVector.
  * It differs from std::vector in that it does not initialize the elements.
  *
  * Made noncopyable so that there are no accidential copies. You can copy the data using `assign` method.
  *
  * Only part of the std::vector interface is supported.
  *
  * The default constructor creates an empty object that does not allocate memory.
  * Then the memory is allocated at least initial_bytes bytes.
  *
  * If you insert elements with push_back, without making a `reserve`, then PODArray is about 2.5 times faster than std::vector.
  *
  * The template parameter `pad_right` - always allocate at the end of the array as many unused bytes.
  * Can be used to make optimistic reading, writing, copying with unaligned SIMD instructions.
  *
  * The template parameter `pad_left` - always allocate memory before 0th element of the array (rounded up to the whole number of elements)
  *  and zero initialize -1th element. It allows to use -1th element that will have value 0.
  * This gives performance benefits when converting an array of offsets to array of sizes.
  *
  * If reserve 4096 bytes, used 512 bytes, pad_left = 16, pad_right = 15, the structure of PODArray is as follows:
  *
  *         16 bytes          512 bytes                 3553 bytes                            15 bytes
  * pad_left ----- c_start -------------c_end ---------------------------- c_end_of_storage ------------- pad_right
  *    ^                                        ^                                                              ^
  *    |                                        |                                                              |
  *    |                                    c_res_mem (>= c_end && <= c_end_of_storage)                        |
  *    |                                                                                                       |
  *    +-------------------------------------- allocated_bytes (4096 bytes) -----------------------------------+
  *
  * Some methods using allocator have TAllocatorParams variadic arguments.
  * These arguments will be passed to corresponding methods of TAllocator.
  * Example: pointer to Arena, that is used for allocations.
  *
  * Why Allocator is not passed through constructor, as it is done in C++ standard library?
  * Because sometimes we have many small objects, that share same allocator with same parameters,
  *  and we must avoid larger object size due to storing the same parameters in each object.
  * This is required for states of aggregate functions.
  *
  * PODArray does not have memset 0 when allocating memory, therefore, the query mem tracker is virtual memory,
  * which will cause the query memory statistics to be higher than the actual physical memory.
  *
  * TODO Pass alignment to Allocator.
  * TODO Allow greater alignment than alignof(T). Example: array of char aligned to page size.
  */
static constexpr size_t EmptyPODArraySize = 1024;
static constexpr size_t TrackingGrowthMinSize = (1ULL << 18); // 256K
extern const char empty_pod_array[EmptyPODArraySize];

/** Base class that depend only on size of element, not on element itself.
  * You can static_cast to this class if you want to insert some data regardless to the actual type T.
  */
template <size_t ELEMENT_SIZE, size_t initial_bytes, typename TAllocator, size_t pad_right_,
          size_t pad_left_>
class PODArrayBase : private boost::noncopyable,
                     private TAllocator /// empty base optimization
{
protected:
    /// Round padding up to an whole number of elements to simplify arithmetic.
    static constexpr size_t pad_right = integerRoundUp(pad_right_, ELEMENT_SIZE);
    /// pad_left is also rounded up to 16 bytes to maintain alignment of allocated memory.
    static constexpr size_t pad_left = integerRoundUp(integerRoundUp(pad_left_, ELEMENT_SIZE), 16);
    /// Empty array will point to this static memory as padding.
    static constexpr char* null = const_cast<char*>(empty_pod_array) + pad_left;

    static_assert(pad_left <= EmptyPODArraySize &&
                  "Left Padding exceeds EmptyPODArraySize. Is the element size too large?");

    char* c_start = null; /// Does not include pad_left.
    char* c_end = null;
    char* c_end_of_storage = null; /// Does not include pad_right.
    char* c_res_mem = null;

    /// The amount of memory occupied by the num_elements of the elements.
    static size_t byte_size(size_t num_elements) {
#ifndef NDEBUG
        size_t amount;
        if (__builtin_mul_overflow(num_elements, ELEMENT_SIZE, &amount)) {
            DCHECK(false)
                    << "Amount of memory requested to allocate is more than allowed, num_elements "
                    << num_elements << ", ELEMENT_SIZE " << ELEMENT_SIZE;
        }
        return amount;
#else
        return num_elements * ELEMENT_SIZE;
#endif
    }

    /// Minimum amount of memory to allocate for num_elements, including padding.
    static size_t minimum_memory_for_elements(size_t num_elements) {
        return byte_size(num_elements) + pad_right + pad_left;
    }

    inline void check_memory(int64_t size) {
        std::string err_msg;
        if (TAllocator::sys_memory_exceed(size, &err_msg) ||
            TAllocator::memory_tracker_exceed(size, &err_msg)) {
            err_msg = fmt::format("PODArray reserve memory failed, {}.", err_msg);
            if (doris::enable_thread_catch_bad_alloc) {
                LOG(WARNING) << err_msg;
                throw doris::Exception(doris::ErrorCode::MEM_ALLOC_FAILED, err_msg);
            } else {
                LOG_EVERY_N(WARNING, 1024) << err_msg;
            }
        }
    }

    inline void reset_resident_memory_impl(const char* c_end_new) {
        static_assert(!TAllocator::need_check_and_tracking_memory(),
                      "TAllocator should `check_and_tracking_memory` is false");
        // Premise conditions:
        //  - c_res_mem <= c_end_of_storage
        //  - c_end_new > c_res_mem
        //  - If padding is not a power of 2, such as 24, allocated_bytes() will also not be a power of 2.
        //
        // If allocated_bytes <= 256K, res_mem_growth = c_end_of_storage - c_res_mem.
        //
        // If capacity >= 512K:
        //      - If `c_end_of_storage - c_res_mem < TrackingGrowthMinSize`, then tracking to c_end_of_storage.
        //      - `c_end_new - c_res_mem` is the size of the physical memory growth,
        //        which is also the minimum tracking size of this time,
        //        `(((c_end_new - c_res_mem) >> 16) << 16)` is aligned down to 64K,
        //        assuming `allocated_bytes >= 512K`, so `(allocated_bytes >> 3)` is at least 64K,
        //        so `(((c_end_new - c_res_mem) >> 16) << 16) + (allocated_bytes() >> 3)`
        //        must be greater than `c_end_new - c_res_mem`.
        //
        //        For example:
        //         - 256K < capacity <= 512K,
        //           it will only tracking twice,
        //           the second time `c_end_of_storage - c_res_mem < TrackingGrowthMinSize` is true,
        //           so it will tracking to c_end_of_storage.
        //         - capacity > 32M, `(((c_end_new - c_res_mem) >> 16) << 16)` align the increased
        //           physical memory size down to 64k, then add `(allocated_bytes() >> 3)` equals 2M,
        //           so `reset_resident_memory` is tracking an additional 2M,
        //           after that, physical memory growth within 2M does not need to reset_resident_memory again.
        //
        // so, when PODArray is expanded by power of 2,
        // the memory is checked and tracked up to 8 times between each expansion,
        // because each time additional tracking `(allocated_bytes() >> 3)`.
        // after each reset_resident_memory, tracking_res_memory >= used_bytes;
        int64_t res_mem_growth =
                c_end_of_storage - c_res_mem < TrackingGrowthMinSize
                        ? c_end_of_storage - c_res_mem
                        : std::min(static_cast<size_t>(c_end_of_storage - c_res_mem),
                                   static_cast<size_t>((((c_end_new - c_res_mem) >> 16) << 16) +
                                                       (allocated_bytes() >> 3)));
        DCHECK(res_mem_growth > 0) << ", c_end_new: " << (c_end_new - c_start)
                                   << ", c_res_mem: " << (c_res_mem - c_start)
                                   << ", c_end_of_storage: " << (c_end_of_storage - c_start)
                                   << ", allocated_bytes: " << allocated_bytes()
                                   << ", res_mem_growth: " << res_mem_growth;
        check_memory(res_mem_growth);
        CONSUME_THREAD_MEM_TRACKER(res_mem_growth);
        c_res_mem = c_res_mem + res_mem_growth;
    }

    inline void reset_resident_memory(const char* c_end_new) {
        if (UNLIKELY(c_end_new > c_res_mem)) {
            reset_resident_memory_impl(c_end_new);
        }
    }

    inline void reset_resident_memory() { reset_resident_memory(c_end); }

    void alloc_for_num_elements(size_t num_elements) {
        alloc(round_up_to_power_of_two_or_zero(minimum_memory_for_elements(num_elements)));
    }

    template <typename... TAllocatorParams>
    void alloc(size_t bytes, TAllocatorParams&&... allocator_params) {
        char* allocated = reinterpret_cast<char*>(
                TAllocator::alloc(bytes, std::forward<TAllocatorParams>(allocator_params)...));

        c_start = allocated + pad_left;
        c_end = c_start;
        c_res_mem = c_start;
        c_end_of_storage = allocated + bytes - pad_right;
        CONSUME_THREAD_MEM_TRACKER(pad_left + pad_right);

        if (pad_left) memset(c_start - ELEMENT_SIZE, 0, ELEMENT_SIZE);
    }

    void dealloc() {
        if (c_start == null) return;
        unprotect();
        RELEASE_THREAD_MEM_TRACKER((c_res_mem - c_start + pad_right + pad_left));
        TAllocator::free(c_start - pad_left, allocated_bytes());
    }

    template <typename... TAllocatorParams>
    void realloc(size_t bytes, TAllocatorParams&&... allocator_params) {
        if (c_start == null) {
            alloc(bytes, std::forward<TAllocatorParams>(allocator_params)...);
            return;
        }

        unprotect();

        ptrdiff_t end_diff = c_end - c_start;
        ptrdiff_t res_mem_diff = c_res_mem - c_start;

        // Realloc can do 2 possible things:
        // - expand existing memory region
        // - allocate new memory block and free the old one
        // Because we don't know which option will be picked we need to make sure there is enough
        // memory for all options.
        check_memory(res_mem_diff);
        char* allocated = reinterpret_cast<char*>(
                TAllocator::realloc(c_start - pad_left, allocated_bytes(), bytes,
                                    std::forward<TAllocatorParams>(allocator_params)...));

        c_start = allocated + pad_left;
        c_end = c_start + end_diff;
        c_res_mem = c_start + res_mem_diff;
        c_end_of_storage = allocated + bytes - pad_right;
    }

    bool is_initialized() const {
        return (c_start != null) && (c_end != null) && (c_end_of_storage != null);
    }

    bool is_allocated_from_stack() const {
        constexpr size_t stack_threshold = TAllocator::get_stack_threshold();
        return (stack_threshold > 0) && (allocated_bytes() <= stack_threshold);
    }

    template <typename... TAllocatorParams>
    void reserve_for_next_size(TAllocatorParams&&... allocator_params) {
        if (size() == 0) {
            // The allocated memory should be multiplication of ELEMENT_SIZE to hold the element, otherwise,
            // memory issue such as corruption could appear in edge case.
            realloc(std::max(integerRoundUp(initial_bytes, ELEMENT_SIZE),
                             minimum_memory_for_elements(1)),
                    std::forward<TAllocatorParams>(allocator_params)...);
        } else
            realloc(allocated_bytes() * 2, std::forward<TAllocatorParams>(allocator_params)...);
    }

#ifndef NDEBUG
    /// Make memory region readonly with mprotect if it is large enough.
    /// The operation is slow and performed only for debug builds.
    void protect_impl(int prot) {
        static constexpr size_t PROTECT_PAGE_SIZE = 4096;

        char* left_rounded_up = reinterpret_cast<char*>(
                (reinterpret_cast<intptr_t>(c_start) - pad_left + PROTECT_PAGE_SIZE - 1) /
                PROTECT_PAGE_SIZE * PROTECT_PAGE_SIZE);
        char* right_rounded_down =
                reinterpret_cast<char*>((reinterpret_cast<intptr_t>(c_end_of_storage) + pad_right) /
                                        PROTECT_PAGE_SIZE * PROTECT_PAGE_SIZE);

        if (right_rounded_down > left_rounded_up) {
            size_t length = right_rounded_down - left_rounded_up;
            if (0 != mprotect(left_rounded_up, length, prot)) throw std::exception();
        }
    }

    /// Restore memory protection in destructor or realloc for further reuse by allocator.
    bool mprotected = false;
#endif

public:
    bool empty() const { return c_end == c_start; }
    size_t size() const { return (c_end - c_start) / ELEMENT_SIZE; }
    size_t capacity() const { return (c_end_of_storage - c_start) / ELEMENT_SIZE; }

    /// This method is safe to use only for information about memory usage.
    size_t allocated_bytes() const {
        if (c_end_of_storage == null) {
            return 0;
        }
        return c_end_of_storage - c_start + pad_right + pad_left;
    }

    void clear() { c_end = c_start; }

    template <typename... TAllocatorParams>
    void reserve(size_t n, TAllocatorParams&&... allocator_params) {
        if (n > capacity())
            realloc(round_up_to_power_of_two_or_zero(minimum_memory_for_elements(n)),
                    std::forward<TAllocatorParams>(allocator_params)...);
    }

    template <typename... TAllocatorParams>
    void resize(size_t n, TAllocatorParams&&... allocator_params) {
        reserve(n, std::forward<TAllocatorParams>(allocator_params)...);
        resize_assume_reserved(n);
    }

    void resize_assume_reserved(const size_t n) {
        c_end = c_start + byte_size(n);
        reset_resident_memory();
    }

    const char* raw_data() const { return c_start; }

    template <typename... TAllocatorParams>
    void push_back_raw(const char* ptr, TAllocatorParams&&... allocator_params) {
        if (UNLIKELY(c_end + ELEMENT_SIZE > c_res_mem)) { // c_res_mem <= c_end_of_storage
            if (UNLIKELY(c_end + ELEMENT_SIZE > c_end_of_storage)) {
                reserve_for_next_size(std::forward<TAllocatorParams>(allocator_params)...);
            }
            reset_resident_memory_impl(c_end + ELEMENT_SIZE);
        }

        memcpy(c_end, ptr, ELEMENT_SIZE);
        c_end += byte_size(1);
    }

    void protect() {
#ifndef NDEBUG
        protect_impl(PROT_READ);
        mprotected = true;
#endif
    }

    void unprotect() {
#ifndef NDEBUG
        if (mprotected) protect_impl(PROT_WRITE);
        mprotected = false;
#endif
    }

    template <typename It1, typename It2>
    void assert_not_intersects(It1 from_begin [[maybe_unused]], It2 from_end [[maybe_unused]]) {
#ifndef NDEBUG
        const char* ptr_begin = reinterpret_cast<const char*>(&*from_begin);
        const char* ptr_end = reinterpret_cast<const char*>(&*from_end);

        /// Also it's safe if the range is empty.
        assert(!((ptr_begin >= c_start && ptr_begin < c_end) ||
                 (ptr_end > c_start && ptr_end <= c_end)) ||
               (ptr_begin == ptr_end));
#endif
    }

    ~PODArrayBase() { dealloc(); }
};

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_,
          size_t pad_left_>
class PODArray : public PODArrayBase<sizeof(T), initial_bytes, TAllocator, pad_right_, pad_left_> {
protected:
    using Base = PODArrayBase<sizeof(T), initial_bytes, TAllocator, pad_right_, pad_left_>;

    T* t_start() { return reinterpret_cast<T*>(this->c_start); }
    T* t_end() { return reinterpret_cast<T*>(this->c_end); }

    const T* t_start() const { return reinterpret_cast<const T*>(this->c_start); }
    const T* t_end() const { return reinterpret_cast<const T*>(this->c_end); }

public:
    using value_type = T;

    /// We cannot use boost::iterator_adaptor, because it defeats loop vectorization,
    ///  see https://github.com/ClickHouse/ClickHouse/pull/9442

    using iterator = T*;
    using const_iterator = const T*;

    PODArray() = default;

    PODArray(size_t n) {
        this->alloc_for_num_elements(n);
        this->c_end += this->byte_size(n);
        this->reset_resident_memory();
    }

    PODArray(size_t n, const T& x) {
        this->alloc_for_num_elements(n);
        assign(n, x);
    }

    PODArray(const_iterator from_begin, const_iterator from_end) {
        this->alloc_for_num_elements(from_end - from_begin);
        insert(from_begin, from_end);
    }

    PODArray(std::initializer_list<T> il) : PODArray(std::begin(il), std::end(il)) {}

    PODArray(PODArray&& other) { this->swap(other); }

    PODArray& operator=(PODArray&& other) {
        this->swap(other);
        return *this;
    }

    T* data() { return t_start(); }
    const T* data() const { return t_start(); }

    /// The index is signed to access -1th element without pointer overflow.
    T& operator[](ssize_t n) {
        /// <= size, because taking address of one element past memory range is Ok in C++ (expression like &arr[arr.size()] is perfectly valid).
        DCHECK_GE(n, (static_cast<ssize_t>(pad_left_) ? -1 : 0));
        DCHECK_LE(n, static_cast<ssize_t>(this->size()));
        return t_start()[n];
    }

    const T& operator[](ssize_t n) const {
        DCHECK_GE(n, (static_cast<ssize_t>(pad_left_) ? -1 : 0));
        DCHECK_LE(n, static_cast<ssize_t>(this->size()));
        return t_start()[n];
    }

    T& front() { return t_start()[0]; }
    T& back() { return t_end()[-1]; }
    const T& front() const { return t_start()[0]; }
    const T& back() const { return t_end()[-1]; }

    iterator begin() { return t_start(); }
    iterator end() { return t_end(); }
    const_iterator begin() const { return t_start(); }
    const_iterator end() const { return t_end(); }
    const_iterator cbegin() const { return t_start(); }
    const_iterator cend() const { return t_end(); }

    void* get_end_ptr() const { return this->c_end; }

    /// Same as resize, but zeroes new elements.
    void resize_fill(size_t n) {
        size_t old_size = this->size();
        const auto new_size = this->byte_size(n);
        if (n > old_size) {
            this->reserve(n);
            this->reset_resident_memory(this->c_start + new_size);
            memset(this->c_end, 0, this->byte_size(n - old_size));
        }
        this->c_end = this->c_start + new_size;
    }

    /// reset the array capacity
    /// fill the new additional elements using the value
    void resize_fill(size_t n, const T& value) {
        size_t old_size = this->size();
        const auto new_size = this->byte_size(n);
        if (n > old_size) {
            this->reserve(n);
            this->reset_resident_memory(this->c_start + new_size);
            std::fill(t_end(), t_end() + n - old_size, value);
        }
        this->c_end = this->c_start + new_size;
    }

    template <typename U, typename... TAllocatorParams>
    void push_back(U&& x, TAllocatorParams&&... allocator_params) {
        if (UNLIKELY(this->c_end + sizeof(T) > this->c_res_mem)) { // c_res_mem <= c_end_of_storage
            if (UNLIKELY(this->c_end + sizeof(T) > this->c_end_of_storage)) {
                this->reserve_for_next_size(std::forward<TAllocatorParams>(allocator_params)...);
            }
            this->reset_resident_memory_impl(this->c_end + this->byte_size(1));
        }

        new (t_end()) T(std::forward<U>(x));
        this->c_end += this->byte_size(1);
    }

    /**
     * you must make sure to reserve podarray before calling this method
     * remove branch if can improve performance
     */
    template <typename U, typename... TAllocatorParams>
    void push_back_without_reserve(U&& x, TAllocatorParams&&... allocator_params) {
        this->reset_resident_memory(this->c_end + this->byte_size(1));
        new (t_end()) T(std::forward<U>(x));
        this->c_end += this->byte_size(1);
    }

    /** This method doesn't allow to pass parameters for Allocator,
      *  and it couldn't be used if Allocator requires custom parameters.
      */
    template <typename... Args>
    void emplace_back(Args&&... args) {
        if (UNLIKELY(this->c_end + sizeof(T) > this->c_res_mem)) { // c_res_mem <= c_end_of_storage
            if (UNLIKELY(this->c_end + sizeof(T) > this->c_end_of_storage)) {
                this->reserve_for_next_size();
            }
            this->reset_resident_memory_impl(this->c_end + this->byte_size(1));
        }

        new (t_end()) T(std::forward<Args>(args)...);
        this->c_end += this->byte_size(1);
    }

    void pop_back() { this->c_end -= this->byte_size(1); }

    /// Do not insert into the array a piece of itself. Because with the resize, the iterators on themselves can be invalidated.
    template <typename It1, typename It2, typename... TAllocatorParams>
    void insert_prepare(It1 from_begin, It2 from_end, TAllocatorParams&&... allocator_params) {
        this->assert_not_intersects(from_begin, from_end);
        size_t required_capacity = this->size() + (from_end - from_begin);
        if (required_capacity > this->capacity())
            this->reserve(round_up_to_power_of_two_or_zero(required_capacity),
                          std::forward<TAllocatorParams>(allocator_params)...);
    }

    /// Do not insert into the array a piece of itself. Because with the resize, the iterators on themselves can be invalidated.
    template <typename It1, typename It2, typename... TAllocatorParams>
    void insert(It1 from_begin, It2 from_end, TAllocatorParams&&... allocator_params) {
        insert_prepare(from_begin, from_end, std::forward<TAllocatorParams>(allocator_params)...);

        insert_assume_reserved(from_begin, from_end);
    }

    /// Works under assumption, that it's possible to read up to 15 excessive bytes after `from_end` and this PODArray is padded.
    template <typename It1, typename It2, typename... TAllocatorParams>
    void insert_small_allow_read_write_overflow15(It1 from_begin, It2 from_end,
                                                  TAllocatorParams&&... allocator_params) {
        static_assert(pad_right_ >= 15);
        insert_prepare(from_begin, from_end, std::forward<TAllocatorParams>(allocator_params)...);
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        this->reset_resident_memory(this->c_end + bytes_to_copy);
        memcpy_small_allow_read_write_overflow15(
                this->c_end, reinterpret_cast<const void*>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    template <typename It1, typename It2>
    void insert(iterator it, It1 from_begin, It2 from_end) {
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        if (!bytes_to_copy) {
            return;
        }
        size_t bytes_to_move = this->byte_size(end() - it);
        insert_prepare(from_begin, from_end);
        this->reset_resident_memory(this->c_end + bytes_to_copy);

        if (UNLIKELY(bytes_to_move)) {
            memmove(this->c_end + bytes_to_copy - bytes_to_move, this->c_end - bytes_to_move,
                    bytes_to_move);
        }

        memcpy(this->c_end - bytes_to_move, reinterpret_cast<const void*>(&*from_begin),
               bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    template <typename It1, typename It2>
    void insert_assume_reserved(It1 from_begin, It2 from_end) {
        this->assert_not_intersects(from_begin, from_end);
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        this->reset_resident_memory(this->c_end + bytes_to_copy);
        memcpy(this->c_end, reinterpret_cast<const void*>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    template <typename It1, typename It2>
    void insert_assume_reserved_and_allow_overflow(It1 from_begin, It2 from_end) {
        size_t bytes_to_copy = this->byte_size(from_end - from_begin);
        this->reset_resident_memory(this->c_end + bytes_to_copy);
        memcpy_small_allow_read_write_overflow15(
                this->c_end, reinterpret_cast<const void*>(&*from_begin), bytes_to_copy);
        this->c_end += bytes_to_copy;
    }

    void swap(PODArray& rhs) {
        DCHECK(this->pad_left == rhs.pad_left && this->pad_right == rhs.pad_right)
                << ", this.pad_left: " << this->pad_left << ", rhs.pad_left: " << rhs.pad_left
                << ", this.pad_right: " << this->pad_right << ", rhs.pad_right: " << rhs.pad_right;
#ifndef NDEBUG
        this->unprotect();
        rhs.unprotect();
#endif

        /// Swap two PODArray objects, arr1 and arr2, that satisfy the following conditions:
        /// - The elements of arr1 are stored on stack.
        /// - The elements of arr2 are stored on heap.
        auto swap_stack_heap = [this](PODArray& arr1, PODArray& arr2) {
            size_t stack_size = arr1.size();
            size_t stack_allocated = arr1.allocated_bytes();
            size_t stack_res_mem_used = arr1.c_res_mem - arr1.c_start;

            size_t heap_size = arr2.size();
            size_t heap_allocated = arr2.allocated_bytes();
            size_t heap_res_mem_used = arr2.c_res_mem - arr2.c_start;

            /// Keep track of the stack content we have to copy.
            char* stack_c_start = arr1.c_start;

            /// arr1 takes ownership of the heap memory of arr2.
            arr1.c_start = arr2.c_start;
            arr1.c_end_of_storage = arr1.c_start + heap_allocated - arr2.pad_right - arr2.pad_left;
            arr1.c_end = arr1.c_start + this->byte_size(heap_size);
            arr1.c_res_mem = arr1.c_start + heap_res_mem_used;

            /// Allocate stack space for arr2.
            arr2.alloc(stack_allocated);
            /// Copy the stack content.
            memcpy(arr2.c_start, stack_c_start, this->byte_size(stack_size));
            arr2.c_end = arr2.c_start + this->byte_size(stack_size);
            arr2.c_res_mem = arr2.c_start + stack_res_mem_used;
        };

        auto do_move = [this](PODArray& src, PODArray& dest) {
            if (src.is_allocated_from_stack()) {
                dest.dealloc();
                dest.alloc(src.allocated_bytes());
                memcpy(dest.c_start, src.c_start, this->byte_size(src.size()));
                dest.c_end = dest.c_start + this->byte_size(src.size());
                dest.c_res_mem = dest.c_start + (src.c_res_mem - src.c_start);

                src.c_start = Base::null;
                src.c_end = Base::null;
                src.c_end_of_storage = Base::null;
                src.c_res_mem = Base::null;
            } else {
                std::swap(dest.c_start, src.c_start);
                std::swap(dest.c_end, src.c_end);
                std::swap(dest.c_end_of_storage, src.c_end_of_storage);
                std::swap(dest.c_res_mem, src.c_res_mem);
            }
        };

        if (!this->is_initialized() && !rhs.is_initialized()) {
            return;
        } else if (!this->is_initialized() && rhs.is_initialized()) {
            do_move(rhs, *this);
            return;
        } else if (this->is_initialized() && !rhs.is_initialized()) {
            do_move(*this, rhs);
            return;
        }

        if (this->is_allocated_from_stack() && rhs.is_allocated_from_stack()) {
            size_t min_size = std::min(this->size(), rhs.size());
            size_t max_size = std::max(this->size(), rhs.size());

            for (size_t i = 0; i < min_size; ++i) std::swap(this->operator[](i), rhs[i]);

            if (this->size() == max_size) {
                for (size_t i = min_size; i < max_size; ++i) rhs[i] = this->operator[](i);
            } else {
                for (size_t i = min_size; i < max_size; ++i) this->operator[](i) = rhs[i];
            }

            size_t lhs_size = this->size();
            size_t lhs_allocated = this->allocated_bytes();
            size_t lhs_res_mem_used = this->c_res_mem - this->c_start;

            size_t rhs_size = rhs.size();
            size_t rhs_allocated = rhs.allocated_bytes();
            size_t rhs_res_mem_used = rhs.c_res_mem - rhs.c_start;

            this->c_end_of_storage =
                    this->c_start + rhs_allocated - Base::pad_right - Base::pad_left;
            rhs.c_end_of_storage = rhs.c_start + lhs_allocated - Base::pad_right - Base::pad_left;

            this->c_end = this->c_start + this->byte_size(rhs_size);
            rhs.c_end = rhs.c_start + this->byte_size(lhs_size);

            this->c_res_mem = this->c_start + rhs_res_mem_used;
            rhs.c_res_mem = rhs.c_start + lhs_res_mem_used;
        } else if (this->is_allocated_from_stack() && !rhs.is_allocated_from_stack()) {
            swap_stack_heap(*this, rhs);
        } else if (!this->is_allocated_from_stack() && rhs.is_allocated_from_stack()) {
            swap_stack_heap(rhs, *this);
        } else {
            std::swap(this->c_start, rhs.c_start);
            std::swap(this->c_end, rhs.c_end);
            std::swap(this->c_end_of_storage, rhs.c_end_of_storage);
            std::swap(this->c_res_mem, rhs.c_res_mem);
        }
    }

    /// reset the array capacity
    /// replace the all elements using the value
    void assign(size_t n, const T& x) {
        this->resize(n);
        std::fill(begin(), end(), x);
    }

    template <typename It1, typename It2>
    void assign(It1 from_begin, It2 from_end) {
        this->assert_not_intersects(from_begin, from_end);
        size_t required_capacity = from_end - from_begin;
        if (required_capacity > this->capacity())
            this->reserve(round_up_to_power_of_two_or_zero(required_capacity));

        size_t bytes_to_copy = this->byte_size(required_capacity);
        this->reset_resident_memory(this->c_start + bytes_to_copy);
        memcpy(this->c_start, reinterpret_cast<const void*>(&*from_begin), bytes_to_copy);
        this->c_end = this->c_start + bytes_to_copy;
    }

    void assign(const PODArray& from) { assign(from.begin(), from.end()); }

    void erase(iterator first, iterator last) {
        size_t items_to_move = end() - last;

        while (items_to_move != 0) {
            *first = *last;

            ++first;
            ++last;

            --items_to_move;
        }

        this->c_end = reinterpret_cast<char*>(first);
    }

    void erase(iterator pos) { this->erase(pos, pos + 1); }

    bool operator==(const PODArray& rhs) const {
        if (this->size() != rhs.size()) {
            return false;
        }

        const_iterator lhs_it = begin();
        const_iterator rhs_it = rhs.begin();

        while (lhs_it != end()) {
            if (*lhs_it != *rhs_it) {
                return false;
            }

            ++lhs_it;
            ++rhs_it;
        }

        return true;
    }

    bool operator!=(const PODArray& rhs) const { return !operator==(rhs); }
};

template <typename T, size_t initial_bytes, typename TAllocator, size_t pad_right_,
          size_t pad_left_>
void swap(PODArray<T, initial_bytes, TAllocator, pad_right_, pad_left_>& lhs,
          PODArray<T, initial_bytes, TAllocator, pad_right_, pad_left_>& rhs) {
    lhs.swap(rhs);
}

} // namespace doris::vectorized
#include "common/compile_check_avoid_end.h"