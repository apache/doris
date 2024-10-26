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

#include <butil/macros.h>

#include <cstdint>
#include <cstring>
#include <string>

#include "gutil/dynamic_annotations.h"
#include "gutil/port.h"
#include "util/memcpy_inlined.h"
#include "util/slice.h"
#include "vec/common/allocator.h"

namespace doris {

// A faststring is similar to a std::string, except that it is faster for many
// common use cases (in particular, resize() will fill with uninitialized data
// instead of memsetting to \0)
// only build() can transfer data to the outside.
class faststring : private Allocator<false, false, false, DefaultMemoryAllocator> {
public:
    enum { kInitialCapacity = 32 };

    faststring() : data_(initial_data_), len_(0), capacity_(kInitialCapacity) {}

    // Construct a string with the given capacity, in bytes.
    explicit faststring(size_t capacity)
            : data_(initial_data_), len_(0), capacity_(kInitialCapacity) {
        if (capacity > capacity_) {
            data_ = reinterpret_cast<uint8_t*>(Allocator::alloc(capacity));
            capacity_ = capacity;
        }
        ASAN_POISON_MEMORY_REGION(data_, capacity_);
    }

    ~faststring() {
        ASAN_UNPOISON_MEMORY_REGION(initial_data_, arraysize(initial_data_));
        if (data_ != initial_data_) {
            Allocator::free(data_, capacity_);
        }
    }

    // Reset the valid length of the string to 0.
    //
    // This does not free up any memory. The capacity of the string remains unchanged.
    void clear() {
        resize(0);
        ASAN_POISON_MEMORY_REGION(data_, capacity_);
    }

    // Resize the string to the given length.
    // If the new length is larger than the old length, the capacity is expanded as necessary.
    //
    // NOTE: in contrast to std::string's implementation, Any newly "exposed" bytes of data are
    // not cleared.
    void resize(size_t newsize) {
        if (newsize > capacity_) {
            reserve(newsize);
        }
        len_ = newsize;
        ASAN_POISON_MEMORY_REGION(data_ + len_, capacity_ - len_);
        ASAN_UNPOISON_MEMORY_REGION(data_, len_);
    }

    // Return the buffer built so far and reset `this` to the initial status (size() == 0).
    // NOTE: the returned data pointer is not necessarily the pointer returned by data()
    OwnedSlice build() {
        uint8_t* ret = data_;
        if (ret == initial_data_) {
            ret = reinterpret_cast<uint8_t*>(Allocator::alloc(capacity_));
            DCHECK(len_ <= capacity_);
            memcpy(ret, data_, len_);
        }
        OwnedSlice result(ret, len_, capacity_);
        len_ = 0;
        capacity_ = kInitialCapacity;
        data_ = initial_data_;
        ASAN_POISON_MEMORY_REGION(data_, capacity_);
        return result;
    }

    // Reserve space for the given total amount of data. If the current capacity is already
    // larger than the newly requested capacity, this is a no-op (i.e. it does not ever free memory).
    //
    // NOTE: even though the new capacity is reserved, it is illegal to begin writing into that memory
    // directly using pointers. If ASAN is enabled, this is ensured using manual memory poisoning.
    void reserve(size_t newcapacity) {
        if (PREDICT_TRUE(newcapacity <= capacity_)) return;
        GrowArray(newcapacity);
    }

    // Append the given data to the string, resizing capacity as necessary.
    void append(const void* src_v, size_t count) {
        const uint8_t* src = reinterpret_cast<const uint8_t*>(src_v);
        EnsureRoomForAppend(count);
        ASAN_UNPOISON_MEMORY_REGION(data_ + len_, count);

        // appending short values is common enough that this
        // actually helps, according to benchmarks. In theory
        // memcpy_inlined should already be just as good, but this
        // was ~20% faster for reading a large prefix-coded string file
        // where each string was only a few chars different
        if (count <= 4) {
            uint8_t* p = &data_[len_];
            for (int i = 0; i < count; i++) {
                *p++ = *src++;
            }
        } else {
            memcpy_inlined(&data_[len_], src, count);
        }
        len_ += count;
    }

    // Append the given string to this string.
    void append(const std::string& str) { append(str.data(), str.size()); }

    // Append the given character to this string.
    void push_back(const char byte) {
        EnsureRoomForAppend(1);
        ASAN_UNPOISON_MEMORY_REGION(data_ + len_, 1);
        data_[len_] = byte;
        len_++;
    }

    // Return the valid length of this string.
    size_t length() const { return len_; }

    // Return the valid length of this string (identical to length())
    size_t size() const { return len_; }

    // Return the allocated capacity of this string.
    size_t capacity() const { return capacity_; }

    // Return a pointer to the data in this string. Note that this pointer
    // may be invalidated by any later non-const operation.
    const uint8_t* data() const { return &data_[0]; }

    // Return a pointer to the data in this string. Note that this pointer
    // may be invalidated by any later non-const operation.
    uint8_t* data() { return &data_[0]; }

    // Return the given element of this string. Note that this does not perform
    // any bounds checking.
    const uint8_t& at(size_t i) const { return data_[i]; }

    // Return the given element of this string. Note that this does not perform
    // any bounds checking.
    const uint8_t& operator[](size_t i) const { return data_[i]; }

    // Return the given element of this string. Note that this does not perform
    // any bounds checking.
    uint8_t& operator[](size_t i) { return data_[i]; }

    // Reset the contents of this string by copying 'len' bytes from 'src'.
    void assign_copy(const uint8_t* src, size_t len) {
        // Reset length so that the first resize doesn't need to copy the current
        // contents of the array.
        len_ = 0;
        resize(len);
        memcpy(data(), src, len);
    }

    // Reset the contents of this string by copying from the given std::string.
    void assign_copy(const std::string& str) {
        assign_copy(reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
    }

    // Reallocates the internal storage to fit only the current data.
    //
    // This may revert to using internal storage if the current length is shorter than
    // kInitialCapacity. In that case, after this call, capacity() will go down to
    // kInitialCapacity.
    //
    // Any pointers within this instance may be invalidated.
    void shrink_to_fit() {
        if (data_ == initial_data_ || capacity_ == len_) return;
        ShrinkToFitInternal();
    }

    // Return a copy of this string as a std::string.
    std::string ToString() const {
        return std::string(reinterpret_cast<const char*>(data()), len_);
    }

private:
    DISALLOW_COPY_AND_ASSIGN(faststring);

    // If necessary, expand the buffer to fit at least 'count' more bytes.
    // If the array has to be grown, it is grown by at least 50%.
    void EnsureRoomForAppend(size_t count) {
        if (PREDICT_TRUE(len_ + count <= capacity_)) {
            return;
        }

        // Call the non-inline slow path - this reduces the number of instructions
        // on the hot path.
        GrowToAtLeast(len_ + count);
    }

    // The slow path of EnsureRoomForAppend. Grows the buffer by either
    // 'count' bytes, or 50%, whichever is more.
    void GrowToAtLeast(size_t newcapacity);

    // Grow the array to the given capacity, which must be more than
    // the current capacity.
    void GrowArray(size_t newcapacity);

    void ShrinkToFitInternal();

    uint8_t* data_ = nullptr;
    uint8_t initial_data_[kInitialCapacity];
    size_t len_;
    // NOTE: we will make a initial buffer as part of the object, so the smallest
    // possible value of capacity_ is kInitialCapacity.
    size_t capacity_;
};

} // namespace doris
