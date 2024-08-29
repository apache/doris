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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "vec/common/allocator.h"

namespace doris {

class faststring;

/// @brief A wrapper around externally allocated data.
///
/// Slice is a simple structure containing a pointer into some external
/// storage and a size. The user of a Slice must ensure that the slice
/// is not used after the corresponding external storage has been
/// deallocated.
///
/// Multiple threads can invoke const methods on a Slice without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same Slice must use
/// external synchronization.
struct Slice {
public:
    char* data = nullptr;
    size_t size;
    // Intentionally copyable

    /// Create an empty slice.
    Slice() : data(const_cast<char*>("")), size(0) {}

    /// Create a slice that refers to a @c char byte array.
    Slice(const char* d, size_t n) : data(const_cast<char*>(d)), size(n) {}

    // Create a slice that refers to a @c uint8_t byte array.
    //
    // @param [in] d
    //   The input array.
    // @param [in] n
    //   Number of bytes in the array.
    Slice(const uint8_t* s, size_t n)
            : data(const_cast<char*>(reinterpret_cast<const char*>(s))), size(n) {}

    /// Create a slice that refers to the contents of the given string.
    Slice(const std::string& s)
            : // NOLINT(runtime/explicit)
              data(const_cast<char*>(s.data())),
              size(s.size()) {}

    Slice(const faststring& s);

    /// Create a slice that refers to a C-string s[0,strlen(s)-1].
    Slice(const char* s)
            : // NOLINT(runtime/explicit)
              data(const_cast<char*>(s)),
              size(strlen(s)) {}

    /// default copy/move constructor and assignment
    Slice(const Slice&) = default;
    Slice& operator=(const Slice&) = default;
    Slice(Slice&&) noexcept = default;
    Slice& operator=(Slice&&) noexcept = default;

    /// @return A pointer to the beginning of the referenced data.
    const char* get_data() const { return data; }

    /// @return A mutable pointer to the beginning of the referenced data.
    char* mutable_data() { return const_cast<char*>(data); }

    /// @return The length (in bytes) of the referenced data.
    size_t get_size() const { return size; }

    /// @return @c true iff the length of the referenced data is zero.
    bool empty() const { return size == 0; }

    /// @return the n-th byte in the referenced data.
    const char& operator[](size_t n) const {
        assert(n < size);
        return data[n];
    }

    /// Change this slice to refer to an empty array.
    void clear() {
        data = const_cast<char*>("");
        size = 0;
    }

    /// Drop the first "n" bytes from this slice.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes that should be dropped from the beginning.
    void remove_prefix(size_t n) {
        assert(n <= size);
        data += n;
        size -= n;
    }

    /// Drop the last "n" bytes from this slice.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes that should be dropped from the last.
    void remove_suffix(size_t n) {
        assert(n <= size);
        size -= n;
    }

    /// Remove leading spaces.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes of space that should be dropped from the beginning.
    void trim_prefix() {
        int32_t begin = 0;
        while (begin < size && data[begin] == ' ') {
            data += 1;
            size -= 1;
        }
    }

    /// Remove quote char '"' or ''' which should exist as first and last char.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes of space that should be dropped from the beginning.
    bool trim_quote() {
        int32_t begin = 0;
        bool change = false;
        if (size >= 2 && ((data[begin] == '"' && data[size - 1] == '"') ||
                          (data[begin] == '\'' && data[size - 1] == '\''))) {
            data += 1;
            size -= 2;
            change = true;
        }
        return change;
    }

    /// Remove quote char '"' which should exist as first and last char.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   Number of bytes of space that should be dropped from the beginning.
    bool trim_double_quotes() {
        int32_t begin = 0;
        if (size >= 2 && (data[begin] == '"' && data[size - 1] == '"')) {
            data += 1;
            size -= 2;
            return true;
        }
        return false;
    }

    /// Truncate the slice to the given number of bytes.
    ///
    /// @pre n <= size
    ///
    /// @note Only the base and bounds of the slice are changed;
    ///   the data is not modified.
    ///
    /// @param [in] n
    ///   The new size of the slice.
    void truncate(size_t n) {
        assert(n <= size);
        size = n;
    }

    /// @return A string that contains a copy of the referenced data.
    std::string to_string() const { return std::string(data, size); }

    /// Do a three-way comparison of the slice's data.
    int compare(const Slice& b) const;

    /// Check whether the slice starts with the given prefix.
    bool starts_with(const Slice& x) const {
        return ((size >= x.size) && (mem_equal(data, x.data, x.size)));
    }

    bool ends_with(const Slice& x) const {
        return ((size >= x.size) && mem_equal(data + (size - x.size), x.data, x.size));
    }

    /// @brief Comparator struct, useful for ordered collections (like STL maps).
    struct Comparator {
        /// Compare two slices using Slice::compare()
        ///
        /// @param [in] a
        ///   The slice to call Slice::compare() at.
        /// @param [in] b
        ///   The slice to use as a parameter for Slice::compare().
        /// @return @c true iff @c a is less than @c b by Slice::compare().
        bool operator()(const Slice& a, const Slice& b) const { return a.compare(b) < 0; }
    };

    /// Relocate/copy the slice's data into a new location.
    ///
    /// @param [in] d
    ///   The new location for the data. If it's the same location, then no
    ///   relocation is done. It is assumed that the new location is
    ///   large enough to fit the data.
    void relocate(char* d) {
        if (data != d) {
            memcpy(d, data, size);
            data = d;
        }
    }

    friend bool operator==(const Slice& x, const Slice& y);

    friend std::ostream& operator<<(std::ostream& os, const Slice& slice);

    static bool mem_equal(const void* a, const void* b, size_t n) { return memcmp(a, b, n) == 0; }

    static int mem_compare(const void* a, const void* b, size_t n) { return memcmp(a, b, n); }

    static size_t compute_total_size(const std::vector<Slice>& slices) {
        size_t total_size = 0;
        for (auto& slice : slices) {
            total_size += slice.size;
        }
        return total_size;
    }

    static std::string to_string(const std::vector<Slice>& slices) {
        std::string buf;
        for (auto& slice : slices) {
            buf.append(slice.data, slice.size);
        }
        return buf;
    }
};

inline std::ostream& operator<<(std::ostream& os, const Slice& slice) {
    os << slice.to_string();
    return os;
}

/// Check whether two slices are identical.
inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size == y.size) && (Slice::mem_equal(x.data, y.data, x.size)));
}

/// Check whether two slices are not identical.
inline bool operator!=(const Slice& x, const Slice& y) {
    return !(x == y);
}

inline int Slice::compare(const Slice& b) const {
    const int min_len = (size < b.size) ? size : b.size;
    int r = mem_compare(data, b.data, min_len);
    if (r == 0) {
        if (size < b.size)
            r = -1;
        else if (size > b.size)
            r = +1;
    }
    return r;
}

/// @brief STL map whose keys are Slices.
///
/// An example of usage:
/// @code
///   typedef SliceMap<int>::type MySliceMap;
///
///   MySliceMap my_map;
///   my_map.insert(MySliceMap::value_type(a, 1));
///   my_map.insert(MySliceMap::value_type(b, 2));
///   my_map.insert(MySliceMap::value_type(c, 3));
///
///   for (const MySliceMap::value_type& pair : my_map) {
///     ...
///   }
/// @endcode
template <typename T>
struct SliceMap {
    /// A handy typedef for the slice map with appropriate comparison operator.
    typedef std::map<Slice, T, Slice::Comparator> type;
};

// A move-only type which manage the lifecycle of externally allocated data.
// Unlike std::unique_ptr<uint8_t[]>, OwnedSlice remembers the size of data so that clients can access
// the underlying buffer as a Slice.
//
// Usage example:
//   OwnedSlice read_page(PagePointer pp);
//   {
//     OwnedSlice page_data(new uint8_t[pp.size], pp.size);
//     Status s = _file.read_at(pp.offset, owned.slice());
//     if (!s.ok()) {
//       return s; // `page_data` destructs, deallocate underlying buffer
//     }
//     return page_data; // transfer ownership of buffer into the caller
//   }
//
// only receive the memory allocated by Allocator and disables mmap,
// otherwise the memory may not be freed correctly, currently only be constructed by faststring.
class OwnedSlice : private Allocator<false, false, false, DefaultMemoryAllocator> {
public:
    OwnedSlice() : _slice((uint8_t*)nullptr, 0) {}

    OwnedSlice(OwnedSlice&& src) : _slice(src._slice), _capacity(src._capacity) {
        src._slice.data = nullptr;
        src._slice.size = 0;
        src._capacity = 0;
    }

    OwnedSlice& operator=(OwnedSlice&& src) {
        if (this != &src) {
            std::swap(_slice, src._slice);
            std::swap(_capacity, src._capacity);
        }
        return *this;
    }

    ~OwnedSlice() { Allocator::free(_slice.data, _capacity); }

    const Slice& slice() const { return _slice; }

private:
    // faststring also inherits Allocator and disables mmap.
    friend class faststring;

    OwnedSlice(uint8_t* _data, size_t size, size_t capacity)
            : _slice(_data, size), _capacity(capacity) {}

private:
    // disable copy constructor and copy assignment
    OwnedSlice(const OwnedSlice&) = delete;
    void operator=(const OwnedSlice&) = delete;

    Slice _slice;
    size_t _capacity = 0;
};

} // namespace doris
