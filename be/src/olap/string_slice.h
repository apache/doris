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

#ifndef BDG_PALO_BE_SRC_OLAP_STRING_SLICE_H
#define BDG_PALO_BE_SRC_OLAP_STRING_SLICE_H

#include <assert.h>
#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>

#include "olap/olap_define.h"

namespace palo {

/// @brief A wrapper around externally allocated data.
///
/// StringSlice is a simple structure containing a pointer into some external
/// storage and a size. The user of a StringSlice must ensure that the slice
/// is not used after the corresponding external storage has been
/// deallocated.
///
/// Multiple threads can invoke const methods on a StringSlice without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same StringSlice must use
/// external synchronization.
struct StringSlice {
public:
    char* data;
    size_t size;
    // Intentionally copyable

    /// Create an empty slice.
    StringSlice() : data(const_cast<char*>("")), size(0) { }


    /// Create a slice that refers to a @c char byte array.
    StringSlice(const char* d, size_t n) :
        data(const_cast<char*>(d)), size(n) { }

    /// Create a slice that refers to the contents of the given string.
    StringSlice(const std::string& s) : // NOLINT(runtime/explicit)
        data(const_cast<char*>(s.data())), size(s.size()) { }

    /// Create a slice that refers to a C-string s[0,strlen(s)-1].
    StringSlice(const char* s) : // NOLINT(runtime/explicit)
        data(const_cast<char*>(s)), size(strlen(s)) { }

    /*
    /// @return A pointer to the beginning of the referenced data.
    const char* data() const { return data; }

    /// @return A mutable pointer to the beginning of the referenced data.
    char* mutable_data() { return const_cast<char*>(data); }

    /// @return The length (in bytes) of the referenced data.
    size_t size() const { return size; }
    */

    /// @return @c true iff the length of the referenced data is zero.
    bool empty() const { return size == 0; }

    /// @return the n-th byte in the referenced data.
    const char operator[](size_t n) const {
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
    int compare(const StringSlice& b) const;

    /// Check whether the slice starts with the given prefix.
    bool starts_with(const StringSlice& x) const {
        return ((size >= x.size) &&
                (mem_equal(data, x.data, x.size)));
    }

    /// @brief Comparator struct, useful for ordered collections (like STL maps).
    struct Comparator {
        /// Compare two slices using StringSlice::compare()
        ///
        /// @param [in] a
        ///   The slice to call StringSlice::compare() at.
        /// @param [in] b
        ///   The slice to use as a parameter for StringSlice::compare().
        /// @return @c true iff @c a is less than @c b by StringSlice::compare().
        bool operator()(const StringSlice& a, const StringSlice& b) const {
            return a.compare(b) < 0;
        }
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

    friend bool operator==(const StringSlice& x, const StringSlice& y);

    static bool mem_equal(const void* a, const void* b, size_t n) {
        return memcmp(a, b, n) == 0;
    }

    static int mem_compare(const void* a, const void* b, size_t n) {
        return memcmp(a, b, n);
    }

};

/// Check whether two slices are identical.
inline bool operator==(const StringSlice& x, const StringSlice& y) {
    return ((x.size == y.size) &&
            (StringSlice::mem_equal(x.data, y.data, x.size)));
}

/// Check whether two slices are not identical.
inline bool operator!=(const StringSlice& x, const StringSlice& y) {
    return !(x == y);
}

inline int StringSlice::compare(const StringSlice& b) const {
    const int min_len = (size < b.size) ? size : b.size;
    int r = mem_compare(data, b.data, min_len);
    if (r == 0) {
        if (size < b.size) r = -1;
        else if (size > b.size) r = +1;
    }
    return r;
}

/// @brief STL map whose keys are StringSlices.
///
/// An example of usage:
/// @code
///   typedef StringSliceMap<int>::type MySliceMap;
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
struct StringSliceMap {
    /// A handy typedef for the slice map with appropriate comparison operator.
    typedef std::map<StringSlice, T, StringSlice::Comparator> type;
};

}  // namespace palo

#endif  // BDG_PALO_BE_SRC_OLAP_STRING_SLICE_H
