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

#ifndef DORIS_BE_RUNTIME_STRING_VALUE_H
#define DORIS_BE_RUNTIME_STRING_VALUE_H

#include <string.h>

#include "udf/udf.h"
#include "util/hash_util.hpp"

namespace doris {

// The format of a string-typed slot.
// The returned StringValue of all functions that return StringValue
// shares its buffer the parent.
struct StringValue {
    const static char MIN_CHAR;
    const static char MAX_CHAR;

    static const int MAX_LENGTH = (1 << 30);
    // TODO: change ptr to an offset relative to a contiguous memory block,
    // so that we can send row batches between nodes without having to swizzle
    // pointers
    // NOTE: This struct should keep the same memory layout with Slice, otherwise
    // it will lead to BE crash.
    // TODO(zc): we should unify this struct with Slice some day.
    char* ptr;
    size_t len;

    StringValue(char* ptr, int len) : ptr(ptr), len(len) {}
    StringValue() : ptr(NULL), len(0) {}

    /// Construct a StringValue from 's'.  's' must be valid for as long as
    /// this object is valid.
    explicit StringValue(const std::string& s) : ptr(const_cast<char*>(s.c_str())), len(s.size()) {
        DCHECK_LE(len, MAX_LENGTH);
    }

    void replace(char* ptr, int len) {
        this->ptr = ptr;
        this->len = len;
    }

    // Byte-by-byte comparison. Returns:
    // this < other: -1
    // this == other: 0
    // this > other: 1
    int compare(const StringValue& other) const;

    // ==
    bool eq(const StringValue& other) const;
    bool operator==(const StringValue& other) const { return eq(other); }
    // !=
    bool ne(const StringValue& other) const { return !eq(other); }
    // <=
    bool le(const StringValue& other) const { return compare(other) <= 0; }
    // >=
    bool ge(const StringValue& other) const { return compare(other) >= 0; }
    // <
    bool lt(const StringValue& other) const { return compare(other) < 0; }
    // >
    bool gt(const StringValue& other) const { return compare(other) > 0; }

    bool operator!=(const StringValue& other) const { return ne(other); }

    bool operator<=(const StringValue& other) const { return le(other); }

    bool operator>=(const StringValue& other) const { return ge(other); }

    bool operator<(const StringValue& other) const { return lt(other); }

    bool operator>(const StringValue& other) const { return gt(other); }

    std::string debug_string() const;

    std::string to_string() const;

    // Returns the substring starting at start_pos until the end of string.
    StringValue substring(int start_pos) const;

    // Returns the substring starting at start_pos with given length.
    // If new_len < 0 then the substring from start_pos to end of string is returned.
    StringValue substring(int start_pos, int new_len) const;

    // Trims leading and trailing spaces.
    StringValue trim() const;

    void to_string_val(doris_udf::StringVal* sv) const {
        *sv = doris_udf::StringVal(reinterpret_cast<uint8_t*>(ptr), len);
    }

    static StringValue from_string_val(const doris_udf::StringVal& sv) {
        return StringValue(reinterpret_cast<char*>(sv.ptr), sv.len);
    }

    static StringValue min_string_val();

    static StringValue max_string_val();
};

// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const StringValue& v) {
    return HashUtil::hash(v.ptr, v.len, 0);
}

std::ostream& operator<<(std::ostream& os, const StringValue& string_value);

std::size_t operator-(const StringValue& v1, const StringValue& v2);

} // namespace doris

#endif
