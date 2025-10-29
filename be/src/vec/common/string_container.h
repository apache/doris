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
// https://github.com/facebookincubator/velox/blob/main/velox/type/StringView.h
// And modified by Doris

#pragma once

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "string_ref.h"

namespace doris {
// Variable length string or binary type for use in vectors. This has
// semantics similar to std::string_view or folly::StringPiece and
// exposes a subset of the interface. If the string is 12 characters
// or less, it is inlined and no reference is held. If it is longer, a
// reference to the string is held and the 4 first characters are
// cached in the StringContainer. This allows failing comparisons early and
// reduces the CPU cache working set when dealing with short strings.

class StringContainer {
#include "common/compile_check_begin.h"
public:
    using value_type = char;
    static constexpr size_t kPrefixSize = 4 * sizeof(char);
    static constexpr size_t kInlineSize = 12;

    StringContainer() {
        static_assert(sizeof(StringContainer) == 16);
        memset(this, 0, sizeof(StringContainer));
    }

    StringContainer(const char* data, uint32_t len) : size_(len) {
        DCHECK_GE(len, 0);
        DCHECK(data || len == 0);
        if (isInline()) {
            // Zero the inline part.
            // this makes sure that inline strings can be compared for equality with 2
            // int64 compares.
            memset(prefix_, 0, kPrefixSize);
            if (size_ == 0) {
                return;
            }
            // small string: inlined. Zero the last 8 bytes first to allow for whole
            // word comparison.
            value_.data = nullptr;
            memcpy(prefix_, data, size_);
        } else {
            // large string: store pointer
            memcpy(prefix_, data, kPrefixSize);
            value_.data = data;
        }
    }

    StringContainer(unsigned char* data, uint32_t len)
            : StringContainer(reinterpret_cast<const char*>(data), len) {}

    bool isInline() const { return isInline(size_); }

    ALWAYS_INLINE static constexpr bool isInline(uint32_t size) { return size <= kInlineSize; }

    explicit StringContainer(std::string&& value) = delete;
    explicit StringContainer(const std::string& value)
            : StringContainer(value.data(), cast_set<uint32_t>(value.size())) {}
    explicit StringContainer(std::string_view value)
            : StringContainer(value.data(), cast_set<uint32_t>(value.size())) {}
    /* implicit */ StringContainer(const char* data)
            : StringContainer(data, cast_set<uint32_t>(strlen(data))) {}
    doris::StringRef to_string_ref() const { return {data(), size()}; }

    operator std::string_view() && = delete;
    explicit operator std::string_view() const& { return {data(), size()}; }
    operator std::string() const { return std::string(data(), size()); }
    std::string str() const { return *this; }

    const char* data() && = delete;
    const char* data() const& { return isInline() ? prefix_ : value_.data; }

    uint32_t size() const { return size_; }
    bool empty() const { return size() == 0; }

    void set_size(uint32_t size) { size_ = size; }

    bool operator==(const StringContainer& other) const;
    friend std::ostream& operator<<(std::ostream& os, const StringContainer& StringContainer) {
        os.write(StringContainer.data(), StringContainer.size());
        return os;
    }
    auto operator<=>(const StringContainer& other) const {
        const auto cmp = compare(other);
        return cmp < 0   ? std::strong_ordering::less
               : cmp > 0 ? std::strong_ordering::greater
                         : std::strong_ordering::equal;
    }

    // Returns 0, if this == other
    //       < 0, if this < other
    //       > 0, if this > other
    int32_t compare(const StringContainer& other) const;

    const char* begin() && = delete;
    const char* begin() const& { return data(); }
    const char* end() && = delete;
    const char* end() const& { return data() + size(); }

    std::string dump_hex() const {
        static const char* kHex = "0123456789ABCDEF";
        std::string out;
        out.reserve(size_ * 2 + 3);
        out.push_back('X');
        out.push_back('\'');
        const char* ptr = data();
        for (uint32_t i = 0; i < size_; ++i) {
            auto c = static_cast<unsigned char>(ptr[i]);
            out.push_back(kHex[c >> 4]);
            out.push_back(kHex[c & 0x0F]);
        }
        out.push_back('\'');
        return out;
    }

private:
    inline int64_t size_and_prefix_as_int64() const {
        return reinterpret_cast<const int64_t*>(this)[0];
    }

    inline int64_t inlined_as_int64() const { return reinterpret_cast<const int64_t*>(this)[1]; }

    int32_t prefix_as_int() const { return *reinterpret_cast<const int32_t*>(&prefix_); }

    uint32_t size_;
    char prefix_[4];
    union {
        char inlined[8];
        const char* data;
    } value_;
};
#include "common/compile_check_end.h"
} // namespace doris