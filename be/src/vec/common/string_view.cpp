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

#include "vec/common/string_view.h"

namespace doris {

bool StringView::operator==(const StringView& other) const {
    // Compare lengths and first 4 characters.
    if (size_and_prefix_as_int64() != other.size_and_prefix_as_int64()) {
        return false;
    }
    if (isInline()) {
        // The inline part is zeroed at construction, so we can compare
        // a word at a time if data extends past 'prefix_'.
        return size_ <= kPrefixSize || inlined_as_int64() == other.inlined_as_int64();
    }
    // Sizes are equal and this is not inline, therefore both are out
    // of line and have kPrefixSize first in common.
    return memcmp(value_.data + kPrefixSize, other.value_.data + kPrefixSize,
                  size_ - kPrefixSize) == 0;
}
int32_t StringView::compare(const StringView& other) const {
    if (prefix_as_int() != other.prefix_as_int()) {
        // The result is decided on prefix. The shorter will be less because the
        // prefix is padded with zeros.
        return memcmp(prefix_, other.prefix_, kPrefixSize);
    }
    int32_t size = std::min(size_, other.size_) - kPrefixSize;
    if (size <= 0) {
        // One ends within the prefix.
        return size_ - other.size_;
    }
    if (isInline() && other.isInline()) {
        int32_t result = memcmp(value_.inlined, other.value_.inlined, size);
        return (result != 0) ? result : size_ - other.size_;
    }
    int32_t result = memcmp(data() + kPrefixSize, other.data() + kPrefixSize, size);
    return (result != 0) ? result : size_ - other.size_;
}

}; // namespace doris