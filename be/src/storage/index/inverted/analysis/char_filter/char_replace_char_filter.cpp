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

#include "storage/index/inverted/analysis/char_filter/char_replace_char_filter.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace doris::segment_v2::inverted_index {

CharReplaceCharFilter::CharReplaceCharFilter(const std::string& pattern,
                                             const std::string& replacement)
        // An empty replacement replaces with NUL, as the reader-based filter did.
        : _replacement(replacement[0]) {
    std::ranges::for_each(pattern, [this](char c) { _patterns.set(static_cast<uint8_t>(c)); });
}

std::string_view CharReplaceCharFilter::apply(std::string_view text, std::string& scratch) {
    const auto matches = [this](char c) { return _patterns.test(static_cast<uint8_t>(c)); };
    size_t first = 0;
    while (first < text.size() && !matches(text[first])) {
        ++first;
    }
    if (first == text.size()) {
        return text;
    }
    scratch.assign(text);
    for (size_t i = first; i < scratch.size(); ++i) {
        if (matches(scratch[i])) {
            scratch[i] = _replacement;
        }
    }
    return scratch;
}

} // namespace doris::segment_v2::inverted_index
