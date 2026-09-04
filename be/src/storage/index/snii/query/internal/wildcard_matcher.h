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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

#include "util/utf8_check.h"

namespace doris::snii::query::internal {

// UTF-8 glob matcher with reusable scratch. '*' matches zero or more code points,
// '?' matches exactly one code point, and every other code point is literal. A
// malformed raw dictionary term falls back to the legacy byte semantics because
// keyword indexes can legally contain arbitrary VARCHAR bytes.
// Matching is anchored at both ends. The DP rows are byte-addressed so they can
// reuse their storage across dictionary terms, but transitions are written only
// at validated UTF-8 boundaries. A whole-dictionary scan therefore performs O(1)
// heap allocations for scratch instead of allocating a code-point representation
// for every visited term.
//
// The allocator is templated only so deterministic allocation-counting tests can
// inject a CountingAllocator; production constructs WildcardMatcher<> (default
// std::allocator). The matcher is request-scoped (a stack local of the calling
// wildcard_query frame), holds no shared mutable state, and is not thread-safe by
// design: each query owns its own instance.
template <class Alloc = std::allocator<uint8_t>>
class WildcardMatcher {
public:
    explicit WildcardMatcher(std::string_view pattern)
            : pattern_(pattern), pattern_valid_(is_valid_utf8(pattern)) {}

    bool operator()(std::string_view text) {
        if (!pattern_valid_) {
            return false;
        }
        const bool use_code_points = is_valid_utf8(text);

        const size_t n = text.size() + 1;
        prev_.assign(n, 0); // reuses the buffer; no realloc once capacity >= n
        curr_.assign(n, 0);
        prev_[0] = 1;

        for (size_t pattern_begin = 0; pattern_begin < pattern_.size();) {
            const size_t pattern_width =
                    use_code_points ? code_point_width(pattern_[pattern_begin]) : 1;
            const char pattern_lead = pattern_[pattern_begin];
            std::fill(curr_.begin(), curr_.end(), 0);
            if (pattern_lead == '*') {
                curr_[0] = prev_[0];
                for (size_t text_begin = 0; text_begin < text.size();) {
                    const size_t text_end =
                            text_begin + (use_code_points ? code_point_width(text[text_begin]) : 1);
                    curr_[text_end] = prev_[text_end] || curr_[text_begin];
                    text_begin = text_end;
                }
            } else {
                for (size_t text_begin = 0; text_begin < text.size();) {
                    const size_t text_width =
                            use_code_points ? code_point_width(text[text_begin]) : 1;
                    const size_t text_end = text_begin + text_width;
                    curr_[text_end] = prev_[text_begin] &&
                                      (pattern_lead == '?' ||
                                       (pattern_width == text_width &&
                                        std::memcmp(pattern_.data() + pattern_begin,
                                                    text.data() + text_begin, text_width) == 0));
                    text_begin = text_end;
                }
            }
            prev_.swap(curr_);
            pattern_begin += pattern_width;
        }
        return prev_[text.size()] != 0;
    }

    bool pattern_valid() const { return pattern_valid_; }

    // Test-only debug accessor: the production path never depends on it. Reports
    // the larger of the two scratch-row capacities so perf tests can assert the
    // buffer stops reallocating after warmup.
    size_t scratch_capacity() const { return std::max(prev_.capacity(), curr_.capacity()); }

private:
    static bool is_valid_utf8(std::string_view text) {
        return text.empty() || validate_utf8(text.data(), text.size());
    }

    static size_t code_point_width(char lead) {
        const auto byte = static_cast<uint8_t>(lead);
        if (byte < 0x80) {
            return 1;
        }
        if (byte < 0xE0) {
            return 2;
        }
        if (byte < 0xF0) {
            return 3;
        }
        return 4;
    }

    std::string_view pattern_;
    bool pattern_valid_ = false;
    std::vector<uint8_t, Alloc> prev_;
    std::vector<uint8_t, Alloc> curr_;
};

} // namespace doris::snii::query::internal
