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
#include <memory>
#include <string_view>
#include <vector>

namespace doris::snii::query::internal {

// Glob matcher with reusable scratch. '*' matches >=0 bytes, '?' matches exactly
// one byte, every other byte is literal; matching is anchored at both ends (full
// match). The matching result is bit-for-bit identical to the former per-call DP
// in wildcard_query.cpp: the only change is that the two DP rows are constructed
// once and reused (assign(), never reallocated once capacity is large enough)
// across every term in a single expansion. A whole-dictionary scan therefore
// performs O(1) heap allocations for scratch instead of O(2N) -- two small
// std::vector<uint8_t> constructions per visited term.
//
// The allocator is templated only so deterministic allocation-counting tests can
// inject a CountingAllocator; production constructs WildcardMatcher<> (default
// std::allocator). The matcher is request-scoped (a stack local of the calling
// wildcard_query frame), holds no shared mutable state, and is not thread-safe by
// design: each query owns its own instance.
template <class Alloc = std::allocator<uint8_t>>
class WildcardMatcher {
public:
    explicit WildcardMatcher(std::string_view pattern) : pattern_(pattern) {}

    bool operator()(std::string_view text) {
        const size_t n = text.size() + 1;
        prev_.assign(n, 0); // reuses the buffer; no realloc once capacity >= n
        curr_.assign(n, 0);
        prev_[0] = 1;
        for (char p : pattern_) {
            std::fill(curr_.begin(), curr_.end(), 0);
            if (p == '*') {
                curr_[0] = prev_[0];
                for (size_t i = 1; i < n; ++i) {
                    curr_[i] = prev_[i] || curr_[i - 1];
                }
            } else {
                for (size_t i = 1; i < n; ++i) {
                    curr_[i] = prev_[i - 1] && (p == '?' || p == text[i - 1]);
                }
            }
            prev_.swap(curr_);
        }
        return prev_[text.size()] != 0;
    }

    // Test-only debug accessor: the production path never depends on it. Reports
    // the larger of the two scratch-row capacities so perf tests can assert the
    // buffer stops reallocating after warmup.
    size_t scratch_capacity() const { return std::max(prev_.capacity(), curr_.capacity()); }

private:
    std::string_view pattern_;
    std::vector<uint8_t, Alloc> prev_;
    std::vector<uint8_t, Alloc> curr_;
};

} // namespace doris::snii::query::internal
