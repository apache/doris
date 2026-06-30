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

#include "storage/index/snii/query/wildcard_query.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/snii/query/internal/term_expansion.h"

namespace doris::snii::query {

namespace {

std::string literal_prefix_for_wildcard(std::string_view pattern) {
    std::string out;
    for (char c : pattern) {
        if (c == '*' || c == '?') {
            break;
        }
        out.push_back(c);
    }
    return out;
}

bool wildcard_match(std::string_view pattern, std::string_view text) {
    std::vector<uint8_t> prev(text.size() + 1, 0);
    std::vector<uint8_t> curr(text.size() + 1, 0);
    prev[0] = 1;

    for (char p : pattern) {
        std::fill(curr.begin(), curr.end(), 0);
        if (p == '*') {
            curr[0] = prev[0];
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i] || curr[i - 1];
            }
        } else {
            for (size_t i = 1; i <= text.size(); ++i) {
                curr[i] = prev[i - 1] && (p == '?' || p == text[i - 1]);
            }
        }
        prev.swap(curr);
    }
    return prev[text.size()] != 0;
}

} // namespace

Status wildcard_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                      std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("wildcard_query: null out");
    }
    docids->clear();
    VectorDocIdSink sink(*docids);
    return wildcard_query(idx, pattern, &sink, max_expansions);
}

Status wildcard_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                      std::vector<uint32_t>* const docids, QueryProfile* profile,
                      int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return wildcard_query(idx, pattern, docids, max_expansions);
}

Status wildcard_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                      DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("wildcard_query: null sink");
    }
    const std::string enum_prefix = literal_prefix_for_wildcard(pattern);
    return internal::emit_expanded_docid_union(
            idx, enum_prefix,
            [pattern](std::string_view term) { return wildcard_match(pattern, term); }, sink,
            max_expansions);
}

} // namespace doris::snii::query
