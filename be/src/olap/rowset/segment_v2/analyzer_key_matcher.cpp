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

#include "olap/rowset/segment_v2/analyzer_key_matcher.h"

#include "olap/rowset/segment_v2/inverted_index_iterator.h"

namespace doris::segment_v2 {

AnalyzerMatchResult AnalyzerKeyMatcher::match(
        std::string_view analyzer_key, const std::vector<ReaderEntry>& entries,
        const std::unordered_map<std::string, std::vector<size_t>>& key_index) {
    AnalyzerMatchResult result;

    if (entries.empty()) {
        return result;
    }

    // Step 1: If fallback is allowed (__default__ or empty), return all readers
    // This allows query-type-based selection (e.g., FULLTEXT for MATCH queries,
    // STRING_TYPE for EQUAL queries) rather than being constrained to a single index.
    if (allows_fallback(analyzer_key)) {
        result.candidates.reserve(entries.size());
        for (const auto& entry : entries) {
            result.candidates.push_back(&entry);
        }
        result.used_fallback = true;
        return result;
    }

    // Step 2: Try exact match for explicit analyzer keys
    std::string key_str(analyzer_key);
    auto it = key_index.find(key_str);
    if (it != key_index.end() && !it->second.empty()) {
        result.candidates.reserve(it->second.size());
        for (size_t idx : it->second) {
            if (idx < entries.size()) {
                result.candidates.push_back(&entries[idx]);
            }
        }
    }

    // Step 3: If explicit key with no match, return empty (caller will bypass)
    return result;
}

} // namespace doris::segment_v2
