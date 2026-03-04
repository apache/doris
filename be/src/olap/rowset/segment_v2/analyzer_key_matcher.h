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

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "olap/inverted_index_parser.h"

namespace doris::segment_v2 {

// Forward declaration
struct ReaderEntry;

// Result of analyzer key matching operation.
// Contains candidate readers that match the requested analyzer key.
struct AnalyzerMatchResult {
    // Pointers to matching reader entries. Valid only within the scope of
    // the InvertedIndexIterator that owns the entries.
    std::vector<const ReaderEntry*> candidates;

    // True if fallback strategy was used (no exact match found)
    bool used_fallback = false;

    // Convenience methods
    bool empty() const { return candidates.empty(); }
    size_t size() const { return candidates.size(); }
};

// Utility class for matching analyzer keys against reader entries.
// Stateless - all methods are static.
//
// Matching strategy:
// 1. If key is empty (user did not specify), return all readers for query-type-based selection
// 2. If key is non-empty (user specified), try exact match
// 3. If explicit key with no match, return empty (caller decides whether to bypass)
class AnalyzerKeyMatcher {
public:
    // Match analyzer key against reader entries.
    // @param analyzer_key: Normalized analyzer key (lowercase, or empty for auto-select)
    // @param entries: Available reader entries
    // @param key_index: Index mapping analyzer_key -> entry indices
    // @return MatchResult with candidates and fallback flag
    static AnalyzerMatchResult match(
            std::string_view analyzer_key, const std::vector<ReaderEntry>& entries,
            const std::unordered_map<std::string, std::vector<size_t>>& key_index);

    // Check if analyzer key is explicitly specified by user.
    // Empty string means "user did not specify" (auto-select mode).
    // Non-empty means "user wrote USING ANALYZER xxx".
    static bool is_explicit(std::string_view key) { return !key.empty(); }

    // Check if fallback (returning all readers) is allowed for this key.
    // Only empty key (user did not specify) allows fallback.
    static bool allows_fallback(std::string_view key) { return key.empty(); }
};

} // namespace doris::segment_v2
