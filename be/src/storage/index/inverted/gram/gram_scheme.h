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
#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"

namespace doris::segment_v2::gram {

// Encoding mode of a gram scheme: DENSE emits every gram from a byte-by-byte sliding window,
// SPARSE emits hash-sampled sparse grams. auto is a third, user-visible value, but it exists
// only on the write side: after the sample is analysed it is stored as one of the two, so auto
// never appears in GramScheme (the source of truth for on-disk data and cache keys).
enum class GramMode : uint8_t { DENSE = 1, SPARSE = 2 };

// All parameters of a gram scheme, the single source of truth for downstream components such as
// GramExtractor and RegexGramCompiler. A GramScheme instance fully determines "which grams a
// given piece of text is split into", so it must be constructible from tokenizer/index
// properties, serializable back to properties (for the segment metadata), and able to produce a
// cache key.
struct GramScheme {
    GramMode mode = GramMode::SPARSE;
    uint32_t min_len = 3;            // n (bytes)
    uint32_t max_len = 16;           // L (bytes, SPARSE only)
    uint32_t density_permille = 250; // p x 1000 (SPARSE only)
    uint32_t stop_df_permille = 100; // tau x 1000; 0 disables high-frequency gram pruning
    bool lower_case = false;
    uint32_t hash_version = 1;

    // Build a GramScheme from tokenizer/index properties; defaults are as above; an illegal
    // value yields InvalidArgument.
    static Status from_properties(const std::map<std::string, std::string>& props, GramScheme* out);
    // Write back to a property table, for persisting the segment metadata and as the input to
    // the cache key computation.
    std::map<std::string, std::string> to_properties() const;
    // Cache key of the form "gram:v1:sparse:3:16:250:100:lc0", uniquely identifying one set of
    // scheme parameters.
    std::string cache_key() const;
    bool operator==(const GramScheme& o) const {
        return mode == o.mode && min_len == o.min_len && max_len == o.max_len &&
               density_permille == o.density_permille && stop_df_permille == o.stop_df_permille &&
               lower_case == o.lower_case && hash_version == o.hash_version;
    }
};

} // namespace doris::segment_v2::gram
