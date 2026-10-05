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
#include <cstdint>
#include <optional>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/stats_block.h"

namespace doris::snii::format {

struct RegionRef {
    uint64_t offset = 0;
    uint64_t length = 0;
};

struct SectionRefs {
    RegionRef dict_region;
    RegionRef posting_region;
    RegionRef norms;
    RegionRef null_bitmap;
    RegionRef bsbf;
};

// Digest of the highest-df terms of one index: (hash, df) pairs small enough to travel with
// the core metadata and stay resident for the life of the segment.
//
// It answers, with no IO at all, the only question the query-side cost gate asks: is this
// node's candidate set already too large for the index to be worth reading? The gate needs
// min(df) across an AND's terms or max(df) across an OR's, and reading those from the
// dictionary costs one remote round trip per term -- measured at 40 seconds on a segment
// whose cache holds nothing, paid in full before the gate decides to give up.
//
// A miss carries as much information as a hit. The digest holds the top-K terms by df, so a
// term absent from it has df <= df_ceiling, and that upper bound is enough for an AND to
// decide. Empty means no bound is available and the caller must read df the usual way.
struct HighDfTerms {
    std::vector<uint64_t> term_hash; // bsbf_hash of each term, ascending
    std::vector<uint32_t> df;        // parallel to term_hash
    uint32_t df_ceiling = 0;         // every term not listed has df <= this

    bool empty() const { return term_hash.empty(); }

    // df of `hash` when the digest holds it, else df_ceiling -- an upper bound either way.
    // The distinction matters only to a caller that wants to know it was exact.
    uint32_t df_upper_bound(uint64_t hash, bool* exact = nullptr) const {
        const auto it = std::ranges::lower_bound(term_hash, hash);
        if (it != term_hash.end() && *it == hash) {
            if (exact != nullptr) {
                *exact = true;
            }
            return df[static_cast<size_t>(it - term_hash.begin())];
        }
        if (exact != nullptr) {
            *exact = false;
        }
        return df_ceiling;
    }
};

struct CoreMetadata {
    IndexConfig index_config = IndexConfig::kDocsOnly;
    StatsBlock stats;
    SectionRefs section_refs;
    HighDfTerms high_df_terms;
    // The chunking scheme of a gram-family index, recorded by the writer that built it. The
    // query side compiles a pattern against the scheme read back from here, so the split a
    // pattern is derived with is always the split the data was written with. nullopt on every
    // index that is not gram family, which leaves those segments' encoded bytes unchanged.
    std::optional<segment_v2::gram::GramScheme> gram_scheme;
};

Status encode_core_metadata(const CoreMetadata& metadata, ByteSink* out);
Status decode_core_metadata(Slice framed_bytes, CoreMetadata* out);

} // namespace doris::snii::format
