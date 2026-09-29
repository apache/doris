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
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::gram {

// Extract local byte grams for writes and query compilation.
// NUL-containing windows produce no grams.
// Hash a byte pair to decide sparse chunk boundaries.
uint16_t boundary_hash16(uint8_t a, uint8_t b);

class GramExtractor {
public:
    explicit GramExtractor(const GramScheme& scheme);

    // Extract deduplicated row grams. Views remain valid until the next call.
    void extract(std::string_view value, std::vector<std::string_view>* out);

    // Return owned grams wholly contained in the literal.
    void grams_of_literal(std::string_view s, std::vector<std::string>* out);

    const GramScheme& scheme() const { return _scheme; }

    // Retunes the boundary rate. The write path calls this once, after solving the density
    // from the column's own bytes and before any of that segment's rows are tokenized, so
    // every row of a segment is cut at one rate and the segment records the rate it used.
    // Nothing may call it once rows have been emitted: the query side reconstructs a segment's
    // grams from that recorded rate, and two rates inside one segment would leave the rows cut
    // at the other one unreachable.
    void set_density_permille(uint16_t density_permille);

    // Bytes of scratch capacity this extractor keeps between calls. All three buffers are sized
    // by the longest row seen so far and are never shrunk, so this is the extractor's steady
    // resident cost -- which is what the SNII writer mirrors into its MemoryReporter. It is not
    // the peak of one extract() call: `out` belongs to the caller and is counted there.
    size_t reserved_bytes() const {
        return _folded.capacity() + _is_boundary_at.capacity() +
               _dedupe_slots.capacity() * sizeof(uint32_t);
    }

    // Boundary test for one byte pair. Compute the deterministic hash on demand: tokenizers
    // are created per indexed value, so enumerating all 65536 pairs in the constructor would
    // dominate short-row extraction (and do entirely unused work in DENSE mode).
    bool is_boundary(uint8_t a, uint8_t b) const;

private:
    // Split one pure-ASCII segment into grams per the scheme (DENSE fixed-length sliding window
    // / SPARSE CDC rule).
    void _ascii_segment(std::string_view seg, std::vector<std::string_view>* out);
    // Deduplicate within the row, preserving the order of first appearance.
    void _dedupe(std::vector<std::string_view>* out);

    GramScheme _scheme;
    uint64_t _boundary_threshold;
    std::string _folded; // ASCII-folded copy used when lower_case; output views may point here
    std::vector<uint8_t> _is_boundary_at; // per-position boundary flags reused in SPARSE mode
    // Open-addressed slot table used by _dedupe, reused across rows. Holds indices into the
    // caller's output vector, not the grams themselves; see _dedupe for the layout.
    std::vector<uint32_t> _dedupe_slots;
};

} // namespace doris::segment_v2::gram
