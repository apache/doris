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

// Split one row of text into grams according to a GramScheme: DENSE uses a fixed-length sliding
// window, SPARSE uses a content-defined chunking (CDC) rule that cuts variable-length grams at
// byte-pair boundaries. The split rule depends only on local byte content (a window of at most
// max_len bytes starting at the current position), so when the query side re-extracts grams from
// a literal, the resulting set is necessarily a subset of what the index side extracted from the
// whole row -- which is what lets the compiler fold regex literals.
//
// Ruling R9: an emitted gram never contains a NUL byte (0x00). A candidate window that spans a
// NUL is skipped entirely and emits nothing, while the computation of the window boundaries
// themselves is unaffected (locality is not broken by it); a non-ASCII code point takes the
// 1-gram path and can never contain NUL, so it needs no extra handling. RegexGramCompiler
// likewise degrades a literal or class item containing NUL to a non-indexable unknown character
// (treated as anyChar), so index and query side stay consistent and a NUL byte is never treated
// as indexable by only one of them, which would filter matches away.
// The 16-bit content hash of one byte pair. A pair is a chunk boundary when this is below the
// scheme's density threshold, so it is also the density at which this position starts being a
// boundary -- which is what lets a density be solved from a column's own bytes rather than
// configured for it (see gram_density.h).
uint16_t boundary_hash16(uint8_t a, uint8_t b);

class GramExtractor {
public:
    explicit GramExtractor(const GramScheme& scheme);

    // Extract the grams of one column value; the returned string_view points either into the
    // extractor's internal buffer (when lower_case) or into value itself, and stays valid until
    // the next extract call. Duplicates within the row are removed, keeping a stable order of
    // first appearance. With lower_case=true the input is ASCII-folded before splitting (the
    // boundary hash is computed over the folded bytes).
    void extract(std::string_view value, std::vector<std::string_view>* out);

    // Used by the query side: returns only the grams whose window falls entirely inside s, by
    // exactly the same rule as extract -- a window emitted by extract always falls entirely
    // inside the ASCII segment (or single code point) it belongs to, so this is equivalent to
    // extract, except that it returns std::string values with an independent lifetime, for the
    // compiler to fold the literal fragments of a regex.
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
