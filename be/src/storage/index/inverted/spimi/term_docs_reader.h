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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

// Reader-side decoder for SPIMI-written `.frq` term blocks. Handles
// both encodings the SPIMI writer emits in Phase 35:
//
//   - `kCodeModeDefault = 0x00`: byte-equal to what CLucene's
//     `SDocumentWriter::appendPostings:1330` writes for the same term.
//     `VInt(docFreq)` followed by per-doc (`deltaDoc << 1 | freq_bit`
//     [+ VInt(freq) if freq != 1]) when `has_prox`; or raw
//     `VInt(deltaDoc)` when `omit_tfap`.
//
//   - `kCodeModeSpimiPfor = 0x05`: Phase 35 PFOR encoding. One or
//     more `SpimiPforEncoder` sub-blocks covering all doc_deltas,
//     followed by — if has_prox — the same number of sub-blocks for
//     freqs. Each sub-block carries its own count + bit-width.
//
// This decoder is intentionally Doris-namespaced (not part of the
// CLucene reader) so it can evolve without touching the `contrib/`
// fork. The cutover plan calls for routing the production reader
// through this decoder once the SPIMI writer becomes the default
// (see SPIMI_DESIGN.md § 9.1b). Today its primary consumer is the
// round-trip regression test in `term_docs_reader_test.cpp`.
class SpimiTermDocsReader {
public:
    // (doc_id, freq) pair recovered from a single term's `.frq` block.
    // `freq` is 1 for terms emitted under `omit_term_freq_and_positions`
    // (the field's frequency was omitted at write time).
    using DocFreq = std::pair<int32_t, int32_t>;

    // Reads one term's `.frq` block starting at `frq_data[0]`. The
    // caller positions the pointer at the term's `freq_pointer` from
    // `.tis` and supplies `frq_length` = at-least the term's block
    // size (extra trailing bytes — e.g. skip-list bytes or
    // subsequent terms — are ignored). `doc_freq` is the term's
    // document frequency from `.tis`. `has_prox` is the field's
    // `!omit_term_freq_and_positions` flag (from `.fnm`).
    //
    // Returns (doc_id, freq) pairs in ascending doc_id order. The
    // decoder consumes exactly `doc_freq` doc records and stops.
    // Crashes via `LOG(FATAL)` on a malformed block — production
    // callers must wrap in a try/catch once wired into the query
    // path; today's callers are tests where a panic is the right
    // signal.
    static std::vector<DocFreq> ReadTerm(const uint8_t* frq_data, size_t frq_length,
                                         int32_t doc_freq, bool has_prox);

    // Convenience overload for callers that hold the whole `.frq` as
    // a vector (i.e. unit tests, where the buffer starts at the only
    // term's freq_pointer = 0).
    static std::vector<DocFreq> ReadTerm(const std::vector<uint8_t>& frq_bytes, int32_t doc_freq,
                                         bool has_prox) {
        return ReadTerm(frq_bytes.data(), frq_bytes.size(), doc_freq, has_prox);
    }
};

} // namespace doris::segment_v2::inverted_index::spimi
