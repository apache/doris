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
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"
#include "storage/index/inverted/spimi/skip_list_writer.h"
#include "storage/index/inverted/spimi/term_dict_writer.h"

namespace doris::segment_v2::inverted_index::spimi {

// Streaming encoder for the Lucene 2.x V0/V1 freq (`.frq`) and prox (`.prx`)
// formats. Targets the "with positions, no payloads" configuration used by
// Doris fulltext indexes: SPIMI segments are written in V0/V1 (pure VInt,
// no PFOR), which the existing CLucene reader still consumes. PFOR / V4
// emission can be layered on top in a follow-up without changing this API.
//
// Format produced (per term, with hasProx = true, storePayloads = false):
//
//   .frq term block:
//     for each doc d_i in ascending order:
//       vint  (doc_delta_i << 1) | (freq_i == 1 ? 1 : 0)
//       vint  freq_i             (only when freq_i != 1)
//     skip list                  (emitted by SkipListWriter; see below)
//
//   .prx term block:
//     for each doc d_i in ascending order:
//       for each position p_j in ascending order within doc d_i:
//         vint  position_delta_j   (delta resets to 0 at every new doc)
//
// Skip-list integration: every skip-interval-th doc boundary (i.e. when the
// running doc count modulo skip_interval == 0), the encoder records the
// current .frq / .prx pointers and the previous doc id into the
// SkipListWriter. After the last doc, FinishTerm() flushes the skip-list
// bytes into .frq right after the term's docs and returns the term's
// persistent metadata for the term dictionary.
//
// Lifecycle: StartTerm → StartDoc → (AddPosition...) → FinishDoc → ... →
// FinishTerm. The encoder is reusable across terms via StartTerm; the
// caller passes the term's known doc frequency so the skip-list level
// count is precise.
class FreqProxEncoder {
public:
    static constexpr int32_t kDefaultSkipInterval = TermDictWriter::kDefaultSkipInterval;
    static constexpr int32_t kDefaultMaxSkipLevels = TermDictWriter::kMaxSkipLevels;

    // Phase 35 — `.frq` block-mode constants. The first byte of every
    // term's `.frq` payload identifies which block encoding follows so
    // the future SPIMI reader can dispatch. These values are
    // deliberately distinct from CLucene's `CodeMode` enum (which uses
    // 0..4 for kDefault / kPfor / kRange / kPfor256 / kPfor128) so a
    // CLucene reader can't accidentally misinterpret a SPIMI block.
    static constexpr uint8_t kCodeModeDefault = 0x00;
    static constexpr uint8_t kCodeModeSpimiPfor = 0x05;

    FreqProxEncoder(ByteOutput* frq_out, ByteOutput* prx_out,
                    int32_t skip_interval = kDefaultSkipInterval,
                    int32_t max_skip_levels = kDefaultMaxSkipLevels,
                    bool omit_term_freq_and_positions = false);

    FreqProxEncoder(const FreqProxEncoder&) = delete;
    FreqProxEncoder& operator=(const FreqProxEncoder&) = delete;

    // Begin a new term posting list. `expected_doc_freq` is the term's
    // exact document frequency (number of distinct docs containing the
    // term). It is used to size the skip-list level count and is validated
    // against the actual emitted doc count at FinishTerm().
    void StartTerm(int32_t expected_doc_freq);

    // Begin a new document within the current term. `doc_id` must be
    // strictly greater than the previous StartDoc's doc_id. `freq` is the
    // number of AddPosition() calls that will follow before FinishDoc().
    void StartDoc(int32_t doc_id, int32_t freq);

    // Record one token position for the current doc. Positions must be
    // non-decreasing within a doc; ties are allowed (delta == 0).
    void AddPosition(int32_t position);

    // Ends the current document. Asserts that AddPosition() was called
    // exactly `freq` times since the matching StartDoc().
    void FinishDoc();

    // Closes the term: writes the skip-list bytes into `.frq` right after
    // the term's docs and returns the term's TermInfo for the term
    // dictionary (`freq_pointer` / `prox_pointer` are the absolute byte
    // offsets at the term's first doc; the dict writer converts them to
    // deltas).
    TermInfo FinishTerm();

private:
    ByteOutput* _frq_out;
    ByteOutput* _prx_out;
    int32_t _skip_interval;
    bool _omit_tfap; // when true: doc-id-only .frq, no .prx writes at all
    SkipListWriter _skip_list_writer;

    bool _term_open = false;
    bool _doc_open = false;
    int32_t _expected_doc_freq = 0;
    int64_t _term_freq_start = 0;
    int64_t _term_prox_start = 0;
    int32_t _doc_freq = 0;
    int32_t _last_doc = 0;
    int32_t _current_freq = 0;
    int32_t _positions_remaining = 0;
    int32_t _last_position_in_doc = 0;

    // Phase 35 PFOR buffered mode. When `expected_doc_freq >=
    // skip_interval`, StartDoc buffers (doc_delta, freq) into these
    // vectors instead of writing immediately. FinishTerm flushes the
    // buffers as one or more PFOR sub-blocks via `SpimiPforEncoder`.
    bool _use_pfor = false;
    std::vector<uint32_t> _pfor_doc_deltas;
    std::vector<uint32_t> _pfor_freqs;
};

} // namespace doris::segment_v2::inverted_index::spimi
