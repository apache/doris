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
#include <vector>

namespace doris::segment_v2::inverted_index::spimi {

// One document's decoded posting data: the doc_id, its term frequency,
// and (when positions are available) the list of token positions within
// that document.
struct DecodedDoc {
    int32_t doc_id = 0;
    int32_t freq = 0;
    std::vector<int32_t> positions; // populated only when has_prox=true
};

// Decodes a single term's `.frq` and `.prx` blocks into a vector of
// `DecodedDoc`.  This extends `SpimiTermDocsReader::ReadTerm` (which
// only recovers doc_id + freq) by additionally decoding position
// deltas from the `.prx` stream.
//
// Supports every `.frq`/`.prx` envelope the writer produces:
//   - SLIM kDefault (is_slim, df < skip_interval): per-doc VInt deltas with NO
//     leading codec byte and NO VInt(doc_count) — dispatched via is_slim, not a
//     codec byte
//   - `.frq` `kCodeModeSpimiPfor` (0x05): PFOR bit-packed sub-blocks
//   - `.frq` `kCodeModeSpimiWindowed` (0x06): V4 windowed framing
//   - `.frq` `kCodeModeZstd` (0x80): whole-term ZSTD wrapper around the PFOR
//     inner mode (never wraps a slim kDefault block)
//   - `.prx` `kProxRaw` / `kProxZstd`: raw or whole-term ZSTD-compressed
//     position-delta stream
//
// The segment merger uses this to recover the raw posting list from
// each spill segment so it can re-encode the merged list through
// `FreqProxEncoder`.
class PostingDecoder {
public:
    // Decodes one term's posting data.
    //
    // `frq_data` / `frq_length`: the byte range starting at the term's
    // `freq_pointer` in the `.frq` file.  Must contain at least the
    // term's block (extra trailing bytes — skip list or next term —
    // are ignored).
    //
    // `prx_data` / `prx_length`: the byte range starting at the term's
    // `prox_pointer` in the `.prx` file.  May be null/zero when
    // `has_prox` is false.
    //
    // `doc_freq`: from the `.tis` TermInfo — number of documents.
    // `has_prox`: field-level flag (!omit_term_freq_and_positions).
    // `is_slim`: TermInfo::is_slim (true ⇔ df < skip_interval ⇔ the SLIM
    //   kDefault layout with NO codec byte and NO VInt(doc_count)). When true
    //   the `.frq` decode skips the codec-byte dispatch and reads exactly
    //   `doc_freq` per-doc VInt deltas. Defaults false for the rare caller that
    //   holds a codec-byte-prefixed (PFOR/windowed/ZSTD) block.
    //
    // Returns documents in ascending doc_id order.
    static std::vector<DecodedDoc> Decode(const uint8_t* frq_data, size_t frq_length,
                                          const uint8_t* prx_data, size_t prx_length,
                                          int32_t doc_freq, bool has_prox, bool is_slim = false);

private:
    // Decodes only the `.frq` stream into per-doc {doc_id, freq}, resolving a
    // whole-term `kCodeModeZstd` envelope (recursing on the decompressed inner
    // block). Positions are attached separately by `Decode`. `is_slim` selects
    // the no-codec-byte SLIM kDefault fast path (see Decode).
    static std::vector<DecodedDoc> DecodeInner(const uint8_t* frq_data, size_t frq_length,
                                               int32_t doc_freq, bool has_prox, bool is_slim);
};

} // namespace doris::segment_v2::inverted_index::spimi
