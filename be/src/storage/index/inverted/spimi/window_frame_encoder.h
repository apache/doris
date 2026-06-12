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

#include "storage/index/inverted/spimi/byte_output.h"

typedef struct ZSTD_CCtx_s ZSTD_CCtx; // NOLINT(modernize-use-using)

namespace doris::segment_v2::inverted_index::spimi {

// V4 "windowed" `.frq` / `.prx` posting-block encoder.
//
// MOTIVATION & THE PRIOR BUG IT FIXES
// -----------------------------------
// A term's docs are chopped into "finest units" of up to 256 consecutive docs
// (the LAST unit of a term may hold fewer). Several adjacent units are then
// grouped into a "window" of W = 256/512/1024/2048 docs (units_per_window
// k = W/256, CLAMPED to >= 1). Each window is compressed (ZSTD) independently
// and prefixed with a decode-free skip entry (doc range + byte offset), so a
// future reader can range-GET / skip a single window without inflating its
// neighbours.
//
// The CRITICAL invariant — the whole reason this is a separate, carefully
// documented helper — is HOW a window's inner bytes are composed from its
// units. The `.frq` decoder (term_docs_reader / posting_decoder) reads, per
// window covering `win_doc_count` docs:
//
//     DecodePforRun(cur, win_doc_count)   // ALL doc-deltas, as one PFOR run
//     DecodePforRun(cur, win_doc_count)   // THEN ALL freqs, as one PFOR run
//
// i.e. all doc-deltas first, then all freqs. The finest unit therefore stores
// its doc-deltas and freqs (and, for `.prx`, its positions) as SEPARATE,
// independently-decodable byte streams ("parts"). A window of k units composes
// its inner bytes PART-WISE:
//
//     inner = concat_j (unit[i+j].PART_DD)  ++  concat_j (unit[i+j].PART_FQ)
//
// NOT unit-wise. Concatenating whole pre-encoded units
// ([u0.dd][u0.fq][u1.dd][u1.fq]...) would interleave the streams; after
// DecodePforRun consumed all the window's doc-deltas it would run straight into
// a freq sub-block header and throw "PFOR sub-block count out of range". The
// part-wise layout is exactly what the decoder expects: every unit's PART_DD is
// a run of valid 128-value PFOR sub-blocks, so concatenating them yields one
// longer valid PFOR run of ALL the window's doc-deltas (doc-deltas are
// continuous across units because each unit's first doc-delta is taken relative
// to the previous unit's last doc — the running last_doc threads through the
// whole term, never resetting per unit).
//
// `.prx` has a single part (PART_POS = the VInt position-delta bytes); a
// window's inner bytes = concat of its units' PART_POS bytes = the contiguous
// VInt position stream for the window's docs.
//
// BYTE LAYOUT — `.frq` block (bytes at TermInfo.freq_pointer)
// -----------------------------------------------------------
//   byte    kCodeModeSpimiWindowed (0x06)
//   byte    inner_mode             // 0x05 PFOR parts, 0x00 VInt parts
//   VInt    W                      // finest window doc-width (256..2048)
//   VInt    num_windows            // >= 1
//   per window w:                  // SLIM skip table, written BEFORE payloads
//     VInt  win_byte_offset_delta  // payload-tuple byte offset of window w MINUS
//                                  //   that of window w-1 (== window w-1's payload
//                                  //   length); 0 for w==0. Reader running-sums to
//                                  //   the absolute offset (relative to the first
//                                  //   payload tuple).
//     VInt  win_min_docid_delta    // (first abs docid in window w) MINUS
//                                  //   (max docid of window w-1); == win_min_docid
//                                  //   for w==0 (prev max seeds at 0). Reader
//                                  //   running-sums to the absolute min_docid.
//     VInt  win_max_docid_delta    // (max docid in window) - win_min_docid
//   // win_doc_count is NOT stored: the reader derives it as
//   //   min(W, doc_freq - docs_so_far)
//   // because every non-last window is exactly W docs and only the term's last
//   // unit (hence its last window) may be a partial 256-doc unit.
//   per window w (in order):       // payloads
//     byte  win_mode               // 0 raw, 1 ZSTD
//     VInt  uncomp_len             // length of inflated part-wise inner bytes
//     VInt  comp_len               // ONLY when win_mode == 1
//     bytes payload
//
// BYTE LAYOUT — `.prx` block (bytes at TermInfo.prox_pointer)
// -----------------------------------------------------------
//   byte    kProxWindowed (0x02)
//   VInt    W                       // .prx-own window width (DECOUPLED from .frq)
//   VInt    num_windows
//   per window w:                   // skip table, written BEFORE payloads
//     VInt  doc_count
//     VInt  win_byte_offset         // relative to the first payload tuple
//     VInt  min_docid
//     VInt  max_docid - min_docid
//   per window w (in order):
//     byte  win_mode               // 0 raw, 1 ZSTD
//     VInt  uncomp_len
//     VInt  comp_len               // ONLY when win_mode == 1
//     bytes payload                // window's part-wise VInt position-deltas
//
// `.prx` windows are framed INDEPENDENTLY of `.frq` (W/num_windows derive from
// config inverted_index_spimi_prx_window_docs + df, never from the .frq search),
// and carry their own skip table so a `.prx` window self-locates without the .frq
// table. Both streams cut on whole 256-doc units, so every `.prx` window's doc
// range is an exact union of `.frq` windows. The decoder threads a single running
// last_doc across windows (window boundaries are plain doc boundaries — no
// re-basing), so concatenating the per-window decoded runs materializes the whole
// term; per-doc position counts come from the covering .frq window(s)' freqs.
class WindowFrameEncoder {
public:
    // `inner_mode` selector values, written as the second `.frq` byte.
    static constexpr uint8_t kInnerPfor = 0x05;
    static constexpr uint8_t kInnerVInt = 0x00;
    // Per-window `win_mode` values.
    static constexpr uint8_t kWinRaw = 0;
    static constexpr uint8_t kWinZstd = 1;

    // Finest unit doc width and the adaptive candidate W set.
    static constexpr int32_t kUnitDocs = 256;

    // Encodes one term.
    //   doc_deltas : the term's df doc-deltas (delta[0] = first_docid - 0).
    //   freqs      : the term's df freqs, or empty when has_prox == false.
    //   pos_vint   : the term's whole VInt position-delta stream (doc-then-
    //                position order, delta resets to 0 per doc). Empty when
    //                has_prox == false.
    //   pos_counts_per_doc : pos_counts_per_doc[i] = number of VInt position
    //                deltas (== freq) for doc i, so PART_POS can be sliced at
    //                doc boundaries. Empty when has_prox == false.
    //   has_prox   : whether freqs + positions are present.
    //   cctx       : reused ZSTD compression context (may be null → no ZSTD).
    //   frq_out    : receives the whole windowed `.frq` block.
    //   prx_out    : receives the whole windowed `.prx` block (only touched
    //                when has_prox).
    //   pos_doc_byte_offsets : optional per-doc byte offsets into pos_vint
    //                (offset where doc i's positions start; the end boundary is
    //                pos_vint.size()). When provided, PART_POS is sliced from
    //                these directly; when null, the boundaries are recovered by
    //                scanning the whole VInt stream with pos_counts_per_doc
    //                (slower — kept for callers that don't track offsets,
    //                e.g. tests).
    static void Encode(const std::vector<uint32_t>& doc_deltas, const std::vector<uint32_t>& freqs,
                       const std::vector<uint8_t>& pos_vint,
                       const std::vector<uint32_t>& pos_counts_per_doc, bool has_prox,
                       ZSTD_CCtx* cctx, ByteOutput* frq_out, ByteOutput* prx_out,
                       const std::vector<uint32_t>* pos_doc_byte_offsets = nullptr);
};

} // namespace doris::segment_v2::inverted_index::spimi
