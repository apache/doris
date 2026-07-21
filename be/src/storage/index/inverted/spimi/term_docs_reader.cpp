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

#include "storage/index/inverted/spimi/term_docs_reader.h"

// `_CLTHROWA` for byte-parser hard-fail on untrusted .frq bytes.

#include <algorithm>

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/frq_window_decode_internal.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// The byte-stream cursor and decode primitives now live in
// `frq_window_decode_internal.h` so the lazy window-addressed reader
// (`window_term_reader.cpp`) shares EXACTLY this code — there is one
// implementation, so eager whole-term decode and lazy per-window decode
// cannot drift byte-for-byte.
using frq_internal::ByteStream;
using frq_internal::DecodePforRun;
using frq_internal::DecompressZstdFrqBlock;
using frq_internal::ReadWindowPayload;

} // namespace

std::vector<SpimiTermDocsReader::DocFreq> SpimiTermDocsReader::ReadTerm(
        const uint8_t* frq_data, size_t frq_length, int32_t doc_freq, bool has_prox, bool is_slim) {
    // Untrusted-byte invariants. `doc_freq` is .tis-validated by the
    // upper layer at this point but defence-in-depth — a negative or
    // zero value on a corrupt segment must not silently produce an
    // empty result. We intentionally do NOT bound `doc_freq` against
    // `frq_length`: PFOR sub-blocks are bit-packed (down to ~0.1 byte
    // per doc at width=1), so doc_freq can legitimately exceed
    // frq_length. The byte-bounds checks inside the decoder
    // (`ByteStream::ReadByte`, `DecodePforRun`) will catch a truly
    // truncated buffer.
    if (doc_freq <= 0 || frq_length == 0U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("SPIMI .frq ReadTerm: bad doc_freq / buffer length");
    }

    ByteStream cur(frq_data, frq_length);

    if (is_slim) {
        // SLIM kDefault block (df < skip_interval): NO codec byte, NO
        // VInt(doc_count). The block is exactly `doc_freq` per-doc VInt deltas.
        // doc_freq is authoritative from .tis, so the loop never over-reads.
        std::vector<DocFreq> out;
        constexpr size_t kSlimReserveCap = 1U << 24;
        out.reserve(std::min(static_cast<size_t>(doc_freq), kSlimReserveCap));
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            if (has_prox) {
                const auto code = static_cast<uint32_t>(cur.ReadVInt());
                last_doc += static_cast<int32_t>(code >> 1U);
                const int32_t freq = ((code & 1U) != 0) ? 1 : cur.ReadVInt();
                out.emplace_back(last_doc, freq);
            } else {
                last_doc += static_cast<int32_t>(cur.ReadVInt());
                out.emplace_back(last_doc, 1);
            }
        }
        return out;
    }

    const uint8_t mode = cur.ReadByte();
    if (mode == FreqProxEncoder::kCodeModeZstd) {
        // Whole-term .frq was ZSTD-compressed: decompress, then decode the inner
        // block. Only the PFOR path (df >= skip_interval) is ever ZSTD-wrapped,
        // so the inner block always begins with its kCodeModeSpimiPfor byte —
        // never a SLIM kDefault block (which is never wrapped). is_slim stays
        // false through the recursion so the inner codec-byte dispatch runs.
        const std::vector<uint8_t> raw = DecompressZstdFrqBlock(cur);
        return ReadTerm(raw.data(), raw.size(), doc_freq, has_prox, /*is_slim=*/false);
    }
    if (mode == FreqProxEncoder::kCodeModeSpimiWindowed) {
        // V4 windowed block: inner_mode, W, num_windows, per-window skip table,
        // then per-window payloads. Decode windows in order, threading last_doc
        // across them, to materialize the whole term.
        const uint8_t inner_mode = cur.ReadByte();
        const int32_t W = cur.ReadVInt(); // window doc-width; derives win_doc_count
        if (W <= 0) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: non-positive W");
        }
        const int32_t num_windows = cur.ReadVInt();
        if (num_windows <= 0 || num_windows > doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: num_windows out of range");
        }
        // SLIM skip table (3 VInts/window): win_doc_count is DERIVED, not stored.
        // Every non-last window is exactly W docs; only the term's last window may
        // be a partial unit, so win_doc_count = min(W, remaining). We only need the
        // counts here (whole-term sequential decode), so the delta-coded offsets /
        // min_docids are stepped over.
        std::vector<int32_t> win_doc_count(static_cast<size_t>(num_windows));
        int64_t total = 0;
        for (int32_t w = 0; w < num_windows; ++w) {
            (void)cur.ReadVInt(); // win_byte_offset_delta (range-GET only; unused here)
            (void)cur.ReadVInt(); // win_min_docid_delta
            (void)cur.ReadVInt(); // win_max_docid_delta
            const int32_t c = static_cast<int32_t>(std::min<int64_t>(W, doc_freq - total));
            if (c <= 0 || c > doc_freq) [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: derived win_doc_count out of range");
            }
            win_doc_count[static_cast<size_t>(w)] = c;
            total += c;
        }
        if (total != doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("SPIMI .frq windowed: window doc counts disagree with .tis");
        }
        std::vector<DocFreq> out;
        constexpr size_t kSafeReserveCap = 1U << 24;
        out.reserve(std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));
        int32_t last_doc = 0;
        for (int32_t w = 0; w < num_windows; ++w) {
            const std::vector<uint8_t> inner = ReadWindowPayload(cur);
            const int32_t wc = win_doc_count[static_cast<size_t>(w)];
            ByteStream wcur(inner.data(), inner.size());
            if (inner_mode == FreqProxEncoder::kCodeModeSpimiPfor) {
                // PART-WISE: all wc doc-deltas first (one PFOR run), then all wc
                // freqs (one PFOR run). doc-deltas are continuous across units,
                // so a window covering multiple 256-doc units decodes as one run.
                const auto dd = DecodePforRun(wcur, wc);
                std::vector<uint32_t> fq;
                if (has_prox) {
                    fq = DecodePforRun(wcur, wc);
                }
                for (int32_t i = 0; i < wc; ++i) {
                    last_doc += static_cast<int32_t>(dd[static_cast<size_t>(i)]);
                    const int32_t f =
                            has_prox ? static_cast<int32_t>(fq[static_cast<size_t>(i)]) : 1;
                    out.emplace_back(last_doc, f);
                }
            } else if (inner_mode == FreqProxEncoder::kCodeModeDefault) {
                for (int32_t i = 0; i < wc; ++i) {
                    if (has_prox) {
                        const auto code = static_cast<uint32_t>(wcur.ReadVInt());
                        last_doc += static_cast<int32_t>(code >> 1U);
                        const int32_t freq = ((code & 1U) != 0) ? 1 : wcur.ReadVInt();
                        out.emplace_back(last_doc, freq);
                    } else {
                        last_doc += static_cast<int32_t>(wcur.ReadVInt());
                        out.emplace_back(last_doc, 1);
                    }
                }
            } else [[unlikely]] {
                SPIMI_THROW_CORRUPT("SPIMI .frq windowed: unknown inner_mode");
            }
        }
        return out;
    }
    std::vector<DocFreq> out;
    // Cap pre-reserve against the same DoS-bounding upper limit as
    // `DecodePforRun` above. Vector grows organically beyond the
    // cap via push_back if `doc_freq` exceeds 16M.
    constexpr size_t kSafeReserveCap = 1U << 24;
    out.reserve(std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));

    // NOTE: a SLIM kDefault top-level block (df < skip_interval) carries NO
    // codec byte and is handled by the is_slim fast path above — it never
    // reaches this codec-byte dispatch. A bare kCodeModeDefault top-level byte
    // is therefore no longer produced; only PFOR / windowed / ZSTD remain.
    if (mode == FreqProxEncoder::kCodeModeSpimiPfor) {
        // Phase 35 PFOR block. Consume doc_freq doc_delta values,
        // then — if has_prox — another doc_freq freqs. The sub-block
        // count is IMPLICIT (kBlockSize per block except the last);
        // DecodePforRun derives it from the run total and pulls until
        // count is met.
        const auto doc_deltas = DecodePforRun(cur, doc_freq);
        std::vector<uint32_t> freqs;
        if (has_prox) {
            freqs = DecodePforRun(cur, doc_freq);
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            last_doc += static_cast<int32_t>(doc_deltas[static_cast<size_t>(i)]);
            const int32_t f = has_prox ? static_cast<int32_t>(freqs[static_cast<size_t>(i)]) : 1;
            out.emplace_back(last_doc, f);
        }
        return out;
    }

    // Unknown CodeMode byte on a corrupt segment must not crash the
    // BE process. Hard-throw so the search-path catch surfaces it
    // as `INVERTED_INDEX_FILE_CORRUPTED`.
    SPIMI_THROW_CORRUPT("SPIMI .frq unknown CodeMode byte");
}

} // namespace doris::segment_v2::inverted_index::spimi
