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

// Opaque ZSTD compression context, forward-declared so this header doesn't pull
// in <zstd.h> (matches zstd.h's own typedef, which is legal to repeat). The
// encoder reuses one context across all terms; see freq_prox_encoder.cpp.
typedef struct ZSTD_CCtx_s ZSTD_CCtx; // NOLINT(modernize-use-using)

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
    // V4 windowed `.frq` outer mode. A V4 term's whole .frq block starts with
    // this byte, followed by the windowed framing (inner_mode, W, num_windows,
    // a per-window skip table, then per-window payloads). Distinct from every
    // existing marker so the reader can dispatch on the byte alone. See
    // window_frame_encoder.h for the exact layout.
    static constexpr uint8_t kCodeModeSpimiWindowed = 0x06;
    // Outer envelope marker: when a term's whole .frq block is ZSTD-compressed,
    // the block starts with this byte (distinct from the inner mode bytes
    // above), followed by VInt(uncompressed_len), VInt(compressed_len), payload.
    // The term-docs reader reads a term's .frq whole (no intra-term skip-seek),
    // so whole-term compression is safe and shrinks the FOR-coded .frq ~2x.
    static constexpr uint8_t kCodeModeZstd = 0x80;

    // `.prx` whole-term block mode marker (first byte of every term's prox
    // block). Public because SpimiProxReader dispatches on these — sharing the
    // writer's constants keeps reader and writer from drifting out of sync.
    static constexpr uint8_t kProxRaw = 0;  // raw VInt position-deltas follow
    static constexpr uint8_t kProxZstd = 1; // VInt(uncomp) VInt(comp) ZSTD-payload
    // V4 windowed `.prx` outer mode. A V4 term's whole .prx block starts with
    // this byte, followed by W, num_windows, then per-window payloads (each a
    // raw or ZSTD blob of that window's VInt position-deltas). Windows align
    // 1:1 with the .frq windows (same W, same per-window doc counts). See
    // window_frame_encoder.h.
    static constexpr uint8_t kProxWindowed = 0x02;

    FreqProxEncoder(ByteOutput* frq_out, ByteOutput* prx_out,
                    int32_t skip_interval = kDefaultSkipInterval,
                    int32_t max_skip_levels = kDefaultMaxSkipLevels,
                    bool omit_term_freq_and_positions = false,
                    // When true, terms are emitted in the V4 windowed `.frq` /
                    // `.prx` format (kCodeModeSpimiWindowed / kProxWindowed)
                    // via WindowFrameEncoder. When false, the legacy
                    // V0..V3 streaming PFOR/VInt path is used (byte-identical
                    // to before this flag existed).
                    bool use_windowed = false);

    ~FreqProxEncoder();

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
    // `_frq_out` points at `_frq_term_buf` (a per-term staging buffer); the real
    // output is `_frq_real`. FinishTerm flushes the term's buffered .frq —
    // raw, or ZSTD-compressed behind a kCodeModeZstd envelope — to `_frq_real`.
    ByteOutput* _frq_out;
    ByteOutput* _frq_real;
    MemoryByteOutput _frq_term_buf;
    ByteOutput* _prx_out;
    int32_t _skip_interval;
    bool _omit_tfap;       // when true: doc-id-only .frq, no .prx writes at all
    bool _use_windowed;    // when true: emit V4 windowed .frq/.prx via WindowFrameEncoder
    SkipListWriter _skip_list_writer;
    // Single ZSTD compression context reused for every term's .frq/.prx block.
    // Created in the ctor, freed in the dtor; ZSTD_compressCCtx reuses its
    // workspace so we don't pay a CCtx alloc/init/free per term.
    ZSTD_CCtx* _cctx = nullptr;

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
    // skip_interval`, StartDoc accumulates doc-deltas one PFOR sub-block at a
    // time and flushes each full sub-block straight to `_frq_out` (so the
    // doc-delta buffer never exceeds one block — the high-DF term no longer
    // forces a whole-term materialization, the dominant finish-phase peak).
    // Freqs still buffer for the whole term because the .frq layout writes the
    // entire freq region after the entire doc-delta region. FinishTerm flushes
    // the trailing partial doc-delta block, then the freq sub-blocks.
    bool _use_pfor = false;
    std::vector<uint32_t> _pfor_doc_deltas; // current doc-delta sub-block (≤kBlockSize)
    std::vector<uint32_t> _pfor_freqs;      // current freq sub-block (≤kBlockSize)
    // Freqs are PFOR-encoded per sub-block into this reused buffer as they fill,
    // and bulk-appended to .frq after the doc-delta region at FinishTerm. The
    // packed form is far smaller than the raw uint32 whole-term freq vector,
    // and the bytes are identical to emitting the freq sub-blocks directly.
    MemoryByteOutput _pfor_freq_blocks;
    // Constant .frq pointer recorded into PFOR skip entries: the position right
    // after the mode header byte. Doc-delta sub-blocks are now flushed
    // incrementally, so the live FilePointer advances during the term; pinning
    // skip entries to this anchor keeps the emitted skip bytes byte-identical
    // to the prior whole-term-buffered behaviour (where nothing was written to
    // .frq until FinishTerm, so FilePointer stayed at this anchor).
    int64_t _pfor_frq_anchor = 0;

    // Per-term position staging: AddPosition appends VInt position-deltas here
    // instead of straight to `_prx_out`; FinishTerm writes the whole-term prox
    // block (a 1-byte mode header + either raw VInt bytes, or — when the block
    // is large enough to benefit — a ZSTD-compressed payload). V4's position
    // deltas are tiny and highly cross-doc-redundant, which entropy coding
    // exploits where bit-packing (TurboPFor) cannot — cutting the .prx stream
    // ~2-4x. The reader (SpimiProxReader) reads a term's whole prox block at
    // its prox_pointer and decodes sequentially, so whole-term framing needs no
    // intra-term seek support.
    std::vector<uint8_t> _prox_raw; // VInt position-deltas for the open term
    // Compress only when the raw block is at least this big; smaller blocks keep
    // raw form so per-term framing overhead stays ~1 byte (the mode header).
    static constexpr size_t kProxCompressMin = 48;
    void FlushProxBlock(); // writes the staged term prox block to _prx_out
    void FlushFrqBlock();  // writes the staged term .frq block (raw/ZSTD) to _frq_real

    // V4 windowed-mode whole-term buffers. In windowed mode the encoder cannot
    // stream sub-blocks (windowing needs the whole term known up front), so
    // StartDoc / AddPosition buffer everything here and FinishTerm hands it to
    // WindowFrameEncoder. Unused (and empty) in the legacy path.
    std::vector<uint32_t> _win_doc_deltas;       // every doc's delta-from-prev
    std::vector<uint32_t> _win_freqs;            // every doc's freq (when has_prox)
    std::vector<uint8_t> _win_pos_vint;          // whole-term VInt position deltas
    std::vector<uint32_t> _win_pos_counts;       // per-doc position count (== freq)
    void FinishTermWindowed(TermInfo* info);     // emits via WindowFrameEncoder
};

} // namespace doris::segment_v2::inverted_index::spimi
