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

#include "storage/index/inverted/spimi/byte_source.h"

namespace doris::segment_v2::inverted_index::spimi {

// One document's decoded posting data: the doc_id, its term frequency,
// and (when positions are available) the list of token positions within
// that document.
struct DecodedDoc {
    int32_t doc_id = 0;
    int32_t freq = 0;
    std::vector<int32_t> positions; // populated only when has_prox=true
};

// Decodes a single term's `.frq` and `.prx` blocks. Two output shapes:
//
//   - `Decode` materializes a `vector<DecodedDoc>` (one positions vector per
//     doc) — convenient for tests and small terms.
//   - `DecodeFlat` APPENDS into a reusable `FlatPostings` (flat doc-delta /
//     freq arrays + the raw position-delta VInt bytes + per-doc byte offsets)
//     — no per-doc heap blocks, so a df=O(million) term costs O(df) flat words
//     instead of millions of vector allocations. This is the segment merger's
//     working set, and the flat shape is exactly what
//     `FreqProxEncoder::EmitWindowedTermPreDecoded` consumes.
//
// Supports every `.frq`/`.prx` envelope the writer produces:
//   - SLIM kDefault (is_slim, df < skip_interval): per-doc VInt deltas with NO
//     leading codec byte and NO VInt(doc_count) — dispatched via is_slim, not a
//     codec byte
//   - `.frq` `kCodeModeSpimiPfor` (0x05): PFOR bit-packed sub-blocks
//   - `.frq` `kCodeModeSpimiWindowed` (0x06): V4 windowed framing
//   - `.frq` `kCodeModeZstd` (0x80): whole-term ZSTD wrapper around the PFOR
//     inner mode (never wraps a slim kDefault block)
//   - `.prx` `kProxRaw` / `kProxZstd` / `kProxWindowed`: raw, whole-term ZSTD,
//     or V4 windowed position-delta stream
class PostingDecoder {
public:
    // Flat per-term posting arrays, appendable across multiple inputs of the
    // SAME term (the k-way merge case): each call to DecodeFlat appends one
    // input's run. Because every spill input carries GLOBAL absolute doc_ids
    // (a term's first stored delta is taken against an implicit 0, i.e. it IS
    // the absolute first id), DecodeFlat re-bases each appended run's first
    // delta against `last_doc` so `doc_deltas` stays one continuous
    // delta-from-previous chain across inputs — exactly the shape the term
    // would have had if written in one piece.
    struct FlatPostings {
        std::vector<uint32_t> doc_deltas; // df deltas (first = delta from doc "last_doc")
        std::vector<uint32_t> freqs;      // df freqs; untouched when !has_prox
        // The term's within-doc position-delta VInt bytes, verbatim
        // (concatenated across inputs; position deltas are self-anchored per
        // doc, so the bytes splice without re-encoding). Untouched when
        // !has_prox.
        std::vector<uint8_t> pos_vint;
        // Byte offset into pos_vint where each doc's positions start (what
        // StartDoc would have recorded). Untouched when !has_prox.
        std::vector<uint32_t> pos_offsets;
        // Absolute doc_id of the last appended doc (re-base anchor for the
        // next input's first delta). 0 before the first append.
        int64_t last_doc = 0;

        void Clear() {
            doc_deltas.clear();
            freqs.clear();
            pos_vint.clear();
            pos_offsets.clear();
            last_doc = 0;
        }
    };

    // Decodes one term's posting data.
    //
    // `frq_data` / `frq_length`: the byte range starting at the term's
    // `freq_pointer` in the `.frq` file.  Must contain at least the
    // term's block (extra trailing bytes — skip list or next term —
    // are ignored).
    //
    // `prx_data` / `prx_length`: the byte range starting at the term's
    // `prox_pointer` in the `.prx` file.  May be null/zero when
    // `has_prox` is false (positions are then left empty).
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

    // Flat variant: APPENDS this term run's `doc_freq` docs to `out` (see
    // FlatPostings). Same envelope support as Decode. When `has_prox` is true
    // the `.prx` block is REQUIRED (a phrase-on term always has positions);
    // a missing/empty block throws the same corrupt-input error the other
    // malformed-stream paths use.
    static void DecodeFlat(const uint8_t* frq_data, size_t frq_length, const uint8_t* prx_data,
                           size_t prx_length, int32_t doc_freq, bool has_prox, bool is_slim,
                           FlatPostings* out);

    // 流式核心（(ptr,len) 重载是它包内存源的薄包装）：从前向滑窗源顺序消费
    // 一个 term 的 .frq / .prx 块。两个块的解码都严格前向且自定界 —— .frq
    // 由 `doc_freq` 驱动（slim 按 doc 数读 VInt；windowed/PFOR 的头表 +
    // 载荷长度都在流内），.prx 信封自带长度（ZSTD/窗口）或由 Σfreq 驱动
    // （raw 逐 VInt 扫描）—— 所以调用方无需预知块尾：消费完毕后源恰好停在
    // 块尾（下一 term SkipForwardTo 自己的指针即可，跳表等间隙字节被跳过）。
    //
    // `frq_src` 必须已定位在该 term 的 freq_pointer；`prx_src` 仅在
    // `has_prox` 时使用且必须已定位在 prox_pointer（DOCS_ONLY 传 nullptr）。
    // 这是 k 路归并的流式工作集入口：每输入只驻留 min(缓冲, 流长) 的窗口，
    // 不再把整条 .frq/.prx 载回内存。
    static void DecodeFlat(ForwardByteSource& frq_src, ForwardByteSource* prx_src, int32_t doc_freq,
                           bool has_prox, bool is_slim, FlatPostings* out);
};

} // namespace doris::segment_v2::inverted_index::spimi
