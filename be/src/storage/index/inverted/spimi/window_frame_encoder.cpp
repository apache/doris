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

#include "storage/index/inverted/spimi/window_frame_encoder.h"

#include <zstd.h>

#include <algorithm>
#include <array>

#include "common/config.h"
#include "common/logging.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"
#include "util/faststring.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Adaptive finest-window candidate widths (docs). units_per_window k = W / 256.
constexpr std::array<int32_t, 4> kCandidateW = {256, 512, 1024, 2048};

// Accept a finer (smaller-W) framing as long as it costs no more than
// 110% of the coarsest (whole-term, single-window) framing — finer granularity
// buys future per-window range-GET locality.
constexpr double kSizeBudget = 1.10;

// A finest unit: byte ranges into the staging buffers for each part, the doc
// count it covers, and its absolute docid range (for the skip table).
struct Unit {
    int32_t doc_count = 0;
    int32_t min_docid = 0;
    int32_t max_docid = 0;
    // PART_DD / PART_FQ live in `frq_parts`; PART_POS lives in `pos_parts`.
    size_t dd_off = 0;
    size_t dd_len = 0;
    size_t fq_off = 0;
    size_t fq_len = 0;
    size_t pos_off = 0;
    size_t pos_len = 0;
};

inline void AppendVInt(std::vector<uint8_t>& buf, uint32_t v) {
    while ((v & ~0x7FU) != 0) {
        buf.push_back(static_cast<uint8_t>((v & 0x7FU) | 0x80U));
        v >>= 7U;
    }
    buf.push_back(static_cast<uint8_t>(v));
}

inline size_t VIntLen(uint32_t v) {
    size_t n = 1;
    while ((v & ~0x7FU) != 0) {
        ++n;
        v >>= 7U;
    }
    return n;
}

// PFOR-encodes `vals[off..off+count)` as a run of <=128-value sub-blocks into
// `out`, returning the byte length written. Concatenating the runs of two
// adjacent units yields one valid longer PFOR run (each sub-block self-describes
// its count), which is what makes part-wise window composition decode correctly.
size_t EncodePforPart(const std::vector<uint32_t>& vals, size_t off, size_t count, bool allow_patch,
                      std::vector<uint8_t>& out) {
    const size_t start = out.size();
    // Wrap `out` in a tiny ByteOutput so we can reuse SpimiPforEncoder.
    struct VecOut final : public ByteOutput {
        std::vector<uint8_t>* v;
        explicit VecOut(std::vector<uint8_t>* vv) : v(vv) {}
        void WriteByte(uint8_t b) override { v->push_back(b); }
        void WriteBytes(const uint8_t* b, size_t len) override { v->insert(v->end(), b, b + len); }
        int64_t FilePointer() const override { return static_cast<int64_t>(v->size()); }
    } vo(&out);
    std::vector<uint32_t> scratch;
    size_t i = 0;
    while (i < count) {
        const size_t n = std::min(static_cast<size_t>(SpimiPforEncoder::kBlockSize), count - i);
        scratch.assign(vals.begin() + static_cast<std::ptrdiff_t>(off + i),
                       vals.begin() + static_cast<std::ptrdiff_t>(off + i + n));
        SpimiPforEncoder::EncodeBlock(scratch.data(), scratch.size(), &vo, allow_patch);
        i += n;
    }
    return out.size() - start;
}

// VInt-encodes `vals[off..off+count)` into `out`, returning byte length. Used
// for the inner_mode == VInt (df < skip_interval) fallback. Layout matches the
// kDefault per-doc encoding sans the leading VInt(docCount): ((delta<<1)|f1)
// [, freq] per doc — see DecodeWindowedDefault in the readers.
size_t EncodeVIntPart(const std::vector<uint32_t>& doc_deltas, const std::vector<uint32_t>& freqs,
                      bool has_prox, size_t off, size_t count, std::vector<uint8_t>& out) {
    const size_t start = out.size();
    for (size_t i = 0; i < count; ++i) {
        const uint32_t dd = doc_deltas[off + i];
        if (has_prox) {
            const uint32_t f = freqs[off + i];
            const uint32_t code = dd << 1U;
            if (f == 1) {
                AppendVInt(out, code | 1U);
            } else {
                AppendVInt(out, code);
                AppendVInt(out, f);
            }
        } else {
            AppendVInt(out, dd);
        }
    }
    return out.size() - start;
}

// One composed window: doc count, docid range, the inflated part-wise FRQ inner
// bytes, and (lazily) its emit-ready payload tuple so the window is ZSTD-
// compressed EXACTLY ONCE. `frq_inner` is freed the moment the payload is cached
// (MeasureAndCacheFrq) — the compressed `frq_payload` is what survives into the
// candidate search and the final emit, so a high-df term never holds several raw
// whole-term copies at once.
struct Window {
    int32_t doc_count = 0;
    int32_t min_docid = 0;
    int32_t max_docid = 0;
    std::vector<uint8_t> frq_inner;   // PART_DD ++ PART_FQ (PFOR) or VInt blob
    std::vector<uint8_t> frq_payload; // cached emit tuple (win_mode + lens + bytes)
    bool cached = false;
};

// Composes FRQ windows by stepping `k` (>=1) units at a time over `units`. The
// last window takes min(k, remaining) units. Inner bytes are composed PART-WISE.
// Positions are deliberately NOT composed here: the adaptive search only needs
// FRQ sizes, and positions for the CHOSEN framing are sliced straight from
// `pos_parts` at emit time (EmitPrxForChosen). This keeps baseline + every
// candidate from each materializing a whole-term position copy (the dominant
// peak-memory cost on high-frequency corpora like wiki).
std::vector<Window> ComposeFrqWindows(const std::vector<Unit>& units, int32_t k,
                                      const std::vector<uint8_t>& frq_parts, bool has_prox,
                                      bool inner_pfor,
                                      // For the VInt inner mode the parts are NOT
                                      // pre-encoded into frq_parts; we re-encode
                                      // per window from these.
                                      const std::vector<uint32_t>& doc_deltas,
                                      const std::vector<uint32_t>& freqs) {
    DCHECK_GE(k, 1) << "units_per_window must be clamped to >= 1 (prior HANG bug)";
    std::vector<Window> windows;
    const auto num_units = static_cast<int32_t>(units.size());
    int32_t unit_doc_base = 0; // running doc index at the start of this window
    for (int32_t i = 0; i < num_units;) {
        const int32_t take = std::min(k, num_units - i);
        DCHECK_GE(take, 1) << "window must consume >= 1 unit";
        Window w;
        w.min_docid = units[static_cast<size_t>(i)].min_docid;
        w.max_docid = units[static_cast<size_t>(i)].max_docid;
        int32_t win_docs = 0;
        for (int32_t j = 0; j < take; ++j) {
            const Unit& u = units[static_cast<size_t>(i + j)];
            win_docs += u.doc_count;
            w.max_docid = std::max(w.max_docid, u.max_docid);
        }
        w.doc_count = win_docs;

        if (inner_pfor) {
            // PART-WISE: all units' PART_DD, THEN all units' PART_FQ.
            for (int32_t j = 0; j < take; ++j) {
                const Unit& u = units[static_cast<size_t>(i + j)];
                w.frq_inner.insert(
                        w.frq_inner.end(),
                        frq_parts.begin() + static_cast<std::ptrdiff_t>(u.dd_off),
                        frq_parts.begin() + static_cast<std::ptrdiff_t>(u.dd_off + u.dd_len));
            }
            if (has_prox) {
                for (int32_t j = 0; j < take; ++j) {
                    const Unit& u = units[static_cast<size_t>(i + j)];
                    w.frq_inner.insert(
                            w.frq_inner.end(),
                            frq_parts.begin() + static_cast<std::ptrdiff_t>(u.fq_off),
                            frq_parts.begin() + static_cast<std::ptrdiff_t>(u.fq_off + u.fq_len));
                }
            }
        } else {
            // VInt inner mode: re-encode the window's doc range directly. The
            // window's first doc-delta is already relative to the previous
            // window's last doc (doc_deltas thread through the term), so no
            // re-basing is needed.
            (void)EncodeVIntPart(doc_deltas, freqs, has_prox, static_cast<size_t>(unit_doc_base),
                                 static_cast<size_t>(win_docs), w.frq_inner);
        }

        windows.push_back(std::move(w));
        unit_doc_base += win_docs;
        i += take;
    }
    return windows;
}

// Emits one window's payload tuple (win_mode + lengths + bytes) to `out`,
// returning bytes written. `inner` is the inflated part-wise inner stream.
// `comp_scratch` is a caller-owned faststring reused across windows so each
// window's ZSTD output buffer is not freshly allocated. The emitted bytes are
// min(raw, zstd) — byte-for-byte identical to the previous EmitWindowPayload.
size_t EmitWindowPayload(const std::vector<uint8_t>& inner, ZSTD_CCtx* cctx,
                         faststring& comp_scratch, ByteOutput* out) {
    const int64_t start = out->FilePointer();
    const size_t raw = inner.size();
    // Skip the ZSTD attempt (and its fixed Huffman/FSE table-build cost) for
    // windows below the configured threshold — they barely compress, so the
    // table-build is pure write-CPU waste. Threshold 0 => always attempt
    // (byte-identical to the pre-gate output).
    if (cctx != nullptr && raw > 0 &&
        static_cast<int64_t>(raw) >= config::inverted_index_spimi_zstd_min_bytes) {
        const size_t bound = ZSTD_compressBound(raw);
        comp_scratch.resize(bound);
        const size_t csize =
                ZSTD_compressCCtx(cctx, comp_scratch.data(), bound, inner.data(), raw, 1);
        if (!ZSTD_isError(csize)) {
            const size_t zsz = 1 + VIntLen(static_cast<uint32_t>(raw)) +
                               VIntLen(static_cast<uint32_t>(csize)) + csize;
            const size_t rawsz = 1 + VIntLen(static_cast<uint32_t>(raw)) + raw;
            if (zsz < rawsz) {
                out->WriteByte(WindowFrameEncoder::kWinZstd);
                out->WriteVInt(static_cast<int32_t>(raw));
                out->WriteVInt(static_cast<int32_t>(csize));
                out->WriteBytes(comp_scratch.data(), csize);
                return static_cast<size_t>(out->FilePointer() - start);
            }
        }
    }
    out->WriteByte(WindowFrameEncoder::kWinRaw);
    out->WriteVInt(static_cast<int32_t>(raw));
    if (raw > 0) {
        out->WriteBytes(inner.data(), raw);
    }
    return static_cast<size_t>(out->FilePointer() - start);
}

// Compresses each not-yet-cached window's frq_inner EXACTLY ONCE, stashing the
// emit-ready payload tuple in `frq_payload` and freeing the raw `frq_inner`, then
// returns the exact emitted `.frq` size (header + skip table + Σ payloads) so the
// adaptive search can compare candidates on real on-disk bytes. Because the
// cached payload is the very bytes EmitFrqCached will write, the search's
// compression is reused at emit time — no discard-then-recompress, no second
// scratch-sizing pass. The size accounting matches the previous FrqEmittedSize
// exactly (EmitWindowPayload's chosen tuple length == FrqEmittedSize's min(raw,
// zstd) best), so the chosen W — and thus every emitted byte — is unchanged.
size_t MeasureAndCacheFrq(std::vector<Window>& windows, ZSTD_CCtx* cctx, faststring& comp_scratch) {
    size_t total = 2 + VIntLen(static_cast<uint32_t>(kCandidateW.back())) +
                   VIntLen(static_cast<uint32_t>(windows.size()));
    for (auto& w : windows) {
        if (!w.cached) {
            MemoryByteOutput payload;
            (void)EmitWindowPayload(w.frq_inner, cctx, comp_scratch, &payload);
            w.frq_payload = std::move(payload.mutable_bytes());
            w.frq_inner.clear();
            w.frq_inner.shrink_to_fit();
            w.cached = true;
        }
    }
    size_t payload_offset = 0;
    for (const auto& w : windows) {
        total += VIntLen(static_cast<uint32_t>(w.doc_count));
        total += VIntLen(static_cast<uint32_t>(payload_offset));
        total += VIntLen(static_cast<uint32_t>(w.min_docid));
        total += VIntLen(static_cast<uint32_t>(w.max_docid - w.min_docid));
        payload_offset += w.frq_payload.size();
    }
    total += payload_offset;
    return total;
}

// Writes the windowed `.frq` block from windows whose payloads were already
// compressed-and-cached by MeasureAndCacheFrq. The skip table's win_byte_offset
// values come from the cached payload lengths (no trial-emit / re-compress), and
// the payloads are written verbatim — byte-for-byte identical to the previous
// EmitFrq, which trial-compressed for sizing then re-compressed for emit.
void EmitFrqCached(const std::vector<Window>& windows, uint8_t inner_mode, int32_t W,
                   ByteOutput* out) {
    out->WriteByte(FreqProxEncoder::kCodeModeSpimiWindowed);
    out->WriteByte(inner_mode);
    out->WriteVInt(W);
    out->WriteVInt(static_cast<int32_t>(windows.size()));
    size_t payload_offset = 0;
    for (const auto& win : windows) {
        DCHECK(win.cached) << "EmitFrqCached requires MeasureAndCacheFrq to have run";
        out->WriteVInt(win.doc_count);
        out->WriteVInt(static_cast<int32_t>(payload_offset));
        out->WriteVInt(win.min_docid);
        out->WriteVInt(win.max_docid - win.min_docid);
        payload_offset += win.frq_payload.size();
    }
    for (const auto& win : windows) {
        out->WriteBytes(win.frq_payload.data(), win.frq_payload.size());
    }
}

// Writes the windowed `.prx` block for the CHOSEN framing only, slicing each
// window's PART_POS straight from `pos_parts` (k units per window, same grouping
// as the frq windows) and compressing it ONCE. Positions are never composed for
// the rejected candidates — the search only ever needed frq sizes. Byte-for-byte
// identical to the previous EmitPrx (same per-window position slices, same
// min(raw, zstd) per-window envelope, no skip table).
void EmitPrxForChosen(const std::vector<Unit>& units, int32_t k,
                      const std::vector<uint8_t>& pos_parts, int32_t W, ZSTD_CCtx* cctx,
                      faststring& comp_scratch, ByteOutput* out) {
    DCHECK_GE(k, 1);
    const auto num_units = static_cast<int32_t>(units.size());
    const int32_t num_windows = (num_units + k - 1) / k;
    out->WriteByte(FreqProxEncoder::kProxWindowed);
    out->WriteVInt(W);
    out->WriteVInt(num_windows);
    std::vector<uint8_t> pos_inner;
    for (int32_t i = 0; i < num_units;) {
        const int32_t take = std::min(k, num_units - i);
        pos_inner.clear();
        for (int32_t j = 0; j < take; ++j) {
            const Unit& u = units[static_cast<size_t>(i + j)];
            pos_inner.insert(
                    pos_inner.end(), pos_parts.begin() + static_cast<std::ptrdiff_t>(u.pos_off),
                    pos_parts.begin() + static_cast<std::ptrdiff_t>(u.pos_off + u.pos_len));
        }
        (void)EmitWindowPayload(pos_inner, cctx, comp_scratch, out);
        i += take;
    }
}

} // namespace

void WindowFrameEncoder::Encode(const std::vector<uint32_t>& doc_deltas,
                                const std::vector<uint32_t>& freqs,
                                const std::vector<uint8_t>& pos_vint,
                                const std::vector<uint32_t>& pos_counts_per_doc, bool has_prox,
                                ZSTD_CCtx* cctx, ByteOutput* frq_out, ByteOutput* prx_out) {
    const auto df = static_cast<int32_t>(doc_deltas.size());
    DCHECK_GT(df, 0);
    DCHECK(!has_prox || freqs.size() == doc_deltas.size());
    DCHECK(!has_prox || pos_counts_per_doc.size() == doc_deltas.size());

    // inner_mode: PFOR for df >= kUnitDocs (skip-interval-equivalent), else the
    // VInt fallback that matches small-term behaviour. (kDefaultSkipInterval is
    // 16; we use kUnitDocs=256 as the windowing threshold — the finest unit is
    // 256 docs, so df<256 is a single window anyway and PFOR rarely pays.)
    const bool inner_pfor = df >= kUnitDocs;
    const uint8_t inner_mode = inner_pfor ? kInnerPfor : kInnerVInt;

    // --- Build finest units (slice at 256-doc boundaries) ---
    std::vector<Unit> units;
    std::vector<uint8_t> frq_parts; // staged PART_DD / PART_FQ bytes (PFOR only)
    std::vector<uint8_t> pos_parts; // staged PART_POS bytes

    // Prefix sums for absolute docids and position byte offsets per doc.
    // doc_deltas[i] is the delta from the previous doc; running sum = abs docid.
    std::vector<int32_t> abs_docid(static_cast<size_t>(df));
    {
        int32_t last = 0;
        for (int32_t i = 0; i < df; ++i) {
            last += static_cast<int32_t>(doc_deltas[static_cast<size_t>(i)]);
            abs_docid[static_cast<size_t>(i)] = last;
        }
    }

    // For .prx: per-doc byte ranges into pos_vint. We need the cumulative VInt
    // byte length up to each doc to slice PART_POS. Walk pos_vint once decoding
    // exactly pos_counts_per_doc[i] VInts per doc to find doc byte boundaries.
    std::vector<size_t> pos_byte_at_doc; // pos_byte_at_doc[i] = byte offset where doc i starts
    if (has_prox) {
        pos_byte_at_doc.resize(static_cast<size_t>(df) + 1);
        size_t p = 0;
        for (int32_t i = 0; i < df; ++i) {
            pos_byte_at_doc[static_cast<size_t>(i)] = p;
            const uint32_t cnt = pos_counts_per_doc[static_cast<size_t>(i)];
            for (uint32_t c = 0; c < cnt; ++c) {
                // Skip one VInt.
                while (p < pos_vint.size() && (pos_vint[p] & 0x80U) != 0) {
                    ++p;
                }
                if (p < pos_vint.size()) {
                    ++p; // terminating byte
                }
            }
        }
        pos_byte_at_doc[static_cast<size_t>(df)] = p;
        DCHECK_EQ(p, pos_vint.size()) << "position VInt stream length mismatch";
    }

    for (int32_t doc_start = 0; doc_start < df; doc_start += kUnitDocs) {
        const int32_t doc_end = std::min(doc_start + kUnitDocs, df);
        const auto count = static_cast<size_t>(doc_end - doc_start);
        Unit u;
        u.doc_count = static_cast<int32_t>(count);
        u.min_docid = abs_docid[static_cast<size_t>(doc_start)];
        u.max_docid = abs_docid[static_cast<size_t>(doc_end - 1)];

        if (inner_pfor) {
            u.dd_off = frq_parts.size();
            u.dd_len = EncodePforPart(doc_deltas, static_cast<size_t>(doc_start), count,
                                      /*allow_patch=*/false, frq_parts);
            if (has_prox) {
                u.fq_off = frq_parts.size();
                u.fq_len = EncodePforPart(freqs, static_cast<size_t>(doc_start), count,
                                          /*allow_patch=*/true, frq_parts);
            }
        }
        // (VInt inner mode composes directly from doc_deltas/freqs per window.)

        if (has_prox) {
            const size_t b0 = pos_byte_at_doc[static_cast<size_t>(doc_start)];
            const size_t b1 = pos_byte_at_doc[static_cast<size_t>(doc_end)];
            u.pos_off = pos_parts.size();
            u.pos_len = b1 - b0;
            pos_parts.insert(pos_parts.end(), pos_vint.begin() + static_cast<std::ptrdiff_t>(b0),
                             pos_vint.begin() + static_cast<std::ptrdiff_t>(b1));
        }
        units.push_back(u);
    }

    const auto num_units = static_cast<int32_t>(units.size());
    DCHECK_GE(num_units, 1);

    // --- Adaptive W selection (measured; compress-each-window-ONCE + cache) ---
    // Each composed candidate's windows are ZSTD-compressed exactly once, in
    // MeasureAndCacheFrq, which both returns the candidate's true on-disk .frq
    // size AND stashes the emit-ready payloads. The chosen framing is then
    // emitted from that cache (EmitFrqCached) — no discard-then-recompress, no
    // EmitFrq scratch-sizing second pass. Positions are sliced only for the
    // chosen framing at emit time, so baseline + every candidate no longer each
    // materialize a whole-term position copy. The W decision is bit-for-bit the
    // same as before (same size accounting, same +10%-of-baseline budget), so
    // the emitted bytes are unchanged.
    faststring comp_scratch; // reused across all per-window compressions
    int32_t chosen_W = kCandidateW.front();
    int32_t chosen_k = 1;
    std::vector<Window> chosen_windows;
    if (df < kUnitDocs || num_units == 1) {
        // Single unit ⇒ single window. k clamped to 1.
        chosen_W = kCandidateW.front();
        chosen_k = 1;
        chosen_windows = ComposeFrqWindows(units, /*k=*/1, frq_parts, has_prox, inner_pfor,
                                           doc_deltas, freqs);
        (void)MeasureAndCacheFrq(chosen_windows, cctx, comp_scratch);
    } else {
        // Baseline: whole-term framing (one window covering all units).
        std::vector<Window> baseline = ComposeFrqWindows(units, /*k=*/num_units, frq_parts,
                                                         has_prox, inner_pfor, doc_deltas, freqs);
        const size_t baseline_size = MeasureAndCacheFrq(baseline, cctx, comp_scratch);
        const auto budget = static_cast<size_t>(static_cast<double>(baseline_size) * kSizeBudget);

        bool picked = false;
        for (const int32_t W : kCandidateW) {
            int32_t k = W / kUnitDocs;
            if (k < 1) {
                k = 1; // clamp (prior HANG bug guard)
            }
            if (k > num_units) {
                continue; // a candidate coarser than the whole term — skip
            }
            std::vector<Window> cand =
                    ComposeFrqWindows(units, k, frq_parts, has_prox, inner_pfor, doc_deltas, freqs);
            const size_t sz = MeasureAndCacheFrq(cand, cctx, comp_scratch);
            if (sz <= budget) {
                // Accept the SMALLEST-W candidate within budget (finer locality).
                chosen_W = W;
                chosen_k = k;
                chosen_windows = std::move(cand);
                picked = true;
                break;
            }
        }
        if (!picked) {
            // No finest-W candidate within +10% — fall back to whole-term framing.
            chosen_W = num_units * kUnitDocs;
            chosen_k = num_units;
            chosen_windows = std::move(baseline);
        }
    }

    DCHECK(!chosen_windows.empty());
#ifndef NDEBUG
    {
        int32_t total = 0;
        for (const auto& w : chosen_windows) {
            total += w.doc_count;
        }
        DCHECK_EQ(total, df) << "sum of window doc counts must equal df";
    }
#endif

    EmitFrqCached(chosen_windows, inner_mode, chosen_W, frq_out);
    if (has_prox) {
        EmitPrxForChosen(units, chosen_k, pos_parts, chosen_W, cctx, comp_scratch, prx_out);
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
