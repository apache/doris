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

#include "storage/index/snii/writer/logical_index_writer.h"

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <memory>
#include <span>
#include <utility>

#include "common/logging.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/bsbf.h"
#include "storage/index/snii/format/dict_block.h"
#include "storage/index/snii/format/dict_block_directory.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/norms_pod.h"
#include "storage/index/snii/format/null_bitmap.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/prx_pod.h"

namespace doris::snii::writer {

using format::BlockRef;
using format::DictBlockBuilder;
using format::DictBlockDirectoryBuilder;
using format::DictEntry;
using format::DictEntryEnc;
using format::DictEntryKind;
using format::FrqPreludeColumns;
using format::PerIndexMetaBuilder;
using format::SampledTermIndexBuilder;
using format::SectionRefs;
using format::WindowMeta;

namespace {

// Target false-positive probability for the block-split bloom XFilter. Sizes
// the filter via Parquet OptimalNumOfBytes; L0 keeps the probe in memory and L1
// keeps the per-query cost at one 32-byte block.
constexpr double kBsbfFpp = 0.01;
// Force-raw level for .frq dd/freq regions. Their plaintext is PFOR-bit-packed
// doc-deltas/freqs -- already high-entropy, so zstd shrinks ~30 MB of input by
// <0.1 MiB while burning ~0.4s CPU (and an extra crc pass over the compressed
// bytes) at 5M. We force raw here and keep zstd only on .prx (which compresses
// ~77%). Output stays self-describing: the region meta records zstd=false.
constexpr int kRawFrqRegion = 0;
// G16-i: auto raw-vs-zstd (choose smaller) for dd regions of docs-only bigram
// windows -- see the EncodeRegionsInto call in BuildWindowedPosting.
constexpr int kAutoZstdRegion = -1;
// Windows per super-block in the two-level prelude directory (design section
// 5).
constexpr uint32_t kPreludeGroupSize = 64;
// zstd level for whole-DICT-block compression comes from
// SniiIndexInput::dict_block_zstd_level (default 3: ~40% on the 64KiB
// front-coded blocks at ~120 MiB/s encode / ~600 MiB/s decode; higher levels
// trade import CPU for size, decode speed unchanged). G16-h made it (and the
// .prx auto level) caller-tunable.

using format::FrqRegionMeta;

// Encodes one window's dd region (and freq region when has_freq) into separate
// buffers, returning their codec metadata. The dd region is the docs-only data;
// the freq region is the skippable suffix. Used for both the grouped windowed
// layout (regions concatenated into posting-level blocks) and the single-window
// slim/inline layout ([dd_region][freq_region]).
Status EncodeRegions(std::span<const uint32_t> docids, std::span<const uint32_t> freqs,
                     uint64_t win_base, bool has_freq, std::vector<uint8_t>* dd_out,
                     FrqRegionMeta* dd_meta, std::vector<uint8_t>* freq_out,
                     FrqRegionMeta* freq_meta) {
    ByteSink dd_sink;
    RETURN_IF_ERROR(format::build_dd_region(docids, win_base, kRawFrqRegion, &dd_sink, dd_meta));
    *dd_out = dd_sink.take();
    if (!has_freq) {
        *freq_out = std::vector<uint8_t>();
        *freq_meta = FrqRegionMeta {};
        return Status::OK();
    }
    ByteSink freq_sink;
    RETURN_IF_ERROR(format::build_freq_region(freqs, kRawFrqRegion, &freq_sink, freq_meta));
    *freq_out = freq_sink.take();
    return Status::OK();
}

// Reusable per-window scratch for the windowed builder. Each ByteSink RETAINS
// its capacity across windows (clear(), not re-construct), so encoding a
// high-df term split into thousands of windows allocates the scratch ONCE
// instead of churning thousands of small buffers (which fragment the heap arena
// and raise peak RSS).
struct WindowScratch {
    ByteSink dd_sink;
    ByteSink freq_sink;
    ByteSink prx_sink;
};

// Encodes one window's dd (and freq) region into the scratch sinks and appends
// the bytes directly to the grouped blocks via LayoutWindowRegions. Reuses the
// sinks.
Status EncodeRegionsInto(WindowScratch* sc, std::span<const uint32_t> docids,
                         std::span<const uint32_t> freqs, uint64_t win_base, bool has_freq,
                         FrqRegionMeta* dd_meta, FrqRegionMeta* freq_meta,
                         int dd_zstd_level = kRawFrqRegion) {
    sc->dd_sink.clear();
    RETURN_IF_ERROR(
            format::build_dd_region(docids, win_base, dd_zstd_level, &sc->dd_sink, dd_meta));
    if (has_freq) {
        sc->freq_sink.clear();
        RETURN_IF_ERROR(format::build_freq_region(freqs, kRawFrqRegion, &sc->freq_sink, freq_meta));
    } else {
        *freq_meta = FrqRegionMeta {};
    }
    return Status::OK();
}

// Builds a single .prx window directly from a FLAT positions slice + its
// parallel freqs slice (doc d owns the next freqs[d] entries). Byte-identical
// to building from per-doc vectors, but with NO vector-of-vectors
// materialization: the writer indexes straight into the term's flat positions
// buffer.
Status MakePrxWindow(std::span<const uint32_t> positions_flat, std::span<const uint32_t> freqs,
                     int prx_zstd_level, std::vector<uint8_t>* out) {
    ByteSink sink;
    // Negative level == prx auto mode at |level| (G16-h); -3 == the historic auto.
    RETURN_IF_ERROR(format::build_prx_window_flat(positions_flat, freqs, -prx_zstd_level, &sink));
    *out = sink.take();
    return Status::OK();
}

uint32_t MaxOf(std::span<const uint32_t> v) {
    uint32_t m = 0;
    for (uint32_t x : v) {
        if (x > m) m = x;
    }
    return m;
}

// Fused single-pass term-level freq statistics: total_freq (running sum) and
// max_freq (running max) in ONE scan, reused by validate_term (has_prx
// position-count budget), stats_.sum_total_term_freq, and the DictEntry
// ttf_delta/max_freq. Byte-identical to the former separate SumOf/MaxOf scans:
// same left-to-right accumulation order and the same max init of 0, so a freq of
// 0 never lowers the max. Bumps the test-only op-count seam exactly once per
// term-level scan (one call per term from process_term).
FreqStats fuse_freq_stats(const std::vector<uint32_t>& freqs) {
    testing::note_term_freq_scan();
    FreqStats fs;
    for (uint32_t f : freqs) {
        fs.total_freq += f;
        fs.max_freq = std::max(f, fs.max_freq);
    }
    return fs;
}

// Computes a window's WAND max_norm: the encoded norm yielding the LARGEST BM25
// length contribution (smallest length penalty), i.e. the SMALLEST encoded norm
// among the window's docs (smaller dl => higher score). When norms are
// unavailable (no scoring), returns 0 -- decode_norm(0)=1.0 is the smallest
// possible dl, giving a correct (loosest) upper bound.
uint8_t WindowMaxNorm(const std::vector<uint8_t>& norms, std::span<const uint32_t> docs) {
    if (norms.empty() || docs.empty()) return 0;
    uint8_t best = 0xFF; // decode_norm uses the byte directly; min byte => max score
    for (uint32_t docid : docs) {
        if (docid >= norms.size()) continue; // defensive: out-of-range doc has no norm
        if (norms[docid] < best) best = norms[docid];
    }
    return best == 0xFF ? 0 : best;
}

// Window doc count by df: high-df windowed terms combine kFrqBaseUnit units
// into larger (kAdaptiveWindowDocs) windows; both are whole multiples of the
// base unit so .prx alignment and win_base/last_docid semantics are preserved.
uint32_t AdaptiveWindowDocs(uint32_t df) {
    return df >= format::kAdaptiveWindowDfThreshold ? format::kAdaptiveWindowDocs
                                                    : format::kFrqBaseUnit;
}

// Builds the two-level .frq prelude for a windowed term and returns its bytes.
Status BuildPrelude(const std::vector<WindowMeta>& windows, bool has_freq, bool has_prx,
                    std::vector<uint8_t>* out) {
    FrqPreludeColumns cols;
    cols.has_freq = has_freq;
    cols.has_prx = has_prx;
    cols.group_size = kPreludeGroupSize;
    cols.windows = windows;
    ByteSink sink;
    RETURN_IF_ERROR(format::build_frq_prelude(cols, &sink));
    *out = sink.take();
    return Status::OK();
}

void AppendBytes(std::vector<uint8_t>* dst, const std::vector<uint8_t>& src) {
    dst->insert(dst->end(), src.begin(), src.end());
}

// One windowed term's grouped .frq layout (design 1.6): all dd regions form the
// dd-block, all freq regions form the freq-block. The final frq span is
// [prelude][dd-block][freq-block]. The .prx windows are STREAMED straight to
// the posting sink (the container output) during pass 1 (not buffered here) --
// so the widest term's ~tens-of-MiB prx bytes never co-exist with the dd/freq
// blocks at peak RSS; only prx_total_len (the entry's prx byte span) is
// tracked. Per-window metadata (region offsets/lens/modes/crcs, prx_off within
// the entry) is recorded for the prelude.
struct WindowedPosting {
    std::vector<uint8_t> dd_block;   // dd_region_0 ++ dd_region_1 ++ ...
    std::vector<uint8_t> freq_block; // freq_region_0 ++ ... (empty if !has_freq)
    uint64_t prx_total_len = 0;      // total .prx bytes streamed for this entry
    std::vector<WindowMeta> windows;
};

// Fills a window's region locator fields in m from its dd/freq region metas and
// the running dd-block / freq-block offsets, then appends the region bytes to
// the blocks. has_freq controls whether the freq region is laid out.
void LayoutWindowRegions(const FrqRegionMeta& dd_meta, const std::vector<uint8_t>& dd_bytes,
                         const FrqRegionMeta& freq_meta, const std::vector<uint8_t>& freq_bytes,
                         bool has_freq, WindowedPosting* out, WindowMeta* m) {
    m->dd_zstd = dd_meta.zstd;
    m->dd_off = static_cast<uint64_t>(out->dd_block.size());
    m->dd_disk_len = dd_meta.disk_len;
    m->dd_uncomp_len = dd_meta.uncomp_len;
    m->crc_dd = dd_meta.crc;
    AppendBytes(&out->dd_block, dd_bytes);
    if (!has_freq) return;
    m->freq_zstd = freq_meta.zstd;
    m->freq_off = static_cast<uint64_t>(out->freq_block.size());
    m->freq_disk_len = freq_meta.disk_len;
    m->freq_uncomp_len = freq_meta.uncomp_len;
    m->crc_freq = freq_meta.crc;
    AppendBytes(&out->freq_block, freq_bytes);
}

// Splits a windowed term's postings into base-unit-aligned windows (size chosen
// by df via AdaptiveWindowDocs). Each window's dd/freq regions are encoded
// separately and grouped: all dd regions into the dd-block, all freq regions
// into the freq-block. Records per-window region metadata + WAND max_norm.
//
// TWO-PASS, MEMORY-AWARE: the widest term (df in the millions) is the dominant
// merge-phase peak-RSS source -- its flat positions_flat alone is tens of MiB
// and would otherwise co-exist with the encoded output blocks at the peak
// moment.
//   pass 1 (prx): builds every window's .prx bytes, then FREES positions_flat
//                 (the single largest source array) before any dd/freq block
//                 grows.
//   pass 2 (dd/freq): encodes the dd/freq regions from docids/freqs only.
// `tp` is taken by mutable reference; positions_flat is freed after pass 1 and
// docids/freqs are freed by the caller after this returns. Output bytes are
// byte-identical to the single-pass build (regions/prelude/prx are
// independent).
Status BuildWindowedPosting(TermPostings& tp, bool has_freq, bool has_prx,
                            const std::vector<uint8_t>& norms, io::FileWriter* posting_out,
                            WindowedPosting* out, uint32_t window_docs_override = 0,
                            int prx_zstd_level = 3) {
    // window_docs_override > 0 (G16-e docs-only bigrams) replaces the df-based
    // adaptive sizing; 0 keeps the byte-identical adaptive layout.
    const uint32_t unit = window_docs_override > 0
                                  ? window_docs_override
                                  : AdaptiveWindowDocs(static_cast<uint32_t>(tp.docids.size()));
    const size_t n = tp.docids.size();
    const std::span<const uint32_t> all_docs(tp.docids);
    const std::span<const uint32_t> all_freqs(tp.freqs);

    // Size the per-term transient blocks up front so a very-high-df term (split
    // into thousands of windows, dd/freq blocks of MiB) does not grow them by
    // geometric doubling -- which would briefly hold the old+new buffer
    // co-resident at the build peak. windows count is exact; dd/freq use a
    // conservative byte/doc upper estimate (PFOR-packed deltas are typically <= 2
    // B/doc). Slack is freed when the term ends.
    out->windows.reserve((n + unit - 1) / unit);
    out->dd_block.reserve(n * 2);
    if (has_freq) out->freq_block.reserve(n);

    WindowScratch sc; // reused across all windows (no per-window allocation churn)

    // ---- pass 1: prx (STREAMED to the output) + window skeleton ----
    // Each window's .prx bytes are appended straight to the posting sink
    // (container output) as they are built, so the entry's full prx payload (tens
    // of MiB for the widest term) is never buffered in RAM alongside the dd/freq
    // blocks that pass 2 grows. m.prx_off is the byte offset WITHIN this entry's
    // prx span (running prx_total_len), matching the reader's prx_off_delta +
    // meta.prx_off contract.
    {
        // Positions come either from the flat buffer or, for very-high-df terms,
        // from a sequential pump (so the term's full positions are never
        // materialized). Both yield the SAME positions in the SAME order, so the
        // prx bytes are identical.
        const bool streamed = static_cast<bool>(tp.pos_pump);
        const std::span<const uint32_t> all_pos(tp.positions_flat);
        std::vector<uint32_t> win_pos_buf; // reused per window when streaming
        uint64_t win_base = 0;
        size_t pos_off = 0;
        for (size_t start = 0; start < n; start += unit) {
            const size_t len = std::min<size_t>(unit, n - start);
            const auto docs = all_docs.subspan(start, len);
            const auto freqs = all_freqs.subspan(start, len);
            WindowMeta m;
            m.last_docid = docs.back();
            m.win_base = win_base;
            m.doc_count = static_cast<uint32_t>(docs.size());
            m.max_freq = MaxOf(freqs);
            m.max_norm = WindowMaxNorm(norms, docs);
            size_t win_pos = 0;
            for (uint32_t f : freqs) win_pos += f;
            if (has_prx) {
                std::span<const uint32_t> pos_span;
                if (streamed) {
                    win_pos_buf.resize(win_pos);
                    if (win_pos != 0) tp.pos_pump(win_pos_buf.data(), win_pos);
                    pos_span = std::span<const uint32_t>(win_pos_buf);
                } else {
                    pos_span = all_pos.subspan(pos_off, win_pos);
                }
                sc.prx_sink.clear();
                RETURN_IF_ERROR(format::build_prx_window_flat(pos_span, freqs, -prx_zstd_level,
                                                              &sc.prx_sink));
                m.prx_off = out->prx_total_len;
                m.prx_len = static_cast<uint64_t>(sc.prx_sink.size());
                RETURN_IF_ERROR(posting_out->append(sc.prx_sink.view()));
                out->prx_total_len += m.prx_len;
            }
            pos_off += win_pos;
            out->windows.push_back(m);
            win_base = m.last_docid;
        }
    }
    // Positions are fully consumed; free the largest source array before pass 2
    // grows the dd/freq blocks, so the source positions never co-exist with them.
    std::vector<uint32_t>().swap(tp.positions_flat);

    // ---- pass 2: dd (and freq) regions from docids/freqs only ----
    uint64_t win_base = 0;
    size_t wi = 0;
    for (size_t start = 0; start < n; start += unit, ++wi) {
        const size_t len = std::min<size_t>(unit, n - start);
        const auto docs = all_docs.subspan(start, len);
        const auto freqs = all_freqs.subspan(start, len);
        FrqRegionMeta dd_meta, freq_meta;
        // G16-i: docs-only bigram windows (window_docs_override > 0) try zstd on
        // the dd region (choose-smaller; FrqRegionMeta.zstd self-describes).
        // Ultra-dense pair postings carry highly repetitive PFOR delta patterns
        // that raw encoding leaves ~3 bits/doc on the table; unigram dd stays
        // force-raw (measured: zstd shrinks ~30MB of PFOR input by <0.1MB).
        RETURN_IF_ERROR(
                EncodeRegionsInto(&sc, docs, freqs, win_base, has_freq, &dd_meta, &freq_meta,
                                  window_docs_override > 0 ? kAutoZstdRegion : kRawFrqRegion));
        LayoutWindowRegions(dd_meta, sc.dd_sink.buffer(), freq_meta, sc.freq_sink.buffer(),
                            has_freq, out, &out->windows[wi]);
        win_base = out->windows[wi].last_docid;
    }
    return Status::OK();
}

} // namespace

namespace testing {
namespace {
// Function-local-static op-count seam backing term_freq_scans(). One atomic,
// relaxed: the writer build path is single-threaded, so only the COUNT matters,
// not ordering (the atomic keeps it race-clean if a test ever parallelizes).
std::atomic<uint64_t>& term_freq_scan_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
} // namespace

void note_term_freq_scan() {
    term_freq_scan_counter().fetch_add(1, std::memory_order_relaxed);
}
uint64_t term_freq_scans() {
    return term_freq_scan_counter().load(std::memory_order_relaxed);
}
void reset_term_freq_scans() {
    term_freq_scan_counter().store(0, std::memory_order_relaxed);
}

namespace {
// G01 bigram-diet counters: bumped once per non-sentinel phrase-bigram TERM (not
// per token) at the process_term materialize/prune decision. Always-on relaxed
// atomics like term_freq_scan_counter -- one branch + one add per bigram term is
// noise next to the term's encode work.
std::atomic<uint64_t>& bigram_materialized_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
std::atomic<uint64_t>& bigram_pruned_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
std::atomic<uint64_t>& bigram_max_pruned_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
std::atomic<uint64_t>& bigram_bloom_dropped_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
std::atomic<uint64_t>& bigram_freq_elided_counter() {
    static std::atomic<uint64_t> counter {0};
    return counter;
}
} // namespace

void note_bigram_term_materialized() {
    bigram_materialized_counter().fetch_add(1, std::memory_order_relaxed);
}
void note_bigram_term_pruned() {
    bigram_pruned_counter().fetch_add(1, std::memory_order_relaxed);
}
void note_bigram_term_max_pruned() {
    bigram_max_pruned_counter().fetch_add(1, std::memory_order_relaxed);
}
void note_bigram_bloom_dropped() {
    bigram_bloom_dropped_counter().fetch_add(1, std::memory_order_relaxed);
}
void note_bigram_freq_elided() {
    bigram_freq_elided_counter().fetch_add(1, std::memory_order_relaxed);
}
uint64_t bigram_terms_materialized() {
    return bigram_materialized_counter().load(std::memory_order_relaxed);
}
uint64_t bigram_terms_pruned() {
    return bigram_pruned_counter().load(std::memory_order_relaxed);
}
uint64_t bigram_terms_max_pruned() {
    return bigram_max_pruned_counter().load(std::memory_order_relaxed);
}
uint64_t bigram_bloom_dropped() {
    return bigram_bloom_dropped_counter().load(std::memory_order_relaxed);
}
uint64_t bigram_freqs_elided() {
    return bigram_freq_elided_counter().load(std::memory_order_relaxed);
}
void reset_bigram_prune_counters() {
    bigram_materialized_counter().store(0, std::memory_order_relaxed);
    bigram_pruned_counter().store(0, std::memory_order_relaxed);
    bigram_max_pruned_counter().store(0, std::memory_order_relaxed);
    bigram_bloom_dropped_counter().store(0, std::memory_order_relaxed);
    bigram_freq_elided_counter().store(0, std::memory_order_relaxed);
}
// Forwards to the real fused helper so pure boundary tests exercise production
// code (not a test-local re-implementation).
FreqStats fuse_freq_stats_for_test(const std::vector<uint32_t>& freqs) {
    return fuse_freq_stats(freqs);
}
} // namespace testing

LogicalIndexWriter::LogicalIndexWriter(const SniiIndexInput& in)
        : index_id_(in.index_id),
          index_suffix_(in.index_suffix),
          tier_(format::tier_of(in.config)),
          has_prx_(format::has_positions(in.config)),
          // G16-c: the caller can drop freq layout entirely (in.write_freq ==
          // false) on a freq-capable tier -- see SniiIndexInput::write_freq.
          has_freq_(format::tier_of(in.config) >= format::IndexTier::kT2 && in.write_freq),
          has_norms_(format::has_scoring(in.config)),
          // Bigram df-pruning only makes sense for positions-capable configs (the
          // only ones that emit hidden phrase bigrams); force 0 otherwise so the
          // meta never (mis)declares pruning on a docs-only index.
          bigram_prune_min_df_(format::has_positions(in.config) ? in.bigram_prune_min_df : 0),
          // G15 upper bound rides the same non-positional force-off: a docs-only
          // index has no bigrams, and declaring pruning there would send every
          // 2-term phrase-shaped miss down a pointless fallback branch.
          bigram_prune_max_df_(format::has_positions(in.config) ? in.bigram_prune_max_df : 0),
          // The direct-load writer only arms this for positional indexes. Keep the
          // format invariant local to the writer as well so core callers cannot
          // publish a no-bigram capability on a docs-only index.
          phrase_bigrams_deferred_(format::has_positions(in.config) && in.phrase_bigrams_deferred),
          // The G04 bloom drop rides the SAME reader contract as the df prune (a
          // dict miss falls back only when the meta declares pruning), so it is
          // honored ONLY when the effective threshold is non-zero. NOTE: the
          // threshold expression above must match -- use the member-init ordering
          // guarantee (bigram_prune_min_df_ is declared before this member).
          bigram_ever_dropped_(bigram_prune_min_df_ > 0 ? in.bigram_ever_dropped : nullptr),
          doc_count_(in.doc_count),
          null_docids_(in.null_docids),
          terms_(in.terms),
          term_source_(in.term_source),
          encoded_norms_(in.encoded_norms),
          target_dict_block_bytes_(in.target_dict_block_bytes != 0
                                           ? in.target_dict_block_bytes
                                           : format::kDefaultTargetDictBlockBytes),
          dict_block_zstd_level_(in.dict_block_zstd_level),
          prx_zstd_level_(in.prx_zstd_level),
          // No independent dict cap: the dict spills via the writer's UNIFIED
          // gate-2 cap (in.mem_reporter->over_cap()); UINT64_MAX disables the local
          // per-buffer cap.
          dict_buf_(UINT64_MAX, "dict", in.mem_reporter) {}

Status LogicalIndexWriter::validate_term(const TermPostings& tp, uint64_t total_freq,
                                         bool expect_positions) const {
    if (tp.freqs.size() != tp.docids.size()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: freqs length must equal docids");
    }
    if (expect_positions) {
        // total_freq is the fused sum(freqs) computed once by the caller (no
        // internal re-sum). Streamed positions (pos_pump set): validate against the
        // declared pos_total (positions_flat is intentionally empty). Otherwise
        // validate the flat buffer. Skipped (expect_positions == false) exactly
        // when this term writes no .prx: G04 position-suppressed bigram terms
        // arrive with an empty positions_flat by design.
        const uint64_t have = tp.pos_pump ? tp.pos_total : tp.positions_flat.size();
        if (total_freq != have) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: positions count must equal sum(freqs)");
        }
    }
    for (size_t i = 1; i < tp.docids.size(); ++i) {
        if (tp.docids[i] <= tp.docids[i - 1]) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "logical_index: docids must be strictly ascending");
        }
    }
    return Status::OK();
}

// Emits a windowed term: splits into base-unit windows, encodes each window's
// dd/freq regions separately, groups them at posting level, builds a two-level
// prelude, and lays out [prx span][prelude][dd-block][freq-block] CONTIGUOUSLY
// in the single posting region (prx span first, then the frq span). Sets
// enc=windowed + has_sb. frq_docs_len = prelude_len + dd_block_len is the
// contiguous docs-only prefix, which stays INSIDE the frq span.
Status LogicalIndexWriter::build_windowed_entry(TermPostings& tp, uint64_t frq_base,
                                                uint64_t prx_base, bool write_prx, bool write_freq,
                                                DictEntry* e) {
    // The prx span starts here: pass 1 streams each .prx window straight into
    // the posting sink, so prx_off_delta is measured against the live
    // posting-sink size. write_prx == false (G01 docs-only bigram) streams no
    // prx bytes at all and leaves the entry's prx locator zeroed; the per-term
    // prelude records has_prx=false so the layout stays self-describing.
    // write_freq == false (G16 docs-only bigram) additionally drops the
    // freq-block and the per-window freq locators/crcs from the prelude; the
    // prelude flags record has_freq=false, so frq_len == frq_docs_len and a
    // want_freq read of such an entry fails closed in region decode.
    const uint64_t prx_off = posting_size();
    // G16-e: docs-only (prune-mode) bigrams use much larger windows -- their
    // 2-term hit path reads the full docs-only prefix (never window-narrowed),
    // and bigger dd regions give the per-region zstd a real context. Legacy
    // bigrams (write_prx) keep the adaptive sizing (byte-identical output).
    const uint32_t window_docs =
            (!write_prx && format::is_phrase_bigram_term(tp.term)) ? format::kBigramWindowDocs : 0;
    WindowedPosting wp;
    RETURN_IF_ERROR(BuildWindowedPosting(tp, write_freq, write_prx, encoded_norms_, posting_out_,
                                         &wp, window_docs, prx_zstd_level_));
    // wp.prx_total_len bytes were just streamed straight to the posting sink (0
    // when !has_prx). docids/freqs are now fully encoded into wp; release the
    // source arrays before the (potentially large) wp blocks are appended to
    // disk.
    std::vector<uint32_t>().swap(tp.docids);
    std::vector<uint32_t>().swap(tp.freqs);
    std::vector<uint8_t> prelude;
    RETURN_IF_ERROR(BuildPrelude(wp.windows, write_freq, write_prx, &prelude));

    e->kind = DictEntryKind::kPodRef;
    e->enc = DictEntryEnc::kWindowed;
    e->has_sb = true; // prelude is always a two-level skip directory.
    e->prelude_len = static_cast<uint64_t>(prelude.size());
    e->frq_docs_len =
            e->prelude_len + static_cast<uint64_t>(wp.dd_block.size()); // [prelude][dd-block]

    // The frq span starts immediately AFTER the prx span, in the SAME sink. The
    // writer-side property frq_off == prx_off + wp.prx_total_len holds because
    // nothing is appended to the posting sink between the prx pass and here --
    // but the delta is measured from the live size, not assumed.
    const uint64_t frq_off = posting_size();
    RETURN_IF_ERROR(posting_out_->append(Slice(prelude)));
    RETURN_IF_ERROR(posting_out_->append(Slice(wp.dd_block)));
    RETURN_IF_ERROR(posting_out_->append(Slice(wp.freq_block)));
    e->frq_off_delta = frq_off - frq_base;
    e->frq_len = posting_size() - frq_off;
    if (write_prx) {
        e->prx_off_delta = prx_off - prx_base;
        e->prx_len = wp.prx_total_len; // == frq_off - prx_off
    }
    return Status::OK();
}

// Emits a slim term as a single .frq window (win_base=0) laid out [dd][freq]:
// inline when the encoded bytes are tiny, otherwise a slim pod_ref (no
// prelude). The dd region is the docs-only prefix; the freq region (when
// has_freq) is the skippable suffix. Region codecs are recorded in the
// DictEntry. For a pod_ref, the term's [prx][frq] spans are appended to the
// single posting region with the prx span FIRST (consistent with the windowed
// path); the reader resolves each delta independently so the relative order is
// not load-bearing.
Status LogicalIndexWriter::build_slim_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                                            bool write_prx, DictEntry* e) {
    std::vector<uint8_t> dd_bytes, freq_bytes;
    FrqRegionMeta dd_meta, freq_meta;
    RETURN_IF_ERROR(EncodeRegions(tp.docids, tp.freqs, /*win_base=*/0, has_freq_, &dd_bytes,
                                  &dd_meta, &freq_bytes, &freq_meta));
    std::vector<uint8_t> frq_win = dd_bytes; // [dd_region][freq_region]
    AppendBytes(&frq_win, freq_bytes);
    std::vector<uint8_t> prx_win;
    if (write_prx) {
        RETURN_IF_ERROR(MakePrxWindow(tp.positions_flat, tp.freqs, prx_zstd_level_, &prx_win));
    }

    e->enc = DictEntryEnc::kSlim;
    e->dd_meta = dd_meta;
    e->freq_meta = freq_meta;

    if (frq_win.size() <= format::kDefaultInlineThreshold) {
        e->kind = DictEntryKind::kInline;
        e->inline_dd_disk_len = dd_meta.disk_len;
        e->frq_bytes = std::move(frq_win);
        // !write_prx (G01 docs-only bigram) leaves prx_bytes empty; the T2 entry
        // encoding round-trips prx_len == 0 fine.
        if (write_prx) e->prx_bytes = std::move(prx_win);
        return Status::OK();
    }

    // POD_REF: write [prx][frq] into the single posting sink, prx span first.
    e->kind = DictEntryKind::kPodRef;
    e->frq_docs_len = dd_meta.disk_len; // docs-only prefix = the single dd region
    if (write_prx) {
        const uint64_t prx_off = posting_size();
        RETURN_IF_ERROR(posting_out_->append(Slice(prx_win)));
        e->prx_off_delta = prx_off - prx_base;
        e->prx_len = posting_size() - prx_off;
    }
    const uint64_t frq_off = posting_size(); // immediately after the prx span
    RETURN_IF_ERROR(posting_out_->append(Slice(frq_win)));
    e->frq_off_delta = frq_off - frq_base;
    e->frq_len = posting_size() - frq_off;
    return Status::OK();
}

// Builds the DictEntry for one term. Inline entries embed their .frq/.prx
// bytes; pod_ref entries append [prx][frq] bytes to the single posting region
// and record off_delta relative to frq_base/prx_base (the posting-region size
// captured when the block opened; both bases hold that same value).
Status LogicalIndexWriter::build_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                                       const FreqStats& fs, bool write_prx, bool write_freq,
                                       DictEntry* e) {
    e->term = tp.term;
    e->df = static_cast<uint32_t>(tp.docids.size());
    e->ttf_delta = fs.total_freq; // reused fused total (was SumOf(tp.freqs))
    e->max_freq = fs.max_freq;    // reused fused max (was MaxOf(tp.freqs))

    if (e->df >= format::kSlimDfThreshold) {
        if (has_freq_ && !write_freq) testing::note_bigram_freq_elided();
        RETURN_IF_ERROR(build_windowed_entry(tp, frq_base, prx_base, write_prx, write_freq, e));
    } else {
        // On a freq-WRITING index (has_freq_), slim/inline entries always keep
        // the freq region: their DictEntry region metadata is tier-conditioned
        // (freq meta fields present iff tier>=T2), so a per-TERM drop is not
        // flag-describable there -- only the windowed prelude (flags bit0) is.
        // A per-INDEX drop (G16-c write_freq=false -> has_freq_=false) removes
        // freq for ALL shapes value-driven: slim/inline then carry a
        // zero-length freq region (frq span == docs-only prefix).
        RETURN_IF_ERROR(build_slim_entry(tp, frq_base, prx_base, write_prx, e));
    }
    // G16 section accounting from the finished entry's own fields (no plumbing):
    // windowed: frq span = [prelude][dd][freq]; slim pod: [dd][freq], prelude 0;
    // inline: bytes live in frq_bytes/prx_bytes (also counted in dict bytes).
    const size_t cls = format::is_phrase_bigram_term(tp.term) ? 1 : 0;
    section_stats_.entries[cls]++;
    if (e->kind == DictEntryKind::kInline) {
        section_stats_.dd_bytes[cls] += e->inline_dd_disk_len;
        section_stats_.freq_bytes[cls] += e->frq_bytes.size() - e->inline_dd_disk_len;
        section_stats_.prx_bytes[cls] += e->prx_bytes.size();
    } else {
        section_stats_.prelude_bytes[cls] += e->prelude_len;
        section_stats_.dd_bytes[cls] += e->frq_docs_len - e->prelude_len;
        section_stats_.freq_bytes[cls] += e->frq_len - e->frq_docs_len;
        section_stats_.prx_bytes[cls] += e->prx_len;
    }
    return Status::OK();
}

// Serializes the current open block, zstd-compresses it (the dict region is the
// single largest section -- term keys + entry meta + inline postings -- and the
// 64KiB blocks compress ~40%), streams the compressed bytes into the dict
// scratch file, and records a directory entry. The block-level crc32c
// (rec.checksum) covers the UNCOMPRESSED bytes, so DictBlockReader::open
// verifies integrity after the reader decompresses. A compressed block also
// shrinks the bytes a term lookup fetches from S3 -- aligning with the
// read-byte thesis. If zstd does not shrink a (tiny) block, it is stored raw so
// a lookup never pays a pointless decompress.
Status LogicalIndexWriter::flush_block(DictBlockBuilder* block, std::string first_term) {
    ByteSink bsink;
    block->finish(&bsink);
    const Slice plain = bsink.view();
    BlockRecord rec;
    rec.rel_offset = dict_buf_.size();
    rec.n_entries = block->n_entries();
    rec.checksum = crc32c(plain); // crc over UNCOMPRESSED block bytes
    rec.first_term = std::move(first_term);

    section_stats_.dict_entry_bytes[0] += block->entry_bytes_total() - block->entry_bytes_bigram();
    section_stats_.dict_entry_bytes[1] += block->entry_bytes_bigram();

    std::vector<uint8_t> comp;
    Status zs = zstd_compress(plain, dict_block_zstd_level_, &comp);
    if (zs.ok() && comp.size() < plain.size()) {
        rec.flags = format::block_ref_flags::kZstd;
        rec.uncomp_len = static_cast<uint64_t>(plain.size());
        rec.length = static_cast<uint64_t>(comp.size());
        RETURN_IF_ERROR(dict_buf_.append_move(std::move(comp)));
    } else {
        rec.flags = 0;
        rec.uncomp_len = 0;
        rec.length = static_cast<uint64_t>(plain.size());
        RETURN_IF_ERROR(dict_buf_.append_move(bsink.take()));
    }
    blocks_.push_back(std::move(rec));
    return Status::OK();
}

// Running state for the in-flight DICT block while terms stream past.
struct LogicalIndexWriter::BlockState {
    std::unique_ptr<DictBlockBuilder> block;
    std::string block_first_term;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

Status LogicalIndexWriter::process_term(TermPostings& tp, BlockState* st) {
    // G01 bigram diet: the materialize/prune decision for hidden phrase-bigram
    // terms happens HERE -- the single choke point both drain paths (streaming
    // for_each_term_sorted and the materialized vector) funnel through, and the
    // first point where the term's FINAL df (tp.docids.size(), post-merge) is
    // known. A pruned term is dropped before ANY materialization side effect:
    // no dict entry, no posting bytes, no bloom membership, no term_count/stats
    // contribution. The sentinel term is never prunable (it flags "bigram
    // feature present" for legacy reader semantics).
    const bool is_bigram = format::is_phrase_bigram_term(tp.term);
    if (is_bigram && !format::is_phrase_bigram_sentinel_term(tp.term)) {
        if (bigram_prune_min_df_ > 0 && tp.docids.size() < bigram_prune_min_df_) {
            testing::note_bigram_term_pruned();
            return Status::OK();
        }
        // G15: symmetric UPPER gate. A near-ubiquitous pair (final df above
        // ratio * doc_count, resolved by the caller) pays dict + posting bytes
        // for almost no selectivity, so it is dropped to the SAME dict-miss
        // fallback. Unlike a min-df miss, the fallback for a pruned-HIGH pair
        // verifies positions over a LARGE unigram candidate set -- the ratio
        // must stay high enough that only stopword-like pairs cross it. Applied
        // ONLY here (never in the mid-feed drain): df grows monotonically, but
        // so does the doc-count-scaled threshold, so a partial df exceeding a
        // partial threshold proves nothing about the final comparison.
        if (bigram_prune_max_df_ > 0 && tp.docids.size() > bigram_prune_max_df_) {
            testing::note_bigram_term_max_pruned();
            return Status::OK();
        }
        // G04: drop a df-surviving bigram the ever-dropped bloom MAY contain --
        // the vocab-cap eviction dropped (at least) one of its docids mid-build,
        // so the postings here are INCOMPLETE; materializing them would silently
        // lose phrase hits, while dropping routes the pair to the G01 dict-miss
        // fallback (bigram_ever_dropped_ is non-null only when the meta declares
        // pruning). A bloom false positive lands here too: pure over-drop, safe.
        if (bigram_ever_dropped_ != nullptr && bigram_ever_dropped_->maybe_contains(tp.term)) {
            testing::note_bigram_bloom_dropped();
            return Status::OK();
        }
        testing::note_bigram_term_materialized();
    }
    // Surviving bigram postings are DOCS+FREQ ONLY in prune mode: the 2-term
    // phrase bigram hit path answers from docid membership alone (it never reads
    // bigram positions -- phrase_prefix multi-tail uses unigram prx and filters
    // bigram tails), so their .prx spans are pure dead bytes. Legacy mode
    // (threshold 0) keeps writing positions, byte-identical to pre-G01 output.
    const bool write_prx = has_prx_ && !(is_bigram && bigram_prune_min_df_ > 0);
    // G16: prune-mode bigram postings also drop their freq region. NO query
    // path reads pair freqs (the 2-term hit path answers from df/docids, the
    // 3+-term chain reads docids only, and scoring can never target a hidden
    // marker term), so the bytes are write-only. Same legacy escape hatch as
    // write_prx: threshold 0 keeps the byte-identical pre-G01 layout. Applied
    // on the WINDOWED path only -- see build_entry for the slim-format
    // constraint.
    const bool write_freq = has_freq_ && !(is_bigram && bigram_prune_min_df_ > 0);

    // ONE fused term-level scan of freqs: total + max in a single pass, reused by
    // validate (position-count budget), stats, and the DictEntry
    // ttf_delta/max_freq. Computed BEFORE validate_term so the validator receives
    // the budget total instead of re-summing. The position-count check is gated
    // on write_prx (== has_prx_ for every term except prune-mode bigrams, whose
    // positions the G04 diet stopped buffering).
    const FreqStats fs = fuse_freq_stats(tp.freqs);
    RETURN_IF_ERROR(validate_term(tp, fs.total_freq, write_prx));
    // Collect only the 8-byte filter key per term (no whole-vocabulary string
    // copy). BSBF key = XXH64 seed 0 (Parquet-canonical).
    term_hashes_.push_back(format::bsbf_hash(tp.term));
    ++term_count_;
    stats_.sum_total_term_freq += fs.total_freq; // reused fused total (was SumOf)

    if (!st->block) {
        // Both bases come from the SAME posting sink, snapshotted at block open.
        const uint64_t base = posting_size();
        st->frq_base = base;
        st->prx_base = base;
        // G16-f: a freq-dropped index also omits the per-entry ttf/max_freq
        // stats (BM25-only); the block header flag makes it self-describing.
        st->block = std::make_unique<DictBlockBuilder>(tier_, has_prx_, st->frq_base, st->prx_base,
                                                       /*anchor_interval=*/16,
                                                       /*term_stats=*/has_freq_);
        st->block_first_term = tp.term;
    }

    DictEntry e;
    RETURN_IF_ERROR(build_entry(tp, st->frq_base, st->prx_base, fs, write_prx, write_freq, &e));
    // `e` is not used after this point, so move it into the block to avoid a
    // per-term DictEntry copy (two vector heap allocations for inline entries).
    st->block->add_entry(std::move(e));

    if (st->block->estimated_bytes() >= target_dict_block_bytes_) {
        RETURN_IF_ERROR(flush_block(st->block.get(), st->block_first_term));
        st->block.reset();
    }
    return Status::OK();
}

Status LogicalIndexWriter::build_blocks() {
    BlockState st;
    if (term_source_ != nullptr) {
        // G06: sink the flush-time phrase-bigram df gate into the buffer drain.
        // bigram_prune_min_df_ is the SAME effective threshold process_term
        // gates with below (already forced to 0 for non-positional configs and
        // in legacy no-prune mode, which disables the gate), so a pair-keyed
        // bigram term the drain drops -- provably-exact df below the gate -- is
        // exactly a term process_term would have pruned AFTER paying its
        // composed-string materialization, chain decode and emission.
        // process_term keeps gating everything that still reaches it
        // (string-keyed terms, spill-materialized pair terms whose df only the
        // merge can total, out-of-order-fed pair terms).
        term_source_->set_bigram_drain_min_df(bigram_prune_min_df_);
        Status streamed = Status::OK();
        // Drain the SPIMI buffer term-by-term; only one TermPostings is alive at a
        // time, so the input+output never fully coexist. The returned Status covers
        // both spill/merge I/O errors and add_token validation errors (the latter
        // flow through merge_runs -> spill_status_), so a separate status() check
        // is no longer needed.
        RETURN_IF_ERROR(term_source_->for_each_term_sorted([&](TermPostings&& tp) {
            if (streamed.ok()) streamed = process_term(tp, &st);
        }));
        RETURN_IF_ERROR(streamed);
    } else {
        // Materialized fallback (tests / callers holding a vector): process_term
        // frees the term's arrays, so feed a per-term COPY to keep terms_ intact
        // for the caller. This path is not the large out-of-core build, so the copy
        // is cheap.
        for (const auto& tp : terms_) {
            TermPostings copy = tp;
            RETURN_IF_ERROR(process_term(copy, &st));
        }
    }
    if (st.block) RETURN_IF_ERROR(flush_block(st.block.get(), st.block_first_term));
    return Status::OK();
}

Status LogicalIndexWriter::build(io::FileWriter* posting_out) {
    if (posting_out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: null posting sink");
    }
    if (has_norms_ && encoded_norms_.size() != doc_count_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "logical_index: norms length must equal doc_count");
    }
    // The interleaved posting region streams STRAIGHT into the container output
    // (no temp round-trip): posting_size() is the region-relative byte count,
    // derived from the output offset advanced since this index's region began.
    // The DICT region is staged in dict_buf_ (tiered: RAM under the cap =
    // spill-only; spills above it) since it must land contiguously after the
    // concurrently-streamed posting region.
    posting_out_ = posting_out;
    posting_off0_ = posting_out->bytes_written();

    RETURN_IF_ERROR(build_blocks());
    // Seal the dict buffer so a spilled temp is flushed before
    // stream_dict_region_into reads it back. A no-op for a RAM-resident dict.
    RETURN_IF_ERROR(dict_buf_.seal());

    stats_.doc_count = doc_count_;
    stats_.indexed_doc_count = doc_count_ - static_cast<uint32_t>(null_docids_.size());
    stats_.term_count = term_count_;
    stats_.null_count = static_cast<uint32_t>(null_docids_.size());

    if (has_norms_) {
        format::NormsPodWriter nw;
        for (uint8_t n : encoded_norms_) nw.add(n);
        ByteSink nsink;
        nw.finish(&nsink);
        norms_section_ = nsink.take();
    }

    if (!null_docids_.empty()) {
        format::NullBitmapWriter null_writer;
        for (uint32_t docid : null_docids_) null_writer.add_null(docid);
        ByteSink null_sink;
        null_writer.finish(doc_count_, &null_sink);
        null_bitmap_section_ = null_sink.take();
    }

    // Build the absent-term filter (block-split bloom, Parquet-canonical) from
    // the per-term keys (no retained strings) as a [28B header][bitset] blob; the
    // compound writer places it as a PHYSICAL section probed one 32-byte block on
    // demand.
    bsbf_bytes_.clear();
    if (!term_hashes_.empty()) {
        format::BsbfBuilder bf;
        RETURN_IF_ERROR(format::BsbfBuilder::create(static_cast<uint32_t>(term_hashes_.size()),
                                                    kBsbfFpp, &bf));
        for (uint64_t k : term_hashes_) bf.insert(k);
        ByteSink bsink;
        RETURN_IF_ERROR(bf.serialize(&bsink));
        bsbf_bytes_ = bsink.take();
    }
    std::vector<uint64_t>().swap(term_hashes_); // release

    const auto& ss = section_stats_;
    LOG(INFO) << "SNII section stats idx=" << index_id_ << " suffix=" << index_suffix_
              << " uni{n=" << ss.entries[0] << " dd=" << ss.dd_bytes[0]
              << " freq=" << ss.freq_bytes[0] << " prx=" << ss.prx_bytes[0]
              << " prelude=" << ss.prelude_bytes[0] << " dict=" << ss.dict_entry_bytes[0]
              << "} bg{n=" << ss.entries[1] << " dd=" << ss.dd_bytes[1]
              << " freq=" << ss.freq_bytes[1] << " prx=" << ss.prx_bytes[1]
              << " prelude=" << ss.prelude_bytes[1] << " dict=" << ss.dict_entry_bytes[1]
              << "} dict_region=" << dict_buf_.size() << " doc_count=" << doc_count_;
    return Status::OK();
}

Status LogicalIndexWriter::finish_meta(const SectionRefs& abs_refs, uint64_t dict_region_offset,
                                       ByteSink* out) const {
    if (out == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("logical_index: null meta sink");

    SampledTermIndexBuilder sti;
    for (const auto& b : blocks_) sti.add_block_first_term(b.first_term);
    ByteSink sti_sink;
    sti.finish(&sti_sink);

    DictBlockDirectoryBuilder dir;
    for (const auto& b : blocks_) {
        BlockRef ref;
        ref.offset = dict_region_offset + b.rel_offset;
        ref.length = b.length;
        ref.n_entries = b.n_entries;
        ref.flags = b.flags;
        ref.checksum = b.checksum;
        ref.uncomp_len = b.uncomp_len;
        dir.add(ref);
    }
    ByteSink dir_sink;
    dir.finish(&dir_sink);

    uint32_t flags = bsbf_bytes_.empty() ? 0u : PerIndexMetaBuilder::kHasBsbf;
    // Persist positions capability explicitly (the R1 fix): the reader must NOT
    // infer it from posting_region.length, which is non-zero for any docs-only
    // pod_ref index.
    if (has_prx_) {
        flags |= PerIndexMetaBuilder::kHasPositions;
    }
    if (phrase_bigrams_deferred_) {
        flags |= PerIndexMetaBuilder::kPhraseBigramsDeferred;
    }
    PerIndexMetaBuilder builder(index_id_, index_suffix_, flags);
    builder.set_stats(stats_);
    builder.set_sampled_term_index(sti_sink.view());
    builder.set_dict_block_directory(dir_sink.view());
    // The BSBF is a physical section (abs_refs.bsbf), not embedded in the meta.
    builder.set_section_refs(abs_refs);
    // G01/G15: declare the APPLIED bigram df-prune thresholds (both 0 emits
    // nothing -- legacy segments stay byte-identical) so the reader's 2-term
    // phrase path knows a bigram dict miss means "fall back", not "empty".
    builder.set_bigram_prune_min_df(bigram_prune_min_df_);
    builder.set_bigram_prune_max_df(bigram_prune_max_df_);
    return builder.finish(out);
}

} // namespace doris::snii::writer
