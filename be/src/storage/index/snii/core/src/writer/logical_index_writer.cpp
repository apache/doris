#include "snii/writer/logical_index_writer.h"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <span>
#include <utility>

#include "snii/common/slice.h"
#include "snii/encoding/crc32c.h"
#include "snii/encoding/zstd_codec.h"
#include "snii/format/bsbf.h"
#include "snii/format/dict_block.h"
#include "snii/format/dict_block_directory.h"
#include "snii/format/frq_pod.h"
#include "snii/format/frq_prelude.h"
#include "snii/format/norms_pod.h"
#include "snii/format/null_bitmap.h"
#include "snii/format/prx_pod.h"

namespace snii::writer {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

using snii::format::BlockRef;
using snii::format::DictBlockBuilder;
using snii::format::DictBlockDirectoryBuilder;
using snii::format::DictEntry;
using snii::format::DictEntryEnc;
using snii::format::DictEntryKind;
using snii::format::FrqPreludeColumns;
using snii::format::PerIndexMetaBuilder;
using snii::format::SampledTermIndexBuilder;
using snii::format::SectionRefs;
using snii::format::WindowMeta;

namespace {

// Target false-positive probability for the block-split bloom XFilter. Sizes
// the filter via Parquet OptimalNumOfBytes; L0 keeps the probe in memory and L1
// keeps the per-query cost at one 32-byte block.
constexpr double kBsbfFpp = 0.01;
// Zstd "auto" sentinel for window builders (raw for tiny payloads).
constexpr int kAutoZstd = -1;
// Force-raw level for .frq dd/freq regions. Their plaintext is PFOR-bit-packed
// doc-deltas/freqs -- already high-entropy, so zstd shrinks ~30 MB of input by
// <0.1 MiB while burning ~0.4s CPU (and an extra crc pass over the compressed
// bytes) at 5M. We force raw here and keep zstd only on .prx (which compresses
// ~77%). Output stays self-describing: the region meta records zstd=false.
constexpr int kRawFrqRegion = 0;
// Windows per super-block in the two-level prelude directory (design section
// 5).
constexpr uint32_t kPreludeGroupSize = 64;
// zstd level for whole-DICT-block compression. Level 3 (zstd default)
// compresses the 64KiB front-coded term-key + entry-meta + inline-posting
// blocks ~40% at ~120 MiB/s encode / ~600 MiB/s decode -- a large size win for
// a small build-CPU cost, and a per-lookup decode (~0.1ms/64KiB) that is
// dominated by the S3 round trip it shrinks. Higher levels gain <1% here for
// materially more CPU.
constexpr int kDictBlockZstdLevel = 3;

using snii::format::FrqRegionMeta;

// Encodes one window's dd region (and freq region when has_freq) into separate
// buffers, returning their codec metadata. The dd region is the docs-only data;
// the freq region is the skippable suffix. Used for both the grouped windowed
// layout (regions concatenated into posting-level blocks) and the single-window
// slim/inline layout ([dd_region][freq_region]).
doris::Status EncodeRegions(std::span<const uint32_t> docids, std::span<const uint32_t> freqs,
                     uint64_t win_base, bool has_freq, std::vector<uint8_t>* dd_out,
                     FrqRegionMeta* dd_meta, std::vector<uint8_t>* freq_out,
                     FrqRegionMeta* freq_meta) {
    ByteSink dd_sink;
    RETURN_IF_ERROR(
            snii::format::build_dd_region(docids, win_base, kRawFrqRegion, &dd_sink, dd_meta));
    *dd_out = dd_sink.take();
    if (!has_freq) {
        *freq_out = std::vector<uint8_t>();
        *freq_meta = FrqRegionMeta {};
        return doris::Status::OK();
    }
    ByteSink freq_sink;
    RETURN_IF_ERROR(
            snii::format::build_freq_region(freqs, kRawFrqRegion, &freq_sink, freq_meta));
    *freq_out = freq_sink.take();
    return doris::Status::OK();
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
doris::Status EncodeRegionsInto(WindowScratch* sc, std::span<const uint32_t> docids,
                         std::span<const uint32_t> freqs, uint64_t win_base, bool has_freq,
                         FrqRegionMeta* dd_meta, FrqRegionMeta* freq_meta) {
    sc->dd_sink.clear();
    RETURN_IF_ERROR(
            snii::format::build_dd_region(docids, win_base, kRawFrqRegion, &sc->dd_sink, dd_meta));
    if (has_freq) {
        sc->freq_sink.clear();
        RETURN_IF_ERROR(
                snii::format::build_freq_region(freqs, kRawFrqRegion, &sc->freq_sink, freq_meta));
    } else {
        *freq_meta = FrqRegionMeta {};
    }
    return doris::Status::OK();
}

// Builds a single .prx window directly from a FLAT positions slice + its
// parallel freqs slice (doc d owns the next freqs[d] entries). Byte-identical
// to building from per-doc vectors, but with NO vector-of-vectors
// materialization: the writer indexes straight into the term's flat positions
// buffer.
doris::Status MakePrxWindow(std::span<const uint32_t> positions_flat, std::span<const uint32_t> freqs,
                     std::vector<uint8_t>* out) {
    ByteSink sink;
    RETURN_IF_ERROR(
            snii::format::build_prx_window_flat(positions_flat, freqs, kAutoZstd, &sink));
    *out = sink.take();
    return doris::Status::OK();
}

uint32_t MaxOf(std::span<const uint32_t> v) {
    uint32_t m = 0;
    for (uint32_t x : v) {
        if (x > m) m = x;
    }
    return m;
}

uint64_t SumOf(const std::vector<uint32_t>& v) {
    uint64_t s = 0;
    for (uint32_t x : v) s += x;
    return s;
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
    return df >= snii::format::kAdaptiveWindowDfThreshold ? snii::format::kAdaptiveWindowDocs
                                                          : snii::format::kFrqBaseUnit;
}

// Builds the two-level .frq prelude for a windowed term and returns its bytes.
doris::Status BuildPrelude(const std::vector<WindowMeta>& windows, bool has_freq, bool has_prx,
                    std::vector<uint8_t>* out) {
    FrqPreludeColumns cols;
    cols.has_freq = has_freq;
    cols.has_prx = has_prx;
    cols.group_size = kPreludeGroupSize;
    cols.windows = windows;
    ByteSink sink;
    RETURN_IF_ERROR(snii::format::build_frq_prelude(cols, &sink));
    *out = sink.take();
    return doris::Status::OK();
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
doris::Status BuildWindowedPosting(TermPostings& tp, bool has_freq, bool has_prx,
                            const std::vector<uint8_t>& norms, snii::io::FileWriter* posting_out,
                            WindowedPosting* out) {
    const uint32_t unit = AdaptiveWindowDocs(static_cast<uint32_t>(tp.docids.size()));
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
                RETURN_IF_ERROR(snii::format::build_prx_window_flat(pos_span, freqs, kAutoZstd,
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
        RETURN_IF_ERROR(
                EncodeRegionsInto(&sc, docs, freqs, win_base, has_freq, &dd_meta, &freq_meta));
        LayoutWindowRegions(dd_meta, sc.dd_sink.buffer(), freq_meta, sc.freq_sink.buffer(),
                            has_freq, out, &out->windows[wi]);
        win_base = out->windows[wi].last_docid;
    }
    return doris::Status::OK();
}

} // namespace

LogicalIndexWriter::LogicalIndexWriter(const SniiIndexInput& in)
        : index_id_(in.index_id),
          index_suffix_(in.index_suffix),
          tier_(snii::format::tier_of(in.config)),
          has_prx_(snii::format::has_positions(in.config)),
          has_freq_(snii::format::tier_of(in.config) >= snii::format::IndexTier::kT2),
          has_norms_(snii::format::has_scoring(in.config)),
          doc_count_(in.doc_count),
          null_docids_(in.null_docids),
          terms_(in.terms),
          term_source_(in.term_source),
          encoded_norms_(in.encoded_norms),
          target_dict_block_bytes_(in.target_dict_block_bytes != 0
                                           ? in.target_dict_block_bytes
                                           : snii::format::kDefaultTargetDictBlockBytes),
          // No independent dict cap: the dict spills via the writer's UNIFIED
          // gate-2 cap (in.mem_reporter->over_cap()); UINT64_MAX disables the local
          // per-buffer cap.
          dict_buf_(UINT64_MAX, "dict", in.mem_reporter) {}

doris::Status LogicalIndexWriter::validate_term(const TermPostings& tp) const {
    if (tp.freqs.size() != tp.docids.size()) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: freqs length must equal docids");
    }
    if (has_prx_) {
        uint64_t total_pos = 0;
        for (uint32_t f : tp.freqs) total_pos += f;
        // Streamed positions (pos_pump set): validate against the declared
        // pos_total (positions_flat is intentionally empty). Otherwise validate the
        // flat buffer.
        const uint64_t have = tp.pos_pump ? tp.pos_total : tp.positions_flat.size();
        if (total_pos != have) {
            return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: positions count must equal sum(freqs)");
        }
    }
    for (size_t i = 1; i < tp.docids.size(); ++i) {
        if (tp.docids[i] <= tp.docids[i - 1]) {
            return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: docids must be strictly ascending");
        }
    }
    return doris::Status::OK();
}

// Emits a windowed term: splits into base-unit windows, encodes each window's
// dd/freq regions separately, groups them at posting level, builds a two-level
// prelude, and lays out [prx span][prelude][dd-block][freq-block] CONTIGUOUSLY
// in the single posting region (prx span first, then the frq span). Sets
// enc=windowed + has_sb. frq_docs_len = prelude_len + dd_block_len is the
// contiguous docs-only prefix, which stays INSIDE the frq span.
doris::Status LogicalIndexWriter::build_windowed_entry(TermPostings& tp, uint64_t frq_base,
                                                uint64_t prx_base, DictEntry* e) {
    // The prx span starts here: pass 1 streams each .prx window straight into
    // the posting sink, so prx_off_delta is measured against the live
    // posting-sink size.
    const uint64_t prx_off = posting_size();
    WindowedPosting wp;
    RETURN_IF_ERROR(
            BuildWindowedPosting(tp, has_freq_, has_prx_, encoded_norms_, posting_out_, &wp));
    // wp.prx_total_len bytes were just streamed straight to the posting sink (0
    // when !has_prx). docids/freqs are now fully encoded into wp; release the
    // source arrays before the (potentially large) wp blocks are appended to
    // disk.
    std::vector<uint32_t>().swap(tp.docids);
    std::vector<uint32_t>().swap(tp.freqs);
    std::vector<uint8_t> prelude;
    RETURN_IF_ERROR(BuildPrelude(wp.windows, has_freq_, has_prx_, &prelude));

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
    if (has_prx_) {
        e->prx_off_delta = prx_off - prx_base;
        e->prx_len = wp.prx_total_len; // == frq_off - prx_off
    }
    return doris::Status::OK();
}

// Emits a slim term as a single .frq window (win_base=0) laid out [dd][freq]:
// inline when the encoded bytes are tiny, otherwise a slim pod_ref (no
// prelude). The dd region is the docs-only prefix; the freq region (when
// has_freq) is the skippable suffix. Region codecs are recorded in the
// DictEntry. For a pod_ref, the term's [prx][frq] spans are appended to the
// single posting region with the prx span FIRST (consistent with the windowed
// path); the reader resolves each delta independently so the relative order is
// not load-bearing.
doris::Status LogicalIndexWriter::build_slim_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                                            DictEntry* e) {
    std::vector<uint8_t> dd_bytes, freq_bytes;
    FrqRegionMeta dd_meta, freq_meta;
    RETURN_IF_ERROR(EncodeRegions(tp.docids, tp.freqs, /*win_base=*/0, has_freq_, &dd_bytes,
                                       &dd_meta, &freq_bytes, &freq_meta));
    std::vector<uint8_t> frq_win = dd_bytes; // [dd_region][freq_region]
    AppendBytes(&frq_win, freq_bytes);
    std::vector<uint8_t> prx_win;
    if (has_prx_) {
        RETURN_IF_ERROR(MakePrxWindow(tp.positions_flat, tp.freqs, &prx_win));
    }

    e->enc = DictEntryEnc::kSlim;
    e->dd_meta = dd_meta;
    e->freq_meta = freq_meta;

    if (frq_win.size() <= snii::format::kDefaultInlineThreshold) {
        e->kind = DictEntryKind::kInline;
        e->inline_dd_disk_len = dd_meta.disk_len;
        e->frq_bytes = std::move(frq_win);
        if (has_prx_) e->prx_bytes = std::move(prx_win);
        return doris::Status::OK();
    }

    // POD_REF: write [prx][frq] into the single posting sink, prx span first.
    e->kind = DictEntryKind::kPodRef;
    e->frq_docs_len = dd_meta.disk_len; // docs-only prefix = the single dd region
    if (has_prx_) {
        const uint64_t prx_off = posting_size();
        RETURN_IF_ERROR(posting_out_->append(Slice(prx_win)));
        e->prx_off_delta = prx_off - prx_base;
        e->prx_len = posting_size() - prx_off;
    }
    const uint64_t frq_off = posting_size(); // immediately after the prx span
    RETURN_IF_ERROR(posting_out_->append(Slice(frq_win)));
    e->frq_off_delta = frq_off - frq_base;
    e->frq_len = posting_size() - frq_off;
    return doris::Status::OK();
}

// Builds the DictEntry for one term. Inline entries embed their .frq/.prx
// bytes; pod_ref entries append [prx][frq] bytes to the single posting region
// and record off_delta relative to frq_base/prx_base (the posting-region size
// captured when the block opened; both bases hold that same value).
doris::Status LogicalIndexWriter::build_entry(TermPostings& tp, uint64_t frq_base, uint64_t prx_base,
                                       DictEntry* e) {
    e->term = tp.term;
    e->df = static_cast<uint32_t>(tp.docids.size());
    e->ttf_delta = SumOf(tp.freqs); // simple: ttf stored directly as ttf_delta
    e->max_freq = MaxOf(tp.freqs);

    if (e->df >= snii::format::kSlimDfThreshold) {
        return build_windowed_entry(tp, frq_base, prx_base, e);
    }
    return build_slim_entry(tp, frq_base, prx_base, e);
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
doris::Status LogicalIndexWriter::flush_block(DictBlockBuilder* block, std::string first_term) {
    ByteSink bsink;
    block->finish(&bsink);
    const Slice plain = bsink.view();
    BlockRecord rec;
    rec.rel_offset = dict_buf_.size();
    rec.n_entries = block->n_entries();
    rec.checksum = snii::crc32c(plain); // crc over UNCOMPRESSED block bytes
    rec.first_term = std::move(first_term);

    std::vector<uint8_t> comp;
    doris::Status zs = snii::zstd_compress(plain, kDictBlockZstdLevel, &comp);
    if (zs.ok() && comp.size() < plain.size()) {
        rec.flags = snii::format::block_ref_flags::kZstd;
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
    return doris::Status::OK();
}

// Running state for the in-flight DICT block while terms stream past.
struct LogicalIndexWriter::BlockState {
    std::unique_ptr<DictBlockBuilder> block;
    std::string block_first_term;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

doris::Status LogicalIndexWriter::process_term(TermPostings& tp, BlockState* st) {
    RETURN_IF_ERROR(validate_term(tp));
    // Collect only the 8-byte filter key per term (no whole-vocabulary string
    // copy). BSBF key = XXH64 seed 0 (Parquet-canonical).
    term_hashes_.push_back(snii::format::bsbf_hash(tp.term));
    ++term_count_;
    stats_.sum_total_term_freq += SumOf(tp.freqs);

    if (!st->block) {
        // Both bases come from the SAME posting sink, snapshotted at block open.
        const uint64_t base = posting_size();
        st->frq_base = base;
        st->prx_base = base;
        st->block = std::make_unique<DictBlockBuilder>(tier_, has_prx_, st->frq_base, st->prx_base);
        st->block_first_term = tp.term;
    }

    DictEntry e;
    RETURN_IF_ERROR(build_entry(tp, st->frq_base, st->prx_base, &e));
    st->block->add_entry(e);

    if (st->block->estimated_bytes() >= target_dict_block_bytes_) {
        RETURN_IF_ERROR(flush_block(st->block.get(), st->block_first_term));
        st->block.reset();
    }
    return doris::Status::OK();
}

doris::Status LogicalIndexWriter::build_blocks() {
    BlockState st;
    if (term_source_ != nullptr) {
        doris::Status streamed = doris::Status::OK();
        // Drain the SPIMI buffer term-by-term; only one TermPostings is alive at a
        // time, so the input+output never fully coexist. The returned doris::Status covers
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
    return doris::Status::OK();
}

doris::Status LogicalIndexWriter::build(snii::io::FileWriter* posting_out) {
    if (posting_out == nullptr) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: null posting sink");
    }
    if (has_norms_ && encoded_norms_.size() != doc_count_) {
        return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: norms length must equal doc_count");
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
        snii::format::NormsPodWriter nw;
        for (uint8_t n : encoded_norms_) nw.add(n);
        ByteSink nsink;
        nw.finish(&nsink);
        norms_section_ = nsink.take();
    }

    if (!null_docids_.empty()) {
        snii::format::NullBitmapWriter null_writer;
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
        snii::format::BsbfBuilder bf;
        RETURN_IF_ERROR(snii::format::BsbfBuilder::create(
                static_cast<uint32_t>(term_hashes_.size()), kBsbfFpp, &bf));
        for (uint64_t k : term_hashes_) bf.insert(k);
        ByteSink bsink;
        RETURN_IF_ERROR(bf.serialize(&bsink));
        bsbf_bytes_ = bsink.take();
    }
    std::vector<uint64_t>().swap(term_hashes_); // release
    return doris::Status::OK();
}

doris::Status LogicalIndexWriter::finish_meta(const SectionRefs& abs_refs, uint64_t dict_region_offset,
                                       ByteSink* out) const {
    if (out == nullptr) return doris::Status::Error<doris::ErrorCode::INVALID_ARGUMENT, false>("logical_index: null meta sink");

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
    if (has_prx_) flags |= PerIndexMetaBuilder::kHasPositions;
    PerIndexMetaBuilder builder(index_id_, index_suffix_, flags);
    builder.set_stats(stats_);
    builder.set_sampled_term_index(sti_sink.view());
    builder.set_dict_block_directory(dir_sink.view());
    // The BSBF is a physical section (abs_refs.bsbf), not embedded in the meta.
    builder.set_section_refs(abs_refs);
    return builder.finish(out);
}

} // namespace snii::writer
