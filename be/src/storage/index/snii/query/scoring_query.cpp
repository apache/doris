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

#include "storage/index/snii/query/scoring_query.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <queue>
#include <unordered_map>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/reader/windowed_posting.h"

namespace doris::snii::query {

using format::DictEntry;
using format::DictEntryEnc;
using format::DictEntryKind;
using format::FrqPreludeReader;
using format::WindowMeta;
using reader::LogicalIndexReader;

namespace {

// One scored posting for one term in one doc.
struct TermPosting {
    uint32_t docid = 0;
    double score = 0.0;
};

// One window's block-max upper bound and the docid range it covers. block_max is
// true when max_score came from the frq_prelude columns (vs the exact-score
// fallback); both are valid upper bounds, so it is informational only.
struct WindowBound {
    uint32_t first_docid = 0; // inclusive
    uint32_t last_docid = 0;  // inclusive
    double max_score = 0.0;   // block-max upper bound for any doc in this window
    bool block_max = false;
};

// All scored postings of one query term plus its block-max metadata.
struct TermCursor {
    std::vector<TermPosting> postings; // ascending docid, exact per-doc scores
    std::vector<WindowBound> windows;  // ascending, covering all postings
    size_t pos = 0;                    // DAAT cursor into postings
};

uint32_t CurrentDoc(const TermCursor& c) {
    return c.pos < c.postings.size() ? c.postings[c.pos].docid
                                     : std::numeric_limits<uint32_t>::max();
}

// Reads one slim .frq window's bytes for a slim pod_ref/inline entry (prelude
// stripped). Windowed entries are handled separately via the prelude decode.
Status FetchSlimWindowBytes(const LogicalIndexReader& idx, const DictEntry& entry,
                            uint64_t frq_base, std::vector<uint8_t>* window_owned, Slice* window) {
    if (entry.kind == DictEntryKind::kInline) {
        *window = Slice(entry.frq_bytes);
        return Status::OK();
    }
    uint64_t win_abs = 0;
    uint64_t win_len = 0;
    RETURN_IF_ERROR(idx.resolve_frq_window(entry, frq_base, &win_abs, &win_len));
    io::BatchRangeFetcher fetcher(idx.reader());
    const size_t h = fetcher.add(win_abs, win_len);
    RETURN_IF_ERROR(fetcher.fetch());
    Slice got = fetcher.get(h);
    window_owned->assign(got.data(), got.data() + got.size());
    *window = Slice(*window_owned);
    return Status::OK();
}

// Reads a windowed entry's frq_prelude (block-max columns live here).
Status FetchPrelude(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                    FrqPreludeReader* out) {
    const auto& region = idx.section_refs().posting_region;
    const uint64_t prelude_abs = region.offset + frq_base + entry.frq_off_delta;
    io::BatchRangeFetcher fetcher(idx.reader());
    const size_t h = fetcher.add(prelude_abs, entry.prelude_len);
    RETURN_IF_ERROR(fetcher.fetch());
    return FrqPreludeReader::open(fetcher.get(h), out);
}

// Builds per-window block-max bounds from a windowed entry's prelude. Each
// WindowMeta carries the window's max_freq / max_norm and its covered docid
// range (win_base+1 .. last_docid), so bounds come straight from the directory.
Status BuildWindowBounds(const FrqPreludeReader& prelude, const ScorerContext& ctx, double avgdl,
                         const Bm25Params& params, std::vector<WindowBound>* windows) {
    const uint32_t n = prelude.n_windows();
    for (uint32_t w = 0; w < n; ++w) {
        WindowMeta m;
        RETURN_IF_ERROR(prelude.window(w, &m));
        if (m.doc_count == 0) continue;
        WindowBound wb;
        wb.first_docid = static_cast<uint32_t>(m.win_base) + (w == 0 ? 0u : 1u);
        wb.last_docid = m.last_docid;
        wb.max_score = ctx.max_score(m.max_freq, m.max_norm, avgdl, params);
        wb.block_max = true;
        windows->push_back(wb);
    }
    return Status::OK();
}

// Fallback single window covering all postings, bounded by the exact max score
// (always a valid upper bound, so pruning stays correct).
void SingleWindowFallback(const std::vector<TermPosting>& postings,
                          std::vector<WindowBound>* windows) {
    if (postings.empty()) return;
    WindowBound wb;
    wb.first_docid = postings.front().docid;
    wb.last_docid = postings.back().docid;
    wb.block_max = false;
    for (const auto& p : postings) wb.max_score = std::max(wb.max_score, p.score);
    windows->push_back(wb);
}

// Computes exact per-doc BM25 scores from decoded (docid, freq) vectors.
Status ScoreDecoded(const stats::SniiStatsProvider& stats, const ScorerContext& ctx,
                    const Bm25Params& params, const std::vector<uint32_t>& docids,
                    const std::vector<uint32_t>& freqs, std::vector<TermPosting>* out) {
    const double avgdl = stats.avgdl();
    out->reserve(docids.size());
    for (size_t i = 0; i < docids.size(); ++i) {
        uint8_t norm = 0;
        RETURN_IF_ERROR(stats.encoded_norm(docids[i], &norm));
        const uint32_t tf = i < freqs.size() ? freqs[i] : 1;
        out->push_back({docids[i], ctx.score(tf, norm, avgdl, params)});
    }
    return Status::OK();
}

// Decodes a slim/inline term's single .frq window ([dd_region][freq_region]) into
// docids/freqs using the entry's region metadata.
Status DecodeSlim(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                  std::vector<uint32_t>* docids, std::vector<uint32_t>* freqs) {
    std::vector<uint8_t> owned;
    Slice window;
    RETURN_IF_ERROR(FetchSlimWindowBytes(idx, entry, frq_base, &owned, &window));
    const uint64_t dd_len = entry.dd_meta.disk_len;
    if (dd_len > window.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "scoring_query: slim dd region exceeds window");
    }
    Slice dd_region = window.subslice(0, static_cast<size_t>(dd_len));
    RETURN_IF_ERROR(format::decode_dd_region(dd_region, entry.dd_meta,
                                             /*win_base=*/0, docids));
    Slice freq_region = window.subslice(static_cast<size_t>(dd_len),
                                        window.size() - static_cast<size_t>(dd_len));
    return format::decode_freq_region(freq_region, entry.freq_meta, docids->size(), freqs);
}

// Builds the cursor for a windowed term: tiles all windows for exact scores and
// reads the prelude once for true per-window block-max bounds.
Status BuildWindowedCursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                           const ScorerContext& ctx, const DictEntry& entry, uint64_t frq_base,
                           uint64_t prx_base, const Bm25Params& params, TermCursor* cursor) {
    reader::DecodedPosting posting;
    // Scoring needs freqs for BM25: fetch the FULL windows (want_freq=true).
    RETURN_IF_ERROR(reader::read_windowed_posting(idx, entry, frq_base, prx_base,
                                                  /*want_positions=*/false,
                                                  /*want_freq=*/true, &posting));
    RETURN_IF_ERROR(
            ScoreDecoded(stats, ctx, params, posting.docids, posting.freqs, &cursor->postings));
    FrqPreludeReader prelude;
    if (FetchPrelude(idx, entry, frq_base, &prelude).ok()) {
        RETURN_IF_ERROR(BuildWindowBounds(prelude, ctx, stats.avgdl(), params, &cursor->windows));
    }
    return Status::OK();
}

// Builds the cursor for one term: postings with exact scores + window bounds.
Status BuildCursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                   const std::string& term, const Bm25Params& params, bool* found,
                   TermCursor* cursor) {
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(term, found, &entry, &frq_base, &prx_base));
    if (!*found) return Status::OK();

    const ScorerContext ctx = ScorerContext::make(stats.indexed_doc_count(), entry.df);

    const bool windowed =
            entry.kind == DictEntryKind::kPodRef && entry.enc == DictEntryEnc::kWindowed;
    if (windowed) {
        RETURN_IF_ERROR(
                BuildWindowedCursor(idx, stats, ctx, entry, frq_base, prx_base, params, cursor));
    } else {
        std::vector<uint32_t> docids;
        std::vector<uint32_t> freqs;
        RETURN_IF_ERROR(DecodeSlim(idx, entry, frq_base, &docids, &freqs));
        RETURN_IF_ERROR(ScoreDecoded(stats, ctx, params, docids, freqs, &cursor->postings));
    }
    if (cursor->windows.empty()) {
        SingleWindowFallback(cursor->postings, &cursor->windows);
    }
    return Status::OK();
}

// Block-max upper bound for a term at a given docid: the max_score of the window
// covering docid (windows are ascending and contiguous). Beyond the last window
// the bound is 0 (the term cannot contribute).
double TermBoundAt(const TermCursor& c, uint32_t docid) {
    // Windows are ascending and contiguous; the first window whose last_docid is
    // >= docid covers it. Its block-max is a valid upper bound for any contained
    // doc, so it also bounds gaps between windows.
    for (const auto& w : c.windows) {
        if (docid <= w.last_docid) return w.max_score;
    }
    return 0.0;
}

// Min-heap keyed on score (smallest at top) maintaining the top-K.
struct TopK {
    explicit TopK(uint32_t k) : k_(k) {}
    void offer(uint32_t docid, double score) {
        if (heap_.size() < k_) {
            heap_.push({score, docid});
            return;
        }
        if (heap_.empty()) return;
        const Entry& worst = heap_.top(); // lowest score; ties: largest docid
        const bool better = score > worst.first || (score == worst.first && docid < worst.second);
        if (better) {
            heap_.pop();
            heap_.push({score, docid});
        }
    }
    double threshold() const { return heap_.size() < k_ ? -1.0 : heap_.top().first; }

    using Entry = std::pair<double, uint32_t>;
    struct Cmp {
        bool operator()(const Entry& a, const Entry& b) const {
            if (a.first != b.first) return a.first > b.first; // min-score at top
            return a.second < b.second; // for ties, largest docid at top (evictable)
        }
    };
    uint32_t k_;
    std::priority_queue<Entry, std::vector<Entry>, Cmp> heap_;
};

void DrainSorted(TopK* topk, std::vector<ScoredDoc>* out) {
    std::vector<ScoredDoc> all;
    while (!topk->heap_.empty()) {
        all.push_back({topk->heap_.top().second, topk->heap_.top().first});
        topk->heap_.pop();
    }
    std::sort(all.begin(), all.end(), [](const ScoredDoc& a, const ScoredDoc& b) {
        if (a.score != b.score) return a.score > b.score;
        return a.docid < b.docid;
    });
    *out = std::move(all);
}

Status BuildCursors(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                    const std::vector<std::string>& terms, const Bm25Params& params,
                    std::vector<TermCursor>* cursors) {
    for (const auto& term : terms) {
        bool found = false;
        TermCursor c;
        RETURN_IF_ERROR(BuildCursor(idx, stats, term, params, &found, &c));
        if (found && !c.postings.empty()) cursors->push_back(std::move(c));
    }
    return Status::OK();
}

} // namespace

Status scoring_query_exhaustive(const LogicalIndexReader& idx,
                                const stats::SniiStatsProvider& stats,
                                const std::vector<std::string>& terms, uint32_t k,
                                const Bm25Params& params, std::vector<ScoredDoc>* out) {
    if (out == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("scoring_query: null out");
    out->clear();
    if (k == 0) return Status::OK();

    std::vector<TermCursor> cursors;
    RETURN_IF_ERROR(BuildCursors(idx, stats, terms, params, &cursors));

    std::unordered_map<uint32_t, double> scores;
    for (const auto& c : cursors)
        for (const auto& p : c.postings) scores[p.docid] += p.score;

    std::vector<ScoredDoc> all;
    all.reserve(scores.size());
    for (const auto& [docid, score] : scores) all.push_back({docid, score});
    std::sort(all.begin(), all.end(), [](const ScoredDoc& a, const ScoredDoc& b) {
        if (a.score != b.score) return a.score > b.score;
        return a.docid < b.docid;
    });
    if (all.size() > k) all.resize(k);
    *out = std::move(all);
    return Status::OK();
}

namespace {

// --- Phase C: selective-fetch (lazy window) WAND -----------------------------
//
// A LazyTermCursor knows its per-window block-max bounds + docid ranges from the
// frq_prelude WITHOUT fetching any .frq window. Each window's exact (docid,score)
// postings are decoded on first access and cached, so a window is fetched at most
// once and ONLY when the WAND control flow touches a posting in it. Combined with
// window-level SkipTo (advance past whole windows whose last_docid < target via
// the prelude, never fetching them), the offer sequence is byte-identical to the
// eager scoring_query_wand path -- only the bytes read differ.
//
// Soundness: a window is fetched only when LazyCurrentDoc/LazySkipTo land the
// cursor inside it, i.e. it covers a candidate the WAND pivot already proved can
// reach the running theta (bound >= theta). LazySkipTo jumps the cursor to the
// SAME posting (first docid >= target) the eager per-doc walk would, so pivots,
// alignments and offers are identical to the eager path; only windows the eager
// path read-through-but-never-offered-from are skipped. Windows whose block-max
// bound never reaches theta are never the pivot, so never fetched.

// One query term's lazily-fetched scoring state.
struct LazyTermCursor {
    const LogicalIndexReader* idx = nullptr;
    const stats::SniiStatsProvider* stats = nullptr;
    ScorerContext ctx = ScorerContext::make(1, 1);
    Bm25Params params;
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    FrqPreludeReader prelude;
    bool windowed = false; // false => slim/inline single block already materialized

    std::vector<WindowBound> windows;  // ascending; from prelude (or slim fallback)
    std::vector<TermPosting> postings; // sparse: only fetched windows are filled
    std::vector<uint32_t> win_start;   // prefix offsets, size = windows.size()+1
    std::vector<char> fetched;         // size = windows.size()
    size_t pos = 0;                    // virtual cursor over all windows' postings
};

// Total posting count across all windows (the virtual stream length).
uint32_t TotalPostings(const LazyTermCursor& c) {
    return c.win_start.empty() ? 0 : c.win_start.back();
}

// Index of the window whose virtual range contains posting index p (p < total).
uint32_t WindowOf(const LazyTermCursor& c, uint32_t p) {
    const auto it = std::upper_bound(c.win_start.begin(), c.win_start.end(), p);
    return static_cast<uint32_t>((it - c.win_start.begin()) - 1);
}

// Fetches + decodes window w into the cursor's posting cache (idempotent). Only
// reached when the WAND proves window w can still contribute to the top-K.
Status MaterializeWindow(LazyTermCursor* c, uint32_t w) {
    if (c->fetched[w]) return Status::OK();
    WindowMeta meta;
    RETURN_IF_ERROR(c->prelude.window(w, &meta));
    reader::WindowAbsRange r;
    RETURN_IF_ERROR(reader::windowed_window_range(
            *c->idx, c->entry, c->frq_base, c->prx_base, c->prelude, w,
            /*want_positions=*/false, /*want_freq=*/true, &r));
    // Scoring needs docids + freqs: fetch the window's dd sub-range AND freq sub-range.
    io::BatchRangeFetcher fetcher(c->idx->reader(), reader::kSameTermCoalesceGap);
    const size_t dh = fetcher.add(r.dd_off, r.dd_len);
    const size_t fh = fetcher.add(r.freq_off, r.freq_len);
    RETURN_IF_ERROR(fetcher.fetch());
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    std::vector<std::vector<uint32_t>> pos;
    RETURN_IF_ERROR(reader::decode_window_slices(meta, fetcher.get(dh), fetcher.get(fh), Slice(),
                                                 /*want_positions=*/false,
                                                 /*want_freq=*/true, &docids, &freqs, &pos));
    if (docids.size() != c->win_start[w + 1] - c->win_start[w]) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "scoring_query: selective window doc-count drift");
    }
    std::vector<TermPosting> scored;
    RETURN_IF_ERROR(ScoreDecoded(*c->stats, c->ctx, c->params, docids, freqs, &scored));
    std::copy(scored.begin(), scored.end(), c->postings.begin() + c->win_start[w]);
    c->fetched[w] = 1;
    return Status::OK();
}

// Current docid at the cursor, fetching the covering window if needed. Exhausted
// cursor -> UINT32_MAX.
Status LazyCurrentDoc(LazyTermCursor* c, uint32_t* docid) {
    if (c->pos >= TotalPostings(*c)) {
        *docid = std::numeric_limits<uint32_t>::max();
        return Status::OK();
    }
    const uint32_t w = WindowOf(*c, static_cast<uint32_t>(c->pos));
    RETURN_IF_ERROR(MaterializeWindow(c, w));
    *docid = c->postings[c->pos].docid;
    return Status::OK();
}

// Advances pos to the first posting with docid >= target, skipping ENTIRE windows
// whose last_docid < target WITHOUT fetching them (prelude-only), then fetching
// just the landing window. Lands on the same posting the eager per-doc walk would.
Status LazySkipTo(LazyTermCursor* c, uint32_t target) {
    const uint32_t total = TotalPostings(*c);
    while (c->pos < total) {
        const uint32_t w = WindowOf(*c, static_cast<uint32_t>(c->pos));
        if (c->windows[w].last_docid >= target) break;
        c->pos = c->win_start[w + 1]; // skip this window entirely (no fetch)
    }
    if (c->pos >= total) return Status::OK();
    const uint32_t w = WindowOf(*c, static_cast<uint32_t>(c->pos));
    RETURN_IF_ERROR(MaterializeWindow(c, w));
    while (c->pos < total && c->postings[c->pos].docid < target) ++c->pos;
    return Status::OK();
}

// Initializes a lazy windowed cursor from the prelude alone: per-window block-max
// bounds + ranges + cache slots, with NO .frq window fetched.
Status BuildLazyWindowed(LazyTermCursor* c) {
    RETURN_IF_ERROR(reader::fetch_windowed_prelude(*c->idx, c->entry, c->frq_base, &c->prelude));
    RETURN_IF_ERROR(
            BuildWindowBounds(c->prelude, c->ctx, c->stats->avgdl(), c->params, &c->windows));
    // BuildWindowBounds keeps only non-empty windows, in window order. Build the
    // matching prefix-sum of doc_counts over those same non-empty windows so the
    // bound list, win_start and fetched stay 1:1.
    const uint32_t nb = static_cast<uint32_t>(c->windows.size());
    c->win_start.assign(nb + 1, 0);
    c->fetched.assign(nb, 0);
    uint32_t bi = 0;
    uint32_t acc = 0;
    for (uint32_t w = 0; w < c->prelude.n_windows() && bi < nb; ++w) {
        WindowMeta meta;
        RETURN_IF_ERROR(c->prelude.window(w, &meta));
        if (meta.doc_count == 0) continue;
        acc += meta.doc_count;
        c->win_start[++bi] = acc;
    }
    c->postings.assign(acc, TermPosting {});
    return Status::OK();
}

// Initializes a slim/inline cursor: its single window is small, so fetch + score
// it eagerly (exactly as the existing path). One bound covers all its postings.
Status BuildLazySlim(LazyTermCursor* c) {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    RETURN_IF_ERROR(DecodeSlim(*c->idx, c->entry, c->frq_base, &docids, &freqs));
    RETURN_IF_ERROR(ScoreDecoded(*c->stats, c->ctx, c->params, docids, freqs, &c->postings));
    SingleWindowFallback(c->postings, &c->windows);
    c->win_start = {0, static_cast<uint32_t>(c->postings.size())};
    c->fetched.assign(1, 1); // already materialized
    return Status::OK();
}

// Builds a LazyTermCursor for one term: prelude-only for windowed terms (no .frq
// fetched), fully-materialized single window for slim/inline (small).
Status BuildLazyCursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                       const std::string& term, const Bm25Params& params, bool* found,
                       LazyTermCursor* c) {
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(term, found, &c->entry, &c->frq_base, &prx_base));
    if (!*found) return Status::OK();
    c->idx = &idx;
    c->stats = &stats;
    c->params = params;
    c->prx_base = prx_base;
    c->ctx = ScorerContext::make(stats.indexed_doc_count(), c->entry.df);
    c->windowed =
            c->entry.kind == DictEntryKind::kPodRef && c->entry.enc == DictEntryEnc::kWindowed;
    return c->windowed ? BuildLazyWindowed(c) : BuildLazySlim(c);
}

Status SelectiveBuildCursors(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                             const std::vector<std::string>& terms, const Bm25Params& params,
                             std::vector<LazyTermCursor>* cursors) {
    for (const auto& term : terms) {
        bool found = false;
        LazyTermCursor c;
        RETURN_IF_ERROR(BuildLazyCursor(idx, stats, term, params, &found, &c));
        if (found && TotalPostings(c) > 0) cursors->push_back(std::move(c));
    }
    return Status::OK();
}

// Block-max upper bound for a lazy cursor at docid: block_max of the window
// covering docid (ascending, contiguous). Beyond the last window -> 0. Same
// semantics as TermBoundAt over the eager cursor's window list.
double LazyTermBoundAt(const LazyTermCursor& c, uint32_t docid) {
    for (const auto& w : c.windows) {
        if (docid <= w.last_docid) return w.max_score;
    }
    return 0.0;
}

// Sorts cursors ascending by current docid (materializing each cursor's current
// covering window), returning the smallest current docid via *front.
Status SelectiveSortByDoc(std::vector<LazyTermCursor>* cursors, uint32_t* front) {
    std::vector<uint32_t> cur(cursors->size());
    for (size_t i = 0; i < cursors->size(); ++i) {
        RETURN_IF_ERROR(LazyCurrentDoc(&(*cursors)[i], &cur[i]));
    }
    std::vector<size_t> order(cursors->size());
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    std::sort(order.begin(), order.end(), [&](size_t a, size_t b) { return cur[a] < cur[b]; });
    std::vector<LazyTermCursor> sorted;
    sorted.reserve(cursors->size());
    for (size_t i : order) sorted.push_back(std::move((*cursors)[i]));
    *cursors = std::move(sorted);
    *front = order.empty() ? std::numeric_limits<uint32_t>::max() : cur[order.front()];
    return Status::OK();
}

// Finds the pivot term: the first cursor (current-docid order) at which the
// accumulated block-max bound reaches theta. >= keeps boundary ties (matching the
// exhaustive total order). *found=false when no remaining doc can beat theta.
Status SelectivePivot(std::vector<LazyTermCursor>* cursors, double theta, size_t* pivot,
                      uint32_t* pivot_doc, bool* found) {
    double bound = 0.0;
    *found = false;
    for (size_t i = 0; i < cursors->size(); ++i) {
        uint32_t d = 0;
        RETURN_IF_ERROR(LazyCurrentDoc(&(*cursors)[i], &d));
        if (d == std::numeric_limits<uint32_t>::max()) break;
        bound += LazyTermBoundAt((*cursors)[i], d);
        if (bound >= theta) {
            *pivot = i;
            *pivot_doc = d;
            *found = true;
            return Status::OK();
        }
    }
    return Status::OK();
}

// Scores the aligned pivot doc exactly (summing all cursors AT pivot_doc) and
// advances those cursors by one posting.
Status SelectiveScorePivot(std::vector<LazyTermCursor>* cursors, uint32_t pivot_doc, TopK* topk) {
    double doc_score = 0.0;
    for (auto& c : *cursors) {
        uint32_t d = 0;
        RETURN_IF_ERROR(LazyCurrentDoc(&c, &d));
        if (d == pivot_doc) {
            doc_score += c.postings[c.pos].score; // window already materialized
            ++c.pos;
        }
    }
    topk->offer(pivot_doc, doc_score);
    return Status::OK();
}

// Advances the first lagging cursor (current doc < pivot_doc) up to pivot_doc.
Status SelectiveAdvanceLagging(std::vector<LazyTermCursor>* cursors, uint32_t pivot_doc) {
    for (auto& c : *cursors) {
        uint32_t d = 0;
        RETURN_IF_ERROR(LazyCurrentDoc(&c, &d));
        if (d < pivot_doc) {
            RETURN_IF_ERROR(LazySkipTo(&c, pivot_doc));
            return Status::OK();
        }
    }
    return Status::OK();
}

// One WAND iteration body: sort, pick pivot, then either score (aligned) or skip
// a lagging cursor forward. *done=true ends the loop.
Status SelectiveStep(std::vector<LazyTermCursor>* cursors, TopK* topk, bool* done) {
    uint32_t front = 0;
    RETURN_IF_ERROR(SelectiveSortByDoc(cursors, &front));
    if (cursors->empty() || front == std::numeric_limits<uint32_t>::max()) {
        *done = true;
        return Status::OK();
    }
    size_t pivot = 0;
    uint32_t pivot_doc = 0;
    bool found_pivot = false;
    RETURN_IF_ERROR(SelectivePivot(cursors, topk->threshold(), &pivot, &pivot_doc, &found_pivot));
    if (!found_pivot) {
        *done = true;
        return Status::OK();
    }
    if (front == pivot_doc) {
        return SelectiveScorePivot(cursors, pivot_doc, topk);
    }
    return SelectiveAdvanceLagging(cursors, pivot_doc);
}

Status SelectiveWandLoop(std::vector<LazyTermCursor>* cursors, TopK* topk) {
    bool done = false;
    while (!done) {
        RETURN_IF_ERROR(SelectiveStep(cursors, topk, &done));
    }
    return Status::OK();
}

} // namespace

Status scoring_query_wand_selective(const LogicalIndexReader& idx,
                                    const stats::SniiStatsProvider& stats,
                                    const std::vector<std::string>& terms, uint32_t k,
                                    const Bm25Params& params, std::vector<ScoredDoc>* out) {
    if (out == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("scoring_query: null out");
    out->clear();
    if (k == 0) return Status::OK();

    std::vector<LazyTermCursor> cursors;
    RETURN_IF_ERROR(SelectiveBuildCursors(idx, stats, terms, params, &cursors));

    TopK topk(k);
    RETURN_IF_ERROR(SelectiveWandLoop(&cursors, &topk));
    DrainSorted(&topk, out);
    return Status::OK();
}

Status scoring_query_wand(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                          const std::vector<std::string>& terms, uint32_t k,
                          const Bm25Params& params, std::vector<ScoredDoc>* out) {
    if (out == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("scoring_query: null out");
    out->clear();
    if (k == 0) return Status::OK();

    std::vector<TermCursor> cursors;
    RETURN_IF_ERROR(BuildCursors(idx, stats, terms, params, &cursors));

    TopK topk(k);
    // Document-at-a-time WAND with block-max bounds.
    while (true) {
        // Sort cursors by current docid (ascending; exhausted cursors sink).
        std::sort(cursors.begin(), cursors.end(), [](const TermCursor& a, const TermCursor& b) {
            return CurrentDoc(a) < CurrentDoc(b);
        });
        if (cursors.empty() ||
            CurrentDoc(cursors.front()) == std::numeric_limits<uint32_t>::max()) {
            break;
        }

        const double theta = topk.threshold();
        // Accumulate block-max upper bounds in docid order to find the pivot term.
        double bound = 0.0;
        size_t pivot = 0;
        bool found_pivot = false;
        for (size_t i = 0; i < cursors.size(); ++i) {
            const uint32_t d = CurrentDoc(cursors[i]);
            if (d == std::numeric_limits<uint32_t>::max()) break;
            bound += TermBoundAt(cursors[i], d);
            // Use >= (not >) so a doc whose upper bound only TIES the K-th threshold is
            // still explored and exact-scored: under the (score desc, docid asc) total
            // order a tie can still evict the current K-th entry (smaller docid wins),
            // exactly as the exhaustive path would. Strict > would wrongly prune ties.
            if (bound >= theta) {
                pivot = i;
                found_pivot = true;
                break;
            }
        }
        if (!found_pivot) break; // no doc can beat the threshold anymore.

        const uint32_t pivot_doc = CurrentDoc(cursors[pivot]);
        if (CurrentDoc(cursors.front()) == pivot_doc) {
            // All cursors at the pivot doc are aligned: score it exactly.
            double doc_score = 0.0;
            for (auto& c : cursors) {
                if (CurrentDoc(c) == pivot_doc) {
                    doc_score += c.postings[c.pos].score;
                    ++c.pos;
                }
            }
            topk.offer(pivot_doc, doc_score);
        } else {
            // Advance a lagging cursor toward pivot_doc (skip docs it cannot win on).
            for (auto& c : cursors) {
                if (CurrentDoc(c) < pivot_doc) {
                    while (c.pos < c.postings.size() && c.postings[c.pos].docid < pivot_doc) {
                        ++c.pos;
                    }
                    break;
                }
            }
        }
    }
    DrainSorted(&topk, out);
    return Status::OK();
}

} // namespace doris::snii::query
