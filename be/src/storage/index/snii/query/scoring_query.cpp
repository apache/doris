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
#include <numeric>
#include <queue>
#include <span>
#include <unordered_map>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/reader/dict_block_cache.h"
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

uint32_t current_doc(const TermCursor& c) {
    return c.pos < c.postings.size() ? c.postings[c.pos].docid
                                     : std::numeric_limits<uint32_t>::max();
}

// Reads one slim .frq window's bytes for a slim pod_ref/inline entry (prelude
// stripped). Windowed entries are handled separately via the prelude decode.
Status fetch_slim_window_bytes(const LogicalIndexReader& idx, const DictEntry& entry,
                               uint64_t frq_base, std::vector<uint8_t>* window_owned,
                               Slice* window) {
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
Status fetch_prelude(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
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
Status build_window_bounds(const FrqPreludeReader& prelude, const ScorerContext& ctx, double avgdl,
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
void single_window_fallback(const std::vector<TermPosting>& postings,
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
Status score_decoded(const stats::SniiStatsProvider& stats, const ScorerContext& ctx, double avgdl,
                     const Bm25Params& params, const std::vector<uint32_t>& docids,
                     const std::vector<uint32_t>& freqs, std::vector<TermPosting>* out) {
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
Status decode_slim(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                   std::vector<uint32_t>* docids, std::vector<uint32_t>* freqs) {
    std::vector<uint8_t> owned;
    Slice window;
    RETURN_IF_ERROR(fetch_slim_window_bytes(idx, entry, frq_base, &owned, &window));
    const uint64_t dd_len = entry.dd_meta.disk_len;
    if (dd_len > window.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "scoring_query: slim dd region exceeds window");
    }
    Slice dd_region = window.subslice(0, static_cast<size_t>(dd_len));
    RETURN_IF_ERROR(format::decode_dd_region(dd_region, entry.dd_meta,
                                             /*win_base=*/0, docids));
    // G16-c freq-dropped segments (write_freq == false) carry a zero-length
    // freq region on slim/inline entries. Fail with the SEMANTIC error and NOT
    // with INVERTED_INDEX_FILE_CORRUPTED: the Doris segment iterator silently
    // downgrades that code to a non-index evaluation, which would mask a
    // by-design layout as data corruption once BM25 runs over mixed segments.
    if (window.size() == static_cast<size_t>(dd_len) && !docids->empty()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "scoring_query: freqs requested but the slim entry has no freq region "
                "(freq-dropped positions index)");
    }
    Slice freq_region = window.subslice(static_cast<size_t>(dd_len),
                                        window.size() - static_cast<size_t>(dd_len));
    return format::decode_freq_region(freq_region, entry.freq_meta, docids->size(), freqs);
}

// Builds the cursor for a windowed term: tiles all windows for exact scores and
// reads the prelude once for true per-window block-max bounds.
Status build_windowed_cursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                             const ScorerContext& ctx, const DictEntry& entry, uint64_t frq_base,
                             uint64_t prx_base, double avgdl, const Bm25Params& params,
                             TermCursor* cursor) {
    reader::DecodedPosting posting;
    // Scoring needs freqs for BM25: fetch the FULL windows (want_freq=true).
    RETURN_IF_ERROR(reader::read_windowed_posting(idx, entry, frq_base, prx_base,
                                                  /*want_positions=*/false,
                                                  /*want_freq=*/true, &posting));
    RETURN_IF_ERROR(score_decoded(stats, ctx, avgdl, params, posting.docids, posting.freqs,
                                  &cursor->postings));
    FrqPreludeReader prelude;
    if (fetch_prelude(idx, entry, frq_base, &prelude).ok()) {
        RETURN_IF_ERROR(build_window_bounds(prelude, ctx, avgdl, params, &cursor->windows));
    }
    return Status::OK();
}

Status build_resolved_cursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                             const ScorerContext& ctx, const DictEntry& entry, uint64_t frq_base,
                             uint64_t prx_base, double avgdl, const Bm25Params& params,
                             TermCursor* cursor) {
    const bool windowed =
            entry.kind == DictEntryKind::kPodRef && entry.enc == DictEntryEnc::kWindowed;
    if (windowed) {
        RETURN_IF_ERROR(build_windowed_cursor(idx, stats, ctx, entry, frq_base, prx_base, avgdl,
                                              params, cursor));
    } else {
        std::vector<uint32_t> docids;
        std::vector<uint32_t> freqs;
        RETURN_IF_ERROR(decode_slim(idx, entry, frq_base, &docids, &freqs));
        RETURN_IF_ERROR(score_decoded(stats, ctx, avgdl, params, docids, freqs, &cursor->postings));
    }
    if (cursor->windows.empty()) {
        single_window_fallback(cursor->postings, &cursor->windows);
    }
    return Status::OK();
}

Status accumulate_decoded_candidate_scores(const stats::SniiStatsProvider& stats,
                                           const ScorerContext& scorer, double avgdl,
                                           const Bm25Params& params,
                                           const std::vector<uint32_t>& docids,
                                           const std::vector<uint32_t>& freqs,
                                           std::span<const uint32_t> candidates,
                                           std::span<double> scores) {
    DCHECK_EQ(candidates.size(), scores.size());
    DCHECK_EQ(docids.size(), freqs.size());
    size_t doc_index = 0;
    size_t candidate_index = 0;
    while (doc_index < docids.size() && candidate_index < candidates.size()) {
        if (docids[doc_index] < candidates[candidate_index]) {
            ++doc_index;
            continue;
        }
        if (candidates[candidate_index] < docids[doc_index]) {
            ++candidate_index;
            continue;
        }
        uint8_t norm = 0;
        RETURN_IF_ERROR(stats.encoded_norm(docids[doc_index], &norm));
        scores[candidate_index] += scorer.score(freqs[doc_index], norm, avgdl, params);
        ++doc_index;
        ++candidate_index;
    }
    return Status::OK();
}

struct CandidateWindowWork {
    WindowMeta meta;
    size_t candidate_begin = 0;
    size_t candidate_end = 0;
    size_t dd_handle = 0;
    size_t freq_handle = 0;
};

Status accumulate_windowed_candidate_scores(const LogicalIndexReader& idx,
                                            const stats::SniiStatsProvider& stats,
                                            const DictEntry& entry, uint64_t frq_base,
                                            uint64_t prx_base,
                                            const std::vector<uint32_t>& candidates,
                                            const ScorerContext& scorer, double avgdl,
                                            const Bm25Params& params, std::vector<double>* scores) {
    FrqPreludeReader prelude;
    RETURN_IF_ERROR(reader::fetch_windowed_prelude(idx, entry, frq_base, &prelude));

    std::vector<uint32_t> windows;
    const uint32_t window_count = prelude.n_windows();
    const bool candidates_cover_segment =
            static_cast<uint64_t>(candidates.size()) == stats.doc_count() && !candidates.empty() &&
            candidates.front() == 0 &&
            static_cast<uint64_t>(candidates.back()) + 1 == stats.doc_count();
    bool scan_all = candidates_cover_segment;
    if (scan_all) {
        windows.resize(window_count);
        std::iota(windows.begin(), windows.end(), 0);
    } else {
        prelude.select_covering_windows(candidates, &windows);
        scan_all = windows.size() == window_count;
    }
    if (windows.empty()) {
        return Status::OK();
    }

    // A sparse set must not be re-expanded into a near-full posting by merging
    // across unselected windows. Dense/all-window reads retain the existing
    // same-term coalescing policy and therefore the existing request shape.
    const uint64_t coalesce_gap = scan_all ? reader::kSameTermCoalesceGap : 0;
    io::BatchRangeFetcher fetcher(idx.reader(), coalesce_gap);
    std::vector<CandidateWindowWork> work;
    work.reserve(windows.size());
    size_t candidate_cursor = 0;
    for (uint32_t window : windows) {
        CandidateWindowWork item;
        RETURN_IF_ERROR(prelude.window(window, &item.meta));
        const uint32_t first_docid =
                window == 0 ? 0 : static_cast<uint32_t>(item.meta.win_base + 1);
        while (candidate_cursor < candidates.size() && candidates[candidate_cursor] < first_docid) {
            ++candidate_cursor;
        }
        item.candidate_begin = candidate_cursor;
        while (candidate_cursor < candidates.size() &&
               candidates[candidate_cursor] <= item.meta.last_docid) {
            ++candidate_cursor;
        }
        item.candidate_end = candidate_cursor;

        reader::WindowAbsRange range;
        RETURN_IF_ERROR(reader::windowed_window_range(
                idx, entry, frq_base, prx_base, prelude, window,
                /*want_positions=*/false, /*want_freq=*/true, &range));
        item.dd_handle = fetcher.add(range.dd_off, range.dd_len);
        item.freq_handle = fetcher.add(range.freq_off, range.freq_len);
        work.push_back(std::move(item));
    }
    RETURN_IF_ERROR(fetcher.fetch());

    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    std::vector<std::vector<uint32_t>> positions;
    for (const auto& item : work) {
        docids.clear();
        freqs.clear();
        RETURN_IF_ERROR(reader::decode_window_slices(
                item.meta, fetcher.get(item.dd_handle), fetcher.get(item.freq_handle), Slice(),
                /*want_positions=*/false, /*want_freq=*/true, &docids, &freqs, &positions));
        const size_t candidate_count = item.candidate_end - item.candidate_begin;
        RETURN_IF_ERROR(accumulate_decoded_candidate_scores(
                stats, scorer, avgdl, params, docids, freqs,
                std::span<const uint32_t>(candidates)
                        .subspan(item.candidate_begin, candidate_count),
                std::span<double>(*scores).subspan(item.candidate_begin, candidate_count)));
    }
    return Status::OK();
}

Status accumulate_resolved_candidate_scores(const LogicalIndexReader& idx,
                                            const stats::SniiStatsProvider& stats,
                                            const DictEntry& entry, uint64_t frq_base,
                                            uint64_t prx_base,
                                            const std::vector<uint32_t>& candidates,
                                            const ScorerContext& scorer, double avgdl,
                                            const Bm25Params& params, std::vector<double>* scores) {
    const bool windowed =
            entry.kind == DictEntryKind::kPodRef && entry.enc == DictEntryEnc::kWindowed;
    if (windowed) {
        return accumulate_windowed_candidate_scores(idx, stats, entry, frq_base, prx_base,
                                                    candidates, scorer, avgdl, params, scores);
    }

    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    RETURN_IF_ERROR(decode_slim(idx, entry, frq_base, &docids, &freqs));
    return accumulate_decoded_candidate_scores(stats, scorer, avgdl, params, docids, freqs,
                                               candidates, *scores);
}

// Builds the cursor for one term: postings with exact scores + window bounds.
Status build_cursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                    const std::string& term, const Bm25Params& params, bool* found,
                    TermCursor* cursor) {
    DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    RETURN_IF_ERROR(idx.lookup(term, found, &entry, &frq_base, &prx_base));
    if (!*found) return Status::OK();

    const ScorerContext ctx = ScorerContext::make(stats.indexed_doc_count(), entry.df);
    return build_resolved_cursor(idx, stats, ctx, entry, frq_base, prx_base, stats.avgdl(), params,
                                 cursor);
}

// Block-max upper bound for a term at a given docid: the max_score of the window
// covering docid (windows are ascending and contiguous). Beyond the last window
// the bound is 0 (the term cannot contribute).
double term_bound_at(const TermCursor& c, uint32_t docid) {
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

void drain_sorted(TopK* topk, std::vector<ScoredDoc>* out) {
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

Status build_cursors(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                     const std::vector<std::string>& terms, const Bm25Params& params,
                     std::vector<TermCursor>* cursors) {
    for (const auto& term : terms) {
        bool found = false;
        TermCursor c;
        RETURN_IF_ERROR(build_cursor(idx, stats, term, params, &found, &c));
        if (found && !c.postings.empty()) cursors->push_back(std::move(c));
    }
    return Status::OK();
}

} // namespace

Status scoring_query_candidates(const LogicalIndexReader& idx,
                                const stats::SniiStatsProvider& segment_stats,
                                const std::vector<CollectionScoringTerm>& terms,
                                const roaring::Roaring& final_candidates, double collection_avgdl,
                                const Bm25Params& params, std::vector<ScoredDoc>* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "scoring_query_candidates: null out");
    }
    out->clear();
    if (final_candidates.isEmpty()) {
        return Status::OK();
    }
    if (!(collection_avgdl > 0.0)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "scoring_query_candidates: collection avgdl must be positive");
    }

    std::vector<uint32_t> candidate_docids;
    candidate_docids.reserve(final_candidates.cardinality());
    for (uint32_t docid : final_candidates) {
        candidate_docids.push_back(docid);
    }
    std::vector<double> candidate_scores(candidate_docids.size(), 0.0);
    reader::DictBlockCache dict_block_cache;

    for (const auto& term : terms) {
        bool found = false;
        DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(idx.lookup(term.physical_term, &found, &entry, &frq_base, &prx_base,
                                   &dict_block_cache));
        if (!found) {
            continue;
        }

        const ScorerContext scorer = ScorerContext::from_idf(term.idf);
        RETURN_IF_ERROR(accumulate_resolved_candidate_scores(
                idx, segment_stats, entry, frq_base, prx_base, candidate_docids, scorer,
                collection_avgdl, params, &candidate_scores));
    }

    std::vector<ScoredDoc> scored_candidates;
    scored_candidates.reserve(candidate_docids.size());
    for (size_t i = 0; i < candidate_docids.size(); ++i) {
        scored_candidates.push_back({.docid = candidate_docids[i], .score = candidate_scores[i]});
    }
    *out = std::move(scored_candidates);
    return Status::OK();
}

Status scoring_query_exhaustive(const LogicalIndexReader& idx,
                                const stats::SniiStatsProvider& stats,
                                const std::vector<std::string>& terms, uint32_t k,
                                const Bm25Params& params, std::vector<ScoredDoc>* out) {
    if (out == nullptr)
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("scoring_query: null out");
    out->clear();
    if (k == 0) return Status::OK();

    std::vector<TermCursor> cursors;
    RETURN_IF_ERROR(build_cursors(idx, stats, terms, params, &cursors));

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
// Soundness: a window is fetched only when lazy_current_doc/lazy_skip_to land the
// cursor inside it, i.e. it covers a candidate the WAND pivot already proved can
// reach the running theta (bound >= theta). lazy_skip_to jumps the cursor to the
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
uint32_t total_postings(const LazyTermCursor& c) {
    return c.win_start.empty() ? 0 : c.win_start.back();
}

// Index of the window whose virtual range contains posting index p (p < total).
uint32_t window_of(const LazyTermCursor& c, uint32_t p) {
    const auto it = std::upper_bound(c.win_start.begin(), c.win_start.end(), p);
    return static_cast<uint32_t>((it - c.win_start.begin()) - 1);
}

// Fetches + decodes window w into the cursor's posting cache (idempotent). Only
// reached when the WAND proves window w can still contribute to the top-K.
Status materialize_window(LazyTermCursor* c, uint32_t w) {
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
    RETURN_IF_ERROR(
            score_decoded(*c->stats, c->ctx, c->stats->avgdl(), c->params, docids, freqs, &scored));
    std::copy(scored.begin(), scored.end(), c->postings.begin() + c->win_start[w]);
    c->fetched[w] = 1;
    return Status::OK();
}

// Current docid at the cursor, fetching the covering window if needed. Exhausted
// cursor -> UINT32_MAX.
Status lazy_current_doc(LazyTermCursor* c, uint32_t* docid) {
    if (c->pos >= total_postings(*c)) {
        *docid = std::numeric_limits<uint32_t>::max();
        return Status::OK();
    }
    const uint32_t w = window_of(*c, static_cast<uint32_t>(c->pos));
    RETURN_IF_ERROR(materialize_window(c, w));
    *docid = c->postings[c->pos].docid;
    return Status::OK();
}

// Advances pos to the first posting with docid >= target, skipping ENTIRE windows
// whose last_docid < target WITHOUT fetching them (prelude-only), then fetching
// just the landing window. Lands on the same posting the eager per-doc walk would.
Status lazy_skip_to(LazyTermCursor* c, uint32_t target) {
    const uint32_t total = total_postings(*c);
    while (c->pos < total) {
        const uint32_t w = window_of(*c, static_cast<uint32_t>(c->pos));
        if (c->windows[w].last_docid >= target) break;
        c->pos = c->win_start[w + 1]; // skip this window entirely (no fetch)
    }
    if (c->pos >= total) return Status::OK();
    const uint32_t w = window_of(*c, static_cast<uint32_t>(c->pos));
    RETURN_IF_ERROR(materialize_window(c, w));
    while (c->pos < total && c->postings[c->pos].docid < target) ++c->pos;
    return Status::OK();
}

// Initializes a lazy windowed cursor from the prelude alone: per-window block-max
// bounds + ranges + cache slots, with NO .frq window fetched.
Status build_lazy_windowed(LazyTermCursor* c) {
    RETURN_IF_ERROR(reader::fetch_windowed_prelude(*c->idx, c->entry, c->frq_base, &c->prelude));
    RETURN_IF_ERROR(
            build_window_bounds(c->prelude, c->ctx, c->stats->avgdl(), c->params, &c->windows));
    // build_window_bounds keeps only non-empty windows, in window order. Build the
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
Status build_lazy_slim(LazyTermCursor* c) {
    std::vector<uint32_t> docids;
    std::vector<uint32_t> freqs;
    RETURN_IF_ERROR(decode_slim(*c->idx, c->entry, c->frq_base, &docids, &freqs));
    RETURN_IF_ERROR(score_decoded(*c->stats, c->ctx, c->stats->avgdl(), c->params, docids, freqs,
                                  &c->postings));
    single_window_fallback(c->postings, &c->windows);
    c->win_start = {0, static_cast<uint32_t>(c->postings.size())};
    c->fetched.assign(1, 1); // already materialized
    return Status::OK();
}

// Builds a LazyTermCursor for one term: prelude-only for windowed terms (no .frq
// fetched), fully-materialized single window for slim/inline (small).
Status build_lazy_cursor(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
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
    return c->windowed ? build_lazy_windowed(c) : build_lazy_slim(c);
}

Status selective_build_cursors(const LogicalIndexReader& idx, const stats::SniiStatsProvider& stats,
                               const std::vector<std::string>& terms, const Bm25Params& params,
                               std::vector<LazyTermCursor>* cursors) {
    for (const auto& term : terms) {
        bool found = false;
        LazyTermCursor c;
        RETURN_IF_ERROR(build_lazy_cursor(idx, stats, term, params, &found, &c));
        if (found && total_postings(c) > 0) cursors->push_back(std::move(c));
    }
    return Status::OK();
}

// Block-max upper bound for a lazy cursor at docid: block_max of the window
// covering docid (ascending, contiguous). Beyond the last window -> 0. Same
// semantics as term_bound_at over the eager cursor's window list.
double lazy_term_bound_at(const LazyTermCursor& c, uint32_t docid) {
    for (const auto& w : c.windows) {
        if (docid <= w.last_docid) return w.max_score;
    }
    return 0.0;
}

// Sorts cursors ascending by current docid (materializing each cursor's current
// covering window), returning the smallest current docid via *front.
Status selective_sort_by_doc(std::vector<LazyTermCursor>* cursors, uint32_t* front) {
    std::vector<uint32_t> cur(cursors->size());
    for (size_t i = 0; i < cursors->size(); ++i) {
        RETURN_IF_ERROR(lazy_current_doc(&(*cursors)[i], &cur[i]));
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
Status selective_pivot(std::vector<LazyTermCursor>* cursors, double theta, size_t* pivot,
                       uint32_t* pivot_doc, bool* found) {
    double bound = 0.0;
    *found = false;
    for (size_t i = 0; i < cursors->size(); ++i) {
        uint32_t d = 0;
        RETURN_IF_ERROR(lazy_current_doc(&(*cursors)[i], &d));
        if (d == std::numeric_limits<uint32_t>::max()) break;
        bound += lazy_term_bound_at((*cursors)[i], d);
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
Status selective_score_pivot(std::vector<LazyTermCursor>* cursors, uint32_t pivot_doc, TopK* topk) {
    double doc_score = 0.0;
    for (auto& c : *cursors) {
        uint32_t d = 0;
        RETURN_IF_ERROR(lazy_current_doc(&c, &d));
        if (d == pivot_doc) {
            doc_score += c.postings[c.pos].score; // window already materialized
            ++c.pos;
        }
    }
    topk->offer(pivot_doc, doc_score);
    return Status::OK();
}

// Advances the first lagging cursor (current doc < pivot_doc) up to pivot_doc.
Status selective_advance_lagging(std::vector<LazyTermCursor>* cursors, uint32_t pivot_doc) {
    for (auto& c : *cursors) {
        uint32_t d = 0;
        RETURN_IF_ERROR(lazy_current_doc(&c, &d));
        if (d < pivot_doc) {
            RETURN_IF_ERROR(lazy_skip_to(&c, pivot_doc));
            return Status::OK();
        }
    }
    return Status::OK();
}

// One WAND iteration body: sort, pick pivot, then either score (aligned) or skip
// a lagging cursor forward. *done=true ends the loop.
Status selective_step(std::vector<LazyTermCursor>* cursors, TopK* topk, bool* done) {
    uint32_t front = 0;
    RETURN_IF_ERROR(selective_sort_by_doc(cursors, &front));
    if (cursors->empty() || front == std::numeric_limits<uint32_t>::max()) {
        *done = true;
        return Status::OK();
    }
    size_t pivot = 0;
    uint32_t pivot_doc = 0;
    bool found_pivot = false;
    RETURN_IF_ERROR(selective_pivot(cursors, topk->threshold(), &pivot, &pivot_doc, &found_pivot));
    if (!found_pivot) {
        *done = true;
        return Status::OK();
    }
    if (front == pivot_doc) {
        return selective_score_pivot(cursors, pivot_doc, topk);
    }
    return selective_advance_lagging(cursors, pivot_doc);
}

Status selective_wand_loop(std::vector<LazyTermCursor>* cursors, TopK* topk) {
    bool done = false;
    while (!done) {
        RETURN_IF_ERROR(selective_step(cursors, topk, &done));
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
    RETURN_IF_ERROR(selective_build_cursors(idx, stats, terms, params, &cursors));

    TopK topk(k);
    RETURN_IF_ERROR(selective_wand_loop(&cursors, &topk));
    drain_sorted(&topk, out);
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
    RETURN_IF_ERROR(build_cursors(idx, stats, terms, params, &cursors));

    TopK topk(k);
    // Document-at-a-time WAND with block-max bounds.
    while (true) {
        // Sort cursors by current docid (ascending; exhausted cursors sink).
        std::sort(cursors.begin(), cursors.end(), [](const TermCursor& a, const TermCursor& b) {
            return current_doc(a) < current_doc(b);
        });
        if (cursors.empty() ||
            current_doc(cursors.front()) == std::numeric_limits<uint32_t>::max()) {
            break;
        }

        const double theta = topk.threshold();
        // Accumulate block-max upper bounds in docid order to find the pivot term.
        double bound = 0.0;
        size_t pivot = 0;
        bool found_pivot = false;
        for (size_t i = 0; i < cursors.size(); ++i) {
            const uint32_t d = current_doc(cursors[i]);
            if (d == std::numeric_limits<uint32_t>::max()) break;
            bound += term_bound_at(cursors[i], d);
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

        const uint32_t pivot_doc = current_doc(cursors[pivot]);
        if (current_doc(cursors.front()) == pivot_doc) {
            // All cursors at the pivot doc are aligned: score it exactly.
            double doc_score = 0.0;
            for (auto& c : cursors) {
                if (current_doc(c) == pivot_doc) {
                    doc_score += c.postings[c.pos].score;
                    ++c.pos;
                }
            }
            topk.offer(pivot_doc, doc_score);
        } else {
            // Advance a lagging cursor toward pivot_doc (skip docs it cannot win on).
            for (auto& c : cursors) {
                if (current_doc(c) < pivot_doc) {
                    while (c.pos < c.postings.size() && c.postings[c.pos].docid < pivot_doc) {
                        ++c.pos;
                    }
                    break;
                }
            }
        }
    }
    drain_sorted(&topk, out);
    return Status::OK();
}

} // namespace doris::snii::query
