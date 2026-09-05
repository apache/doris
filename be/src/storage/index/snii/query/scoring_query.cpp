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
#include <numeric>
#include <span>
#include <unordered_map>
#include <vector>

#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_pod.h"
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

// 磁盘上没有词频区：BM25 的 tf 就是该 term 在文档里的位置个数（与 Lucene 系打分定义一致）。
Status require_positions(const LogicalIndexReader& idx) {
    if (!idx.has_positions()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "scoring_query: BM25 scoring requires a positional index");
    }
    return Status::OK();
}

uint32_t term_frequency(const std::vector<uint32_t>& positions) {
    return static_cast<uint32_t>(positions.size());
}

// Fetches one absolute byte range of the index file into an owned buffer.
Status fetch_range(const LogicalIndexReader& idx, uint64_t offset, uint64_t length,
                   std::vector<uint8_t>* owned) {
    io::BatchRangeFetcher fetcher(idx.reader());
    const size_t h = fetcher.add(offset, length);
    RETURN_IF_ERROR(fetcher.fetch());
    const Slice got = fetcher.get(h);
    owned->assign(got.data(), got.data() + got.size());
    return Status::OK();
}

// Decodes a slim/inline term's single .frq window (the dd region) and its .prx
// window into docids and per-doc term frequencies.
Status decode_slim(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                   uint64_t prx_base, std::vector<uint32_t>* docids, std::vector<uint32_t>* tfs) {
    std::vector<uint8_t> frq_owned;
    std::vector<uint8_t> prx_owned;
    Slice dd_region;
    Slice prx_window;
    if (entry.kind == DictEntryKind::kInline) {
        dd_region = Slice(entry.frq_bytes);
        prx_window = Slice(entry.prx_bytes);
    } else {
        uint64_t frq_abs = 0;
        uint64_t frq_len = 0;
        RETURN_IF_ERROR(idx.resolve_frq_window(entry, frq_base, &frq_abs, &frq_len));
        RETURN_IF_ERROR(fetch_range(idx, frq_abs, frq_len, &frq_owned));
        dd_region = Slice(frq_owned);
        uint64_t prx_abs = 0;
        uint64_t prx_len = 0;
        RETURN_IF_ERROR(idx.resolve_prx_window(entry, prx_base, &prx_abs, &prx_len));
        RETURN_IF_ERROR(fetch_range(idx, prx_abs, prx_len, &prx_owned));
        prx_window = Slice(prx_owned);
    }
    RETURN_IF_ERROR(format::decode_dd_region(dd_region, entry.dd_meta, /*win_base=*/0, docids));

    std::vector<std::vector<uint32_t>> positions;
    ByteSource psrc(prx_window);
    RETURN_IF_ERROR(format::read_prx_window(&psrc, &positions));
    if (!psrc.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "scoring_query: trailing bytes after slim prx frame");
    }
    if (positions.size() != docids->size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "scoring_query: slim prx/frq doc-count mismatch");
    }
    tfs->clear();
    tfs->reserve(positions.size());
    for (const auto& doc_positions : positions) {
        tfs->push_back(term_frequency(doc_positions));
    }
    return Status::OK();
}

// Decodes a windowed term completely (docids + per-doc term frequencies).
Status decode_windowed(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                       uint64_t prx_base, std::vector<uint32_t>* docids,
                       std::vector<uint32_t>* tfs) {
    reader::DecodedPosting posting;
    RETURN_IF_ERROR(reader::read_windowed_posting(idx, entry, frq_base, prx_base,
                                                  /*want_positions=*/true, &posting));
    if (posting.positions.size() != posting.docids.size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "scoring_query: windowed prx/frq doc-count mismatch");
    }
    *docids = std::move(posting.docids);
    tfs->clear();
    tfs->reserve(posting.positions.size());
    for (const auto& doc_positions : posting.positions) {
        tfs->push_back(term_frequency(doc_positions));
    }
    return Status::OK();
}

Status decode_term(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                   uint64_t prx_base, std::vector<uint32_t>* docids, std::vector<uint32_t>* tfs) {
    const bool windowed =
            entry.kind == DictEntryKind::kPodRef && entry.enc == DictEntryEnc::kWindowed;
    if (windowed) {
        return decode_windowed(idx, entry, frq_base, prx_base, docids, tfs);
    }
    return decode_slim(idx, entry, frq_base, prx_base, docids, tfs);
}

// Computes exact per-doc BM25 scores from decoded (docid, tf) vectors.
Status score_decoded(const stats::SniiStatsProvider& stats, const ScorerContext& ctx, double avgdl,
                     const Bm25Params& params, const std::vector<uint32_t>& docids,
                     const std::vector<uint32_t>& tfs, std::vector<TermPosting>* out) {
    DCHECK_EQ(docids.size(), tfs.size());
    out->reserve(docids.size());
    for (size_t i = 0; i < docids.size(); ++i) {
        uint8_t norm = 0;
        RETURN_IF_ERROR(stats.encoded_norm(docids[i], &norm));
        out->push_back({docids[i], ctx.score(tfs[i], norm, avgdl, params)});
    }
    return Status::OK();
}

Status accumulate_decoded_candidate_scores(const stats::SniiStatsProvider& stats,
                                           const ScorerContext& scorer, double avgdl,
                                           const Bm25Params& params,
                                           const std::vector<uint32_t>& docids,
                                           const std::vector<uint32_t>& tfs,
                                           std::span<const uint32_t> candidates,
                                           std::span<double> scores) {
    DCHECK_EQ(candidates.size(), scores.size());
    DCHECK_EQ(docids.size(), tfs.size());
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
        scores[candidate_index] += scorer.score(tfs[doc_index], norm, avgdl, params);
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
    size_t prx_handle = 0;
};

// Scores the candidates covered by a windowed term, fetching only the windows
// that contain candidate docids (their dd sub-ranges and .prx windows).
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
        RETURN_IF_ERROR(reader::windowed_window_range(idx, entry, frq_base, prx_base, prelude,
                                                      window, /*want_positions=*/true, &range));
        item.dd_handle = fetcher.add(range.dd_off, range.dd_len);
        item.prx_handle = fetcher.add(range.prx_off, range.prx_len);
        work.push_back(std::move(item));
    }
    RETURN_IF_ERROR(fetcher.fetch());

    std::vector<uint32_t> docids;
    std::vector<uint32_t> tfs;
    std::vector<std::vector<uint32_t>> positions;
    for (const auto& item : work) {
        docids.clear();
        tfs.clear();
        positions.clear();
        RETURN_IF_ERROR(reader::decode_window_slices(item.meta, fetcher.get(item.dd_handle),
                                                     fetcher.get(item.prx_handle),
                                                     /*want_positions=*/true, &docids,
                                                     &positions));
        tfs.reserve(positions.size());
        for (const auto& doc_positions : positions) {
            tfs.push_back(term_frequency(doc_positions));
        }
        const size_t candidate_count = item.candidate_end - item.candidate_begin;
        RETURN_IF_ERROR(accumulate_decoded_candidate_scores(
                stats, scorer, avgdl, params, docids, tfs,
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
    std::vector<uint32_t> tfs;
    RETURN_IF_ERROR(decode_slim(idx, entry, frq_base, prx_base, &docids, &tfs));
    return accumulate_decoded_candidate_scores(stats, scorer, avgdl, params, docids, tfs,
                                               candidates, *scores);
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
    RETURN_IF_ERROR(require_positions(idx));

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
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("scoring_query: null out");
    }
    out->clear();
    if (k == 0) {
        return Status::OK();
    }
    RETURN_IF_ERROR(require_positions(idx));

    std::unordered_map<uint32_t, double> scores;
    for (const auto& term : terms) {
        bool found = false;
        DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        RETURN_IF_ERROR(idx.lookup(term, &found, &entry, &frq_base, &prx_base));
        if (!found) {
            continue;
        }
        const ScorerContext ctx = ScorerContext::make(stats.indexed_doc_count(), entry.df);
        std::vector<uint32_t> docids;
        std::vector<uint32_t> tfs;
        RETURN_IF_ERROR(decode_term(idx, entry, frq_base, prx_base, &docids, &tfs));
        std::vector<TermPosting> postings;
        RETURN_IF_ERROR(score_decoded(stats, ctx, stats.avgdl(), params, docids, tfs, &postings));
        for (const auto& p : postings) {
            scores[p.docid] += p.score;
        }
    }

    std::vector<ScoredDoc> all;
    all.reserve(scores.size());
    for (const auto& [docid, score] : scores) {
        all.push_back({docid, score});
    }
    std::sort(all.begin(), all.end(), [](const ScoredDoc& a, const ScoredDoc& b) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
        return a.docid < b.docid;
    });
    if (all.size() > k) {
        all.resize(k);
    }
    *out = std::move(all);
    return Status::OK();
}

} // namespace doris::snii::query
