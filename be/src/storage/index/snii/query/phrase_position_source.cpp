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

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/check.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_query_cost.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/frq_pod.h"
#include "storage/index/snii/format/frq_prelude.h"
#include "storage/index/snii/format/prx_frame.h"
#include "storage/index/snii/format/prx_pod.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_set_ops.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/query/internal/phrase_query_split.h"
#include "storage/index/snii/query/internal/plain_term_routing.h"
#include "storage/index/snii/query/internal/position_math.h"
#include "storage/index/snii/query/internal/query_test_counters.h"
#include "storage/index/snii/query/internal/resolved_phrase_plan.h"
#include "storage/index/snii/query/internal/term_expansion.h"
#include "storage/index/snii/query/phrase_prx_validation.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/phrase_verify_timer.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/reader/windowed_posting.h"
#include "util/debug_points.h"

namespace doris::snii::query::phrase_impl {

using query::internal::DocidChunk;
using query::internal::DocidSource;
using query::internal::ResolvedQueryTerm;
using query::internal::TermPlan;
using reader::LogicalIndexReader;
using internal::PhraseVerifyTimer;

PhraseTermMapping build_phrase_term_mapping(const std::vector<std::string>& terms) {
    PhraseTermMapping mapping;
    mapping.phrase_plan_index.reserve(terms.size());
    for (const std::string& term : terms) {
        auto it = std::ranges::find(mapping.unique_terms, term);
        if (it == mapping.unique_terms.end()) {
            mapping.phrase_plan_index.push_back(mapping.unique_terms.size());
            mapping.unique_terms.push_back(term);
            continue;
        }
        mapping.phrase_plan_index.push_back(static_cast<size_t>(it - mapping.unique_terms.begin()));
    }
    return mapping;
}

namespace {
Status accumulate_frame_position_work(Slice frames, uint64_t* work) {
    ByteSource source(frames);
    while (!source.eof()) {
        format::PrxFrameView frame;
        RETURN_IF_ERROR(format::read_prx_frame(&source, &frame));
        uint64_t frame_work = frame.uncompressed_length;
        if (frame.codec == format::PrxCodec::kPfor) {
            ByteSource payload(frame.payload);
            uint32_t doc_count = 0;
            uint32_t total_positions = 0;
            RETURN_IF_ERROR(payload.get_varint32(&doc_count));
            RETURN_IF_ERROR(payload.get_varint32(&total_positions));
            if (doc_count > format::kReaderPrxWindowLimits.max_docs ||
                total_positions > format::kReaderPrxWindowLimits.max_positions) {
                return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                        "phrase_query: PFOR routing metadata exceeds sane cap");
            }
            frame_work = total_positions;
        }
        *work += frame_work;
    }
    return Status::OK();
}

Status populate_logical_position_work(const std::vector<TermPlan>& plans,
                                      std::vector<PosSource>* sources) {
    DORIS_CHECK_EQ(plans.size(), sources->size());
    for (size_t plan_index = 0; plan_index < plans.size(); ++plan_index) {
        if (plans[plan_index].entry.term_stats_present) {
            continue;
        }
        for (const PosChunk& chunk : (*sources)[plan_index].chunks) {
            (*sources)[plan_index].logical_position_docs += chunk.prx_doc_count;
            RETURN_IF_ERROR(accumulate_frame_position_work(
                    chunk.prx, &(*sources)[plan_index].logical_position_work));
        }
    }
    return Status::OK();
}

Status append_prx_doc_ordinal(size_t ordinal, std::vector<uint32_t>* out) {
    if (ordinal > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx doc ordinal exceeds u32");
    }
    out->push_back(static_cast<uint32_t>(ordinal));
    return Status::OK();
}

Status append_selected_ordinal(size_t doc_index, const std::vector<uint32_t>& prx_doc_ordinals,
                               std::vector<uint32_t>* selected_ordinals) {
    if (!prx_doc_ordinals.empty()) {
        selected_ordinals->push_back(prx_doc_ordinals[doc_index]);
        return Status::OK();
    }
    return append_prx_doc_ordinal(doc_index, selected_ordinals);
}

Status append_selected_doc(size_t doc_index, uint32_t docid,
                           const std::vector<uint32_t>& prx_doc_ordinals,
                           std::vector<uint32_t>* selected_docids,
                           std::vector<uint32_t>* selected_ordinals) {
    selected_docids->push_back(docid);
    return append_selected_ordinal(doc_index, prx_doc_ordinals, selected_ordinals);
}

Status materialize_selected_prefix(size_t count, size_t capacity,
                                   const std::vector<uint32_t>& docids,
                                   const std::vector<uint32_t>& prx_doc_ordinals,
                                   std::vector<uint32_t>* selected_docids,
                                   std::vector<uint32_t>* selected_ordinals) {
    selected_docids->reserve(capacity);
    selected_ordinals->reserve(capacity);
    selected_docids->insert(selected_docids->end(), docids.begin(), docids.begin() + count);
    for (size_t i = 0; i < count; ++i) {
        RETURN_IF_ERROR(append_selected_ordinal(i, prx_doc_ordinals, selected_ordinals));
    }
    return Status::OK();
}

Status materialize_selected_prefix_if_needed(bool* selected_all, size_t count, size_t capacity,
                                             const std::vector<uint32_t>& docids,
                                             const std::vector<uint32_t>& prx_doc_ordinals,
                                             std::vector<uint32_t>* selected_docids,
                                             std::vector<uint32_t>* selected_ordinals) {
    if (!*selected_all) {
        return Status::OK();
    }
    *selected_all = false;
    return materialize_selected_prefix(count, capacity, docids, prx_doc_ordinals, selected_docids,
                                       selected_ordinals);
}

Status select_candidate_docs_for_prx(std::vector<uint32_t>* docids,
                                     std::vector<uint32_t>* prx_doc_ordinals,
                                     uint32_t prx_doc_count,
                                     const std::vector<uint32_t>& candidates, PosChunk* chunk) {
    chunk->docids.clear();
    chunk->prx_doc_ordinals.clear();
    if (prx_doc_count == 0 && docids->size() > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx doc count exceeds u32");
    }
    chunk->prx_doc_count =
            prx_doc_count == 0 ? static_cast<uint32_t>(docids->size()) : prx_doc_count;
    if (docids->empty() || candidates.empty()) {
        return Status::OK();
    }
    if (!prx_doc_ordinals->empty() && prx_doc_ordinals->size() != docids->size()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "phrase_query: prx ordinal/docid count mismatch");
    }

    std::vector<uint32_t> selected_docids;
    std::vector<uint32_t> selected_ordinals;
    bool selected_all = true;
    const size_t selected_capacity = std::min(docids->size(), candidates.size());

    auto candidate_it = std::ranges::lower_bound(candidates, docids->front());
    size_t candidate_index = static_cast<size_t>(candidate_it - candidates.begin());
    for (size_t doc_index = 0; doc_index < docids->size(); ++doc_index) {
        const uint32_t docid = (*docids)[doc_index];
        while (candidate_index < candidates.size() && candidates[candidate_index] < docid) {
            ++candidate_index;
        }
        if (candidate_index == candidates.size()) {
            RETURN_IF_ERROR(materialize_selected_prefix_if_needed(
                    &selected_all, doc_index, selected_capacity, *docids, *prx_doc_ordinals,
                    &selected_docids, &selected_ordinals));
            break;
        }
        if (candidates[candidate_index] != docid) {
            RETURN_IF_ERROR(materialize_selected_prefix_if_needed(
                    &selected_all, doc_index, selected_capacity, *docids, *prx_doc_ordinals,
                    &selected_docids, &selected_ordinals));
            continue;
        }

        if (!selected_all) {
            RETURN_IF_ERROR(append_selected_doc(doc_index, docid, *prx_doc_ordinals,
                                                &selected_docids, &selected_ordinals));
        }
        ++candidate_index;
    }

    if (selected_all) {
        chunk->docids = std::move(*docids);
        chunk->prx_doc_ordinals = std::move(*prx_doc_ordinals);
        docids->clear();
        prx_doc_ordinals->clear();
        return Status::OK();
    }
    if (selected_docids.empty()) {
        return Status::OK();
    }
    chunk->docids = std::move(selected_docids);
    chunk->prx_doc_ordinals = std::move(selected_ordinals);
    return Status::OK();
}

// PRX byte ranges for every candidate-bearing chunk across all phrase terms are
// added to one shared BatchRangeFetcher and fetched in a single batched round
// (T02). Pass 1 records, for each chunk that needs on-disk PRX bytes, where to
// write the fetched slice back: which plan's PosSource, which chunk within it,
// and the fetcher handle.

struct PrxRangeAssignment {
    size_t plan_index;
    size_t chunk_index;
    size_t handle;
};

void record_prx_assignment(std::vector<PrxRangeAssignment>* assignments, size_t plan_index,
                           size_t chunk_index, size_t handle) {
    assignments->push_back(PrxRangeAssignment {
            .plan_index = plan_index, .chunk_index = chunk_index, .handle = handle});
}

Status build_flat_position_source(const LogicalIndexReader& idx,
                                  const io::BatchRangeFetcher& round1, DocidSource* doc_source,
                                  const TermPlan& p, const std::vector<uint32_t>& candidates,
                                  size_t plan_index, io::BatchRangeFetcher* prx_fetcher,
                                  std::vector<PrxRangeAssignment>* assignments, PosSource* src) {
    PosChunk chunk;
    std::vector<uint32_t> docids;
    std::vector<uint32_t> prx_doc_ordinals;
    const bool docids_are_final_candidates =
            doc_source->docids_are_final_candidates && !doc_source->chunks.empty();
    if (!doc_source->chunks.empty()) {
        DocidChunk& doc_chunk = doc_source->chunks.front();
        docids = std::move(doc_chunk.docids);
        prx_doc_ordinals = std::move(doc_chunk.prx_doc_ordinals);
        chunk.prx_doc_count = doc_chunk.prx_doc_count;
    }
    // pod_ref PRX bytes are read from the shared fetcher (one batched round for the
    // whole phrase); inline PRX bytes already live in the dict entry. The pod_ref
    // range is added unconditionally to keep the bytes read identical to the prior
    // per-term fetch(); the handle is only recorded as an assignment when the chunk
    // is kept (an empty chunk reads the same bytes but needs no backfill).
    bool has_prx_handle = false;
    size_t prx_handle = 0;
    if (p.pod_ref) {
        uint64_t poff = 0;
        uint64_t plen = 0;
        RETURN_IF_ERROR(idx.resolve_prx_window(p.entry, p.prx_base, &poff, &plen));
        prx_handle = prx_fetcher->add(poff, plen);
        has_prx_handle = true;
    } else {
        chunk.prx = Slice(p.entry.prx_bytes);
    }
    if (docids.empty()) {
        Slice dd;
        if (p.pod_ref) {
            dd = round1.get(p.frq_handle);
        } else {
            RETURN_IF_ERROR(internal::inline_dd_region(p.entry, &dd));
        }
        RETURN_IF_ERROR(format::decode_dd_region(dd, p.entry.dd_meta,
                                                 /*win_base=*/0, &docids));
        if (docids.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: prx doc count exceeds u32");
        }
        chunk.prx_doc_count = static_cast<uint32_t>(docids.size());
    }
    if (docids_are_final_candidates) {
        chunk.docids = std::move(docids);
        chunk.prx_doc_ordinals = std::move(prx_doc_ordinals);
        if (!chunk.docids.empty()) {
            if (has_prx_handle) {
                record_prx_assignment(assignments, plan_index, src->chunks.size(), prx_handle);
            }
            src->chunks.push_back(std::move(chunk));
        }
        return Status::OK();
    }
    RETURN_IF_ERROR(select_candidate_docs_for_prx(&docids, &prx_doc_ordinals, chunk.prx_doc_count,
                                                  candidates, &chunk));
    if (!chunk.docids.empty()) {
        if (has_prx_handle) {
            record_prx_assignment(assignments, plan_index, src->chunks.size(), prx_handle);
        }
        src->chunks.push_back(std::move(chunk));
    }
    return Status::OK();
}

bool chunk_may_contain_candidate(const DocidChunk& chunk, const std::vector<uint32_t>& candidates) {
    if (chunk.docids.empty() || candidates.empty()) {
        return false;
    }
    const auto it = std::ranges::lower_bound(candidates, chunk.docids.front());
    return it != candidates.end() && *it <= chunk.docids.back();
}

Status decode_windowed_position_source(const LogicalIndexReader& idx, const TermPlan& p,
                                       DocidSource* doc_source,
                                       const std::vector<uint32_t>& candidates, size_t plan_index,
                                       io::BatchRangeFetcher* prx_fetcher,
                                       std::vector<PrxRangeAssignment>* assignments,
                                       PosSource* src) {
    for (size_t i = 0; i < doc_source->chunks.size(); ++i) {
        DocidChunk& doc_chunk = doc_source->chunks[i];
        if (!doc_source->docids_are_final_candidates &&
            !chunk_may_contain_candidate(doc_chunk, candidates)) {
            continue;
        }
        if (!doc_chunk.windowed) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "phrase_query: expected windowed doc chunk");
        }
        PosChunk chunk;
        if (doc_source->docids_are_final_candidates) {
            chunk.docids = std::move(doc_chunk.docids);
            chunk.prx_doc_ordinals = std::move(doc_chunk.prx_doc_ordinals);
            chunk.prx_doc_count = doc_chunk.prx_doc_count;
        } else {
            RETURN_IF_ERROR(
                    select_candidate_docs_for_prx(&doc_chunk.docids, &doc_chunk.prx_doc_ordinals,
                                                  doc_chunk.prx_doc_count, candidates, &chunk));
        }
        if (chunk.docids.empty()) {
            continue;
        }

        reader::WindowAbsRange range;
        RETURN_IF_ERROR(reader::windowed_window_range(
                idx, p.entry, p.frq_base, p.prx_base, p.prelude, doc_chunk.window,
                /*want_positions=*/true, /*want_freq=*/false, &range));
        chunk.windowed = true;
        chunk.window = doc_chunk.window;
        const size_t prx_handle = prx_fetcher->add(range.prx_off, range.prx_len);
        record_prx_assignment(assignments, plan_index, src->chunks.size(), prx_handle);
        src->chunks.push_back(std::move(chunk));
    }
    return Status::OK();
}

} // namespace
Status build_position_sources_for_candidates(
        const LogicalIndexReader& idx, const io::BatchRangeFetcher& round1,
        const std::vector<TermPlan>& plans, std::vector<DocidSource>* doc_sources,
        const std::vector<uint32_t>& candidates,
        std::vector<std::unique_ptr<io::BatchRangeFetcher>>* owners, std::vector<PosSource>* srcs,
        format::PrxDecodeContext* observer_context) {
    srcs->assign(plans.size(), PosSource {});
    for (PosSource& source : *srcs) {
        source.observer_context = observer_context;
    }
    // All phrase terms share one PRX fetcher: pass 1 adds every candidate-bearing
    // chunk's PRX range and records a backfill assignment; a single fetch() then
    // issues one batched read (one serial round on a remote reader); pass 2 fills
    // in each chunk's PRX slice. This collapses the prior per-term fetch() -- O(n)
    // serial remote rounds for an n-term phrase -- into one.
    auto prx_fetcher =
            std::make_unique<io::BatchRangeFetcher>(idx.reader(), reader::kSameTermCoalesceGap);
    std::vector<PrxRangeAssignment> assignments;
    for (size_t i = 0; i < plans.size(); ++i) {
        const TermPlan& p = plans[i];
        if (p.windowed) {
            RETURN_IF_ERROR(decode_windowed_position_source(idx, p, &(*doc_sources)[i], candidates,
                                                            i, prx_fetcher.get(), &assignments,
                                                            &(*srcs)[i]));
            continue;
        }
        RETURN_IF_ERROR(build_flat_position_source(idx, round1, &(*doc_sources)[i], p, candidates,
                                                   i, prx_fetcher.get(), &assignments,
                                                   &(*srcs)[i]));
    }
    if (prx_fetcher->pending() > 0) {
        if (observer_context == nullptr || observer_context->stats == nullptr) {
            RETURN_IF_ERROR(prx_fetcher->fetch());
        } else {
            const auto fetch_start = std::chrono::steady_clock::now();
            RETURN_IF_ERROR(prx_fetcher->fetch());
            const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                         std::chrono::steady_clock::now() - fetch_start)
                                         .count();
            observer_context->stats->fetch_ns +=
                    std::max<uint64_t>(1, static_cast<uint64_t>(elapsed));
        }
    }
    for (const PrxRangeAssignment& a : assignments) {
        (*srcs)[a.plan_index].chunks[a.chunk_index].prx = prx_fetcher->get(a.handle);
    }
    RETURN_IF_ERROR(populate_logical_position_work(plans, srcs));
    // Keep the fetcher alive only when some chunk slice references its buffers.
    if (!assignments.empty()) {
        owners->push_back(std::move(prx_fetcher));
    }
    return Status::OK();
}

} // namespace doris::snii::query::phrase_impl
