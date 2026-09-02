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

#include "storage/index/snii/compaction/posting_run_merger.h"

#include <algorithm>
#include <atomic>
#include <limits>
#include <string_view>
#include <utility>

#include "common/check.h"

namespace doris::snii::compaction {

namespace {

Status invalid_source(std::string_view reason) {
    return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("posting_run_merger: {}", reason);
}

Status merge_corruption(std::string_view reason) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("posting_run_merger: {}",
                                                                          reason);
}

bool posting_after(uint32_t lhs_segment, uint32_t lhs_docid, uint32_t rhs_segment,
                   uint32_t rhs_docid) {
    return lhs_segment > rhs_segment || (lhs_segment == rhs_segment && lhs_docid > rhs_docid);
}

#ifdef BE_TEST
std::atomic<uint64_t> posting_run_frontier_update_counter {0};
std::atomic<uint64_t> posting_run_frontier_comparison_counter {0};
std::atomic<uint64_t> posting_run_document_counter {0};
std::atomic<uint64_t> posting_run_emitted_run_counter {0};
std::atomic<uint64_t> posting_run_boundary_search_counter {0};
std::atomic<uint64_t> posting_run_shape_scan_document_counter {0};
std::atomic<uint64_t> posting_run_legacy_fill_call_counter {0};
std::atomic<uint64_t> posting_run_copied_document_counter {0};
#endif

size_t lower_bound_docid(std::span<const uint32_t> docids, size_t begin, uint32_t target) {
#ifdef BE_TEST
    posting_run_boundary_search_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    const size_t size = docids.size();
    if (begin >= size || docids[begin] >= target) {
        return begin;
    }
    // Gallop before the binary search: when many sources interleave (a full
    // compaction merging dozens of sorted segments), destination runs shrink to
    // one or two documents, and a plain binary search over the whole remaining
    // chunk costs O(log chunk) comparisons per emitted run. Doubling probes
    // resolve those short runs in O(1) while keeping O(log run) for long runs.
    size_t less = begin;
    size_t probe = 1;
    while (less + probe < size && docids[less + probe] < target) {
        less += probe;
        probe *= 2;
    }
    size_t low = less + 1;
    size_t high = std::min(less + probe, size);
    while (low < high) {
        const size_t middle = low + (high - low) / 2;
        if (docids[middle] < target) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return low;
}

} // namespace

void MergedPostingRuns::ActivePostingChunk::refresh_frontier() {
    DCHECK_LT(ordinal, chunk.destination_docids.size());
    frontier_segment = chunk.destination_segment;
    frontier_docid = chunk.destination_docids[ordinal];
}

Status MergedPostingRuns::ActivePostingChunk::validate_and_refresh_frontier(
        bool retain_positions, std::span<const uint32_t> destination_doc_counts) {
    if (chunk.destination_docids.empty() || ordinal >= chunk.destination_docids.size()) {
        return merge_corruption("destination posting run is empty");
    }
    if (chunk.destination_segment >= destination_doc_counts.size()) {
        return merge_corruption("destination posting run segment is out of range");
    }
    if (retain_positions) {
        if (chunk.freqs.size() != chunk.destination_docids.size() ||
            chunk.position_offsets.size() != chunk.destination_docids.size() + 1) {
            return merge_corruption("positioned posting run has an invalid shape");
        }
        const uint32_t position_begin = chunk.position_offsets.front();
        const uint32_t position_end = chunk.position_offsets.back();
        if (position_end < position_begin ||
            position_end - position_begin != chunk.positions_flat.size()) {
            return merge_corruption("positioned posting run has invalid offsets");
        }
    } else if (!chunk.freqs.empty() || !chunk.position_offsets.empty() ||
               !chunk.positions_flat.empty()) {
        return merge_corruption("docs-only posting run has positioned payload");
    }

    uint32_t previous_offset = retain_positions ? chunk.position_offsets.front() : 0;
    for (size_t document = 0; document < chunk.destination_docids.size(); ++document) {
        const uint32_t docid = chunk.destination_docids[document];
        if (docid >= destination_doc_counts[chunk.destination_segment]) {
            return merge_corruption("destination posting run document is out of range");
        }
        if (document > 0 && docid <= chunk.destination_docids[document - 1]) {
            return merge_corruption("destination posting run is not strictly monotone");
        }
        if (retain_positions) {
            const uint32_t next_offset = chunk.position_offsets[document + 1];
            if (next_offset < previous_offset ||
                next_offset - previous_offset != chunk.freqs[document]) {
                return merge_corruption("positioned posting run offsets differ from frequencies");
            }
            previous_offset = next_offset;
        }
#ifdef BE_TEST
        posting_run_shape_scan_document_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    }
    if (has_previous_chunk_posting &&
        !posting_after(chunk.destination_segment, chunk.destination_docids.front(),
                       previous_chunk_segment, previous_chunk_docid)) {
        return merge_corruption("source posting chunks are not globally monotone");
    }
    previous_chunk_segment = chunk.destination_segment;
    previous_chunk_docid = chunk.destination_docids.back();
    has_previous_chunk_posting = true;
    refresh_frontier();
    return Status::OK();
}

bool MergedPostingRuns::FrontierBefore::operator()(size_t lhs, size_t rhs) const {
#ifdef BE_TEST
    posting_run_frontier_comparison_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    const ActivePostingChunk& lhs_chunk = (*active_chunks)[lhs];
    const ActivePostingChunk& rhs_chunk = (*active_chunks)[rhs];
    if (lhs_chunk.frontier_segment != rhs_chunk.frontier_segment) {
        return lhs_chunk.frontier_segment < rhs_chunk.frontier_segment;
    }
    if (lhs_chunk.frontier_docid != rhs_chunk.frontier_docid) {
        return lhs_chunk.frontier_docid < rhs_chunk.frontier_docid;
    }
    return lhs < rhs;
}

MergedPostingRuns::MergedPostingRuns(std::vector<std::unique_ptr<SniiPostingCursor>> cursors,
                                     bool retain_positions, bool counts_as_semantic_token,
                                     std::span<const uint32_t> destination_doc_counts,
                                     std::span<uint64_t> destination_semantic_token_counts)
        : cursors_(std::move(cursors)),
          active_frontier_(FrontierBefore {.active_chunks = &active_chunks_}),
          retain_positions_(retain_positions),
          counts_as_semantic_token_(counts_as_semantic_token),
          destination_doc_counts_(destination_doc_counts),
          destination_semantic_token_counts_(destination_semantic_token_counts) {}

Status MergedPostingRuns::init() {
    if (initialized_) {
        return invalid_source("source initialized twice");
    }
    if (cursors_.empty() || destination_doc_counts_.empty()) {
        return invalid_source("source or destination set is empty");
    }
    if (counts_as_semantic_token_ &&
        destination_semantic_token_counts_.size() != destination_doc_counts_.size()) {
        return invalid_source("semantic token counters differ from destination count");
    }
    active_chunks_.resize(cursors_.size());
    for (size_t cursor_ordinal = 0; cursor_ordinal < cursors_.size(); ++cursor_ordinal) {
        bool has_chunk = false;
        RETURN_IF_ERROR(cursors_[cursor_ordinal]->next_chunk(&active_chunks_[cursor_ordinal].chunk,
                                                             &has_chunk));
        if (has_chunk) {
            RETURN_IF_ERROR(active_chunks_[cursor_ordinal].validate_and_refresh_frontier(
                    retain_positions_, destination_doc_counts_));
        }
    }
    active_frontier_.build(cursors_.size(), [this](size_t source) {
        return !active_chunks_[source].chunk.destination_docids.empty();
    });
    initialized_ = true;
    return Status::OK();
}

bool MergedPostingRuns::empty() const {
    DCHECK(initialized_);
    return active_frontier_.empty();
}

uint32_t MergedPostingRuns::next_destination() const {
    DCHECK(initialized_);
    DCHECK(!active_destination_.has_value());
    DCHECK(!pending_source_.has_value());
    return front_segment();
}

Status MergedPostingRuns::begin_destination(uint32_t destination) {
    if (!initialized_ || active_destination_.has_value() || pending_source_.has_value() ||
        active_frontier_.empty()) {
        return invalid_source("cannot begin destination");
    }
    if (front_segment() != destination) {
        return invalid_source("destination differs from frontier");
    }
    active_destination_ = destination;
    return Status::OK();
}

Status MergedPostingRuns::next_run(uint32_t max_docs, writer::PostingRunView* run, bool* has_run) {
    if (max_docs == 0 || run == nullptr || has_run == nullptr) {
        return invalid_source("invalid next_run arguments");
    }
    if (!initialized_ || !active_destination_.has_value()) {
        return invalid_source("source has no active destination");
    }
    *run = {};
    *has_run = false;
    RETURN_IF_ERROR(settle_pending_run());
    if (active_frontier_.empty() || front_segment() != *active_destination_) {
        active_destination_.reset();
        return Status::OK();
    }
    RETURN_IF_ERROR(select_front_run(max_docs, run));
    *has_run = true;
    return Status::OK();
}

Status MergedPostingRuns::fill(uint32_t target_docs, writer::TermPostingBuffer* out,
                               bool* exhausted) {
    if (target_docs == 0 || out == nullptr || exhausted == nullptr) {
        return invalid_source("invalid fill arguments");
    }
    if (!initialized_ || !active_destination_.has_value()) {
        return invalid_source("source has no active destination");
    }
    if (!out->empty()) {
        return invalid_source("output must be empty");
    }
#ifdef BE_TEST
    posting_run_legacy_fill_call_counter.fetch_add(1, std::memory_order_relaxed);
#endif

    while (out->document_count() < target_docs) {
        writer::PostingRunView run;
        bool has_run = false;
        RETURN_IF_ERROR(next_run(static_cast<uint32_t>(target_docs - out->document_count()), &run,
                                 &has_run));
        if (!has_run) {
            *exhausted = true;
            return Status::OK();
        }
        const size_t position_count = run.positions_flat.size();
        writer::MutableTermPostingSpan destination;
        RETURN_IF_ERROR(out->grow_uninitialized(run.docids.size(), retain_positions_,
                                                position_count, &destination));
        std::ranges::copy(run.docids, destination.docids.begin());
        if (retain_positions_) {
            std::ranges::copy(run.freqs, destination.freqs.begin());
            std::ranges::copy(run.positions_flat, destination.positions_flat.begin());
        }
#ifdef BE_TEST
        posting_run_copied_document_counter.fetch_add(run.docids.size(), std::memory_order_relaxed);
#endif
    }

    RETURN_IF_ERROR(settle_pending_run());
    *exhausted = active_frontier_.empty() || front_segment() != *active_destination_;
    if (*exhausted) {
        active_destination_.reset();
    }
    return Status::OK();
}

uint32_t MergedPostingRuns::front_segment() const {
    return active_chunks_[active_frontier_.winner()].frontier_segment;
}

Status MergedPostingRuns::select_front_run(size_t max_docs, writer::PostingRunView* run) {
    DCHECK_GT(max_docs, 0);
    DCHECK(!pending_source_.has_value());
    const size_t cursor_ordinal = active_frontier_.winner();
    std::optional<std::pair<uint32_t, uint32_t>> next_frontier;
    const size_t runner_up = active_frontier_.runner_up();
    if (runner_up != IndexedWinnerTree<FrontierBefore>::kNoSource) {
        const ActivePostingChunk& next = active_chunks_[runner_up];
        next_frontier = std::pair(next.frontier_segment, next.frontier_docid);
    }

    ActivePostingChunk& active = active_chunks_[cursor_ordinal];
    RETURN_IF_ERROR(select_run(&active, max_docs, next_frontier, run));
    pending_source_ = cursor_ordinal;
    return Status::OK();
}

Status MergedPostingRuns::select_run(ActivePostingChunk* active, size_t max_docs,
                                     std::optional<std::pair<uint32_t, uint32_t>> next_frontier,
                                     writer::PostingRunView* run) {
    DCHECK(active != nullptr);
    const auto docids = active->chunk.destination_docids;
    if (active->chunk.destination_segment != *active_destination_) {
        return merge_corruption("posting run differs from the active destination");
    }
    const size_t begin = active->ordinal;
    size_t end = begin + std::min(max_docs, docids.size() - begin);
    if (next_frontier.has_value() && next_frontier->first == active->chunk.destination_segment) {
        end = std::min(end, lower_bound_docid(docids, begin, next_frontier->second));
    }
    if (end == begin) {
        return merge_corruption("destination postings contain an equal merge frontier");
    }
    if (has_previous_posting_ && !posting_after(active->frontier_segment, active->frontier_docid,
                                                previous_segment_, previous_docid_)) {
        return merge_corruption("destination postings are duplicated or not globally monotone");
    }

    const size_t document_count = end - begin;
    size_t position_begin = 0;
    size_t position_count = 0;
    if (retain_positions_) {
        const size_t position_base = active->chunk.position_offsets.front();
        const size_t absolute_position_begin = active->chunk.position_offsets[begin];
        const size_t absolute_position_end = active->chunk.position_offsets[end];
        DCHECK_GE(absolute_position_begin, position_base);
        DCHECK_GE(absolute_position_end, absolute_position_begin);
        DCHECK_LE(absolute_position_end - position_base, active->chunk.positions_flat.size());
        position_begin = absolute_position_begin - position_base;
        position_count = absolute_position_end - absolute_position_begin;
    }

    run->docids = docids.subspan(begin, document_count);
    run->freqs = retain_positions_ ? active->chunk.freqs.subspan(begin, document_count)
                                   : std::span<const uint32_t> {};
    run->position_offsets =
            retain_positions_ ? active->chunk.position_offsets.subspan(begin, document_count + 1)
                              : std::span<const uint32_t> {};
    run->positions_flat =
            retain_positions_ ? active->chunk.positions_flat.subspan(position_begin, position_count)
                              : std::span<const uint32_t> {};

    if (counts_as_semantic_token_) {
        uint64_t& token_count = destination_semantic_token_counts_[*active_destination_];
        if (position_count > std::numeric_limits<uint64_t>::max() - token_count) {
            return merge_corruption("semantic token count overflows uint64");
        }
        token_count += position_count;
    }
    previous_segment_ = active->chunk.destination_segment;
    previous_docid_ = docids[end - 1];
    has_previous_posting_ = true;
    active->ordinal = end;
#ifdef BE_TEST
    posting_run_document_counter.fetch_add(document_count, std::memory_order_relaxed);
    posting_run_emitted_run_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    return Status::OK();
}

Status MergedPostingRuns::settle_pending_run() {
    if (!pending_source_.has_value()) {
        return Status::OK();
    }
    const size_t cursor_ordinal = *pending_source_;
    RETURN_IF_ERROR(advance_front_source(cursor_ordinal, &active_chunks_[cursor_ordinal]));
    pending_source_.reset();
    return Status::OK();
}

Status MergedPostingRuns::advance_front_source(size_t cursor_ordinal, ActivePostingChunk* active) {
    DCHECK(active != nullptr);
    bool has_chunk = active->ordinal < active->chunk.destination_docids.size();
    if (!has_chunk) {
        active->chunk = {};
        active->ordinal = 0;
        RETURN_IF_ERROR(cursors_[cursor_ordinal]->next_chunk(&active->chunk, &has_chunk));
    }
    if (has_chunk) {
        if (active->ordinal == 0) {
            RETURN_IF_ERROR(active->validate_and_refresh_frontier(retain_positions_,
                                                                  destination_doc_counts_));
        } else {
            active->refresh_frontier();
        }
    }
    active_frontier_.update(cursor_ordinal, has_chunk);
#ifdef BE_TEST
    posting_run_frontier_update_counter.fetch_add(1, std::memory_order_relaxed);
#endif
    return Status::OK();
}

#ifdef BE_TEST
namespace testing {

void reset_posting_run_merge_counters() {
    posting_run_frontier_update_counter.store(0, std::memory_order_relaxed);
    posting_run_frontier_comparison_counter.store(0, std::memory_order_relaxed);
    posting_run_document_counter.store(0, std::memory_order_relaxed);
    posting_run_emitted_run_counter.store(0, std::memory_order_relaxed);
    posting_run_boundary_search_counter.store(0, std::memory_order_relaxed);
    posting_run_shape_scan_document_counter.store(0, std::memory_order_relaxed);
    posting_run_legacy_fill_call_counter.store(0, std::memory_order_relaxed);
    posting_run_copied_document_counter.store(0, std::memory_order_relaxed);
}

uint64_t posting_run_frontier_updates() {
    return posting_run_frontier_update_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_frontier_comparisons() {
    return posting_run_frontier_comparison_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_documents() {
    return posting_run_document_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_emitted_runs() {
    return posting_run_emitted_run_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_boundary_searches() {
    return posting_run_boundary_search_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_shape_scan_documents() {
    return posting_run_shape_scan_document_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_legacy_fill_calls() {
    return posting_run_legacy_fill_call_counter.load(std::memory_order_relaxed);
}

uint64_t posting_run_copied_documents() {
    return posting_run_copied_document_counter.load(std::memory_order_relaxed);
}

} // namespace testing
#endif

} // namespace doris::snii::compaction
