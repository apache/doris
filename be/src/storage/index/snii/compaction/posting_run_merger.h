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
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/compaction/indexed_winner_tree.h"
#include "storage/index/snii/compaction/posting_cursor.h"
#include "storage/index/snii/writer/posting_window_emitter.h"
#include "storage/index/snii/writer/term_posting_source.h"

namespace doris::snii::compaction {

// K-way merge of destination-homogeneous cursor chunks. next_run() borrows the
// selected cursor workspace without copying; the view remains valid until a
// later next_run() call advances that cursor.
class MergedPostingRuns final : public writer::TermPostingSource {
    struct ActivePostingChunk {
        RemappedPostingChunk chunk;
        size_t ordinal = 0;
        uint32_t frontier_segment = 0;
        uint32_t frontier_docid = 0;
        uint32_t previous_chunk_segment = 0;
        uint32_t previous_chunk_docid = 0;
        bool has_previous_chunk_posting = false;

        void refresh_frontier();
        Status validate_and_refresh_frontier(bool retain_positions,
                                             std::span<const uint32_t> destination_doc_counts);
    };

    struct FrontierBefore {
        const std::vector<ActivePostingChunk>* active_chunks = nullptr;

        bool operator()(size_t lhs, size_t rhs) const;
    };

public:
    MergedPostingRuns(std::vector<std::unique_ptr<SniiPostingCursor>> cursors,
                      bool retain_positions, bool counts_as_semantic_token,
                      std::span<const uint32_t> destination_doc_counts,
                      std::span<uint64_t> destination_semantic_token_counts);

    Status init();
    bool empty() const;
    uint32_t next_destination() const;
    Status begin_destination(uint32_t destination);
    Status next_run(uint32_t max_docs, writer::PostingRunView* run, bool* has_run);

    // Transitional streamed-session adapter. Compaction tests use next_run()
    // directly; the assembler integration removes this handoff in the next
    // milestone.
    Status fill(uint32_t target_docs, writer::TermPostingBuffer* out, bool* exhausted) override;

private:
    uint32_t front_segment() const;
    Status select_front_run(size_t max_docs, writer::PostingRunView* run);
    Status select_run(ActivePostingChunk* active, size_t max_docs,
                      std::optional<std::pair<uint32_t, uint32_t>> next_frontier,
                      writer::PostingRunView* run);
    Status settle_pending_run();
    Status advance_front_source(size_t cursor_ordinal, ActivePostingChunk* active);

    std::vector<std::unique_ptr<SniiPostingCursor>> cursors_;
    std::vector<ActivePostingChunk> active_chunks_;
    IndexedWinnerTree<FrontierBefore> active_frontier_;
    bool retain_positions_ = true;
    bool counts_as_semantic_token_ = false;
    std::span<const uint32_t> destination_doc_counts_;
    std::span<uint64_t> destination_semantic_token_counts_;
    std::optional<uint32_t> active_destination_;
    std::optional<size_t> pending_source_;
    uint32_t previous_segment_ = 0;
    uint32_t previous_docid_ = 0;
    bool has_previous_posting_ = false;
    bool initialized_ = false;
};

#ifdef BE_TEST
namespace testing {

void reset_posting_run_merge_counters();
uint64_t posting_run_frontier_updates();
uint64_t posting_run_frontier_comparisons();
uint64_t posting_run_documents();
uint64_t posting_run_emitted_runs();
uint64_t posting_run_boundary_searches();
uint64_t posting_run_shape_scan_documents();
uint64_t posting_run_legacy_fill_calls();
uint64_t posting_run_copied_documents();

} // namespace testing
#endif

} // namespace doris::snii::compaction
