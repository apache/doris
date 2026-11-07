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
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

class RowSourcesBuffer;
// One source of VerticalMaskMergeIterator, defined in mask_merge_iterator.cpp.
class VerticalMergeIteratorContext;

// Owned by one vertical_merge_rowsets invocation and updated synchronously by its iterators.
struct VerticalCompactionContextStats {
    int64_t active_segment_contexts = 0;
    int64_t active_segment_contexts_peak = 0;
};

// --------------- RowBatch for batch optimization ------------- //
// Batch of continuous rows from the same segment context
struct RowBatch {
    std::shared_ptr<Block> block; // source block snapshot
    uint32_t start_row;           // start row position in source block
    uint32_t count;               // number of rows

    RowBatch(std::shared_ptr<Block> b, uint32_t start, uint32_t cnt)
            : block(std::move(b)), start_row(start), count(cnt) {}
};

// --------------- VerticalMaskMergeIterator ------------- //
// A value group of a vertical compaction: the rows of its sources in the order the key group
// recorded in the RowSourcesBuffer.
class VerticalMaskMergeIterator : public RowwiseIterator {
public:
    // VerticalMaskMergeIterator takes the ownership of input iterators
    VerticalMaskMergeIterator(std::vector<RowwiseIteratorUPtr>&& iters,
                              RowSourcesBuffer* row_sources_buf,
                              VerticalCompactionContextStats* context_stats);

    ~VerticalMaskMergeIterator() override;
    VerticalMaskMergeIterator(const VerticalMaskMergeIterator&) = delete;
    VerticalMaskMergeIterator& operator=(const VerticalMaskMergeIterator&) = delete;

    Status init(const StorageReadOptions& opts, CompactionSampleInfo* sample_info) override;

    // Every row, the ones with the agg flag set included. Returns EndOfFile only without rows.
    Status next_batch(Block* block) override;

    // The same, plus the agg flag of every row as its same-key flag (AGG_KEYS folds).
    Status next_batch(Block* block, std::vector<uint8_t>* same_key) override;

    const ReadSchema& schema() const override { return *_schema; }

    Status unique_key_next_row(IteratorRowRef* ref) override;

    // Batch version of unique_key_next_row for performance optimization
    // Returns batches of continuous rows from the same segment
    Status unique_key_next_batch(std::vector<RowBatch>* batches, size_t max_rows,
                                 size_t* actual_rows);

    uint64_t merged_rows() const override { return _filtered_rows; }

private:
    // The two next_batch() overloads; `same_key` may be null.
    Status _next_batch(Block* block, std::vector<uint8_t>* same_key);
    Status check_all_iter_finished();
    // Advance the row-source cursor and release its context after the final reference.
    void consume_row_sources(uint16_t order, size_t count = 1);

    // released after build ctx
    std::vector<RowwiseIteratorUPtr> _origin_iters;

    std::vector<std::unique_ptr<VerticalMergeIteratorContext>> _origin_iter_ctx;

    const ReadSchema* _schema = nullptr;

    int _block_row_max = 0;
    size_t _filtered_rows = 0;
    RowSourcesBuffer* _row_sources_buf;
    VerticalCompactionContextStats* _context_stats;
    // StorageReadOptions is supplied by init(), after this iterator is constructed, and must be
    // retained for lazily initialized segment contexts.
    std::optional<StorageReadOptions> _opts;
    CompactionSampleInfo* _sample_info = nullptr;
};

std::shared_ptr<RowwiseIterator> new_vertical_mask_merge_iterator(
        std::vector<RowwiseIteratorUPtr>&& inputs, RowSourcesBuffer* row_sources_buf,
        VerticalCompactionContextStats* context_stats);

} // namespace doris
