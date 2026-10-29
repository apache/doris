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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/data_type/data_type.h"
#include "storage/iterators.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_reader.h"

#pragma once

namespace doris {
class AggKeyBlockIterator;
class RowSourcesBuffer;
class RowsetMeta;
struct RowBatch;
struct VerticalCompactionContextStats;
class VerticalBlockReaderTestAccessor;

class VerticalBlockReader final : public TabletReader {
public:
    VerticalBlockReader(RowSourcesBuffer* row_sources_buffer,
                        VerticalCompactionContextStats* context_stats = nullptr);

    ~VerticalBlockReader() override;

    // Initialize VerticalBlockReader with tablet, data version and fetch range.
    Status init(const ReaderParams& read_params) override;
    Status init(const ReaderParams& read_params, CompactionSampleInfo* sample_info);

    Status next_block_with_aggregation(Block* block, bool* eof) override;

    uint64_t merged_rows() const override {
        DCHECK(_vcollect_iter);
        return _vcollect_iter->merged_rows();
    }

    static uint64_t nextId;

private:
    friend class VerticalBlockReaderTestAccessor;

    // The number of segments of each group among the `num_segment_iters` segments
    // [start, end) of `segment_offsets` (the whole rowset when start == end) of a rowset whose
    // segments are ordered within each group (NONOVERLAPPING_WITHIN_GROUP), in segment order.
    static std::vector<size_t> _segment_group_sizes(const RowsetMeta& rowset_meta,
                                                    std::pair<int64_t, int64_t> segment_offsets,
                                                    size_t num_segment_iters);

    // Directly read row from rowset and pass to upper caller. No need to do aggregation.
    // This is usually used for DUPLICATE KEY tables
    Status _direct_next_block(Block* block, bool* eof);
    // AGGREGATE KEY tables: the key group keeps the first row of every key, the value groups
    // fold the rows of every key.
    Status _agg_key_next_block(Block* block, bool* eof);
    // For UNIQUE KEY tables, read data by a merge heap.
    // The difference from _agg_key_next_block is that it will read the data from high version to low version,
    // to minimize the comparison time in merge heap.
    Status _unique_key_next_block(Block* block, bool* eof);

    Status _init_collect_iter(const ReaderParams& read_params, CompactionSampleInfo* sample_info);

    // The sources the key group merges and the value groups replay, in (rowset version,
    // segment) order. Every group builds the same sources, so that a row source names the same
    // one in all of them.
    Status _get_sources(const ReaderParams& read_params, std::vector<RowwiseIteratorUPtr>* sources);
    // Append the segments read one after another as one source.
    void _append_source(std::vector<RowwiseIteratorUPtr> segment_iters,
                        std::vector<RowwiseIteratorUPtr>* sources) const;
    MergeDecision _key_group_merge(const ReaderParams& read_params) const;

    // Helper functions for sparse column optimization
    void _prepare_sparse_columns(MutableColumns& columns, size_t actual_rows,
                                 std::vector<ColumnNullable*>& nullable_dst_cols,
                                 std::vector<bool>& supports_replace);
    void _process_sparse_column(ColumnNullable* nullable_dst, bool supports_replace,
                                MutableColumnPtr& target_col, const ColumnPtr& src_col,
                                const RowBatch& batch, size_t dst_offset);
    void _copy_non_null_runs(ColumnNullable* nullable_dst, const ColumnNullable* nullable_src,
                             const uint8_t* null_map, const RowBatch& batch, size_t dst_offset);

private:
    size_t _id;
    bool _is_key_column_group = false;
    std::shared_ptr<RowwiseIterator> _vcollect_iter;
    IteratorRowRef _next_row {{}, -1, false};

    bool _eof = false;

    Status (VerticalBlockReader::*_next_block_func)(Block* block, bool* eof) = nullptr;

    RowSourcesBuffer* _row_sources_buffer;
    VerticalCompactionContextStats* _context_stats;
    ColumnPtr _delete_filter_column;

    // The value groups of an AGGREGATE KEY table: the fold over `_vcollect_iter`, and the rows
    // it folded into an earlier row of their key.
    std::unique_ptr<AggKeyBlockIterator> _agg_block_iter;
    uint64_t _folded_rows = 0;

    // For sparse column compaction optimization
    // Set from reader_params.enable_sparse_optimization (calculated in Merger::vertical_merge_rowsets)
    bool _enable_sparse_optimization = false;

    // For tracking NULL cell count during compaction (used for sparse optimization threshold)
    CompactionSampleInfo* _sample_info = nullptr;
};

} // namespace doris
