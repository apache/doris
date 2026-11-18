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
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "storage/iterator/heap_merge_iterator.h"
#include "storage/iterator/row_sources_buffer.h"
#include "storage/iterator/same_key_iterator.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

// The key group of a vertical compaction: VHeapMergeIterator sorts the sources by the key, and
// every merged row is recorded in the RowSourcesBuffer as the source it came from, for the value
// groups to replay the merge.
//
// With `keep_first_row_of_key` (UNIQUE_KEYS without a cluster key, AGG_KEYS) only the first row
// of every key, the newest one, stays in the key Block; the others are recorded with their agg
// flag set, and the value groups skip them (UNIQUE_KEYS) or fold them into that first row
// (AGG_KEYS). Example with key 1 in source 0 and source 1 of a UNIQUE_KEYS table: the newer row
// of source 1 is kept and recorded as (1, false), the row of source 0 is dropped and recorded as
// (0, true). Otherwise every row is kept and recorded without the flag.
class VerticalKeyMergeIterator final : public RowwiseIterator {
public:
    VerticalKeyMergeIterator(std::vector<RowwiseIteratorUPtr> sources, ReadSchemaSPtr read_schema,
                             MergeDecision decision, bool keep_first_row_of_key,
                             RowSourcesBuffer* row_sources_buf);

    // `sample_info` collects the bytes and rows of the first merged Block.
    Status init(const StorageReadOptions& opts, CompactionSampleInfo* sample_info) override;
    Status next_batch(Block* block) override;
    const ReadSchema& schema() const override { return _merge.schema(); }
    // Rows dropped by keep_first_row_of_key.
    uint64_t merged_rows() const override { return _merged_rows; }

private:
    // Record the rows of `block` from `first_row` on, which come from `_row_children`, and drop
    // the repeated keys with keep_first_row_of_key.
    Status _record_rows(Block* block, size_t first_row);

    VHeapMergeIterator _merge;
    const bool _keep_first_row_of_key;
    SameKeyFlags _same_key_flags;
    RowSourcesBuffer* _row_sources_buf;
    CompactionSampleInfo* _sample_info = nullptr;
    uint64_t _merged_rows = 0;
    // Reused by every next_batch().
    std::vector<uint32_t> _row_children;
    std::vector<uint8_t> _same_key;
    IColumn::Filter _filter;
    std::vector<RowSource> _row_sources;
};

std::shared_ptr<RowwiseIterator> new_vertical_key_merge_iterator(
        std::vector<RowwiseIteratorUPtr> sources, ReadSchemaSPtr read_schema,
        const MergeDecision& decision, bool keep_first_row_of_key,
        RowSourcesBuffer* row_sources_buf);

} // namespace doris
