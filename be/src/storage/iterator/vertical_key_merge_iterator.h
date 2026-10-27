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

#include <memory>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "storage/iterator/heap_merge_iterator.h"
#include "storage/iterator/row_sources_buffer.h"
#include "storage/iterators.h"
#include "storage/schema.h"

namespace doris {

// The key group of a vertical compaction: VHeapMergeIterator merges the sources by the key, and
// every merged row is recorded in the RowSourcesBuffer as the source it came from, a row the
// merge drops included with its agg flag set, for the value groups to replay the merge.
// Example with key 1 in source 0 and source 1 of a UNIQUE_KEYS table: the newer row of source 1
// is copied and recorded as (1, false), the dropped row of source 0 as (0, true).
class VerticalKeyMergeIterator final : public RowwiseIterator {
public:
    VerticalKeyMergeIterator(std::vector<RowwiseIteratorUPtr> sources, ReadSchemaSPtr read_schema,
                             MergeDecision decision, RowSourcesBuffer* row_sources_buf);

    // `sample_info` collects the bytes and rows of the first merged Block.
    Status init(const StorageReadOptions& opts, CompactionSampleInfo* sample_info) override;
    Status next_batch(Block* block) override;
    const ReadSchema& schema() const override { return _merge.schema(); }
    uint64_t merged_rows() const override { return _merge.merged_rows(); }

private:
    VHeapMergeIterator _merge;
    RowSourcesBuffer* _row_sources_buf;
    CompactionSampleInfo* _sample_info = nullptr;
    // Reused by every next_batch().
    std::vector<VHeapMergeIterator::MergedRun> _runs;
    std::vector<RowSource> _row_sources;
};

std::shared_ptr<RowwiseIterator> new_vertical_key_merge_iterator(
        std::vector<RowwiseIteratorUPtr> sources, ReadSchemaSPtr read_schema,
        const MergeDecision& decision, RowSourcesBuffer* row_sources_buf);

} // namespace doris
