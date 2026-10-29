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

#include "storage/iterator/vertical_key_merge_iterator.h"

#include <cstdint>
#include <utility>

#include "common/status.h"

namespace doris {
using namespace ErrorCode;

VerticalKeyMergeIterator::VerticalKeyMergeIterator(std::vector<RowwiseIteratorUPtr> sources,
                                                   ReadSchemaSPtr read_schema,
                                                   MergeDecision decision,
                                                   RowSourcesBuffer* row_sources_buf)
        : _merge(std::move(sources), std::move(read_schema), std::move(decision)),
          _row_sources_buf(row_sources_buf) {}

Status VerticalKeyMergeIterator::init(const StorageReadOptions& opts,
                                      CompactionSampleInfo* sample_info) {
    _sample_info = sample_info;
    return _merge.init(opts);
}

Status VerticalKeyMergeIterator::next_batch(Block* block) {
    Status st = _merge.next_batch(block, &_runs);
    if (!st.ok() && !st.is<END_OF_FILE>()) {
        return st;
    }
    // The runs of an EndOfFile hold the rows dropped last, which the value groups skip too.
    _row_sources.clear();
    for (const auto& run : _runs) {
        const auto source = static_cast<uint16_t>(run.child);
        _row_sources.emplace_back(source, run.first_same);
        _row_sources.insert(_row_sources.end(), run.rows - 1, RowSource(source, false));
    }
    RETURN_IF_ERROR(_row_sources_buf->append(_row_sources));
    if (_sample_info != nullptr && block->rows() > 0) {
        _sample_info->bytes += block->allocated_bytes();
        _sample_info->rows += block->rows();
        _sample_info = nullptr;
    }
    return st;
}

std::shared_ptr<RowwiseIterator> new_vertical_key_merge_iterator(
        std::vector<RowwiseIteratorUPtr> sources, ReadSchemaSPtr read_schema,
        const MergeDecision& decision, RowSourcesBuffer* row_sources_buf) {
    return std::make_shared<VerticalKeyMergeIterator>(std::move(sources), std::move(read_schema),
                                                      decision, row_sources_buf);
}

} // namespace doris
