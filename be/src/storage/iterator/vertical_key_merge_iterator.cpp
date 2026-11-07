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

#include <algorithm>
#include <cstdint>
#include <utility>

#include "common/exception.h"
#include "common/status.h"

namespace doris {
using namespace ErrorCode;

VerticalKeyMergeIterator::VerticalKeyMergeIterator(std::vector<RowwiseIteratorUPtr> sources,
                                                   ReadSchemaSPtr read_schema,
                                                   MergeDecision decision,
                                                   bool keep_first_row_of_key,
                                                   RowSourcesBuffer* row_sources_buf)
        : _merge(std::move(sources), std::move(read_schema), decision),
          _keep_first_row_of_key(keep_first_row_of_key),
          _same_key_flags(std::move(decision.compare_ordinals)),
          _row_sources_buf(row_sources_buf) {}

Status VerticalKeyMergeIterator::init(const StorageReadOptions& opts,
                                      CompactionSampleInfo* sample_info) {
    _sample_info = sample_info;
    return _merge.init(opts);
}

Status VerticalKeyMergeIterator::next_batch(Block* block) {
    const size_t first_row = block->rows();
    do {
        RETURN_IF_ERROR(_merge.next_batch_with_children(block, &_row_children));
        RETURN_IF_ERROR(_record_rows(block, first_row));
        // A batch whose rows all repeated the key before them reads on: an OK batch adds at
        // least one row.
    } while (block->rows() == first_row);
    if (_sample_info != nullptr) {
        _sample_info->bytes += block->allocated_bytes();
        _sample_info->rows += block->rows();
        _sample_info = nullptr;
    }
    return Status::OK();
}

Status VerticalKeyMergeIterator::_record_rows(Block* block, size_t first_row) {
    DCHECK_EQ(_row_children.size(), block->rows() - first_row);
    _row_sources.clear();
    if (!_keep_first_row_of_key) {
        for (const uint32_t child : _row_children) {
            _row_sources.emplace_back(static_cast<uint16_t>(child), false);
        }
        return _row_sources_buf->append(_row_sources);
    }

    _same_key_flags.compute(*block, first_row, &_same_key);
    for (size_t i = 0; i < _row_children.size(); ++i) {
        _row_sources.emplace_back(static_cast<uint16_t>(_row_children[i]), _same_key[i] != 0);
    }
    RETURN_IF_ERROR(_row_sources_buf->append(_row_sources));
    const auto dropped = static_cast<size_t>(std::ranges::count(_same_key, 1));
    if (dropped > 0) {
        _merged_rows += dropped;
        _filter.resize_fill(block->rows(), 1);
        for (size_t i = 0; i < _same_key.size(); ++i) {
            _filter[first_row + i] = !_same_key[i];
        }
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, _filter));
    }
    return Status::OK();
}

std::shared_ptr<RowwiseIterator> new_vertical_key_merge_iterator(
        std::vector<RowwiseIteratorUPtr> sources, ReadSchemaSPtr read_schema,
        const MergeDecision& decision, bool keep_first_row_of_key,
        RowSourcesBuffer* row_sources_buf) {
    return std::make_shared<VerticalKeyMergeIterator>(std::move(sources), std::move(read_schema),
                                                      decision, keep_first_row_of_key,
                                                      row_sources_buf);
}

} // namespace doris
