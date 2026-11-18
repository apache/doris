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

#include "storage/iterator/unique_key_iterator.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "core/block/block.h"

namespace doris {

UniqueKeyIterator::UniqueKeyIterator(RowwiseIteratorUPtr rows, const MergeDecision& decision)
        : _rows(std::move(rows)), _flags(decision.compare_ordinals) {
    // Rows equal on a prefix of the key are not versions of one key.
    DORIS_CHECK(decision.drop_older_versions && decision.compare_whole_key);
}

Status UniqueKeyIterator::next_batch(Block* block) {
    const size_t first_row = block->rows();
    while (true) {
        RETURN_IF_ERROR(_rows->next_batch(block));
        _flags.compute(*block, first_row, &_same_key);
        const auto dropped = static_cast<size_t>(std::ranges::count(_same_key, 1));
        if (dropped == 0) {
            return Status::OK();
        }
        _merged_rows += dropped;
        _filter.resize_fill(block->rows(), 1);
        for (size_t i = 0; i < _same_key.size(); ++i) {
            _filter[first_row + i] = !_same_key[i];
        }
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, _filter));
        if (block->rows() > first_row) {
            return Status::OK();
        }
        // Every row repeated the key before it: read on, an OK batch adds at least one row.
    }
}

RowwiseIteratorUPtr new_unique_key_iterator(RowwiseIteratorUPtr rows,
                                            const MergeDecision& decision) {
    return std::make_unique<UniqueKeyIterator>(std::move(rows), decision);
}

} // namespace doris
