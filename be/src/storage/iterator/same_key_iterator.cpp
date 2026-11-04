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

#include "storage/iterator/same_key_iterator.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "core/block/block.h"

namespace doris {

void SameKeyFlags::compute(const Block& block, size_t first_row, std::vector<uint8_t>* flags) {
    const size_t rows = block.rows();
    DCHECK_LT(first_row, rows);
    flags->assign(rows - first_row, 1);
    (*flags)[0] = !_last_key.empty();
    for (size_t i = 0; i < _key_ordinals.size(); ++i) {
        const IColumn& column = *block.get_by_position(_key_ordinals[i]).column;
        if ((*flags)[0] && column.compare_at(first_row, 0, *_last_key[i], -1) != 0) {
            (*flags)[0] = 0;
        }
        for (size_t row = first_row + 1; row < rows; ++row) {
            uint8_t& flag = (*flags)[row - first_row];
            if (flag && column.compare_at(row, row - 1, column, -1) != 0) {
                flag = 0;
            }
        }
    }

    if (_last_key.empty()) {
        for (uint32_t ordinal : _key_ordinals) {
            _last_key.push_back(block.get_by_position(ordinal).column->clone_empty());
        }
    }
    for (size_t i = 0; i < _key_ordinals.size(); ++i) {
        _last_key[i]->clear();
        _last_key[i]->insert_from(*block.get_by_position(_key_ordinals[i]).column, rows - 1);
    }
}

SameKeyIterator::SameKeyIterator(RowwiseIteratorUPtr rows, const MergeDecision& decision)
        : _rows(std::move(rows)),
          _drop_older_versions(decision.drop_older_versions),
          _flags(decision.compare_ordinals) {
    // Rows equal on a prefix of the key are not versions of one key.
    DORIS_CHECK(!_drop_older_versions || decision.compare_whole_key);
}

Status SameKeyIterator::next_batch(Block* block) {
    if (!_drop_older_versions) {
        return _rows->next_batch(block);
    }
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

Status SameKeyIterator::next_batch(Block* block, std::vector<uint8_t>* same_key) {
    DORIS_CHECK(!_drop_older_versions);
    same_key->clear();
    const size_t first_row = block->rows();
    RETURN_IF_ERROR(_rows->next_batch(block));
    _flags.compute(*block, first_row, same_key);
    return Status::OK();
}

RowwiseIteratorUPtr new_same_key_iterator(RowwiseIteratorUPtr rows, const MergeDecision& decision) {
    return std::make_unique<SameKeyIterator>(std::move(rows), decision);
}

} // namespace doris
