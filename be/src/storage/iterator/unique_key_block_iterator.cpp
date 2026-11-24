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

#include "storage/iterator/unique_key_block_iterator.h"

#include <glog/logging.h>

#include <ranges>
#include <utility>

#include "common/exception.h"
#include "core/block/block.h"
#include "util/simd/bits.h"

namespace doris {

UniqueKeyBlockIterator::UniqueKeyBlockIterator(RowwiseIteratorUPtr input,
                                               ReadSchemaSPtr read_schema, uint64_t* merged_rows)
        : _input(std::move(input)),
          _read_schema(std::move(read_schema)),
          _num_key_columns(_read_schema->num_key_columns()),
          _merged_rows(merged_rows) {
    DORIS_CHECK_GT(_num_key_columns, 0);
}

void UniqueKeyBlockIterator::_mark_same_keys(const Block& block) {
    const size_t rows = block.rows();
    _same_key.assign(rows, 1);
    for (size_t col_idx : std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
        block.get_by_position(col_idx).column->update_equal_flags(_same_key);
        if (!simd::contain_one(_same_key.data() + 1, rows - 1)) {
            break;
        }
    }
    bool same_first = _has_last_key;
    for (size_t col_idx : std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
        if (!same_first) {
            break;
        }
        same_first = _last_key_columns[col_idx]->compare_at(
                             0, 0, *block.get_by_position(col_idx).column, -1) == 0;
    }
    _same_key[0] = same_first;
}

void UniqueKeyBlockIterator::_save_last_key(const Block& block) {
    const size_t last_row = block.rows() - 1;
    if (!_has_last_key) {
        _last_key_columns.reserve(_num_key_columns);
        for (size_t col_idx = 0; col_idx < _num_key_columns; ++col_idx) {
            _last_key_columns.push_back(block.get_by_position(col_idx).column->clone_empty());
        }
        _has_last_key = true;
    } else {
        for (auto& col : _last_key_columns) {
            col->clear();
        }
    }
    for (size_t col_idx = 0; col_idx < _num_key_columns; ++col_idx) {
        _last_key_columns[col_idx]->insert_from(*block.get_by_position(col_idx).column, last_row);
    }
}

Status UniqueKeyBlockIterator::next_block(Block* block, bool* eof) {
    DCHECK_EQ(block->rows(), 0);
    *eof = false;
    while (true) {
        block->clear_column_data();
        Status st = _input->next_batch(block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            *eof = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        const size_t rows = block->rows();
        DCHECK_GT(rows, 0);

        _mark_same_keys(*block);
        const size_t kept =
                simd::count_zero_num(reinterpret_cast<const int8_t*>(_same_key.data()), rows);
        const size_t dropped = rows - kept;
        if (dropped == rows) {
            // Every row repeated `_last_key_columns`; read on until a new key or EOF.
            *_merged_rows += rows;
            continue;
        }

        RETURN_IF_CATCH_EXCEPTION(_save_last_key(*block));
        if (dropped == 0) {
            return Status::OK();
        }

        *_merged_rows += dropped;
        _filter.resize(rows);
        for (size_t i = 0; i < rows; ++i) {
            _filter[i] = !_same_key[i];
        }
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(block, _filter));
        return Status::OK();
    }
}

} // namespace doris
