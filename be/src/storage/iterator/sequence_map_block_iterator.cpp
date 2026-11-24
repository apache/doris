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

#include "storage/iterator/sequence_map_block_iterator.h"

#include <glog/logging.h>

#include <algorithm>
#include <ranges>
#include <utility>

#include "common/exception.h"
#include "util/simd/bits.h"

namespace doris {

SequenceMapBlockIterator::SequenceMapBlockIterator(RowwiseIteratorUPtr input, size_t block_row_max,
                                                   size_t preferred_block_size_bytes,
                                                   ReadSchemaSPtr read_schema,
                                                   uint64_t* merged_rows)
        : _input(std::move(input)),
          _block_row_max(block_row_max),
          _preferred_block_size_bytes(preferred_block_size_bytes),
          _read_schema(std::move(read_schema)),
          _num_key_columns(_read_schema->num_key_columns()),
          _merged_rows(merged_rows),
          _input_block(_read_schema->create_read_block()),
          _batch_columns(_read_schema->create_read_block().mutate_columns()) {
    DORIS_CHECK_GT(_num_key_columns, 0);
    DORIS_CHECK_GT(_block_row_max, 0);
}

size_t SequenceMapBlockIterator::_compute_batch_row_limit() const {
    if (_preferred_block_size_bytes == 0) {
        return _block_row_max;
    }
    const size_t row_bytes = std::max<size_t>(1, _input_block.bytes() / _input_block.rows());
    return std::clamp<size_t>(_preferred_block_size_bytes / row_bytes, 1, _block_row_max);
}

void SequenceMapBlockIterator::_mark_same_keys(const Block& source) {
    const size_t rows = source.rows();
    _same_key.assign(rows, 1);
    for (size_t col_idx : std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
        source.get_by_position(col_idx).column->update_equal_flags(_same_key);
        if (!simd::contain_one(_same_key.data() + 1, rows - 1)) {
            break;
        }
    }
    bool same_first = _batch_rows > 0;
    for (size_t col_idx : std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
        if (!same_first) {
            break;
        }
        const IColumn& batch = *_batch_columns[col_idx];
        same_first = batch.compare_at(batch.size() - 1, 0, *source.get_by_position(col_idx).column,
                                      -1) == 0;
    }
    _same_key[0] = same_first;
}

void SequenceMapBlockIterator::_replace_by_sequence(const Block& source, size_t src_pos) {
    const size_t dst_pos = _batch_rows - 1;
    for (const auto& [seq_col_idx, val_col_idxs] : _read_schema->sequence_map()) {
        auto* dst_seq_col = _batch_columns[seq_col_idx].get();
        const auto& src_seq_col = source.get_by_position(seq_col_idx).column;
        // The existing row comes from a newer rowset/segment, so it wins ties (>= 0).
        if (dst_seq_col->compare_at(dst_pos, src_pos, *src_seq_col, -1) >= 0) {
            continue;
        }
        for (auto val_col_idx : val_col_idxs) {
            auto* dst_val_col = _batch_columns[val_col_idx].get();
            dst_val_col->pop_back(1);
            dst_val_col->insert_from(*source.get_by_position(val_col_idx).column, src_pos);
        }
        dst_seq_col->pop_back(1);
        dst_seq_col->insert_from(*src_seq_col, src_pos);
    }
}

void SequenceMapBlockIterator::_fold_block(const Block& source) {
    const size_t rows = source.rows();
    DCHECK_GT(rows, 0);
    _mark_same_keys(source);
    const size_t num_columns = _batch_columns.size();
    if (!simd::contain_one(_same_key.data(), rows)) {
        for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
            _batch_columns[col_idx]->insert_range_from(*source.get_by_position(col_idx).column, 0,
                                                       rows);
        }
        _batch_rows += rows;
        return;
    }
    for (size_t pos = 0; pos < rows; ++pos) {
        if (!_same_key[pos]) {
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx) {
                _batch_columns[col_idx]->insert_from(*source.get_by_position(col_idx).column, pos);
            }
            ++_batch_rows;
        } else {
            ++(*_merged_rows);
            _replace_by_sequence(source, pos);
        }
    }
}

void SequenceMapBlockIterator::_output_finished_keys(Block* block) {
    DCHECK_EQ(block->rows(), 0);
    DCHECK_GT(_batch_rows, 1);
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    const size_t last_key = _batch_rows - 1;
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        std::swap(columns[col_idx], _batch_columns[col_idx]);
        _batch_columns[col_idx]->insert_from(*columns[col_idx], last_key);
        columns[col_idx]->pop_back(1);
    }
    _batch_rows = 1;
}

void SequenceMapBlockIterator::_output_all_keys(Block* block) {
    DCHECK_EQ(block->rows(), 0);
    DCHECK_GT(_batch_rows, 0);
    auto columns_guard = block->mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        std::swap(columns[col_idx], _batch_columns[col_idx]);
    }
    _batch_rows = 0;
}

Status SequenceMapBlockIterator::next_block(Block* block, bool* eof) {
    DCHECK_EQ(block->rows(), 0);
    *eof = false;
    while (true) {
        _input_block.clear_column_data();
        Status st = _input->next_batch(&_input_block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        RETURN_IF_ERROR(st);
        RETURN_IF_CATCH_EXCEPTION(_fold_block(_input_block));
        if (_batch_rows > _compute_batch_row_limit()) {
            RETURN_IF_CATCH_EXCEPTION(_output_finished_keys(block));
            return Status::OK();
        }
    }
    if (_batch_rows > 0) {
        RETURN_IF_CATCH_EXCEPTION(_output_all_keys(block));
        return Status::OK();
    }
    *eof = true;
    return Status::OK();
}

} // namespace doris
