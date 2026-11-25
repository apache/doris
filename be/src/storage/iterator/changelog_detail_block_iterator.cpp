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

#include "storage/iterator/changelog_detail_block_iterator.h"

#include <glog/logging.h>

#include <utility>

#include "common/exception.h"

namespace doris {

ChangelogDetailBlockIterator::ChangelogDetailBlockIterator(std::unique_ptr<BlockIterator> input,
                                                           size_t block_row_max,
                                                           ReadSchemaSPtr read_schema)
        : _cursor(std::move(input), read_schema),
          _block_row_max(block_row_max),
          _read_schema(std::move(read_schema)),
          _op_col_idx(_read_schema->op_ordinal()) {
    DORIS_CHECK_GT(_block_row_max, 0);
    DORIS_CHECK_GE(_op_col_idx, 0);
}

Status ChangelogDetailBlockIterator::_append_change_row(MutableColumns& target_columns,
                                                        changelog::StreamChangeOp output_op,
                                                        bool use_before) const {
    const Block& src_block = _cursor.block();
    const size_t pos = _cursor.row_pos();
    for (size_t col_idx = 0; col_idx < target_columns.size(); ++col_idx) {
        if (std::cmp_equal(col_idx, _op_col_idx)) {
            RETURN_IF_ERROR(write_changelog_op(*target_columns[col_idx], output_op));
            continue;
        }
        const uint32_t src_col_idx =
                use_before ? _read_schema->before_column_ordinal(static_cast<uint32_t>(col_idx))
                           : static_cast<uint32_t>(col_idx);
        target_columns[col_idx]->insert_from(*src_block.get_by_position(src_col_idx).column, pos);
    }
    return Status::OK();
}

Status ChangelogDetailBlockIterator::_emit_change(ChangelogOp op, MutableColumns& target_columns,
                                                  size_t* output_rows) {
    if (op == ChangelogOp::UPDATE) {
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::UPDATE_BEFORE, true));
        ++(*output_rows);
        if (*output_rows >= _block_row_max) {
            return _append_change_row(_pending_row.park(target_columns),
                                      changelog::StreamChangeOp::UPDATE_AFTER, false);
        }
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::UPDATE_AFTER, false));
        ++(*output_rows);
    } else if (op == ChangelogOp::APPEND) {
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::INSERT, false));
        ++(*output_rows);
    } else if (op == ChangelogOp::DELETE) {
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::DELETE, true));
        ++(*output_rows);
    }
    return Status::OK();
}

Status ChangelogDetailBlockIterator::next_block(Block* block, bool* eof) {
    DCHECK_EQ(block->rows(), 0);
    RETURN_IF_ERROR(_cursor.ensure_started());
    if (_cursor.eof() && !_pending_row.has_row()) {
        *eof = true;
        return Status::OK();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_rows = 0;
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION([&]() -> Status {
        while (output_rows < _block_row_max) {
            if (_pending_row.emit(target_columns)) {
                ++output_rows;
                continue;
            }
            if (_cursor.eof()) {
                break;
            }
            const ChangelogOp op = read_changelog_op(
                    *_cursor.block().get_by_position(static_cast<size_t>(_op_col_idx)).column,
                    _cursor.row_pos());
            RETURN_IF_ERROR(_emit_change(op, target_columns, &output_rows));
            RETURN_IF_ERROR(_cursor.advance());
        }
        return Status::OK();
    }());
    *eof = output_rows == 0 && _cursor.eof() && !_pending_row.has_row();
    return Status::OK();
}

} // namespace doris
