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

#include "storage/iterator/changelog_block_iterator.h"

#include <glog/logging.h>

#include <ranges>
#include <utility>

#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/changelog.h"
#include "storage/iterator/changelog_block_reader_utils.h"
#include "util/simd/bits.h"

namespace doris {

namespace {

ChangelogOp read_changelog_op(const IColumn& col, size_t row) {
    const IColumn* cur = &col;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*cur)) {
        if (nullable->is_null_at(row)) {
            return ChangelogOp::UNKNOWN;
        }
        cur = &nullable->get_nested_column();
    }
    if (const auto* int64_col = check_and_get_column<ColumnInt64>(*cur)) {
        return static_cast<ChangelogOp>(int64_col->get_element(row));
    }
    return ChangelogOp::UNKNOWN;
}

Status write_changelog_op(IColumn& col, changelog::StreamChangeOp op) {
    IColumn* cur = &col;
    ColumnNullable* nullable = nullptr;
    if (auto* n = typeid_cast<ColumnNullable*>(cur)) {
        nullable = n;
        cur = &nullable->get_nested_column();
    }
    if (auto* int64_col = typeid_cast<ColumnInt64*>(cur)) {
        int64_col->insert_value(static_cast<int64_t>(op));
    } else {
        return Status::InternalError("invalid column type");
    }
    if (nullable != nullptr) {
        nullable->get_null_map_data().push_back(0);
    }
    return Status::OK();
}

} // namespace

MutableColumns& ChangelogPendingRow::park(const MutableColumns& target_columns) {
    DCHECK(!_has_row);
    if (_columns.empty()) {
        _columns.reserve(target_columns.size());
        for (const auto& column : target_columns) {
            _columns.push_back(column->clone_empty());
        }
    }
    _has_row = true;
    return _columns;
}

bool ChangelogPendingRow::emit(MutableColumns& target_columns) {
    if (!_has_row) {
        return false;
    }
    for (size_t i = 0; i < _columns.size(); ++i) {
        target_columns[i]->insert_from(*_columns[i], 0);
        _columns[i]->clear();
    }
    _has_row = false;
    return true;
}

ChangelogInputCursor::ChangelogInputCursor(std::unique_ptr<BlockIterator> input,
                                           ReadSchemaSPtr read_schema)
        : _input(std::move(input)), _read_schema(std::move(read_schema)) {}

Status ChangelogInputCursor::ensure_started() {
    if (_started) {
        return Status::OK();
    }
    _started = true;
    _block = _read_schema->create_read_block();
    return _refill();
}

Status ChangelogInputCursor::advance() {
    return advance_to(_pos + 1);
}

Status ChangelogInputCursor::advance_to(size_t pos) {
    if (_eof) {
        return Status::OK();
    }
    if (pos < _block.rows()) {
        _pos = pos;
        return Status::OK();
    }
    return _refill();
}

Status ChangelogInputCursor::_refill() {
    while (true) {
        _block.clear_column_data();
        bool input_eof = false;
        RETURN_IF_ERROR(_input->next_block(&_block, &input_eof));
        if (input_eof) {
            _eof = true;
            _pos = 0;
            return Status::OK();
        }
        if (_block.rows() > 0) {
            _pos = 0;
            ++_block_id;
            return Status::OK();
        }
    }
}

ChangelogMinDeltaBlockIterator::ChangelogMinDeltaBlockIterator(std::unique_ptr<BlockIterator> input,
                                                               size_t block_row_max,
                                                               ReadSchemaSPtr read_schema)
        : _cursor(std::move(input), read_schema),
          _block_row_max(block_row_max),
          _read_schema(std::move(read_schema)),
          _num_key_columns(_read_schema->num_key_columns()),
          _op_col_idx(_read_schema->op_ordinal()),
          _tso_col_idx(_read_schema->tso_ordinal()),
          _lsn_col_idx(_read_schema->lsn_ordinal()) {
    DORIS_CHECK_GT(_block_row_max, 0);
    DORIS_CHECK_GE(_op_col_idx, 0);
}

void ChangelogMinDeltaBlockIterator::_mark_block_same_keys() {
    const Block& block = _cursor.block();
    const size_t rows = block.rows();
    _same_key.assign(rows, 1);
    _same_key[0] = 0;
    if (rows > 1) {
        for (size_t col_idx :
             std::views::iota(size_t {0}, _num_key_columns) | std::views::reverse) {
            block.get_by_position(col_idx).column->update_equal_flags(_same_key);
            if (!simd::contain_one(_same_key.data() + 1, rows - 1)) {
                break;
            }
        }
    }
    _same_key_block_id = _cursor.block_id();
}

void ChangelogMinDeltaBlockIterator::_set_group_row(size_t row_pos, bool replace_last) {
    const Block& block = _cursor.block();
    for (size_t col_idx = 0; col_idx < _group_columns.size(); ++col_idx) {
        if (replace_last) {
            _group_columns[col_idx]->pop_back(1);
        }
        _group_columns[col_idx]->insert_from(*block.get_by_position(col_idx).column, row_pos);
    }
}

bool ChangelogMinDeltaBlockIterator::_first_row_matches_group() const {
    const Block& block = _cursor.block();
    for (size_t col_idx = 0; col_idx < _num_key_columns; ++col_idx) {
        if (_group_columns[col_idx]->compare_at(0, 0, *block.get_by_position(col_idx).column, -1) !=
            0) {
            return false;
        }
    }
    return true;
}

Status ChangelogMinDeltaBlockIterator::_collect_group() {
    if (_same_key_block_id != _cursor.block_id()) {
        _mark_block_same_keys();
    }
    size_t pos = _cursor.row_pos();
    _set_group_row(pos, /*replace_last=*/false);
    bool first_block_of_group = true;
    while (true) {
        const size_t block_rows = _cursor.block().rows();
        const size_t next_diff = simd::find_zero(_same_key, pos + 1);
        const size_t run_last = next_diff - 1;
        if (!first_block_of_group || run_last > pos) {
            _set_group_row(run_last, /*replace_last=*/_group_columns[0]->size() > 1);
        }
        RETURN_IF_ERROR(_cursor.advance_to(next_diff));
        if (next_diff < block_rows || _cursor.eof()) {
            return Status::OK();
        }
        _mark_block_same_keys();
        if (!_first_row_matches_group()) {
            return Status::OK();
        }
        pos = 0;
        first_block_of_group = false;
    }
}

bool ChangelogMinDeltaBlockIterator::_group_values_unchanged() {
    const auto& value_column_pairs = _read_schema->changelog_value_column_pairs();
    if (!_read_schema->changelog_value_pairs_complete() || value_column_pairs.empty() ||
        _value_compare_unsupported) {
        return false;
    }
    const size_t last_row = _last_group_row();
    bool before_image_available = false;
    for (const auto& [after_col_idx, before_col_idx] : value_column_pairs) {
        const auto* before_column = _group_columns[before_col_idx].get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*before_column)) {
            before_image_available |= !nullable->is_null_at(0);
        } else {
            before_image_available = true;
        }
        try {
            if (before_column->compare_at(0, last_row, *_group_columns[after_col_idx], -1) != 0) {
                return false;
            }
        } catch (const Exception& e) {
            if (e.code() != ErrorCode::NOT_IMPLEMENTED_ERROR) {
                throw;
            }
            _value_compare_unsupported = true;
            return false;
        }
    }
    // An all-NULL BEFORE image can mean the before image was unavailable (historical lookup or
    // compatibility writer), so retain the UPDATE rather than risking hiding DELETE + NULL INSERT.
    return before_image_available;
}

Status ChangelogMinDeltaBlockIterator::_append_insert_row(MutableColumns& columns) const {
    const size_t last_row = _last_group_row();
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        if (std::cmp_equal(col_idx, _op_col_idx)) {
            RETURN_IF_ERROR(
                    write_changelog_op(*columns[col_idx], changelog::StreamChangeOp::INSERT));
        } else {
            columns[col_idx]->insert_from(*_group_columns[col_idx], last_row);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_append_delete_row(MutableColumns& columns) const {
    const size_t last_row = _last_group_row();
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        if (std::cmp_equal(col_idx, _op_col_idx)) {
            RETURN_IF_ERROR(
                    write_changelog_op(*columns[col_idx], changelog::StreamChangeOp::DELETE));
        } else if (std::cmp_equal(col_idx, _lsn_col_idx) || std::cmp_equal(col_idx, _tso_col_idx)) {
            columns[col_idx]->insert_from(*_group_columns[col_idx], last_row);
        } else {
            const uint32_t before_col_idx =
                    _read_schema->before_column_ordinal(static_cast<uint32_t>(col_idx));
            columns[col_idx]->insert_from(*_group_columns[before_col_idx], 0);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_append_update_before_row(MutableColumns& columns) const {
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        if (std::cmp_equal(col_idx, _op_col_idx)) {
            RETURN_IF_ERROR(write_changelog_op(*columns[col_idx],
                                               changelog::StreamChangeOp::UPDATE_BEFORE));
        } else {
            const uint32_t before_col_idx =
                    _read_schema->before_column_ordinal(static_cast<uint32_t>(col_idx));
            columns[col_idx]->insert_from(*_group_columns[before_col_idx], 0);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_append_update_after_row(MutableColumns& columns) const {
    const size_t last_row = _last_group_row();
    for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
        if (std::cmp_equal(col_idx, _op_col_idx)) {
            RETURN_IF_ERROR(
                    write_changelog_op(*columns[col_idx], changelog::StreamChangeOp::UPDATE_AFTER));
        } else {
            columns[col_idx]->insert_from(*_group_columns[col_idx], last_row);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_emit_update_pair(MutableColumns& target_columns,
                                                         size_t* output_rows) {
    RETURN_IF_ERROR(_append_update_before_row(target_columns));
    ++(*output_rows);
    if (*output_rows >= _block_row_max) {
        return _append_update_after_row(_pending_row.park(target_columns));
    }
    RETURN_IF_ERROR(_append_update_after_row(target_columns));
    ++(*output_rows);
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_emit_group(MutableColumns& target_columns,
                                                   size_t* output_rows) {
    const auto& op_column = *_group_columns[static_cast<size_t>(_op_col_idx)];
    const ChangelogOp first_op = read_changelog_op(op_column, 0);
    const ChangelogOp last_op = read_changelog_op(op_column, _last_group_row());

    switch (changelog::AggregateFunctionMinDelta::calculate_result(first_op, last_op)) {
    case changelog::AggregateFunctionMinDelta::ResultType::SKIP:
        return Status::OK();
    case changelog::AggregateFunctionMinDelta::ResultType::INSERT:
        RETURN_IF_ERROR(_append_insert_row(target_columns));
        ++(*output_rows);
        return Status::OK();
    case changelog::AggregateFunctionMinDelta::ResultType::DELETE:
        RETURN_IF_ERROR(_append_delete_row(target_columns));
        ++(*output_rows);
        return Status::OK();
    case changelog::AggregateFunctionMinDelta::ResultType::UPDATE_BEFORE_AFTER:
        if (changelog::is_valid_changelog_op(first_op) &&
            changelog::is_valid_changelog_op(last_op) && _group_values_unchanged()) {
            return Status::OK();
        }
        return _emit_update_pair(target_columns, output_rows);
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::next_block(Block* block, bool* eof) {
    DCHECK_EQ(block->rows(), 0);
    RETURN_IF_ERROR(_cursor.ensure_started());
    if (_cursor.eof() && !_pending_row.has_row()) {
        *eof = true;
        return Status::OK();
    }
    if (_group_columns.empty() && !_cursor.eof()) {
        _group_columns = _cursor.block().clone_empty_columns();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_rows = 0;
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(_fill_block(target_columns, &output_rows));
    *eof = output_rows == 0 && _cursor.eof() && !_pending_row.has_row();
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_fill_block(MutableColumns& target_columns,
                                                   size_t* output_rows) {
    while (*output_rows < _block_row_max) {
        if (_pending_row.emit(target_columns)) {
            ++(*output_rows);
            continue;
        }
        if (_cursor.eof()) {
            break;
        }
        RETURN_IF_ERROR(_collect_group());
        RETURN_IF_ERROR(_emit_group(target_columns, output_rows));
        for (auto& column : _group_columns) {
            column->clear();
        }
    }
    return Status::OK();
}

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
