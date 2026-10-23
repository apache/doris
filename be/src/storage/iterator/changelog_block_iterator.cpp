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

#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/changelog.h"
#include "storage/iterator/changelog_block_reader_utils.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"

namespace doris {
using namespace ErrorCode;

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

// The op column of every changelog read schema; ReadSchema checks its presence.
size_t changelog_op_ordinal(const ReadSchema& read_schema) {
    const int32_t op_ordinal = read_schema.op_ordinal();
    DORIS_CHECK_GE(op_ordinal, 0);
    return static_cast<size_t>(op_ordinal);
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

ChangelogMinDeltaBlockIterator::ChangelogMinDeltaBlockIterator(RowwiseIterator* rows,
                                                               OutputBlockLimits limits,
                                                               ReadSchemaSPtr read_schema)
        : _cursor(rows),
          _limits(limits),
          _read_schema(std::move(read_schema)),
          _op_ordinal(changelog_op_ordinal(*_read_schema)) {}

void ChangelogMinDeltaBlockIterator::_append_row_to_group() {
    const IteratorRowRef& row = _cursor.row();
    const bool replace_most_recent = _group_columns[0]->size() > 1;
    for (size_t i = 0; i < _group_columns.size(); ++i) {
        if (replace_most_recent) {
            _group_columns[i]->pop_back(1);
        }
        _group_columns[i]->insert_from(*row.block->get_by_position(i).column, row.row_pos);
    }
}

bool ChangelogMinDeltaBlockIterator::_row_belongs_to_group() const {
    // is_same cannot be used here: it marks cross-segment key matches for dedup, so
    // consecutive same-key rows that a compaction/quick-merge folded into one segment are left
    // unmarked, which would split one key's change chain into several groups. Compare the
    // leading key columns directly against the group's first row instead.
    const IteratorRowRef& row = _cursor.row();
    const size_t num_key_columns = _read_schema->num_key_columns();
    for (size_t k = 0; k < num_key_columns; ++k) {
        if (_group_columns[k]->compare_at(0, row.row_pos, *row.block->get_by_position(k).column,
                                          -1) != 0) {
            return false;
        }
    }
    return true;
}

bool ChangelogMinDeltaBlockIterator::_group_values_unchanged() {
    const auto& value_column_pairs = _read_schema->changelog_value_column_pairs();
    if (!_read_schema->changelog_value_pairs_complete() || value_column_pairs.empty() ||
        _value_compare_unsupported) {
        return false;
    }
    const size_t last_row = _last_group_row();
    bool before_image_available = false;
    for (const auto& [after_idx, before_idx] : value_column_pairs) {
        const auto* before_column = _group_columns[before_idx].get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*before_column)) {
            before_image_available |= !nullable->is_null_at(0);
        } else {
            before_image_available = true;
        }
        try {
            if (before_column->compare_at(0, last_row, *_group_columns[after_idx], -1) != 0) {
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
    // Historical lookup and the compatibility writer both represent an unavailable BEFORE image
    // as an all-NULL row. Without a persisted validity bit, retaining the UPDATE is the only safe
    // choice; otherwise a missing-key DELETE followed by an all-NULL INSERT would disappear.
    return before_image_available;
}

Status ChangelogMinDeltaBlockIterator::_append_insert_row(MutableColumns& columns) const {
    const size_t last_row = _last_group_row();
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        if (ordinal == _op_ordinal) {
            RETURN_IF_ERROR(
                    write_changelog_op(*columns[ordinal], changelog::StreamChangeOp::INSERT));
        } else {
            columns[ordinal]->insert_from(*_group_columns[ordinal], last_row);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_append_delete_row(MutableColumns& columns) const {
    const size_t last_row = _last_group_row();
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        if (ordinal == _op_ordinal) {
            RETURN_IF_ERROR(
                    write_changelog_op(*columns[ordinal], changelog::StreamChangeOp::DELETE));
        } else if (std::cmp_equal(ordinal, _read_schema->lsn_ordinal()) ||
                   std::cmp_equal(ordinal, _read_schema->tso_ordinal())) {
            columns[ordinal]->insert_from(*_group_columns[ordinal], last_row);
        } else {
            const auto before_ordinal =
                    _read_schema->before_column_ordinal(static_cast<ColumnId>(ordinal));
            columns[ordinal]->insert_from(*_group_columns[before_ordinal], 0);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_append_update_before_row(MutableColumns& columns) const {
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        if (ordinal == _op_ordinal) {
            RETURN_IF_ERROR(write_changelog_op(*columns[ordinal],
                                               changelog::StreamChangeOp::UPDATE_BEFORE));
        } else {
            const auto before_ordinal =
                    _read_schema->before_column_ordinal(static_cast<ColumnId>(ordinal));
            columns[ordinal]->insert_from(*_group_columns[before_ordinal], 0);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_append_update_after_row(MutableColumns& columns) const {
    const size_t last_row = _last_group_row();
    for (size_t ordinal = 0; ordinal < columns.size(); ++ordinal) {
        if (ordinal == _op_ordinal) {
            RETURN_IF_ERROR(
                    write_changelog_op(*columns[ordinal], changelog::StreamChangeOp::UPDATE_AFTER));
        } else {
            columns[ordinal]->insert_from(*_group_columns[ordinal], last_row);
        }
    }
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_emit_update_pair(MutableColumns& target_columns,
                                                         size_t* output_rows) {
    RETURN_IF_ERROR(_append_update_before_row(target_columns));
    (*output_rows)++;
    // The AFTER row is parked when the BEFORE row filled the Block.
    if (*output_rows >= _limits.max_rows) {
        return _append_update_after_row(_pending_row.park(target_columns));
    }
    RETURN_IF_ERROR(_append_update_after_row(target_columns));
    (*output_rows)++;
    return Status::OK();
}

Status ChangelogMinDeltaBlockIterator::_emit_group(MutableColumns& target_columns,
                                                   size_t* output_rows) {
    const auto& op_column = *_group_columns[_op_ordinal];
    const ChangelogOp first_op = read_changelog_op(op_column, 0);
    const ChangelogOp last_op = read_changelog_op(op_column, _last_group_row());

    switch (changelog::AggregateFunctionMinDelta::calculate_result(first_op, last_op)) {
    case changelog::AggregateFunctionMinDelta::ResultType::SKIP:
        return Status::OK();
    case changelog::AggregateFunctionMinDelta::ResultType::INSERT:
        RETURN_IF_ERROR(_append_insert_row(target_columns));
        (*output_rows)++;
        return Status::OK();
    case changelog::AggregateFunctionMinDelta::ResultType::DELETE:
        RETURN_IF_ERROR(_append_delete_row(target_columns));
        (*output_rows)++;
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
    if (UNLIKELY(_cursor.eof() && !_pending_row.has_row())) {
        *eof = true;
        return Status::OK();
    }
    if (_group_columns.empty()) {
        DCHECK(!_cursor.eof());
        _group_columns = _cursor.row().block->clone_empty_columns();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_rows = 0;
    while (output_rows < _limits.max_rows) {
        if (_pending_row.emit(target_columns)) {
            output_rows++;
            continue;
        }
        if (_cursor.eof()) {
            break;
        }
        _append_row_to_group();
        RETURN_IF_ERROR(_cursor.advance());

        // Extend the group while the next row has the same user key. A group is a run of
        // consecutive rows sharing the key: the read is globally key-ordered and the key
        // columns lead every merged Block, so the group ends exactly where the key changes.
        if (!_cursor.eof() && (_cursor.row().is_same || _row_belongs_to_group())) {
            continue;
        }

        RETURN_IF_ERROR(_emit_group(target_columns, &output_rows));
        for (auto& column : _group_columns) {
            column->clear();
        }
    }
    // Rows went out, or everything has been emitted: either way the next call settles it.
    *eof = output_rows == 0 && _cursor.eof() && !_pending_row.has_row();
    return Status::OK();
}

ChangelogDetailBlockIterator::ChangelogDetailBlockIterator(RowwiseIterator* rows,
                                                           OutputBlockLimits limits,
                                                           ReadSchemaSPtr read_schema)
        : _cursor(rows),
          _limits(limits),
          _read_schema(std::move(read_schema)),
          _op_ordinal(changelog_op_ordinal(*_read_schema)) {}

Status ChangelogDetailBlockIterator::_append_change_row(MutableColumns& target_columns,
                                                        changelog::StreamChangeOp output_op,
                                                        bool use_before) const {
    const IteratorRowRef& row = _cursor.row();
    for (size_t ordinal = 0; ordinal < target_columns.size(); ++ordinal) {
        if (ordinal == _op_ordinal) {
            RETURN_IF_ERROR(write_changelog_op(*target_columns[ordinal], output_op));
            continue;
        }
        // Changelog meta columns map to themselves.
        const auto source_ordinal =
                use_before ? _read_schema->before_column_ordinal(static_cast<ColumnId>(ordinal))
                           : static_cast<ColumnId>(ordinal);
        target_columns[ordinal]->insert_from(*row.block->get_by_position(source_ordinal).column,
                                             row.row_pos);
    }
    return Status::OK();
}

Status ChangelogDetailBlockIterator::_emit_change(ChangelogOp op, MutableColumns& target_columns,
                                                  size_t* output_rows) {
    if (op == ChangelogOp::UPDATE) {
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::UPDATE_BEFORE, true));
        (*output_rows)++;
        // The AFTER row is parked when the BEFORE row filled the Block.
        if (*output_rows >= _limits.max_rows) {
            return _append_change_row(_pending_row.park(target_columns),
                                      changelog::StreamChangeOp::UPDATE_AFTER, false);
        }
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::UPDATE_AFTER, false));
        (*output_rows)++;
    } else if (op == ChangelogOp::APPEND) {
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::INSERT, false));
        (*output_rows)++;
    } else if (op == ChangelogOp::DELETE) {
        RETURN_IF_ERROR(
                _append_change_row(target_columns, changelog::StreamChangeOp::DELETE, true));
        (*output_rows)++;
    }
    return Status::OK();
}

Status ChangelogDetailBlockIterator::next_block(Block* block, bool* eof) {
    if (UNLIKELY(_cursor.eof() && !_pending_row.has_row())) {
        *eof = true;
        return Status::OK();
    }

    auto target_columns_guard = block->mutate_columns_scoped();
    auto& target_columns = target_columns_guard.mutable_columns();
    size_t output_rows = 0;
    while (output_rows < _limits.max_rows) {
        if (_pending_row.emit(target_columns)) {
            output_rows++;
            continue;
        }
        if (_cursor.eof()) {
            break;
        }

        const IteratorRowRef& row = _cursor.row();
        const ChangelogOp op =
                read_changelog_op(*row.block->get_by_position(_op_ordinal).column, row.row_pos);
        RETURN_IF_ERROR(_emit_change(op, target_columns, &output_rows));
        RETURN_IF_ERROR(_cursor.advance());
    }
    // Rows went out, or everything has been emitted: either way the next call settles it.
    *eof = output_rows == 0 && _cursor.eof() && !_pending_row.has_row();
    return Status::OK();
}

} // namespace doris
