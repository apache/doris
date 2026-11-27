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

#include "storage/iterator/changelog_iterator_utils.h"

#include <glog/logging.h>

#include <utility>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"

namespace doris::changelog {

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

Status write_changelog_op(IColumn* col, StreamChangeOp op) {
    IColumn* cur = col;
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

bool ChangelogPendingRow::emit(MutableColumns* target_columns) {
    if (!_has_row) {
        return false;
    }
    for (size_t i = 0; i < _columns.size(); ++i) {
        (*target_columns)[i]->insert_from(*_columns[i], 0);
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

} // namespace doris::changelog
