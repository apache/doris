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

#pragma once

#include <vector>

#include "olap/aggregate_func.h"
#include "olap/field.h"
#include "olap/row_cursor_cell.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "runtime/descriptors.h"

namespace doris {

// The class is used to represent row's format in memory.  Each row contains
// multiple columns, some of which are key-columns (the rest are value-columns).
// NOTE: If both key-columns and value-columns exist, then the key-columns
// must be placed before value-columns.
//
// To compare two rows whose schemas are different, but they are from the same origin
// we store all column schema maybe accessed here. And default access through column id
class Schema {
public:
    Schema(const TabletSchema& tablet_schema) {
        size_t num_columns = tablet_schema.num_columns();
        std::vector<ColumnId> col_ids(num_columns);
        std::vector<TabletColumn> columns;
        columns.reserve(num_columns);

        size_t num_key_columns = 0;
        for (uint32_t cid = 0; cid < num_columns; ++cid) {
            col_ids[cid] = cid;
            const TabletColumn& column = tablet_schema.column(cid);
            if (column.is_key()) {
                ++num_key_columns;
            }
            columns.push_back(column);
        }
        _delete_sign_idx = tablet_schema.delete_sign_idx();
        if (tablet_schema.has_sequence_col()) {
            _has_sequence_col = true;
        }
        _init(columns, col_ids, num_key_columns);
    }

    // All the columns of one table may exist in the columns param, but col_ids is only a subset.
    Schema(const std::vector<TabletColumn>& columns, const std::vector<ColumnId>& col_ids) {
        size_t num_key_columns = 0;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (columns[i].is_key()) {
                ++num_key_columns;
            }
            if (columns[i].name() == DELETE_SIGN) {
                _delete_sign_idx = i;
            }
        }
        _init(columns, col_ids, num_key_columns);
    }

    // Only for UT
    Schema(const std::vector<TabletColumn>& columns, size_t num_key_columns) {
        std::vector<ColumnId> col_ids(columns.size());
        for (uint32_t cid = 0; cid < columns.size(); ++cid) {
            col_ids[cid] = cid;
        }

        _init(columns, col_ids, num_key_columns);
    }

    Schema(const std::vector<const Field*>& cols, size_t num_key_columns) {
        std::vector<ColumnId> col_ids(cols.size());
        for (uint32_t cid = 0; cid < cols.size(); ++cid) {
            col_ids[cid] = cid;
            if (cols.at(cid)->name() == DELETE_SIGN) {
                _delete_sign_idx = cid;
            }
        }

        _init(cols, col_ids, num_key_columns);
    }

    Schema(const Schema&);
    Schema& operator=(const Schema& other);

    ~Schema();

    static vectorized::DataTypePtr get_data_type_ptr(const Field& field);

    static vectorized::IColumn::MutablePtr get_predicate_column_ptr(FieldType type);

    static vectorized::IColumn::MutablePtr get_predicate_column_nullable_ptr(FieldType type, bool is_null = false);

    const std::vector<Field*>& columns() const { return _cols; }

    const Field* column(ColumnId cid) const { return _cols[cid]; }

    Field* mutable_column(ColumnId cid) const { return _cols[cid]; }

    size_t num_key_columns() const { return _num_key_columns; }
    size_t schema_size() const { return _schema_size; }

    size_t column_offset(ColumnId cid) const { return _col_offsets[cid]; }

    // TODO(lingbin): What is the difference between column_size() and index_size()
    size_t column_size(ColumnId cid) const { return _cols[cid]->size(); }

    size_t index_size(ColumnId cid) const { return _cols[cid]->index_size(); }

    bool is_null(const char* row, int index) const {
        return *reinterpret_cast<const bool*>(row + _col_offsets[index]);
    }
    void set_is_null(void* row, uint32_t cid, bool is_null) const {
        *reinterpret_cast<bool*>((char*)row + _col_offsets[cid]) = is_null;
    }

    size_t num_columns() const { return _cols.size(); }
    size_t num_column_ids() const { return _col_ids.size(); }
    const std::vector<ColumnId>& column_ids() const { return _col_ids; }
    ColumnId column_id(size_t index) const { return _col_ids[index]; }
    int32_t delete_sign_idx() const { return _delete_sign_idx; }
    bool has_sequence_col() const { return _has_sequence_col; }

private:
    void _init(const std::vector<TabletColumn>& cols, const std::vector<ColumnId>& col_ids,
               size_t num_key_columns);
    void _init(const std::vector<const Field*>& cols, const std::vector<ColumnId>& col_ids,
               size_t num_key_columns);

    void _copy_from(const Schema& other);

    // NOTE: The ColumnId here represents the sequential index number (starting from 0) of
    // a column in current row, not the unique id-identifier of each column
    std::vector<ColumnId> _col_ids;
    // NOTE: Both _cols[cid] and _col_offsets[cid] can only be accessed when the cid is
    // contained in _col_ids
    std::vector<Field*> _cols;
    // The value of each item indicates the starting offset of the corresponding column in
    // current row. e.g. _col_offsets[idx] is the offset of _cols[idx] (idx must in _col_ids)
    std::vector<size_t> _col_offsets;

    size_t _num_key_columns;
    size_t _schema_size;
    int32_t _delete_sign_idx = -1;
    bool _has_sequence_col = false;
};

} // namespace doris
