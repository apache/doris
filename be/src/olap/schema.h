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
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/field.h"
#include "olap/row_cursor_cell.h"
#include "runtime/descriptors.h"

namespace doris {

class RowBlockRow;

// The class is used to represent row's format in memory.
// One row contains some columns, within these columns there may be key columns which
// must be the first few columns.
//
// To compare two rows whose schemas are different, but they are from the same origin
// we store all column schema maybe accessed here. And default access through column
// id
class Schema {
public:
    Schema(const TabletSchema& schema) {
        std::vector<Field> cols;
        size_t num_key_columns = 0;
        for (int i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            cols.emplace_back(column.aggregation(), column.type(), column.index_length(), column.is_nullable());
            if (column.is_key()) {
                num_key_columns++;
            }
        }

        reset(cols, num_key_columns);
    }

    Schema(const std::vector<TabletColumn>& columns, const std::vector<ColumnId>& col_ids) {
        std::vector<Field> cols;
        size_t num_key_columns = 0;
        for (int i = 0; i < columns.size(); ++i) {
            const TabletColumn& column = columns[i];
            cols.emplace_back(column.aggregation(), column.type(), column.index_length(), column.is_nullable());
            if (column.is_key()) {
                num_key_columns++;
            }
        }

        reset(cols, col_ids, num_key_columns);
    }

    Schema(const std::vector<Field>& cols, size_t num_key_columns) {
        reset(cols, num_key_columns);
    }

    Schema(const Schema&);
    Schema& operator=(const Schema& other);

    void copy_from(const Schema& other);

    ~Schema();

    void reset(const std::vector<Field>& cols, size_t num_key_columns);

    void reset(const std::vector<Field>& cols,
               const std::vector<ColumnId>& col_ids,
               size_t num_key_columns);

    const std::vector<Field*>& columns() const { return _cols; }
    const Field* column(int idx) const { return _cols[idx]; }

    size_t num_key_columns() const { return _num_key_columns; }

    size_t column_offset(ColumnId cid) const {
        return _col_offsets[cid];
    }

    size_t column_size(ColumnId cid) const {
        return _cols[cid]->size();
    }

    size_t index_size(ColumnId cid) const {
        return _cols[cid]->index_size();
    }

    bool is_null(const char* row, int index) const {
        return *reinterpret_cast<const bool*>(row + _col_offsets[index]);
    }

    void set_is_null(void* row, uint32_t cid, bool is_null) const {
        *reinterpret_cast<bool*>((char*)row + _col_offsets[cid]) = is_null;
    }

    size_t schema_size() const {
        size_t size = _col_ids.size();
        for (auto cid : _col_ids) {
            size += _cols[cid]->size();
        }
        return size;
    }

    size_t num_columns() const { return _cols.size(); }
    size_t num_column_ids() const { return _col_ids.size(); }
    const std::vector<ColumnId>& column_ids() const { return _col_ids; }
private:
    std::vector<Field*> _cols;
    std::vector<ColumnId> _col_ids;
    std::vector<size_t> _col_offsets;
    size_t _num_key_columns;
};

} // namespace doris
