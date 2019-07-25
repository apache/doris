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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "olap/column_block.h"
#include "olap/schema.h"
#include "olap/types.h"

namespace doris {

class Arena;
class RowCursor;

// This struct contains a block of rows, in which each column's data is stored
// in a vector. We don't use VectorizedRowBatch because it doesn't own the data
// in block, however it is used by old code, which we don't want to change.
class RowBlockV2 {
public:
    RowBlockV2(const Schema& schema, uint16_t capacity, Arena* arena);
    ~RowBlockV2();

    void resize(size_t num_rows) { _num_rows = num_rows; }
    size_t num_rows() const { return _num_rows; }
    size_t capacity() const { return _capacity; }
    Arena* arena() const { return _arena; }

    // Copy the row_idx row's data into given row_cursor.
    // This function will use shallow copy, so the client should
    // notice the life time of returned value
    Status copy_to_row_cursor(size_t row_idx, RowCursor* row_cursor);

    // Get column block for input column index. This input is the index in
    // this row block, is not the index in table's schema
    ColumnBlock column_block(size_t col_idx) const {
        const TypeInfo* type_info = _schema.column(col_idx).type_info();
        uint8_t* data = _column_datas[col_idx];
        uint8_t* null_bitmap = _column_null_bitmaps[col_idx];
        return ColumnBlock(type_info, data, null_bitmap, _arena);
    }

    RowBlockRow row(size_t row_idx) const;

    const Schema& schema() const { return _schema; }

private:
    Schema _schema;
    std::vector<uint8_t*> _column_datas;
    std::vector<uint8_t*> _column_null_bitmaps;
    size_t _capacity;
    size_t _num_rows;
    Arena* _arena;
};

// Stands for a row in RowBlockV2. It is consisted of a RowBlockV2 reference
// and row index.
class RowBlockRow {
public:
    RowBlockRow(const RowBlockV2* block, size_t row_index) : _block(block), _row_index(row_index) { }

    const RowBlockV2* row_block() const { return _block; }
    size_t row_index() const { return _row_index; }

    ColumnBlock column_block(size_t col_idx) const {
        return _block->column_block(col_idx);
    }
    bool is_null(size_t col_idx) const {
        return column_block(col_idx).is_null(_row_index);
    }
    void set_is_null(size_t col_idx, bool is_null) {
        return column_block(col_idx).set_is_null(_row_index, is_null);
    }
    uint8_t* mutable_cell_ptr(size_t col_idx) const {
        return column_block(col_idx).mutable_cell_ptr(_row_index);
    }
    const uint8_t* cell_ptr(size_t col_idx) const {
        return column_block(col_idx).cell_ptr(_row_index);
    }
    const Schema& schema() const { return _block->schema(); }

    std::string debug_string() const;
private:
    const RowBlockV2* _block;
    size_t _row_index;
};

// Deep copy src row to dst row. Schema of src and dst row must be same.
void copy_row(RowBlockRow* dst, const RowBlockRow& src, Arena* arena);

inline RowBlockRow RowBlockV2::row(size_t row_idx) const {
    return RowBlockRow(this, row_idx);
}

}
