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
    RowBlockV2(const Schema& schema, uint16_t capacity);
    ~RowBlockV2();

    // update number of rows contained in this block
    void set_num_rows(size_t num_rows) { _num_rows = num_rows; }
    // return number of rows contained in this block
    size_t num_rows() const { return _num_rows; }
    // return the maximum number of rows that can be contained in this block.
    // invariant: 0 <= num_rows() <= capacity()
    size_t capacity() const { return _capacity; }
    Arena* arena() const { return _arena.get(); }

    // reset the state of the block so that it can be reused for write.
    // all previously returned ColumnBlocks are invalidated after clear(), accessing them
    // will result in undefined behavior.
    void clear() {
        _num_rows = 0;
        _arena.reset(new Arena);
    }

    // Copy the row_idx row's data into given row_cursor.
    // This function will use shallow copy, so the client should
    // notice the life time of returned value
    Status copy_to_row_cursor(size_t row_idx, RowCursor* row_cursor);

    // Get the column block for one of the columns in this row block.
    // `cid` must be one of `schema()->column_ids()`.
    ColumnBlock column_block(ColumnId cid) const {
        const TypeInfo* type_info = _schema.column(cid)->type_info();
        uint8_t* data = _column_datas[cid];
        uint8_t* null_bitmap = _column_null_bitmaps[cid];
        return ColumnBlock(type_info, data, null_bitmap, _arena.get());
    }

    RowBlockRow row(size_t row_idx) const;

    const Schema* schema() const { return &_schema; }

private:
    Schema _schema;
    size_t _capacity;
    // keeps fixed-size (field_size x capacity) data vector for each column,
    // _column_datas[cid] == null if cid is not in `_schema`.
    // memory are not allocated from `_arena` because we don't wan't to reallocate them in clear()
    std::vector<uint8_t*> _column_datas;
    // keeps null bitmap for each column,
    // _column_null_bitmaps[cid] == null if cid is not in `_schema` or the column is not null.
    // memory are not allocated from `_arena` because we don't wan't to reallocate them in clear()
    std::vector<uint8_t*> _column_null_bitmaps;
    size_t _num_rows;
    // manages the memory for slice's data
    std::unique_ptr<Arena> _arena;
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
    const Schema* schema() const { return _block->schema(); }

    std::string debug_string() const;

    ColumnBlockCell cell(uint32_t cid) const {
        return column_block(cid).cell(_row_index);
    }
private:
    const RowBlockV2* _block;
    size_t _row_index;
};

inline RowBlockRow RowBlockV2::row(size_t row_idx) const {
    return RowBlockRow(this, row_idx);
}

}
