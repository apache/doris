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
#include "olap/row_block.h"
#include "olap/schema.h"
#include "olap/selection_vector.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

class MemPool;
class RowBlockRow;
class RowCursor;

// This struct contains a block of rows, in which each column's data is stored
// in a vector. We don't use VectorizedRowBatch because it doesn't own the data
// in block, however it is used by old code, which we don't want to change.
class RowBlockV2 {
public:
    RowBlockV2(const Schema& schema, uint16_t capacity);

    RowBlockV2(const Schema& schema, uint16_t capacity, std::shared_ptr<MemTracker> parent);
    ~RowBlockV2();

    // update number of rows contained in this block
    void set_num_rows(size_t num_rows) { _num_rows = num_rows; }
    // low-level API to get the number of rows contained in this block.
    // note that some rows may have been un-selected by selection_vector(),
    // client should use selected_size() to get the actual number of selected rows.
    size_t num_rows() const { return _num_rows; }
    // return the maximum number of rows that can be contained in this block.
    // invariant: 0 <= num_rows() <= capacity()
    size_t capacity() const { return _capacity; }
    MemPool* pool() const { return _pool.get(); }

    // reset the state of the block so that it can be reused for write.
    // all previously returned ColumnBlocks are invalidated after clear(), accessing them
    // will result in undefined behavior.
    void clear() {
        _num_rows = 0;
        _pool->clear();
        _selected_size = _capacity;
        for (int i = 0; i < _selected_size; ++i) {
            _selection_vector[i] = i;
        }
        _delete_state = DEL_NOT_SATISFIED;
    }

    // convert RowBlockV2 to RowBlock
    Status convert_to_row_block(RowCursor* helper, RowBlock* dst);

    // convert RowBlockV2 to vectorized::Block
    Status convert_to_vec_block(vectorized::Block* block);

    // low-level API to access memory for each column block(including data array and nullmap).
    // `cid` must be one of `schema()->column_ids()`.
    ColumnBlock column_block(ColumnId cid) const {
        ColumnVectorBatch* batch = _column_vector_batches[cid].get();
        return {batch, _pool.get()};
    }

    // low-level API to access the underlying memory for row at `row_idx`.
    // client should use selection_vector() to iterate over row index of selected rows.
    // TODO(gdy) DO NOT expose raw rows which may be un-selected to clients
    RowBlockRow row(size_t row_idx) const;

    const Schema* schema() const { return &_schema; }

    uint16_t* selection_vector() const { return _selection_vector; }

    uint16_t selected_size() const { return _selected_size; }

    void set_selected_size(uint16_t selected_size) { _selected_size = selected_size; }

    DelCondSatisfied delete_state() const { return _delete_state; }

    void set_delete_state(DelCondSatisfied delete_state) {
        // if the set _delete_state is DEL_PARTIAL_SATISFIED,
        // we can not change _delete_state to DEL_NOT_SATISFIED;
        if (_delete_state == DEL_PARTIAL_SATISFIED && delete_state != DEL_SATISFIED) {
            return;
        }
        _delete_state = delete_state;
    }
    std::string debug_string();

private:
    Status _copy_data_to_column(int cid, vectorized::MutableColumnPtr& mutable_column_ptr);

    const Schema& _schema;
    size_t _capacity;
    // _column_vector_batches[cid] == null if cid is not in `_schema`.
    // memory are not allocated from `_pool` because we don't wan't to reallocate them in clear()
    std::vector<std::unique_ptr<ColumnVectorBatch>> _column_vector_batches;

    size_t _num_rows;
    // manages the memory for slice's data
    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _pool;

    // index of selected rows for rows passed the predicate
    uint16_t* _selection_vector;
    // selected rows number
    uint16_t _selected_size;

    // block delete state
    DelCondSatisfied _delete_state;
};

// Stands for a row in RowBlockV2. It is consisted of a RowBlockV2 reference
// and row index.
class RowBlockRow {
public:
    RowBlockRow(const RowBlockV2* block, size_t row_index) : _block(block), _row_index(row_index) {}

    const RowBlockV2* row_block() const { return _block; }
    size_t row_index() const { return _row_index; }

    ColumnBlock column_block(size_t col_idx) const { return _block->column_block(col_idx); }
    bool is_null(size_t col_idx) const { return column_block(col_idx).is_null(_row_index); }
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

    ColumnBlockCell cell(uint32_t cid) const { return column_block(cid).cell(_row_index); }

private:
    const RowBlockV2* _block;
    size_t _row_index;
};

inline RowBlockRow RowBlockV2::row(size_t row_idx) const {
    return RowBlockRow(this, row_idx);
}

} // namespace doris
