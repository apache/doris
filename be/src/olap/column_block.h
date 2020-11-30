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

#include <cstdint>

#include "olap/column_vector.h"
#include "olap/types.h"
#include "util/bitmap.h"

namespace doris {

class MemPool;
class TypeInfo;
struct ColumnBlockCell;

// Block of data belong to a single column.
// It doesn't own any data, user should keep the life of input data.
// TODO llj Remove this class
class ColumnBlock {
public:
    ColumnBlock(ColumnVectorBatch* batch, MemPool* pool) : _batch(batch), _pool(pool) {}

    const TypeInfo* type_info() const { return _batch->type_info(); }
    uint8_t* data() const { return _batch->data(); }
    bool is_nullable() const { return _batch->is_nullable(); }
    MemPool* pool() const { return _pool; }
    const uint8_t* cell_ptr(size_t idx) const { return _batch->cell_ptr(idx); }
    uint8_t* mutable_cell_ptr(size_t idx) const { return _batch->mutable_cell_ptr(idx); }
    bool is_null(size_t idx) const { return _batch->is_null_at(idx); }
    void set_is_null(size_t idx, bool is_null) const { _batch->set_is_null(idx, is_null); }

    void set_null_bits(size_t offset, size_t num_rows, bool val) const {
        _batch->set_null_bits(offset, num_rows, val);
    }

    ColumnVectorBatch* vector_batch() const { return _batch; }

    ColumnBlockCell cell(size_t idx) const;

    void set_delete_state(DelCondSatisfied delete_state) { _batch->set_delete_state(delete_state); }

    DelCondSatisfied delete_state() const { return _batch->delete_state(); }

private:
    ColumnVectorBatch* _batch;
    MemPool* _pool;
};

struct ColumnBlockCell {
    ColumnBlockCell(ColumnBlock block, size_t idx) : _block(block), _idx(idx) {}

    bool is_null() const { return _block.is_null(_idx); }
    void set_is_null(bool is_null) const { return _block.set_is_null(_idx, is_null); }
    uint8_t* mutable_cell_ptr() const { return _block.mutable_cell_ptr(_idx); }
    const uint8_t* cell_ptr() const { return _block.cell_ptr(_idx); }

private:
    ColumnBlock _block;
    size_t _idx;
};

inline ColumnBlockCell ColumnBlock::cell(size_t idx) const {
    return ColumnBlockCell(*this, idx);
}

// Wrap ColumnBlock and offset, easy to access data at the specified offset
// Used to read data from page decoder
class ColumnBlockView {
public:
    explicit ColumnBlockView(ColumnBlock* block, size_t row_offset = 0)
            : _block(block), _row_offset(row_offset) {}
    void advance(size_t skip) { _row_offset += skip; }
    ColumnBlock* column_block() { return _block; }
    const TypeInfo* type_info() const { return _block->type_info(); }
    MemPool* pool() const { return _block->pool(); }
    void set_null_bits(size_t num_rows, bool val) {
        _block->set_null_bits(_row_offset, num_rows, val);
    }
    bool is_nullable() const { return _block->is_nullable(); }
    uint8_t* data() const { return _block->mutable_cell_ptr(_row_offset); }
    size_t current_offset() { return _row_offset; }

private:
    ColumnBlock* _block;
    size_t _row_offset;
};

} // namespace doris
