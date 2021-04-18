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

#ifndef DORIS_BE_SRC_RUNTIME_VECTORIZED_ROW_BATCH_H
#define DORIS_BE_SRC_RUNTIME_VECTORIZED_ROW_BATCH_H

#include <cstddef>
#include <memory>
#include <vector>

#include "olap/field.h"
#include "olap/row_cursor.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/row_batch.h"
#include "runtime/row_batch_interface.hpp"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "util/mem_util.hpp"

namespace doris {

class VectorizedRowBatch;
class RowBlock;

class ColumnVector {
public:
    ColumnVector() {}
    ~ColumnVector() {}

    bool* is_null() const { return _is_null; }

    void set_is_null(bool* is_null) { _is_null = is_null; }

    bool no_nulls() const { return _no_nulls; }

    void set_no_nulls(bool no_nulls) { _no_nulls = no_nulls; }

    void* col_data() { return _col_data; }
    void set_col_data(void* data) { _col_data = data; }

private:
    void* _col_data = nullptr;
    bool _no_nulls = false;
    bool* _is_null = nullptr;
};

class VectorizedRowBatch {
public:
    VectorizedRowBatch(const TabletSchema* schema, const std::vector<uint32_t>& cols, int capacity,
                       const std::shared_ptr<MemTracker>& parent_tracker = nullptr);

    ~VectorizedRowBatch() {
        for (auto vec : _col_vectors) {
            delete vec;
        }
        delete[] _selected;
    }

    MemPool* mem_pool() { return _mem_pool.get(); }

    ColumnVector* column(int column_index) { return _col_vectors[column_index]; }

    const std::vector<uint32_t>& columns() const { return _cols; }

    uint16_t capacity() { return _capacity; }

    uint16_t size() { return _size; }

    void set_size(int size) {
        DCHECK_LE(size, _capacity);
        _size = size;
    }

    inline int num_rows() { return _size; }

    bool selected_in_use() const { return _selected_in_use; }

    void set_selected_in_use(bool selected_in_use) { _selected_in_use = selected_in_use; }

    uint16_t* selected() const { return _selected; }

    inline void clear() {
        _size = 0;
        _selected_in_use = false;
        _limit = _capacity;
        _mem_pool->clear();
    }

    uint16_t limit() const { return _limit; }
    void set_limit(uint16_t limit) { _limit = limit; }
    void set_block_status(uint8_t status) { _block_status = status; }
    uint8_t block_status() const { return _block_status; }

    // Dump this vector batch to RowBlock;
    void dump_to_row_block(RowBlock* row_block);

private:
    const TabletSchema* _schema;
    const std::vector<uint32_t>& _cols;
    const uint16_t _capacity;
    uint16_t _size = 0;
    uint16_t* _selected = nullptr;
    std::vector<ColumnVector*> _col_vectors;

    bool _selected_in_use = false;
    uint8_t _block_status;

    std::shared_ptr<MemTracker> _tracker;
    std::unique_ptr<MemPool> _mem_pool;
    uint16_t _limit;
};

} // namespace doris

#endif // _DORIS_BE_SRC_RUNTIME_VECTORIZED_ROW_BATCH_H

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
