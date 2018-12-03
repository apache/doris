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

#include "runtime/vectorized_row_batch.h"

#include "common/logging.h"
#include "olap/row_block.h"

namespace doris {

VectorizedRowBatch::VectorizedRowBatch(
        const std::vector<FieldInfo>& schema,
        const std::vector<uint32_t>& cols,
        int capacity)
            : _schema(schema), _cols(cols), _capacity(capacity), _limit(capacity) {
    _selected_in_use = false;
    _size = 0;

    _tracker.reset(new MemTracker(-1));
    _mem_pool.reset(new MemPool(_tracker.get()));

    _selected = reinterpret_cast<uint16_t*>(new char[sizeof(uint16_t) * _capacity]);
    for (ColumnId column_id : cols) {
        ColumnVector* col_vec = new ColumnVector();
        _col_map[column_id] = col_vec;
    }
}

void VectorizedRowBatch::dump_to_row_block(RowBlock* row_block) {
    if (_selected_in_use) {
        for (auto column_id : _cols) {
            bool no_nulls = _col_map[column_id]->no_nulls();
            // pointer of this field's vector
            char* vec_field_ptr = (char*)_col_map[column_id]->col_data();
            // pointer of this field in row block
            char* row_field_ptr =
                row_block->_mem_buf + row_block->_field_offset_in_memory[column_id];
            const FieldInfo& field_info = _schema[column_id];
            size_t field_size = 0;
            if (field_info.type == OLAP_FIELD_TYPE_CHAR ||
                field_info.type == OLAP_FIELD_TYPE_VARCHAR ||
                field_info.type == OLAP_FIELD_TYPE_HLL) {
                field_size = sizeof(Slice);
            } else {
                field_size = field_info.length;
            }
            if (no_nulls) {
                for (int row = 0; row < _size; ++row) {
                    char* vec_field =
                        vec_field_ptr + _selected[row] * field_size;
                    // Set not null
                    *row_field_ptr = 0;
                    memory_copy(row_field_ptr + 1, vec_field, field_size);

                    // point to next row
                    row_field_ptr += row_block->_mem_row_bytes;
                }
            } else {
                bool* is_null = _col_map[column_id]->is_null();
                for (int row = 0; row < _size; ++row) {
                    if (is_null[_selected[row]]) {
                        *row_field_ptr = 1;
                    } else {
                        char* vec_field =
                            vec_field_ptr + _selected[row] * field_size;
                        // Set not null
                        *row_field_ptr = 0;
                        memory_copy(row_field_ptr + 1, vec_field, field_size);
                    }
                    row_field_ptr += row_block->_mem_row_bytes;
                }
            }
        }
    } else {
        for (auto column_id : _cols) {
            bool no_nulls = _col_map[column_id]->no_nulls();

            char* vec_field_ptr = (char*)_col_map[column_id]->col_data();
            char* row_field_ptr =
                row_block->_mem_buf + row_block->_field_offset_in_memory[column_id];
            const FieldInfo& field_info = _schema[column_id];

            size_t field_size = 0;
            if (field_info.type == OLAP_FIELD_TYPE_CHAR ||
                field_info.type == OLAP_FIELD_TYPE_VARCHAR ||
                field_info.type == OLAP_FIELD_TYPE_HLL) {
                field_size = sizeof(Slice);
            } else {
                field_size = field_info.length;
            }

            if (no_nulls) {
                for (int row = 0; row < _size; ++row) {
                    char* vec_field = vec_field_ptr;
                    // Set not null
                    *row_field_ptr = 0;
                    memory_copy(row_field_ptr + 1, vec_field, field_size);
                    row_field_ptr += row_block->_mem_row_bytes;
                    vec_field_ptr += field_size;
                }
            } else {
                bool* is_null = _col_map[column_id]->is_null();
                for (int row = 0; row < _size; ++row) {
                    if (is_null[row]) {
                        *row_field_ptr = 1;
                    } else {
                        char* vec_field = vec_field_ptr;
                        // Set not null
                        *row_field_ptr = 0;
                        memory_copy(row_field_ptr + 1, vec_field, field_size);
                    }
                    row_field_ptr += row_block->_mem_row_bytes;
                    vec_field_ptr += field_size;
                }
            }
        }
    }

    row_block->_pos = 0;
    row_block->_limit = _size;
    row_block->_info.row_num = _size;
    row_block->_block_status = _block_status;
    row_block->mem_pool()->free_all();
    row_block->mem_pool()->acquire_data(_mem_pool.get(), false);
}

} // namespace doris
