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
#include "olap/schema2.h"
#include "olap/types.h"

namespace doris {

class Arena;
class RowCursor;

// This struct contains a block of rows, in which each column's data is stored
// in a vector. We don't use VectorizedRowBatch because it doesn't own the data
// in block, however it is used by old code, which we don't want to change.
class RowBlockV2 {
public:
    // TODO(zc): think make wrap column_infos and column_ids into a class.
    RowBlockV2(const std::vector<ColumnSchemaV2>& column_infos,
               const std::vector<uint32_t>& column_ids,
               uint16_t capacity, Arena* arena);
    ~RowBlockV2();

    void resize(size_t num_rows) { _num_rows = num_rows; }
    size_t num_rows() const { return _num_rows; }
    size_t capacity() const { return _capacity; }
    Arena* arena() const { return _arena; }

    const std::vector<ColumnSchemaV2>& column_schemas() { return _column_schemas; }
    const std::vector<uint32_t>& column_ids() { return _column_ids; }

    // Copy the row_idx row's data into given row_cursor.
    // This function will use shallow copy, so the client should
    // notice the life time of returned value
    Status copy_to_row_cursor(size_t row_idx, RowCursor* row_cursor);

    // Get column block for input column index. This input is the index in
    // this row block, is not the index in table's schema
    ColumnBlock column_block(size_t col_idx) {
        const TypeInfo* type_info = _column_schemas[col_idx].type_info();
        uint8_t* data = _column_data[col_idx];
        uint8_t* null_bitmap = _column_null_bitmaps[col_idx];
        return ColumnBlock(type_info, data, null_bitmap, _arena);
    }

private:
    std::vector<ColumnSchemaV2> _column_schemas;
    std::vector<uint32_t> _column_ids;
    std::vector<uint8_t*> _column_data;
    std::vector<uint8_t*> _column_null_bitmaps;
    size_t _capacity;
    size_t _num_rows;
    Arena* _arena;
};

}
