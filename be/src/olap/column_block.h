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

#include "olap/types.h"
#include "util/bitmap.h"

namespace doris {

class Arena;
class TypeInfo;

// Block of data belong to a single column.
// It doesn't own any data, user should keep the life of input data.
class ColumnBlock {
public:
    ColumnBlock(const TypeInfo* type_info, uint8_t* data, uint8_t* null_bitmap, Arena* arena)
        : _type_info(type_info), _data(data), _null_bitmap(null_bitmap), _arena(arena) { }

    const TypeInfo* type_info() const { return _type_info; }
    uint8_t* data() const { return _data; }
    uint8_t* null_bitmap() const { return _null_bitmap; }
    bool is_nullable() const { return _null_bitmap != nullptr; }
    Arena* arena() const { return _arena; }
    const uint8_t* cell_ptr(size_t idx) const { return _data + idx * _type_info->size(); }
    uint8_t* mutable_cell_ptr(size_t idx) const { return _data + idx * _type_info->size(); }
    bool is_null(size_t idx) const {
        return BitmapTest(_null_bitmap, idx);
    }
    void set_is_null(size_t idx, bool is_null) {
        return BitmapChange(_null_bitmap, idx, is_null);
    }
private:
    const TypeInfo* _type_info;
    uint8_t* _data;
    uint8_t* _null_bitmap;
    Arena* _arena;
};

// Wrap ColumnBlock and offset, easy to access data at the specified offset
// Used to read data from page decoder
class ColumnBlockView {
public:
    explicit ColumnBlockView(ColumnBlock* block, size_t row_offset = 0)
        : _block(block), _row_offset(row_offset) {
    }
    void advance(size_t skip) { _row_offset += skip; }
    size_t first_row_index() const { return _row_offset; }
    ColumnBlock* column_block() { return _block; }
    Arena* arena() const { return _block->arena(); }
    void set_null_bits(size_t num_rows, bool val) {
        BitmapChangeBits(_block->null_bitmap(), _row_offset, num_rows, val);
    }
    bool is_nullable() const { return _block->is_nullable(); }
    uint8_t* data() const { return _block->mutable_cell_ptr(_row_offset); }
private:
    ColumnBlock* _block;
    size_t _row_offset;
};

}
