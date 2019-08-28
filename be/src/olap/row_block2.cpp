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

#include "olap/row_block2.h"

#include <sstream>

#include "gutil/strings/substitute.h"
#include "olap/row_cursor.h"
#include "util/bitmap.h"

using strings::Substitute;
namespace doris {

RowBlockV2::RowBlockV2(const Schema& schema, uint16_t capacity)
    : _schema(schema),
      _capacity(capacity),
      _column_datas(_schema.num_columns(), nullptr),
      _column_null_bitmaps(_schema.num_columns(), nullptr) {
    auto bitmap_size = BitmapSize(capacity);
    int i = 0;
    for (auto& col_schema : _schema.columns()) {
        size_t data_size = col_schema->type_info()->size() * _capacity;
        _column_datas[i] = new uint8_t[data_size];

        uint8_t* null_bitmap = nullptr;
        if (col_schema->is_nullable()) {
            null_bitmap = new uint8_t[bitmap_size];
        }
        _column_null_bitmaps[i] = null_bitmap;
        i++;
    }
    clear();
}

RowBlockV2::~RowBlockV2() {
    for (auto data : _column_datas) {
        delete[] data;
    }
    for (auto null_bitmap : _column_null_bitmaps) {
        delete[] null_bitmap;
    }
}

Status RowBlockV2::copy_to_row_cursor(size_t row_idx, RowCursor* cursor) {
    if (row_idx >= _num_rows) {
        return Status::InvalidArgument(
            Substitute("invalid row index $0 (num_rows=$1)", row_idx, _num_rows));
    }
    for (auto cid : _schema.column_ids()) {
        bool is_null = _schema.column(cid)->is_nullable() && BitmapTest(_column_null_bitmaps[cid], row_idx);
        if (is_null) {
            cursor->set_null(cid);
        } else {
            cursor->set_not_null(cid);
            cursor->set_field_content_shallow(cid, reinterpret_cast<const char*>(column_block(cid).cell_ptr(row_idx)));
        }
    }
    return Status::OK();
}

std::string RowBlockRow::debug_string() const {
    std::stringstream ss;
    ss << "[";
    for (int i = 0; i < _block->schema()->num_columns(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        auto col_schema = _block->schema()->column(i);
        if (col_schema->is_nullable() && is_null(i)) {
            ss << "NULL";
        } else {
            ss << col_schema->type_info()->to_string(cell_ptr(i));
        }
    }
    ss << "]";
    return ss.str();
}

}
