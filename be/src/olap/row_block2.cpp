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

RowBlockV2::RowBlockV2(const Schema& schema,
                       uint16_t capacity, Arena* arena)
        : _schema(schema),
        _column_datas(_schema.num_columns(), nullptr),
        _column_null_bitmaps(_schema.num_columns(), nullptr),
        _capacity(capacity),
        _num_rows(0),
        _arena(arena) {
    auto bitmap_size = BitmapSize(capacity);
    int i = 0;
    for (auto& col_schema : _schema.columns()) {
        size_t data_size = col_schema.type_info()->size() * _capacity;
        _column_datas[i] = new uint8_t[data_size];

        uint8_t* null_bitmap = nullptr;
        if (col_schema.is_nullable()) {
            null_bitmap = new uint8_t[bitmap_size];
        }
        _column_null_bitmaps[i] = null_bitmap;

        i++;
    }
}

RowBlockV2::~RowBlockV2() {
    for (auto data : _column_datas) {
        delete data;
    }
    for (auto null_bitmap : _column_null_bitmaps) {
        delete null_bitmap;
    }
}

Status RowBlockV2::copy_to_row_cursor(size_t row_idx, RowCursor* cursor) {
    if (row_idx >= _num_rows) {
        return Status::InvalidArgument(
            Substitute("Row index is large than number rows, $0 vs $1", row_idx, _num_rows));
    }
#if 0
    for (int i = 0; i < _column_ids.size(); ++i) {
        auto cid = _column_ids[i];
        bool is_null = _column_schemas[cid].field_info().is_allow_null
            && BitmapTest(_column_null_bitmaps[i], row_idx);
        if (is_null) {
            cursor->set_null(cid);
        } else {
            const TypeInfo* type_info = _column_schemas[cid].type_info();
            cursor->set_not_null(cid);
            char* dest = cursor->get_field_content_ptr(cid);
            char* src = (char*)_column_datas[i] + row_idx * type_info->size();
            type_info->copy_without_pool(dest, src);
        }
    }
#endif
    return Status::OK();
}

std::string RowBlockRow::debug_string() const {
    std::stringstream ss;
    ss << "[";
    for (int i = 0; i < _block->schema().num_columns(); ++i) {
        if (i != 0) {
            ss << ",";
        }
        auto& col_schema = _block->schema().column(i);
        if (col_schema.is_nullable() && is_null(i)) {
            ss << "NULL";
        } else {
            ss << col_schema.type_info()->to_string(cell_ptr(i));
        }
    }
    ss << "]";
    return ss.str();
}

void copy_row(RowBlockRow* dst, const RowBlockRow& src, Arena* arena) {
    for (int i = 0; i < src.schema().num_columns(); ++i) {
        dst->set_is_null(i, src.is_null(i));
        if (!src.is_null(i)) {
            src.schema().column(i).type_info()->copy_with_arena(dst->mutable_cell_ptr(i), src.cell_ptr(i), arena);
        }
    }
}

}
