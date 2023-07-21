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

#include "data_type_bitmap_serde.h"

#include <gen_cpp/types.pb.h>

#include <string>

#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"

namespace doris {

namespace vectorized {
class IColumn;

Status DataTypeBitMapSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                               int end) const {
    auto ptype = result.mutable_type();
    ptype->set_id(PGenericType::BITMAP);
    auto& data_column = assert_cast<const ColumnBitmap&>(column);
    int row_count = end - start;
    result.mutable_bytes_value()->Reserve(row_count);
    for (int row = start; row < end; ++row) {
        auto& value = const_cast<BitmapValue&>(data_column.get_element(row));
        std::string memory_buffer;
        int bytesize = value.getSizeInBytes();
        memory_buffer.resize(bytesize);
        value.write_to(const_cast<char*>(memory_buffer.data()));
        result.add_bytes_value(memory_buffer);
    }
    return Status::OK();
}
Status DataTypeBitMapSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnBitmap&>(column);
    for (int i = 0; i < arg.bytes_value_size(); ++i) {
        BitmapValue value(arg.bytes_value(i).data());
        col.insert_value(value);
    }
    return Status::OK();
}

void DataTypeBitMapSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    auto& data_column = assert_cast<const ColumnBitmap&>(column);
    result.writeKey(col_id);
    auto bitmap_value = const_cast<BitmapValue&>(data_column.get_element(row_num));
    // serialize the content of string
    auto size = bitmap_value.getSizeInBytes();
    // serialize the content of string
    auto ptr = mem_pool->alloc(size);
    bitmap_value.write_to(const_cast<char*>(ptr));
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(ptr), size);
    result.writeEndBinary();
}

void DataTypeBitMapSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    auto& col = reinterpret_cast<ColumnBitmap&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    BitmapValue bitmap_value(blob->getBlob());
    col.insert_value(bitmap_value);
}

template <bool is_binary_format>
Status DataTypeBitMapSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& data_column = assert_cast<const ColumnBitmap&>(column);
    if (_return_object_as_string) {
        const auto col_index = index_check_const(row_idx, col_const);
        BitmapValue bitmapValue = data_column.get_element(col_index);
        size_t size = bitmapValue.getSizeInBytes();
        std::unique_ptr<char[]> buf = std::make_unique<char[]>(size);
        bitmapValue.write_to(buf.get());
        if (0 != result.push_string(buf.get(), size)) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        if (0 != result.push_null()) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeBitMapSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeBitMapSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris
