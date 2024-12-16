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

#include <arrow/array/builder_binary.h>
#include <gen_cpp/types.pb.h>

#include <string>

#include "util/bitmap_value.h"
#include "util/jsonb_document.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/serde/data_type_nullable_serde.h"

namespace doris {

namespace vectorized {
class IColumn;
#include "common/compile_check_begin.h"

Status DataTypeBitMapSerDe::serialize_column_to_json(const IColumn& column, int64_t start_idx,
                                                     int64_t end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeBitMapSerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    /**
    * For null values in ordinary types, we use \N to represent them;
    * for null values in nested types, we use null to represent them, just like the json format.
    */
    if (_nesting_level >= 2) {
        bw.write(DataTypeNullableSerDe::NULL_IN_COMPLEX_TYPE.c_str(),
                 strlen(NULL_IN_COMPLEX_TYPE.c_str()));
    } else {
        bw.write(DataTypeNullableSerDe::NULL_IN_CSV_FOR_ORDINARY_TYPE.c_str(),
                 strlen(NULL_IN_CSV_FOR_ORDINARY_TYPE.c_str()));
    }
    return Status::OK();
}

Status DataTypeBitMapSerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}
Status DataTypeBitMapSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options) const {
    auto& data_column = assert_cast<ColumnBitmap&>(column);
    auto& data = data_column.get_data();

    BitmapValue value;
    if (!value.deserialize(slice.data)) {
        return Status::InternalError("deserialize BITMAP from string fail!");
    }
    data.push_back(std::move(value));
    return Status::OK();
}

Status DataTypeBitMapSerDe::write_column_to_pb(const IColumn& column, PValues& result,
                                               int64_t start, int64_t end) const {
    auto ptype = result.mutable_type();
    ptype->set_id(PGenericType::BITMAP);
    auto& data_column = assert_cast<const ColumnBitmap&>(column);
    auto row_count = cast_set<int>(end - start);
    result.mutable_bytes_value()->Reserve(row_count);
    for (auto row = start; row < end; ++row) {
        auto& value = const_cast<BitmapValue&>(data_column.get_element(row));
        std::string memory_buffer;
        auto bytesize = value.getSizeInBytes();
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
                                                  int64_t row_num) const {
    const auto& data_column = assert_cast<const ColumnBitmap&>(column);
    result.writeKey(cast_set<JsonbKeyValue::keyid_type>(col_id));
    auto bitmap_value = const_cast<BitmapValue&>(data_column.get_element(row_num));
    // serialize the content of string
    auto size = bitmap_value.getSizeInBytes();
    // serialize the content of string
    auto* ptr = mem_pool->alloc(size);
    bitmap_value.write_to(const_cast<char*>(ptr));
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(ptr), size);
    result.writeEndBinary();
}

void DataTypeBitMapSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int64_t start,
                                                int64_t end, const cctz::time_zone& ctz) const {
    const auto& col = assert_cast<const ColumnBitmap&>(column);
    auto& builder = assert_cast<arrow::BinaryBuilder&>(*array_builder);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            auto& bitmap_value = const_cast<BitmapValue&>(col.get_element(string_i));
            std::string memory_buffer(bitmap_value.getSizeInBytes(), '0');
            bitmap_value.write_to(memory_buffer.data());
            checkArrowStatus(
                    builder.Append(memory_buffer.data(), static_cast<int>(memory_buffer.size())),
                    column.get_name(), array_builder->type()->name());
        }
    }
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
                                                   int64_t row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    auto& data_column = assert_cast<const ColumnBitmap&>(column);
    if (_return_object_as_string) {
        const auto col_index = index_check_const(row_idx, col_const);
        BitmapValue bitmapValue = data_column.get_element(col_index);
        size_t size = bitmapValue.getSizeInBytes();
        std::unique_ptr<char[]> buf = std::make_unique_for_overwrite<char[]>(size);
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
                                                  MysqlRowBuffer<true>& row_buffer, int64_t row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeBitMapSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer,
                                                  int64_t row_idx, bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeBitMapSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch,
                                                int64_t start, int64_t end,
                                                std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnBitmap&>(column);
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    INIT_MEMORY_FOR_ORC_WRITER()

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            auto bitmap_value = const_cast<BitmapValue&>(col_data.get_element(row_id));
            size_t len = bitmap_value.getSizeInBytes();

            REALLOC_MEMORY_FOR_ORC_WRITER()

            bitmap_value.write_to(const_cast<char*>(bufferRef.data) + offset);
            cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + offset;
            cur_batch->length[row_id] = len;
            offset += len;
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
