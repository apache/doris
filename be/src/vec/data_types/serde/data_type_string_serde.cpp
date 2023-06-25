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

#include "data_type_string_serde.h"

#include <assert.h>
#include <gen_cpp/types.pb.h>
#include <stddef.h>

#include "arrow/array/builder_binary.h"
#include "gutil/casts.h"
#include "util/jsonb_document.h"
#include "util/jsonb_utils.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"

namespace doris {
namespace vectorized {
class Arena;

Status DataTypeStringSerDe::write_column_to_pb(const IColumn& column, PValues& result, int start,
                                               int end) const {
    result.mutable_bytes_value()->Reserve(end - start);
    for (size_t row_num = start; row_num < end; ++row_num) {
        StringRef data = column.get_data_at(row_num);
        result.add_string_value(data.to_string());
    }
    return Status::OK();
}
Status DataTypeStringSerDe::read_column_from_pb(IColumn& column, const PValues& arg) const {
    auto& col = reinterpret_cast<ColumnString&>(column);
    col.reserve(arg.string_value_size());
    for (int i = 0; i < arg.string_value_size(); ++i) {
        column.insert_data(arg.string_value(i).c_str(), arg.string_value(i).size());
    }
    return Status::OK();
}

void DataTypeStringSerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                  Arena* mem_pool, int32_t col_id,
                                                  int row_num) const {
    result.writeKey(col_id);
    const auto& data_ref = column.get_data_at(row_num);
    result.writeStartBinary();
    result.writeBinary(reinterpret_cast<const char*>(data_ref.data), data_ref.size);
    result.writeEndBinary();
}
void DataTypeStringSerDe::read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const {
    assert(arg->isBinary());
    auto& col = reinterpret_cast<ColumnString&>(column);
    auto blob = static_cast<const JsonbBlobVal*>(arg);
    col.insert_data(blob->getBlob(), blob->getBlobLen());
}

void DataTypeStringSerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    const auto& string_column = assert_cast<const ColumnString&>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && null_map[string_i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
            continue;
        }
        std::string_view string_ref = string_column.get_data_at(string_i).to_string_view();
        if (column.get_data_type() == TypeIndex::JSONB) {
            std::string json_string =
                    JsonbToJson::jsonb_to_json_string(string_ref.data(), string_ref.size());
            checkArrowStatus(builder.Append(json_string.data(), json_string.size()),
                             column.get_name(), array_builder->type()->name());
        } else {
            checkArrowStatus(builder.Append(string_ref.data(), string_ref.size()),
                             column.get_name(), array_builder->type()->name());
        }
    }
}

void DataTypeStringSerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    auto& column_chars_t = assert_cast<ColumnString&>(column).get_chars();
    auto& column_offsets = assert_cast<ColumnString&>(column).get_offsets();
    if (arrow_array->type_id() == arrow::Type::STRING ||
        arrow_array->type_id() == arrow::Type::BINARY) {
        auto concrete_array = down_cast<const arrow::BinaryArray*>(arrow_array);
        std::shared_ptr<arrow::Buffer> buffer = concrete_array->value_data();

        for (size_t offset_i = start; offset_i < end; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = buffer->data() + concrete_array->value_offset(offset_i);
                column_chars_t.insert(raw_data, raw_data + concrete_array->value_length(offset_i));
            }
            column_offsets.emplace_back(column_chars_t.size());
        }
    } else if (arrow_array->type_id() == arrow::Type::FIXED_SIZE_BINARY) {
        auto concrete_array = down_cast<const arrow::FixedSizeBinaryArray*>(arrow_array);
        uint32_t width = concrete_array->byte_width();
        const auto* array_data = concrete_array->GetValue(start);

        for (size_t offset_i = 0; offset_i < end - start; ++offset_i) {
            if (!concrete_array->IsNull(offset_i)) {
                const auto* raw_data = array_data + (offset_i * width);
                column_chars_t.insert(raw_data, raw_data + width);
            }
            column_offsets.emplace_back(column_chars_t.size());
        }
    }
}

template <bool is_binary_format>
Status DataTypeStringSerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& col = assert_cast<const ColumnString&>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    const auto string_val = col.get_data_at(col_index);
    if (string_val.data == nullptr) {
        if (string_val.size == 0) {
            // 0x01 is a magic num, not useful actually, just for present ""
            char* tmp_val = reinterpret_cast<char*>(0x01);
            if (UNLIKELY(0 != result.push_string(tmp_val, string_val.size))) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        } else {
            if (UNLIKELY(0 != result.push_null())) {
                return Status::InternalError("pack mysql buffer failed.");
            }
        }
    } else {
        if (UNLIKELY(0 != result.push_string(string_val.data, string_val.size))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeStringSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeStringSerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris