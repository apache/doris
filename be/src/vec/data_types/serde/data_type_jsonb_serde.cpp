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

#include "data_type_jsonb_serde.h"

#include "arrow/array/builder_binary.h"
#include "runtime/jsonb_value.h"
namespace doris {
namespace vectorized {

template <bool is_binary_format>
Status DataTypeJsonbSerDe::_write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<is_binary_format>& result,
                                                  int row_idx, bool col_const) const {
    auto& data = assert_cast<const ColumnString&>(column);
    const auto col_index = index_check_const(row_idx, col_const);
    const auto jsonb_val = data.get_data_at(col_index);
    // jsonb size == 0 is NULL
    if (jsonb_val.data == nullptr || jsonb_val.size == 0) {
        if (UNLIKELY(0 != result.push_null())) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    } else {
        std::string json_str = JsonbToJson::jsonb_to_json_string(jsonb_val.data, jsonb_val.size);
        if (UNLIKELY(0 != result.push_string(json_str.c_str(), json_str.size()))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeJsonbSerDe::write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                 bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeJsonbSerDe::write_column_to_mysql(const IColumn& column,
                                                 MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                 bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeJsonbSerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                    int end_idx, BufferWritable& bw,
                                                    FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeJsonbSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                      BufferWritable& bw,
                                                      FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    const StringRef& s = assert_cast<const ColumnString&>(*ptr).get_data_at(row_num);
    if (s.size > 0) {
        bw.write(s.data, s.size);
    }
    return Status::OK();
}

Status DataTypeJsonbSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                               std::vector<Slice>& slices,
                                                               int* num_deserialized,
                                                               const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeJsonbSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                          const FormatOptions& options) const {
    JsonBinaryValue value;
    RETURN_IF_ERROR(value.from_json_string(slice.data, slice.size));

    auto& column_string = assert_cast<ColumnString&>(column);
    column_string.insert_data(value.value(), value.size());
    return Status::OK();
}

void DataTypeJsonbSerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                               arrow::ArrayBuilder* array_builder, int start,
                                               int end) const {
    const auto& string_column = assert_cast<const ColumnString&>(column);
    auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t string_i = start; string_i < end; ++string_i) {
        if (null_map && (*null_map)[string_i]) {
            checkArrowStatus(builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
            continue;
        }
        std::string_view string_ref = string_column.get_data_at(string_i).to_string_view();
        std::string json_string =
                JsonbToJson::jsonb_to_json_string(string_ref.data(), string_ref.size());
        checkArrowStatus(builder.Append(json_string.data(), json_string.size()), column.get_name(),
                         array_builder->type()->name());
    }
}

Status DataTypeJsonbSerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                               const NullMap* null_map,
                                               orc::ColumnVectorBatch* orc_col_batch, int start,
                                               int end, std::vector<StringRef>& buffer_list) const {
    return Status::NotSupported("write_column_to_orc with type [{}]", column.get_name());
}

} // namespace vectorized
} // namespace doris
