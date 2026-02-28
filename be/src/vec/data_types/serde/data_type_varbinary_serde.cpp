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

#include "data_type_varbinary_serde.h"

#include "vec/columns/column_varbinary.h"

namespace doris::vectorized {

void DataTypeVarbinarySerDe::write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result,
                                                     Arena& arena, int32_t col_id, int64_t row_num,
                                                     const FormatOptions& options) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} read_one_cell_from_jsonb ostr not implement.",
                           column.get_name());
}

void DataTypeVarbinarySerDe::read_one_cell_from_jsonb(IColumn& column,
                                                      const JsonbValue* arg) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "Data type {} read_one_cell_from_jsonb ostr not implement.",
                           column.get_name());
}

Status DataTypeVarbinarySerDe::write_column_to_mysql_binary(const IColumn& column,
                                                            MysqlRowBinaryBuffer& result,
                                                            int64_t row_idx, bool col_const,
                                                            const FormatOptions& options) const {
    auto col_index = index_check_const(row_idx, col_const);
    const auto& data = assert_cast<const ColumnVarbinary&>(column).get_data()[col_index];

    if (0 != result.push_string(data.data(), data.size())) {
        return Status::InternalError("pack mysql buffer failed.");
    }

    return Status::OK();
}

Status DataTypeVarbinarySerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                     arrow::ArrayBuilder* array_builder,
                                                     int64_t start, int64_t end,
                                                     const cctz::time_zone& ctz) const {
    auto lambda_function = [&](auto& builder) -> Status {
        const auto& varbinary_column_data = assert_cast<const ColumnVarbinary&>(column).get_data();
        for (size_t i = start; i < end; ++i) {
            if (null_map && (*null_map)[i]) {
                RETURN_IF_ERROR(checkArrowStatus(builder.AppendNull(), column.get_name(),
                                                 builder.type()->name()));
                continue;
            }
            const auto& string_view = varbinary_column_data[i];
            RETURN_IF_ERROR(checkArrowStatus(builder.Append(string_view.data(), string_view.size()),
                                             column.get_name(), builder.type()->name()));
        }
        return Status::OK();
    };
    if (array_builder->type()->id() == arrow::Type::BINARY) {
        auto& builder = assert_cast<arrow::BinaryBuilder&>(*array_builder);
        return lambda_function(builder);
    } else if (array_builder->type()->id() == arrow::Type::STRING) {
        auto& builder = assert_cast<arrow::StringBuilder&>(*array_builder);
        return lambda_function(builder);
    } else {
        return Status::InvalidArgument("Unsupported arrow type for varbinary column: {}",
                                       array_builder->type()->name());
    }
    return Status::OK();
}

Status DataTypeVarbinarySerDe::write_column_to_orc(const std::string& timezone,
                                                   const IColumn& column, const NullMap* null_map,
                                                   orc::ColumnVectorBatch* orc_col_batch,
                                                   int64_t start, int64_t end,
                                                   vectorized::Arena& arena,
                                                   const FormatOptions& options) const {
    auto* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);
    const auto& varbinary_column_data = assert_cast<const ColumnVarbinary&>(column).get_data();

    for (auto row_id = start; row_id < end; row_id++) {
        cur_batch->data[row_id] = const_cast<char*>(varbinary_column_data[row_id].data());
        cur_batch->length[row_id] = varbinary_column_data[row_id].size();
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeVarbinarySerDe::serialize_one_cell_to_json(const IColumn& column, int64_t row_num,
                                                          BufferWritable& bw,
                                                          FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;
    const auto& value = assert_cast<const ColumnVarbinary&>(*ptr).get_data_at(row_num);
    bw.write(value.data, value.size);
    return Status::OK();
}

Status DataTypeVarbinarySerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                              const FormatOptions& options) const {
    assert_cast<ColumnVarbinary&>(column).insert_data(slice.data, slice.size);
    return Status::OK();
}

void DataTypeVarbinarySerDe::to_string(const IColumn& column, size_t row_num, BufferWritable& bw,
                                       const FormatOptions& options) const {
    const auto& value = assert_cast<const ColumnVarbinary&>(column).get_data()[row_num];
    if (_nesting_level >= 2) { // in complex type, need to dump as hex string by hand
        const auto& hex_str = value.dump_hex();
        bw.write(hex_str.data(), hex_str.size());
    } else { // mysql protocol will be handle as hex binary data directly
        bw.write(value.data(), value.size());
    }
}

} // namespace doris::vectorized
