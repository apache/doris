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

#include "data_type_datetimev2_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "vec/columns/column_const.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {

void DataTypeDateTimeV2SerDe::serialize_column_to_text(const IColumn& column, int start_idx,
                                                       int end_idx, BufferWritable& bw,
                                                       FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_TEXT()
}

void DataTypeDateTimeV2SerDe::serialize_one_cell_to_text(const IColumn& column, int row_num,
                                                         BufferWritable& bw,
                                                         FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt64 int_val = assert_cast<const ColumnUInt64&>(*ptr).get_element(row_num);
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    if (options.date_olap_format) {
        std::string format = "%Y-%m-%d %H:%i:%s.%f";
        char buf[30];
        val.to_format_string(format.c_str(), format.size(), buf);
        std::string s = std::string(buf);
        bw.write(s.c_str(), s.length());
    } else {
        char buf[64];
        char* pos = val.to_string(buf);
        bw.write(buf, pos - buf - 1);
    }
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_text_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_TEXT_VECTOR()
    return Status::OK();
}
Status DataTypeDateTimeV2SerDe::deserialize_one_cell_from_text(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnUInt64&>(column);
    UInt64 val = 0;
    if (options.date_olap_format) {
        doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType> datetimev2_value;
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
        if (datetimev2_value.from_date_format_str(date_format.data(), date_format.size(),
                                                  slice.data, slice.size)) {
            val = datetimev2_value.to_date_int_val();
        } else {
            val = doris::vectorized::MIN_DATETIME_V2;
        }

    } else if (ReadBuffer rb(slice.data, slice.size);
               !read_datetime_v2_text_impl<UInt64>(val, rb)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

void DataTypeDateTimeV2SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                    arrow::ArrayBuilder* array_builder, int start,
                                                    int end) const {
    auto& col_data = static_cast<const ColumnVector<UInt64>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const vectorized::DateV2Value<vectorized::DateTimeV2ValueType>* time_val =
                (const vectorized::DateV2Value<vectorized::DateTimeV2ValueType>*)(&col_data[i]);
        int len = time_val->to_buffer(buf);
        if (null_map && (*null_map)[i]) {
            checkArrowStatus(string_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            checkArrowStatus(string_builder.Append(buf, len), column.get_name(),
                             array_builder->type()->name());
        }
    }
}

template <bool is_binary_format>
Status DataTypeDateTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int row_idx, bool col_const) const {
    auto& data = assert_cast<const ColumnVector<UInt64>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    char buf[64];
    DateV2Value<DateTimeV2ValueType> date_val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(data[col_index]);
    char* pos = date_val.to_string(buf, scale);
    if (UNLIKELY(0 != result.push_string(buf, pos - buf - 1))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                      bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int row_idx, bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

} // namespace vectorized
} // namespace doris
