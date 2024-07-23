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

#include "data_type_datev2_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "vec/columns/column_const.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {

// This number represents the number of days from 0000-01-01 to 1970-01-01
static const int32_t date_threshold = 719528;

Status DataTypeDateV2SerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateV2SerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw,
                                                       FormatOptions& options) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt32 int_val = assert_cast<const ColumnUInt32&>(*ptr).get_element(row_num);
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf);
    // DateTime to_string the end is /0
    bw.write(buf, pos - buf - 1);
    return Status::OK();
}

Status DataTypeDateV2SerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeDateV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnUInt32&>(column);
    UInt32 val = 0;
    if (options.date_olap_format) {
        tm time_tm;
        char* res = strptime(slice.data, "%Y-%m-%d", &time_tm);
        if (nullptr != res) {
            val = ((time_tm.tm_year + 1900) << 9) | ((time_tm.tm_mon + 1) << 5) | time_tm.tm_mday;
        } else {
            val = MIN_DATE_V2;
        }
    } else if (ReadBuffer rb(slice.data, slice.size); !read_date_v2_text_impl<UInt32>(val, rb)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

void DataTypeDateV2SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end, const cctz::time_zone& ctz) const {
    const auto& col_data = static_cast<const ColumnVector<UInt32>&>(column).get_data();
    auto& date32_builder = assert_cast<arrow::Date32Builder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        int32_t daynr = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(col_data[i]).daynr() -
                        date_threshold;
        if (null_map && (*null_map)[i]) {
            checkArrowStatus(date32_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            checkArrowStatus(date32_builder.Append(daynr), column.get_name(),
                             array_builder->type()->name());
        }
    }
}

void DataTypeDateV2SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnVector<UInt32>&>(column).get_data();
    auto concrete_array = dynamic_cast<const arrow::Date32Array*>(arrow_array);
    int64_t divisor = 1;
    int64_t multiplier = 1;

    multiplier = 24 * 60 * 60; // day => secs
    for (size_t value_i = start; value_i < end; ++value_i) {
        DateV2Value<DateV2ValueType> v;
        v.from_unixtime(static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier,
                        ctz);
        col_data.emplace_back(binary_cast<DateV2Value<DateV2ValueType>, UInt32>(v));
    }
}

template <bool is_binary_format>
Status DataTypeDateV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const,
                                                   const FormatOptions& options) const {
    auto& data = assert_cast<const ColumnVector<UInt32>&>(column).get_data();
    auto col_index = index_check_const(row_idx, col_const);
    DateV2Value<DateV2ValueType> date_val =
            binary_cast<UInt32, DateV2Value<DateV2ValueType>>(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(date_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeDateV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateV2SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const,
                                                  const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateV2SerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    const auto& col_data = assert_cast<const ColumnVector<UInt32>&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::LongVectorBatch*>(orc_col_batch);
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }
        cur_batch->data[row_id] =
                binary_cast<UInt32, DateV2Value<DateV2ValueType>>(col_data[row_id]).daynr() -
                date_threshold;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeDateV2SerDe::deserialize_column_from_fixed_json(IColumn& column, Slice& slice,
                                                               int rows, int* num_deserialized,
                                                               const FormatOptions& options) const {
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }
    DataTypeDateV2SerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

void DataTypeDateV2SerDe::insert_column_last_value_multiple_times(IColumn& column,
                                                                  int times) const {
    auto& col = static_cast<ColumnVector<UInt32>&>(column);
    auto sz = col.size();
    UInt32 val = col.get_element(sz - 1);

    col.insert_many_vals(val, times);
}

} // namespace vectorized
} // namespace doris
