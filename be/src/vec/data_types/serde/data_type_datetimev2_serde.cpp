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

#include <chrono> // IWYU pragma: keep

#include "vec/columns/column_const.h"
#include "vec/io/io_helper.h"

enum {
    DIVISOR_FOR_SECOND = 1,
    DIVISOR_FOR_MILLI = 1000,
    DIVISOR_FOR_MICRO = 1000000,
    DIVISOR_FOR_NANO = 1000000000
};

namespace doris::vectorized {
static const int64_t timestamp_threshold = -2177481943;
static const int64_t timestamp_diff = 343;
static const int64_t micr_to_nano_second = 1000;

Status DataTypeDateTimeV2SerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                         int end_idx, BufferWritable& bw,
                                                         FormatOptions& options) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDateTimeV2SerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
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
        char buf[30 + SAFE_FORMAT_STRING_MARGIN];
        val.to_format_string_conservative(format.c_str(), format.size(), buf,
                                          30 + SAFE_FORMAT_STRING_MARGIN);
        std::string s = std::string(buf);
        bw.write(s.c_str(), s.length());
    } else {
        char buf[64];
        char* pos = val.to_string(buf);
        bw.write(buf, pos - buf - 1);
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_json_vector(
        IColumn& column, std::vector<Slice>& slices, int* num_deserialized,
        const FormatOptions& options) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}
Status DataTypeDateTimeV2SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                               const FormatOptions& options) const {
    auto& column_data = assert_cast<ColumnUInt64&>(column);
    UInt64 val = 0;
    if (options.date_olap_format) {
        DateV2Value<DateTimeV2ValueType> datetimev2_value;
        std::string date_format = "%Y-%m-%d %H:%i:%s.%f";
        if (datetimev2_value.from_date_format_str(date_format.data(), date_format.size(),
                                                  slice.data, slice.size)) {
            val = datetimev2_value.to_date_int_val();
        } else {
            val = MIN_DATETIME_V2;
        }

    } else if (ReadBuffer rb(slice.data, slice.size);
               !read_datetime_v2_text_impl<UInt64>(val, rb, scale)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

void DataTypeDateTimeV2SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                    arrow::ArrayBuilder* array_builder, int start,
                                                    int end, const cctz::time_zone& ctz) const {
    const auto& col_data = static_cast<const ColumnVector<UInt64>&>(column).get_data();
    auto& timestamp_builder = assert_cast<arrow::TimestampBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        if (null_map && (*null_map)[i]) {
            checkArrowStatus(timestamp_builder.AppendNull(), column.get_name(),
                             array_builder->type()->name());
        } else {
            int64_t timestamp = 0;
            DateV2Value<DateTimeV2ValueType> datetime_val =
                    binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[i]);
            datetime_val.unix_timestamp(&timestamp, ctz);

            if (scale > 3) {
                uint32_t microsecond = datetime_val.microsecond();
                timestamp = (timestamp * 1000000) + microsecond;
            } else if (scale > 0) {
                uint32_t millisecond = datetime_val.microsecond() / 1000;
                timestamp = (timestamp * 1000) + millisecond;
            }
            checkArrowStatus(timestamp_builder.Append(timestamp), column.get_name(),
                             array_builder->type()->name());
        }
    }
}

void DataTypeDateTimeV2SerDe::read_column_from_arrow(IColumn& column,
                                                     const arrow::Array* arrow_array, int start,
                                                     int end, const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnDateTimeV2&>(column).get_data();
    int64_t divisor = 1;
    if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        const auto* concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        switch (type->unit()) {
        case arrow::TimeUnit::type::SECOND: {
            divisor = DIVISOR_FOR_SECOND;
            break;
        }
        case arrow::TimeUnit::type::MILLI: {
            divisor = DIVISOR_FOR_MILLI;
            break;
        }
        case arrow::TimeUnit::type::MICRO: {
            divisor = DIVISOR_FOR_MICRO;
            break;
        }
        case arrow::TimeUnit::type::NANO: {
            divisor = DIVISOR_FOR_NANO;
            break;
        }
        default: {
            LOG(WARNING) << "not support convert to datetimev2 from time_unit:" << type->unit();
            return;
        }
        }
        for (size_t value_i = start; value_i < end; ++value_i) {
            auto utc_epoch = static_cast<UInt64>(concrete_array->Value(value_i));

            DateV2Value<DateTimeV2ValueType> v;
            // convert second
            v.from_unixtime(utc_epoch / divisor, ctz);
            // get rest time
            v.set_microsecond(utc_epoch % divisor);
            col_data.emplace_back(binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(v));
        }
    } else {
        LOG(WARNING) << "not support convert to datetimev2 from arrow type:"
                     << arrow_array->type()->id();
    }
}

template <bool is_binary_format>
Status DataTypeDateTimeV2SerDe::_write_column_to_mysql(const IColumn& column,
                                                       MysqlRowBuffer<is_binary_format>& result,
                                                       int row_idx, bool col_const,
                                                       const FormatOptions& options) const {
    const auto& data = assert_cast<const ColumnVector<UInt64>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    DateV2Value<DateTimeV2ValueType> date_val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(data[col_index]);
    // _nesting_level >= 2 means this datetimev2 is in complex type
    // and we should add double quotes
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    if (UNLIKELY(0 != result.push_vec_datetime(date_val, scale))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    if (_nesting_level >= 2 && options.wrapper_len > 0) {
        if (UNLIKELY(0 != result.push_string(options.nested_string_wrapper, options.wrapper_len))) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                      bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_mysql(const IColumn& column,
                                                      MysqlRowBuffer<false>& row_buffer,
                                                      int row_idx, bool col_const,
                                                      const FormatOptions& options) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const, options);
}

Status DataTypeDateTimeV2SerDe::write_column_to_orc(const std::string& timezone,
                                                    const IColumn& column, const NullMap* null_map,
                                                    orc::ColumnVectorBatch* orc_col_batch,
                                                    int start, int end,
                                                    std::vector<StringRef>& buffer_list) const {
    const auto& col_data = assert_cast<const ColumnVector<UInt64>&>(column).get_data();
    auto* cur_batch = dynamic_cast<orc::TimestampVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        int64_t timestamp = 0;
        DateV2Value<DateTimeV2ValueType> datetime_val =
                binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(col_data[row_id]);
        if (!datetime_val.unix_timestamp(&timestamp, timezone)) {
            return Status::InternalError("get unix timestamp error.");
        }

        // -2177481943 represent '1900-12-31 23:54:17'
        // but -2177481944 represent '1900-12-31 23:59:59'
        // so for timestamp <= -2177481944, we subtract 343 (5min 43s)
        // Reference: https://www.timeanddate.com/time/change/china/shanghai?year=1900
        if (timezone == TimezoneUtils::default_time_zone && timestamp < timestamp_threshold) {
            timestamp -= timestamp_diff;
        }

        cur_batch->data[row_id] = timestamp;
        cur_batch->nanoseconds[row_id] = datetime_val.microsecond() * micr_to_nano_second;
    }
    cur_batch->numElements = end - start;
    return Status::OK();
}

Status DataTypeDateTimeV2SerDe::deserialize_column_from_fixed_json(
        IColumn& column, Slice& slice, int rows, int* num_deserialized,
        const FormatOptions& options) const {
    Status st = deserialize_one_cell_from_json(column, slice, options);
    if (!st.ok()) {
        return st;
    }

    DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(column, rows - 1);
    *num_deserialized = rows;
    return Status::OK();
}

void DataTypeDateTimeV2SerDe::insert_column_last_value_multiple_times(IColumn& column,
                                                                      int times) const {
    auto& col = static_cast<ColumnVector<UInt64>&>(column);
    auto sz = col.size();
    UInt64 val = col.get_element(sz - 1);
    col.insert_many_vals(val, times);
}

} // namespace doris::vectorized
