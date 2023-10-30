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

#include "data_type_date64_serde.h"

#include <arrow/builder.h>

#include <type_traits>

#include "vec/columns/column_const.h"
#include "vec/io/io_helper.h"

namespace doris {
namespace vectorized {

Status DataTypeDate64SerDe::serialize_column_to_json(const IColumn& column, int start_idx,
                                                     int end_idx, BufferWritable& bw,
                                                     FormatOptions& options,
                                                     int nesting_level) const {
    SERIALIZE_COLUMN_TO_JSON();
}

Status DataTypeDate64SerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                       BufferWritable& bw, FormatOptions& options,
                                                       int nesting_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    Int64 int_val = assert_cast<const ColumnInt64&>(*ptr).get_element(row_num);
    if (options.date_olap_format) {
        tm time_tm;
        memset(&time_tm, 0, sizeof(time_tm));
        time_tm.tm_mday = static_cast<int>(int_val & 31);
        time_tm.tm_mon = static_cast<int>(int_val >> 5 & 15) - 1;
        time_tm.tm_year = static_cast<int>(int_val >> 9) - 1900;
        char buf[20] = {'\0'};
        strftime(buf, sizeof(buf), "%Y-%m-%d", &time_tm);
        std::string s = std::string(buf);
        bw.write(s.c_str(), s.length());
    } else {
        doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

        char buf[64];
        char* pos = value.to_string(buf);
        bw.write(buf, pos - buf - 1);
    }
    return Status::OK();
}

Status DataTypeDate64SerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                std::vector<Slice>& slices,
                                                                int* num_deserialized,
                                                                const FormatOptions& options,
                                                                int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR();
    return Status::OK();
}

Status DataTypeDate64SerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                           const FormatOptions& options,
                                                           int nesting_level) const {
    auto& column_data = assert_cast<ColumnInt64&>(column);
    Int64 val = 0;
    if (options.date_olap_format) {
        tm time_tm;
        char* res = strptime(slice.data, "%Y-%m-%d", &time_tm);
        if (nullptr != res) {
            val = (time_tm.tm_year + 1900) * 16 * 32 + (time_tm.tm_mon + 1) * 32 + time_tm.tm_mday;
        } else {
            // 1400 - 01 - 01
            val = 716833;
        }
    } else if (ReadBuffer rb(slice.data, slice.size); !read_date_text_impl<Int64>(val, rb)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

Status DataTypeDateTimeSerDe::serialize_column_to_json(
        const IColumn& column, int start_idx, int end_idx, BufferWritable& bw,
        FormatOptions& options, int nesting_level) const {SERIALIZE_COLUMN_TO_JSON()}

Status DataTypeDateTimeSerDe::serialize_one_cell_to_json(const IColumn& column, int row_num,
                                                         BufferWritable& bw, FormatOptions& options,
                                                         int nesting_level) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    Int64 int_val = assert_cast<const ColumnInt64&>(*ptr).get_element(row_num);
    if (options.date_olap_format) {
        tm time_tm;
        int64 part1 = (int_val / 1000000L);
        int64 part2 = (int_val - part1 * 1000000L);
        time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
        time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
        time_tm.tm_mday = static_cast<int>(part1 % 100);

        time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
        time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
        time_tm.tm_sec = static_cast<int>(part2 % 100);
        char buf[20] = {'\0'};
        strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
        std::string s = std::string(buf);
        bw.write(s.c_str(), s.length());
    } else {
        doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

        char buf[64];
        char* pos = value.to_string(buf);
        bw.write(buf, pos - buf - 1);
    }
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_column_from_json_vector(IColumn& column,
                                                                  std::vector<Slice>& slices,
                                                                  int* num_deserialized,
                                                                  const FormatOptions& options,
                                                                  int nesting_level) const {
    DESERIALIZE_COLUMN_FROM_JSON_VECTOR()
    return Status::OK();
}

Status DataTypeDateTimeSerDe::deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                                             const FormatOptions& options,
                                                             int nesting_level) const {
    auto& column_data = assert_cast<ColumnInt64&>(column);
    Int64 val = 0;
    if (options.date_olap_format) {
        tm time_tm;
        char* res = strptime(slice.data, "%Y-%m-%d %H:%M:%S", &time_tm);
        if (nullptr != res) {
            val = ((time_tm.tm_year + 1900) * 10000L + (time_tm.tm_mon + 1) * 100L +
                   time_tm.tm_mday) *
                          1000000L +
                  time_tm.tm_hour * 10000L + time_tm.tm_min * 100L + time_tm.tm_sec;
        } else {
            // 1400 - 01 - 01
            val = 14000101000000L;
        }
    } else if (ReadBuffer rb(slice.data, slice.size); !read_datetime_text_impl<Int64>(val, rb)) {
        return Status::InvalidArgument("parse datetime fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data.insert_value(val);
    return Status::OK();
}

void DataTypeDate64SerDe::write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    auto& col_data = static_cast<const ColumnVector<Int64>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const VecDateTimeValue* time_val = (const VecDateTimeValue*)(&col_data[i]);
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

static int64_t time_unit_divisor(arrow::TimeUnit::type unit) {
    // Doris only supports seconds
    switch (unit) {
    case arrow::TimeUnit::type::SECOND: {
        return 1L;
    }
    case arrow::TimeUnit::type::MILLI: {
        return 1000L;
    }
    case arrow::TimeUnit::type::MICRO: {
        return 1000000L;
    }
    case arrow::TimeUnit::type::NANO: {
        return 1000000000L;
    }
    default:
        return 0L;
    }
}

void DataTypeDate64SerDe::read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array,
                                                 int start, int end,
                                                 const cctz::time_zone& ctz) const {
    auto& col_data = static_cast<ColumnVector<Int64>&>(column).get_data();
    int64_t divisor = 1;
    int64_t multiplier = 1;
    if (arrow_array->type()->id() == arrow::Type::DATE64) {
        auto concrete_array = dynamic_cast<const arrow::Date64Array*>(arrow_array);
        divisor = 1000; //ms => secs
        for (size_t value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        auto concrete_array = dynamic_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid Time Type: " + type->name());
        }
        for (size_t value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::DATE32) {
        auto concrete_array = dynamic_cast<const arrow::Date32Array*>(arrow_array);
        multiplier = 24 * 60 * 60; // day => secs
        for (size_t value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            v.cast_to_date();
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    }
}

template <bool is_binary_format>
Status DataTypeDate64SerDe::_write_column_to_mysql(const IColumn& column,
                                                   MysqlRowBuffer<is_binary_format>& result,
                                                   int row_idx, bool col_const) const {
    auto& data = assert_cast<const ColumnVector<Int64>&>(column).get_data();
    const auto col_index = index_check_const(row_idx, col_const);
    auto time_num = data[col_index];
    VecDateTimeValue time_val = binary_cast<Int64, VecDateTimeValue>(time_num);
    if (UNLIKELY(0 != result.push_vec_datetime(time_val))) {
        return Status::InternalError("pack mysql buffer failed.");
    }
    return Status::OK();
}

Status DataTypeDate64SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<true>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeDate64SerDe::write_column_to_mysql(const IColumn& column,
                                                  MysqlRowBuffer<false>& row_buffer, int row_idx,
                                                  bool col_const) const {
    return _write_column_to_mysql(column, row_buffer, row_idx, col_const);
}

Status DataTypeDate64SerDe::write_column_to_orc(const std::string& timezone, const IColumn& column,
                                                const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    auto& col_data = static_cast<const ColumnVector<Int64>&>(column).get_data();
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    char* ptr = (char*)malloc(BUFFER_UNIT_SIZE);
    if (!ptr) {
        return Status::InternalError(
                "malloc memory error when write largeint column data to orc file.");
    }
    StringRef bufferRef;
    bufferRef.data = ptr;
    bufferRef.size = BUFFER_UNIT_SIZE;
    size_t offset = 0;
    const size_t begin_off = offset;

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 0) {
            continue;
        }

        int len = binary_cast<Int64, VecDateTimeValue>(col_data[row_id])
                          .to_buffer(const_cast<char*>(bufferRef.data) + offset);

        REALLOC_MEMORY_FOR_ORC_WRITER()

        cur_batch->length[row_id] = len;
        offset += len;
    }
    size_t data_off = 0;
    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            cur_batch->data[row_id] = const_cast<char*>(bufferRef.data) + begin_off + data_off;
            data_off += cur_batch->length[row_id];
        }
    }

    buffer_list.emplace_back(bufferRef);
    cur_batch->numElements = end - start;
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
