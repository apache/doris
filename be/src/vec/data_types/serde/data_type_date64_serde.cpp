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

#include "gutil/casts.h"
#include "vec/columns/column_const.h"

namespace doris {
namespace vectorized {

void DataTypeDate64SerDe::write_column_to_arrow(const IColumn& column, const UInt8* null_map,
                                                arrow::ArrayBuilder* array_builder, int start,
                                                int end) const {
    auto& col_data = static_cast<const ColumnVector<Int64>&>(column).get_data();
    auto& string_builder = assert_cast<arrow::StringBuilder&>(*array_builder);
    for (size_t i = start; i < end; ++i) {
        char buf[64];
        const vectorized::VecDateTimeValue* time_val =
                (const vectorized::VecDateTimeValue*)(&col_data[i]);
        int len = time_val->to_buffer(buf);
        if (null_map && null_map[i]) {
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
        auto concrete_array = down_cast<const arrow::Date64Array*>(arrow_array);
        divisor = 1000; //ms => secs
        for (size_t value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::TIMESTAMP) {
        auto concrete_array = down_cast<const arrow::TimestampArray*>(arrow_array);
        const auto type = std::static_pointer_cast<arrow::TimestampType>(arrow_array->type());
        divisor = time_unit_divisor(type->unit());
        if (divisor == 0L) {
            LOG(FATAL) << "Invalid Time Type:" << type->name();
        }
        for (size_t value_i = start; value_i < end; ++value_i) {
            VecDateTimeValue v;
            v.from_unixtime(
                    static_cast<Int64>(concrete_array->Value(value_i)) / divisor * multiplier, ctz);
            col_data.emplace_back(binary_cast<VecDateTimeValue, Int64>(v));
        }
    } else if (arrow_array->type()->id() == arrow::Type::DATE32) {
        auto concrete_array = down_cast<const arrow::Date32Array*>(arrow_array);
        multiplier = 24 * 60 * 60; // day => secs
        for (size_t value_i = start; value_i < end; ++value_i) {
            //            std::cout << "serde : " <<  concrete_array->Value(value_i) << std::endl;
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

} // namespace vectorized
} // namespace doris
