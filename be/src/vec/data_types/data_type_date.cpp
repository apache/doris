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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDate.cpp
// and modified by Doris

#include "vec/data_types/data_type_date.h"

#include "runtime/datetime_value.h"
#include "vec/columns/columns_number.h"
#include "util/binary_cast.hpp"
#include "vec/runtime/vdatetime_value.h"
namespace doris::vectorized {
bool DataTypeDate::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeDate::to_string(const IColumn& column, size_t row_num) const {
    Int64 int_val = assert_cast<const ColumnInt64&>(*column.convert_to_full_column_if_const().get())
                             .get_data()[row_num];
    doris::vectorized::VecDateTimeValue value = binary_cast<Int64, doris::vectorized::VecDateTimeValue>(int_val);
    std::stringstream ss;
    // Year
    uint32_t temp = value.year() / 100;
    ss << (char)('0' + (temp / 10)) << (char)('0' + (temp % 10));
    temp = value.year() % 100;
    ss << (char)('0' + (temp / 10)) << (char)('0' + (temp % 10)) << '-';
    // Month
    ss << (char)('0' + (value.month() / 10)) << (char)('0' + (value.month() % 10)) << '-';
    // Day
    ss << (char)('0' + (value.day() / 10)) << (char)('0' + (value.day() % 10));
    return ss.str();
}

void DataTypeDate::to_string(const IColumn & column, size_t row_num, BufferWritable & ostr) const {
    Int64 int_val = assert_cast<const ColumnInt64&>(*column.convert_to_full_column_if_const().get())
                             .get_data()[row_num];
    doris::vectorized::VecDateTimeValue value = binary_cast<Int64, doris::vectorized::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    // DateTime to_string the end is /0
    ostr.write(buf, pos - buf - 1);
}

void DataTypeDate::cast_to_date(Int64& x) {
    auto value = binary_cast<Int64, VecDateTimeValue>(x);
    value.cast_to_date();
    x = binary_cast<VecDateTimeValue, Int64>(value);
}

} // namespace doris::vectorized
