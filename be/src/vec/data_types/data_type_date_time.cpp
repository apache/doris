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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDateTime.cpp
// and modified by Doris

#include "vec/data_types/data_type_date_time.h"

#include <typeinfo>
#include <utility>

#include "util/binary_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

bool DataTypeDateTime::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

size_t DataTypeDateTime::number_length() const {
    //2024-01-01 00:00:00
    return 20;
}

void DataTypeDateTime::push_number(ColumnString::Chars& chars, const Int64& num) const {
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(num);
    CastToString::push_date_or_datetime(value, chars);
}

std::string DataTypeDateTime::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    Int64 int_val = assert_cast<const ColumnDateTime&>(*ptr).get_element(row_num);
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    value.to_string(buf);
    // DateTime to_string the end is /0
    return buf;
}

std::string DataTypeDateTime::to_string(Int64 int_val) const {
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    value.to_string(buf);
    // DateTime to_string the end is /0
    return buf;
}
void DataTypeDateTime::to_string(const IColumn& column, size_t row_num,
                                 BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    Int64 int_val = assert_cast<const ColumnDateTime&>(*ptr).get_element(row_num);
    doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    // DateTime to_string the end is /0
    ostr.write(buf, pos - buf - 1);
}

void DataTypeDateTime::cast_to_date_time(Int64& x) {
    auto value = binary_cast<Int64, doris::VecDateTimeValue>(x);
    value.to_datetime();
    x = binary_cast<doris::VecDateTimeValue, Int64>(value);
}

MutableColumnPtr DataTypeDateTime::create_column() const {
    return DataTypeNumberBase<PrimitiveType::TYPE_DATETIME>::create_column();
}

} // namespace doris::vectorized
