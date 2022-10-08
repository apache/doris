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

#include "vec/data_types/data_type_time_v2.h"

#include "util/binary_cast.hpp"
#include "vec/columns/columns_number.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
bool DataTypeDateV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeDateV2::to_string(const IColumn& column, size_t row_num) const {
    UInt32 int_val =
            assert_cast<const ColumnUInt32&>(*column.convert_to_full_column_if_const().get())
                    .get_data()[row_num];
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);
    std::stringstream ss;
    ss << val;
    return ss.str();
}

void DataTypeDateV2::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    UInt32 int_val =
            assert_cast<const ColumnUInt32&>(*column.convert_to_full_column_if_const().get())
                    .get_data()[row_num];
    DateV2Value<DateV2ValueType> value = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);

    char buf[64];
    char* pos = value.to_string(buf);
    // DateTime to_string the end is /0
    ostr.write(buf, pos - buf - 1);
}

MutableColumnPtr DataTypeDateV2::create_column() const {
    return DataTypeNumberBase<UInt32>::create_column();
}

void DataTypeDateV2::cast_to_date_time(const UInt32 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)to;
    auto& from_value = (DateV2Value<DateV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATETIME);
}

void DataTypeDateV2::cast_to_date(const UInt32 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)(to);
    auto& from_value = (DateV2Value<DateV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATE);
}

void DataTypeDateV2::cast_to_date_time_v2(const UInt32 from, UInt64& to) {
    to = ((UInt64)from) << TIME_PART_LENGTH;
}

void DataTypeDateV2::cast_from_date(const Int64 from, UInt32& to) {
    auto& to_value = (DateV2Value<DateV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.set_time(from_value.year(), from_value.month(), from_value.day(), 0, 0, 0, 0);
}

void DataTypeDateV2::cast_from_date_time(const Int64 from, UInt32& to) {
    auto& to_value = (DateV2Value<DateV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.set_time(from_value.year(), from_value.month(), from_value.day(), 0, 0, 0, 0);
}

bool DataTypeDateTimeV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeDateTimeV2::to_string(const IColumn& column, size_t row_num) const {
    UInt64 int_val =
            assert_cast<const ColumnUInt64&>(*column.convert_to_full_column_if_const().get())
                    .get_data()[row_num];
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf, scale_);
    return std::string(buf, pos - buf - 1);
}

void DataTypeDateTimeV2::to_string(const IColumn& column, size_t row_num,
                                   BufferWritable& ostr) const {
    UInt64 int_val =
            assert_cast<const ColumnUInt64&>(*column.convert_to_full_column_if_const().get())
                    .get_data()[row_num];
    DateV2Value<DateTimeV2ValueType> value =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    char* pos = value.to_string(buf, scale_);
    // DateTime to_string the end is /0
    ostr.write(buf, pos - buf - 1);
}

MutableColumnPtr DataTypeDateTimeV2::create_column() const {
    return DataTypeNumberBase<UInt64>::create_column();
}

void DataTypeDateTimeV2::cast_to_date_time(const UInt64 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)to;
    auto& from_value = (DateV2Value<DateTimeV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATETIME);
}

void DataTypeDateTimeV2::cast_to_date(const UInt64 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)(to);
    auto& from_value = (DateV2Value<DateTimeV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATE);
}

void DataTypeDateTimeV2::cast_from_date(const Int64 from, UInt64& to) {
    auto& to_value = (DateV2Value<DateTimeV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.set_time(from_value.year(), from_value.month(), from_value.day(), from_value.hour(),
                      from_value.minute(), from_value.second(), 0);
}

void DataTypeDateTimeV2::cast_from_date_time(const Int64 from, UInt64& to) {
    auto& to_value = (DateV2Value<DateTimeV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.set_time(from_value.year(), from_value.month(), from_value.day(), from_value.hour(),
                      from_value.minute(), from_value.second(), 0);
}

void DataTypeDateTimeV2::cast_to_date_v2(const UInt64 from, UInt32& to) {
    to = from >> TIME_PART_LENGTH;
}
} // namespace doris::vectorized
