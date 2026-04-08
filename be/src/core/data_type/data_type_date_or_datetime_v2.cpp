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

#include "core/data_type/data_type_date_or_datetime_v2.h"

#include <gen_cpp/data.pb.h>

#include <memory>
#include <ostream>
#include <string>
#include <typeinfo>
#include <utility>

#include "core/assert_cast.h"
#include "core/binary_cast.hpp"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_datetimev2_impl.hpp"
#include "exprs/function/cast/cast_to_datev2_impl.hpp"
#include "exprs/function/cast/cast_to_string.h"
#include "util/io_helper.h"

namespace doris {
#include "common/compile_check_begin.h"
class IColumn;
} // namespace doris

// FIXME: This file contains widespread UB due to unsafe type-punning casts.
//        These must be properly refactored to eliminate reliance on reinterpret-style behavior.
//
// Temporarily suppress GCC 15+ warnings on user-defined type casts to allow build to proceed.
#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-user-defined"
#endif

namespace doris {

Field DataTypeDateV2::get_field(const TExprNode& node) const {
    DateV2Value<DateV2ValueType> value;
    CastParameters params;
    if (CastToDateV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                {node.date_literal.value.c_str(), node.date_literal.value.size()}, value, nullptr,
                params)) {
        return Field::create_field<TYPE_DATEV2>(std::move(value));
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type DateV2", node.date_literal.value);
    }
}

Field DataTypeDateTimeV2::get_field(const TExprNode& node) const {
    DateV2Value<DateTimeV2ValueType> value;
    const int32_t scale = node.type.types.empty() ? -1 : node.type.types.front().scalar_type.scale;
    CastParameters params;
    if (CastToDatetimeV2::from_string_strict_mode<DatelikeParseMode::STRICT>(
                {node.date_literal.value.c_str(), node.date_literal.value.size()}, value, nullptr,
                scale, params)) {
        return Field::create_field<TYPE_DATETIMEV2>(std::move(value));
    } else {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type DateTimeV2({})", node.date_literal.value,
                               _scale);
    }
}

bool DataTypeDateV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

MutableColumnPtr DataTypeDateV2::create_column() const {
    return DataTypeNumberBase<PrimitiveType::TYPE_DATEV2>::create_column();
}

void DataTypeDateV2::cast_to_date_time(const DateV2Value<DateV2ValueType> from,
                                       VecDateTimeValue& to) {
    auto& to_value = (VecDateTimeValue&)to;
    auto& from_value = (DateV2Value<DateV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATETIME);
}

void DataTypeDateV2::cast_to_date(const DateV2Value<DateV2ValueType> from, VecDateTimeValue& to) {
    auto& to_value = (VecDateTimeValue&)(to);
    auto& from_value = (DateV2Value<DateV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATE);
}

void DataTypeDateV2::cast_to_date_time_v2(const DateV2Value<DateV2ValueType> from,
                                          DateV2Value<DateTimeV2ValueType>& to) {
    to = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>((UInt64)from.to_date_int_val()
                                                               << TIME_PART_LENGTH);
}

void DataTypeDateV2::cast_from_date(const VecDateTimeValue from, DateV2Value<DateV2ValueType>& to) {
    auto& to_value = (DateV2Value<DateV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(), 0, 0, 0,
                                0);
}

void DataTypeDateV2::cast_from_date_time(const VecDateTimeValue from,
                                         DateV2Value<DateV2ValueType>& to) {
    auto& to_value = (DateV2Value<DateV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(), 0, 0, 0,
                                0);
}

bool DataTypeDateTimeV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this) &&
           _scale == static_cast<const DataTypeDateTimeV2&>(rhs)._scale;
}

void DataTypeDateTimeV2::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->mutable_decimal_param()->set_scale(_scale);
}

MutableColumnPtr DataTypeDateTimeV2::create_column() const {
    return DataTypeNumberBase<PrimitiveType::TYPE_DATETIMEV2>::create_column();
}

void DataTypeDateTimeV2::cast_to_date_time(const DateV2Value<DateTimeV2ValueType> from,
                                           VecDateTimeValue& to) {
    auto& to_value = (VecDateTimeValue&)to;
    auto& from_value = (DateV2Value<DateTimeV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATETIME);
}

void DataTypeDateTimeV2::cast_to_date(const DateV2Value<DateTimeV2ValueType> from,
                                      VecDateTimeValue& to) {
    auto& to_value = (VecDateTimeValue&)(to);
    auto& from_value = (DateV2Value<DateTimeV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATE);
}

void DataTypeDateTimeV2::cast_from_date(const VecDateTimeValue from,
                                        DateV2Value<DateTimeV2ValueType>& to) {
    auto& to_value = (DateV2Value<DateTimeV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(),
                                from_value.hour(), from_value.minute(), from_value.second(), 0);
}

void DataTypeDateTimeV2::cast_from_date_time(const VecDateTimeValue from,
                                             DateV2Value<DateTimeV2ValueType>& to) {
    auto& to_value = (DateV2Value<DateTimeV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(),
                                from_value.hour(), from_value.minute(), from_value.second(), 0);
}

FieldWithDataType DataTypeDateTimeV2::get_field_with_data_type(const IColumn& column,
                                                               size_t row_num) const {
    const auto& column_data =
            assert_cast<const ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(column);
    Field field;
    column_data.get(row_num, field);
    return FieldWithDataType {.field = std::move(field),
                              .base_scalar_type_id = get_primitive_type(),
                              .precision = -1,
                              .scale = static_cast<int>(get_scale())};
}

void DataTypeDateTimeV2::cast_to_date_v2(const DateV2Value<DateTimeV2ValueType> from,
                                         DateV2Value<DateV2ValueType>& to) {
    to = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(
            UInt32(from.to_date_int_val() >> TIME_PART_LENGTH));
}

DataTypePtr create_datetimev2(UInt64 scale_value) {
    if (scale_value > 6) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "scale_value {} > 6",
                               scale_value);
    }
    return std::make_shared<DataTypeDateTimeV2>(scale_value);
}

} // namespace doris

#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic pop
#endif
