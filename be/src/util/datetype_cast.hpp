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

#pragma once

#include <type_traits>

#include "vec/columns/columns_number.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/runtime/vdatetime_value.h"

/*
 * We use these function family to clarify our types of datelike type. for example:
 *      DataTypeDate -------------------> ColumnDate -----------------------> Int64
 *           |          TypeToColumn                    ValueTypeOfColumn
 *           | TypeToValueType
 *      VecDateTimeValue
 */
namespace doris::date_cast {

// e.g. DataTypeDate -> ColumnDate
template <typename DataType>
struct TypeToColumn {};
template <>
struct TypeToColumn<vectorized::DataTypeDate> {
    using type = vectorized::ColumnDate;
};
template <>
struct TypeToColumn<vectorized::DataTypeDateTime> {
    using type = vectorized::ColumnDateTime;
};
template <>
struct TypeToColumn<vectorized::DataTypeDateV2> {
    using type = vectorized::ColumnDateV2;
};
template <>
struct TypeToColumn<vectorized::DataTypeDateTimeV2> {
    using type = vectorized::ColumnDateTimeV2;
};

template <typename DataType>
using TypeToColumnV = TypeToColumn<DataType>::type;

// e.g. DateTypeDate -> VecDateTimeValue
template <typename DataType>
struct TypeToValueType {};
template <>
struct TypeToValueType<vectorized::DataTypeDate> {
    using type = VecDateTimeValue;
};
template <>
struct TypeToValueType<vectorized::DataTypeDateTime> {
    using type = VecDateTimeValue;
};
template <>
struct TypeToValueType<vectorized::DataTypeDateV2> {
    using type = DateV2Value<DateV2ValueType>;
};
template <>
struct TypeToValueType<vectorized::DataTypeDateTimeV2> {
    using type = DateV2Value<DateTimeV2ValueType>;
};

template <typename DataType>
using TypeToValueTypeV = TypeToValueType<DataType>::type;

// e.g. ColumnDate -> Int64 (see also columns_number.h)
template <typename ColumnType>
    requires requires { typename ColumnType::value_type; }
struct ValueTypeOfColumn {
    using type = ColumnType::value_type;
};

template <typename ColumnType>
using ValueTypeOfColumnV = ValueTypeOfColumn<ColumnType>::type;

// check V1 or V2 for both DateType/ColumnType/ValueType/ValueOfColumnType
template <typename Type>
constexpr bool IsV1() {
    return static_cast<bool>(std::is_same_v<Type, vectorized::DataTypeDate> ||
                             std::is_same_v<Type, vectorized::DataTypeDateTime> ||
                             std::is_same_v<Type, VecDateTimeValue> ||
                             std::is_same_v<Type, vectorized::ColumnDate> ||
                             std::is_same_v<Type, vectorized::ColumnDateTime> ||
                             std::is_same_v<Type, vectorized::Int64>);
}

template <typename Type>
constexpr bool IsV2() {
    return !IsV1<Type>();
}

} // namespace doris::date_cast
