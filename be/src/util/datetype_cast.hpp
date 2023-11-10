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

#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_time_v2.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::date_cast {

// DataTypeDate -> ColumnDate
template <typename DataType>
struct DatatypeToColumn {};
template <>
struct DatatypeToColumn<vectorized::DataTypeDate> {
    using type = vectorized::ColumnDate;
};
template <>
struct DatatypeToColumn<vectorized::DataTypeDateTime> {
    using type = vectorized::ColumnDateTime;
};
template <>
struct DatatypeToColumn<vectorized::DataTypeDateV2> {
    using type = vectorized::ColumnDateV2;
};
template <>
struct DatatypeToColumn<vectorized::DataTypeDateTimeV2> {
    using type = vectorized::ColumnDateTimeV2;
};

template <typename DataType>
using DateToColumnV = DatatypeToColumn<DataType>::type;

// DateTypeDate -> VecDateTimeValue
template <typename DataType>
struct DateToDateValueType {};
template <>
struct DateToDateValueType<vectorized::DataTypeDate> {
    using type = VecDateTimeValue;
};
template <>
struct DateToDateValueType<vectorized::DataTypeDateTime> {
    using type = VecDateTimeValue;
};
template <>
struct DateToDateValueType<vectorized::DataTypeDateV2> {
    using type = DateV2Value<DateV2ValueType>;
};
template <>
struct DateToDateValueType<vectorized::DataTypeDateTimeV2> {
    using type = DateV2Value<DateTimeV2ValueType>;
};

template <typename DataType>
using DateToDateValueTypeV = DateToDateValueType<DataType>::type;

// ColumnDate -> Int64 (see also columns_number.h)
template <typename ColumnType>
    requires requires { typename ColumnType::value_type; }
struct ValueTypeOfDateColumn {
    using type = ColumnType::value_type;
};

template <typename ColumnType>
using ValueTypeOfDateColumnV = ValueTypeOfDateColumn<ColumnType>::type;

} // namespace doris::date_cast
