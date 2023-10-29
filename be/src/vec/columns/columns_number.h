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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnsNumber.h
// and modified by Doris

#pragma once

#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"

namespace doris::vectorized {

/** Columns with numbers. */

using ColumnUInt8 = ColumnVector<UInt8>;
using ColumnUInt16 = ColumnVector<UInt16>;
using ColumnUInt32 = ColumnVector<UInt32>;
using ColumnUInt64 = ColumnVector<UInt64>;
using ColumnUInt128 = ColumnVector<UInt128>;

using ColumnInt8 = ColumnVector<Int8>;
using ColumnInt16 = ColumnVector<Int16>;
using ColumnInt32 = ColumnVector<Int32>;
using ColumnInt64 = ColumnVector<Int64>;
using ColumnInt128 = ColumnVector<Int128>;

using ColumnDate = ColumnVector<Int64>;
using ColumnDateTime = ColumnVector<Int64>;

using ColumnDateV2 = ColumnVector<UInt32>;
using ColumnDateTimeV2 = ColumnVector<UInt64>;

using ColumnFloat32 = ColumnVector<Float32>;
using ColumnFloat64 = ColumnVector<Float64>;

using ColumnDecimal32 = ColumnDecimal<Decimal32>;
using ColumnDecimal64 = ColumnDecimal<Decimal64>;
using ColumnDecimal128 = ColumnDecimal<Decimal128>;
using ColumnDecimal128I = ColumnDecimal<Decimal128I>;
using ColumnDecimal256 = ColumnDecimal<Decimal256>;

using ColumnIPv4 = ColumnVector<IPv4>;
using ColumnIPv6 = ColumnVector<IPv6>;

template <typename T>
struct IsFixLenColumnType {
    static constexpr bool value = false;
};
template <typename... Arg>
struct IsFixLenColumnType<ColumnVector<Arg...>> {
    static constexpr bool value = true;
};
template <typename... Arg>
struct IsFixLenColumnType<ColumnDecimal<Arg...>> {
    static constexpr bool value = true;
};

} // namespace doris::vectorized
