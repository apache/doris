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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/callOnTypeIndex.h
// and modified by Doris

#pragma once

#include <utility>

#include "vec/core/types.h"
#include "vec/data_types/data_type_time.h"

namespace doris::vectorized {

template <typename T, typename U>
struct TypePair {
    using LeftType = T;
    using RightType = U;
};

template <typename T, bool _int, bool _float, bool _decimal, bool _datetime, typename F>
bool call_on_basic_type(PrimitiveType number, F&& f) {
    if constexpr (_int) {
        switch (number) {
        case PrimitiveType::TYPE_BOOLEAN:
            return f(TypePair<T, UInt8>());
        case PrimitiveType::TYPE_TINYINT:
            return f(TypePair<T, Int8>());
        case PrimitiveType::TYPE_SMALLINT:
            return f(TypePair<T, Int16>());
        case PrimitiveType::TYPE_INT:
            return f(TypePair<T, Int32>());
        case PrimitiveType::TYPE_BIGINT:
            return f(TypePair<T, Int64>());
        case PrimitiveType::TYPE_LARGEINT:
            return f(TypePair<T, Int128>());

        default:
            break;
        }
    }

    if constexpr (_decimal) {
        switch (number) {
        case PrimitiveType::TYPE_DECIMAL32:
            return f(TypePair<T, Decimal32>());
        case PrimitiveType::TYPE_DECIMAL64:
            return f(TypePair<T, Decimal64>());
        case PrimitiveType::TYPE_DECIMALV2:
            return f(TypePair<T, Decimal128V2>());
        case PrimitiveType::TYPE_DECIMAL128I:
            return f(TypePair<T, Decimal128V3>());
        case PrimitiveType::TYPE_DECIMAL256:
            return f(TypePair<T, Decimal256>());
        default:
            break;
        }
    }

    if constexpr (_float) {
        switch (number) {
        case PrimitiveType::TYPE_FLOAT:
            return f(TypePair<T, Float32>());
        case PrimitiveType::TYPE_DOUBLE:
            return f(TypePair<T, Float64>());
        default:
            break;
        }
    }

    return false;
}

/// Unroll template using TypeIndex
template <bool _int, bool _float, bool _decimal, bool _datetime, typename F>
bool call_on_basic_types(PrimitiveType type_num1, PrimitiveType type_num2, F&& f) {
    if constexpr (_int) {
        switch (type_num1) {
        case PrimitiveType::TYPE_BOOLEAN:
            return call_on_basic_type<UInt8, _int, _float, _decimal, _datetime>(type_num2,
                                                                                std::forward<F>(f));
        case PrimitiveType::TYPE_TINYINT:
            return call_on_basic_type<Int8, _int, _float, _decimal, _datetime>(type_num2,
                                                                               std::forward<F>(f));
        case PrimitiveType::TYPE_SMALLINT:
            return call_on_basic_type<Int16, _int, _float, _decimal, _datetime>(type_num2,
                                                                                std::forward<F>(f));
        case PrimitiveType::TYPE_INT:
            return call_on_basic_type<Int32, _int, _float, _decimal, _datetime>(type_num2,
                                                                                std::forward<F>(f));
        case PrimitiveType::TYPE_BIGINT:
            return call_on_basic_type<Int64, _int, _float, _decimal, _datetime>(type_num2,
                                                                                std::forward<F>(f));
        case PrimitiveType::TYPE_LARGEINT:
            return call_on_basic_type<Int128, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        default:
            break;
        }
    }

    if constexpr (_decimal) {
        switch (type_num1) {
        case PrimitiveType::TYPE_DECIMAL32:
            return call_on_basic_type<Decimal32, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        case PrimitiveType::TYPE_DECIMAL64:
            return call_on_basic_type<Decimal64, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        case PrimitiveType::TYPE_DECIMALV2:
            return call_on_basic_type<Decimal128V2, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        case PrimitiveType::TYPE_DECIMAL128I:
            return call_on_basic_type<Decimal128V3, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        case PrimitiveType::TYPE_DECIMAL256:
            return call_on_basic_type<Decimal256, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        default:
            break;
        }
    }

    if constexpr (_float) {
        switch (type_num1) {
        case PrimitiveType::TYPE_FLOAT:
            return call_on_basic_type<Float32, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        case PrimitiveType::TYPE_DOUBLE:
            return call_on_basic_type<Float64, _int, _float, _decimal, _datetime>(
                    type_num2, std::forward<F>(f));
        default:
            break;
        }
    }

    return false;
}

class DataTypeDate;
class DataTypeDateV2;
class DataTypeDateTimeV2;
class DataTypeDateTime;
class DataTypeIPv4;
class DataTypeIPv6;
class DataTypeString;
template <typename T>
class DataTypeEnum;
template <typename T>
class DataTypeNumber;
template <typename T>
class DataTypeDecimal;

template <typename T, typename F>
bool call_on_index_and_data_type(PrimitiveType number, F&& f) {
    switch (number) {
    case PrimitiveType::TYPE_BOOLEAN:
        return f(TypePair<DataTypeNumber<UInt8>, T>());
    case PrimitiveType::TYPE_TINYINT:
        return f(TypePair<DataTypeNumber<Int8>, T>());
    case PrimitiveType::TYPE_SMALLINT:
        return f(TypePair<DataTypeNumber<Int16>, T>());
    case PrimitiveType::TYPE_INT:
        return f(TypePair<DataTypeNumber<Int32>, T>());
    case PrimitiveType::TYPE_BIGINT:
        return f(TypePair<DataTypeNumber<Int64>, T>());
    case PrimitiveType::TYPE_LARGEINT:
        return f(TypePair<DataTypeNumber<Int128>, T>());

    case PrimitiveType::TYPE_FLOAT:
        return f(TypePair<DataTypeNumber<Float32>, T>());
    case PrimitiveType::TYPE_DOUBLE:
        return f(TypePair<DataTypeNumber<Float64>, T>());
    case PrimitiveType::TYPE_TIME:
        return f(TypePair<DataTypeTimeV2, T>());
    case PrimitiveType::TYPE_TIMEV2:
        return f(TypePair<DataTypeTimeV2, T>());
    case PrimitiveType::TYPE_DECIMAL32:
        return f(TypePair<DataTypeDecimal<Decimal32>, T>());
    case PrimitiveType::TYPE_DECIMAL64:
        return f(TypePair<DataTypeDecimal<Decimal64>, T>());
    case PrimitiveType::TYPE_DECIMALV2:
        return f(TypePair<DataTypeDecimal<Decimal128V2>, T>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return f(TypePair<DataTypeDecimal<Decimal128V3>, T>());
    case PrimitiveType::TYPE_DECIMAL256:
        return f(TypePair<DataTypeDecimal<Decimal256>, T>());

    case PrimitiveType::TYPE_DATE:
        return f(TypePair<DataTypeDate, T>());
    case PrimitiveType::TYPE_DATEV2:
        return f(TypePair<DataTypeDateV2, T>());
    case PrimitiveType::TYPE_DATETIMEV2:
        return f(TypePair<DataTypeDateTimeV2, T>());
    case PrimitiveType::TYPE_DATETIME:
        return f(TypePair<DataTypeDateTime, T>());

    case PrimitiveType::TYPE_IPV4:
        return f(TypePair<DataTypeIPv4, T>());
    case PrimitiveType::TYPE_IPV6:
        return f(TypePair<DataTypeIPv6, T>());

    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return f(TypePair<DataTypeString, T>());

    default:
        break;
    }
    return false;
}

template <typename T, typename F>
bool call_on_index_and_number_data_type(PrimitiveType number, F&& f) {
    switch (number) {
    case PrimitiveType::TYPE_BOOLEAN:
        return f(TypePair<DataTypeNumber<UInt8>, T>());
    case PrimitiveType::TYPE_TINYINT:
        return f(TypePair<DataTypeNumber<Int8>, T>());
    case PrimitiveType::TYPE_SMALLINT:
        return f(TypePair<DataTypeNumber<Int16>, T>());
    case PrimitiveType::TYPE_INT:
        return f(TypePair<DataTypeNumber<Int32>, T>());
    case PrimitiveType::TYPE_BIGINT:
        return f(TypePair<DataTypeNumber<Int64>, T>());
    case PrimitiveType::TYPE_LARGEINT:
        return f(TypePair<DataTypeNumber<Int128>, T>());
    case PrimitiveType::TYPE_FLOAT:
        return f(TypePair<DataTypeNumber<Float32>, T>());
    case PrimitiveType::TYPE_DOUBLE:
        return f(TypePair<DataTypeNumber<Float64>, T>());
    case PrimitiveType::TYPE_TIMEV2:
        return f(TypePair<DataTypeTimeV2, T>());
    case PrimitiveType::TYPE_DECIMAL32:
        return f(TypePair<DataTypeDecimal<Decimal32>, T>());
    case PrimitiveType::TYPE_DECIMAL64:
        return f(TypePair<DataTypeDecimal<Decimal64>, T>());
    case PrimitiveType::TYPE_DECIMALV2:
        return f(TypePair<DataTypeDecimal<Decimal128V2>, T>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return f(TypePair<DataTypeDecimal<Decimal128V3>, T>());
    case PrimitiveType::TYPE_DECIMAL256:
        return f(TypePair<DataTypeDecimal<Decimal256>, T>());
    default:
        break;
    }
    return false;
}

} // namespace doris::vectorized
