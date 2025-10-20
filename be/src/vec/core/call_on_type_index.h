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

class DataTypeDate;
class DataTypeDateV2;
class DataTypeDateTimeV2;
class DataTypeDateTime;
class DataTypeIPv4;
class DataTypeIPv6;
class DataTypeString;
template <typename T>
class DataTypeEnum;
template <PrimitiveType T>
class DataTypeNumber;
template <PrimitiveType T>
class DataTypeDecimal;

template <typename T, typename F>
bool call_on_index_and_data_type(PrimitiveType number, F&& f) {
    switch (number) {
    case PrimitiveType::TYPE_BOOLEAN:
        return f(TypePair<DataTypeBool, T>());
    case PrimitiveType::TYPE_TINYINT:
        return f(TypePair<DataTypeInt8, T>());
    case PrimitiveType::TYPE_SMALLINT:
        return f(TypePair<DataTypeInt16, T>());
    case PrimitiveType::TYPE_INT:
        return f(TypePair<DataTypeInt32, T>());
    case PrimitiveType::TYPE_BIGINT:
        return f(TypePair<DataTypeInt64, T>());
    case PrimitiveType::TYPE_LARGEINT:
        return f(TypePair<DataTypeInt128, T>());

    case PrimitiveType::TYPE_FLOAT:
        return f(TypePair<DataTypeFloat32, T>());
    case PrimitiveType::TYPE_DOUBLE:
        return f(TypePair<DataTypeFloat64, T>());
    case PrimitiveType::TYPE_DECIMAL32:
        return f(TypePair<DataTypeDecimal32, T>());
    case PrimitiveType::TYPE_DECIMAL64:
        return f(TypePair<DataTypeDecimal64, T>());
    case PrimitiveType::TYPE_DECIMALV2:
        return f(TypePair<DataTypeDecimalV2, T>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return f(TypePair<DataTypeDecimal128, T>());
    case PrimitiveType::TYPE_DECIMAL256:
        return f(TypePair<DataTypeDecimal256, T>());

    case PrimitiveType::TYPE_DATE:
        return f(TypePair<DataTypeDate, T>());
    case PrimitiveType::TYPE_DATEV2:
        return f(TypePair<DataTypeDateV2, T>());
    case PrimitiveType::TYPE_DATETIMEV2:
        return f(TypePair<DataTypeDateTimeV2, T>());
    case PrimitiveType::TYPE_DATETIME:
        return f(TypePair<DataTypeDateTime, T>());
    case PrimitiveType::TYPE_TIMEV2:
        return f(TypePair<DataTypeTimeV2, T>());

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
        return f(TypePair<DataTypeBool, T>());
    case PrimitiveType::TYPE_TINYINT:
        return f(TypePair<DataTypeInt8, T>());
    case PrimitiveType::TYPE_SMALLINT:
        return f(TypePair<DataTypeInt16, T>());
    case PrimitiveType::TYPE_INT:
        return f(TypePair<DataTypeInt32, T>());
    case PrimitiveType::TYPE_BIGINT:
        return f(TypePair<DataTypeInt64, T>());
    case PrimitiveType::TYPE_LARGEINT:
        return f(TypePair<DataTypeInt128, T>());
    case PrimitiveType::TYPE_FLOAT:
        return f(TypePair<DataTypeFloat32, T>());
    case PrimitiveType::TYPE_DOUBLE:
        return f(TypePair<DataTypeFloat64, T>());
    case PrimitiveType::TYPE_TIMEV2:
        return f(TypePair<DataTypeTimeV2, T>());
    case PrimitiveType::TYPE_DECIMAL32:
        return f(TypePair<DataTypeDecimal32, T>());
    case PrimitiveType::TYPE_DECIMAL64:
        return f(TypePair<DataTypeDecimal64, T>());
    case PrimitiveType::TYPE_DECIMALV2:
        return f(TypePair<DataTypeDecimalV2, T>());
    case PrimitiveType::TYPE_DECIMAL128I:
        return f(TypePair<DataTypeDecimal128, T>());
    case PrimitiveType::TYPE_DECIMAL256:
        return f(TypePair<DataTypeDecimal256, T>());
    default:
        break;
    }
    return false;
}

template <PrimitiveType PT>
struct DispatchDataType {
    constexpr static auto PType = PT;
    using DataType = typename PrimitiveTypeTraits<PT>::DataType;
    using ColumnType = typename PrimitiveTypeTraits<PT>::ColumnType;
};

struct DispatchDataTypeMask {
    static constexpr uint32_t INT = 1 << 0;
    static constexpr uint32_t FLOAT = 1 << 1;
    static constexpr uint32_t DECIMAL = 1 << 2;
    static constexpr uint32_t DATETIME = 1 << 3;
    static constexpr uint32_t IP = 1 << 4;
    static constexpr uint32_t STRING = 1 << 5;

    static constexpr uint32_t SCALAR = INT | FLOAT | DECIMAL | DATETIME | IP;
    static constexpr uint32_t NUMBER = INT | FLOAT | DECIMAL;
    static constexpr uint32_t ALL = INT | FLOAT | DECIMAL | DATETIME | IP | STRING;
};

template <typename F, uint32_t TypeMaskV = DispatchDataTypeMask::ALL>
bool dispatch_type_base(PrimitiveType number, F&& f) {
    if constexpr ((TypeMaskV & DispatchDataTypeMask::INT) != 0) {
        switch (number) {
        case PrimitiveType::TYPE_BOOLEAN:
            return f(DispatchDataType<TYPE_BOOLEAN>());
        case PrimitiveType::TYPE_TINYINT:
            return f(DispatchDataType<TYPE_TINYINT>());
        case PrimitiveType::TYPE_SMALLINT:
            return f(DispatchDataType<TYPE_SMALLINT>());
        case PrimitiveType::TYPE_INT:
            return f(DispatchDataType<TYPE_INT>());
        case PrimitiveType::TYPE_BIGINT:
            return f(DispatchDataType<TYPE_BIGINT>());
        case PrimitiveType::TYPE_LARGEINT:
            return f(DispatchDataType<TYPE_LARGEINT>());
        default:
            break;
        }
    }

    if constexpr ((TypeMaskV & DispatchDataTypeMask::FLOAT) != 0) {
        switch (number) {
        case PrimitiveType::TYPE_FLOAT:
            return f(DispatchDataType<TYPE_FLOAT>());
        case PrimitiveType::TYPE_DOUBLE:
            return f(DispatchDataType<TYPE_DOUBLE>());
        default:
            break;
        }
    }

    if constexpr ((TypeMaskV & DispatchDataTypeMask::DECIMAL) != 0) {
        switch (number) {
        case PrimitiveType::TYPE_DECIMAL32:
            return f(DispatchDataType<TYPE_DECIMAL32>());
        case PrimitiveType::TYPE_DECIMAL64:
            return f(DispatchDataType<TYPE_DECIMAL64>());
        case PrimitiveType::TYPE_DECIMALV2:
            return f(DispatchDataType<TYPE_DECIMALV2>());
        case PrimitiveType::TYPE_DECIMAL128I:
            return f(DispatchDataType<TYPE_DECIMAL128I>());
        case PrimitiveType::TYPE_DECIMAL256:
            return f(DispatchDataType<TYPE_DECIMAL256>());
        default:
            break;
        }
    }

    if constexpr ((TypeMaskV & DispatchDataTypeMask::DATETIME) != 0) {
        switch (number) {
        case PrimitiveType::TYPE_DATE:
            return f(DispatchDataType<TYPE_DATE>());
        case PrimitiveType::TYPE_DATEV2:
            return f(DispatchDataType<TYPE_DATEV2>());
        case PrimitiveType::TYPE_DATETIMEV2:
            return f(DispatchDataType<TYPE_DATETIMEV2>());
        case PrimitiveType::TYPE_DATETIME:
            return f(DispatchDataType<TYPE_DATETIME>());
        case PrimitiveType::TYPE_TIMEV2:
            return f(DispatchDataType<TYPE_TIMEV2>());
        default:
            break;
        }
    }

    if constexpr ((TypeMaskV & DispatchDataTypeMask::IP) != 0) {
        switch (number) {
        case PrimitiveType::TYPE_IPV4:
            return f(DispatchDataType<TYPE_IPV4>());
        case PrimitiveType::TYPE_IPV6:
            return f(DispatchDataType<TYPE_IPV6>());
        default:
            break;
        }
    }

    if constexpr ((TypeMaskV & DispatchDataTypeMask::STRING) != 0) {
        switch (number) {
        case PrimitiveType::TYPE_STRING:
        case PrimitiveType::TYPE_CHAR:
        case PrimitiveType::TYPE_VARCHAR:
            return f(DispatchDataType<TYPE_STRING>());
        default:
            break;
        }
    }

    return false;
}

template <typename F>
bool dispatch_switch_all(PrimitiveType number, F&& f) {
    return dispatch_type_base<F, DispatchDataTypeMask::ALL>(number, std::forward<F>(f));
}

template <typename F>
bool dispatch_switch_scalar(PrimitiveType number, F&& f) {
    return dispatch_type_base<F, DispatchDataTypeMask::SCALAR>(number, std::forward<F>(f));
}

template <typename F>
bool dispatch_switch_number(PrimitiveType number, F&& f) {
    return dispatch_type_base<F, DispatchDataTypeMask::NUMBER>(number, std::forward<F>(f));
}

template <typename F>
bool dispatch_switch_decimal(PrimitiveType number, F&& f) {
    return dispatch_type_base<F, DispatchDataTypeMask::DECIMAL>(number, std::forward<F>(f));
}

} // namespace doris::vectorized
