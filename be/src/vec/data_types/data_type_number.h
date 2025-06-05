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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeNumber.h
// and modified by Doris

#pragma once

#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_number_base.h"

namespace doris::vectorized {

template <PrimitiveType T>
class DataTypeNumber final : public DataTypeNumberBase<T> {
public:
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
    using FieldType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<T>::template to_string_batch_impl<DataTypeNumber<T>>(column, column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars,
                     const typename PrimitiveTypeTraits<T>::ColumnItemType& num) const;
};

using DataTypeInt8 = DataTypeNumber<TYPE_TINYINT>;
using DataTypeInt16 = DataTypeNumber<TYPE_SMALLINT>;
using DataTypeInt32 = DataTypeNumber<TYPE_INT>;
using DataTypeInt64 = DataTypeNumber<TYPE_BIGINT>;
using DataTypeInt128 = DataTypeNumber<TYPE_LARGEINT>;
using DataTypeFloat32 = DataTypeNumber<TYPE_FLOAT>;
using DataTypeFloat64 = DataTypeNumber<TYPE_DOUBLE>;
using DataTypeBool = DataTypeNumber<TYPE_BOOLEAN>;
using DataTypeUInt8 = DataTypeNumber<TYPE_BOOLEAN>;

template <typename DataType>
constexpr bool IsDataTypeNumber = false;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeBool> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeInt8> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeInt16> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeInt32> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeInt64> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeInt128> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeFloat32> = true;
template <>
inline constexpr bool IsDataTypeNumber<DataTypeFloat64> = true;

} // namespace doris::vectorized
