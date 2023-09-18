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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDateTime.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_date64_serde.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class ReadBuffer;
class IColumn;
class DataTypeDate;
class DataTypeDateV2;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** DateTime stores time as unix timestamp.
	* The value itself is independent of time zone.
	*
	* In binary format it is represented as unix timestamp.
	* In text format it is serialized to and parsed from YYYY-MM-DD hh:mm:ss format.
	* The text format is dependent of time zone.
	*
	* To convert from/to text format, time zone may be specified explicitly or implicit time zone may be used.
	*
	* Time zone may be specified explicitly as type parameter, example: DateTime('Europe/Moscow').
	* As it does not affect the internal representation of values,
	*  all types with different time zones are equivalent and may be used interchangingly.
	* Time zone only affects parsing and displaying in text formats.
	*
	* If time zone is not specified (example: DateTime without parameter), then default time zone is used.
	* Default time zone is server time zone, if server is doing transformations
	*  and if client is doing transformations, unless 'use_client_time_zone' setting is passed to client;
	* Server time zone is the time zone specified in 'timezone' parameter in configuration file,
	*  or system time zone at the moment of server startup.
	*/
class DataTypeDateTime final : public DataTypeNumberBase<Int64> {
public:
    DataTypeDateTime() = default;

    const char* get_family_name() const override { return "DateTime"; }
    std::string do_get_name() const override { return "DateTime"; }
    TypeIndex get_type_id() const override { return TypeIndex::DateTime; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_DATETIME);
    }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::DATETIME;
    }

    doris::FieldType get_type_as_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATETIME;
    }

    bool can_be_inside_nullable() const override { return true; }

    bool equals(const IDataType& rhs) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeDateTimeSerDe>(nesting_level);
    }

    Field get_field(const TExprNode& node) const override {
        VecDateTimeValue value;
        if (value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            value.to_datetime();
            return Int64(*reinterpret_cast<__int64_t*>(&value));
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type DateTime", node.date_literal.value);
        }
    }

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    Status from_string(ReadBuffer& rb, IColumn* column) const override;

    static void cast_to_date_time(Int64& x);

    MutableColumnPtr create_column() const override;
};

template <typename DataType>
constexpr bool IsDateTimeType = false;
template <>
inline constexpr bool IsDateTimeType<DataTypeDateTime> = true;

template <typename DataType>
constexpr bool IsDateType = false;
template <>
inline constexpr bool IsDateType<DataTypeDate> = true;

template <typename DataType>
constexpr bool IsDateV2Type = false;
template <>
inline constexpr bool IsDateV2Type<DataTypeDateV2> = true;

template <typename DataType>
constexpr bool IsDateTimeV2Type = false;
template <>
inline constexpr bool IsDateTimeV2Type<DataTypeDateTimeV2> = true;

template <typename DataType>
constexpr bool IsTimeType = IsDateTimeType<DataType> || IsDateType<DataType>;

template <typename DataType>
constexpr bool IsTimeV2Type = IsDateTimeV2Type<DataType> || IsDateV2Type<DataType>;

} // namespace doris::vectorized
