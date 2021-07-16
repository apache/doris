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

#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_number_base.h"

class DateLUTImpl;

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
class DataTypeDateTime final : public DataTypeNumberBase<Int128> {
public:
    DataTypeDateTime();

    const char* get_family_name() const override { return "DateTime"; }
    std::string do_get_name() const override;
    TypeIndex get_type_id() const override { return TypeIndex::DateTime; }

    bool can_be_used_as_version() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    bool equals(const IDataType& rhs) const override;

    std::string to_string(const IColumn& column, size_t row_num) const;

    void to_string(const IColumn &column, size_t row_num, BufferWritable &ostr) const override;

    static void cast_to_date_time(Int128 &x);
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
constexpr bool IsTimeType = IsDateTimeType<DataType> || IsDateType<DataType>;

} // namespace doris::vectorized
