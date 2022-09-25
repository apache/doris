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

#include "vec/data_types/data_type_number_base.h"

namespace doris::vectorized {

/**
 * Use UInt32 as underlying type to represent DateV2 type.
 * Specifically, a dateV2 type is represented as (YYYY (23 bits), MM (4 bits), dd (5 bits)).
 */
class DataTypeDateV2 final : public DataTypeNumberBase<UInt32> {
public:
    TypeIndex get_type_id() const override { return TypeIndex::DateV2; }
    const char* get_family_name() const override { return "DateV2"; }
    std::string do_get_name() const override { return "DateV2"; }

    bool can_be_used_as_version() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    MutableColumnPtr create_column() const override;

    static void cast_to_date(const UInt32 from, Int64& to);
    static void cast_to_date_time(const UInt32 from, Int64& to);
    static void cast_to_date_time_v2(const UInt32 from, UInt64& to);
    static void cast_from_date(const Int64 from, UInt32& to);
    static void cast_from_date_time(const Int64 from, UInt32& to);
};

/**
 * Use UInt64 as underlying type to represent DateTimeV2 type.
 *                                                    +---------------date part---------------+-----------------------time part------------------------+
 *                                                    |                  27 bits              |                         37 bits                        |
 * Specifically, a dateTimeV2 type is represented as (YYYY (18 bits), MM (4 bits), dd (5 bits), HH (5 bits), mm (6 bits), SS (6 bits), ssssss (20 bits)).
 */
class DataTypeDateTimeV2 final : public DataTypeNumberBase<UInt64> {
public:
    static constexpr bool is_parametric = true;

    DataTypeDateTimeV2(UInt32 scale = 0) : scale_(scale) {
        if (UNLIKELY(scale > 6)) {
            LOG(FATAL) << fmt::format("Scale {} is out of bounds", scale);
        }
    }

    DataTypeDateTimeV2(const DataTypeDateTimeV2& rhs) : scale_(rhs.scale_) {}
    TypeIndex get_type_id() const override { return TypeIndex::DateTimeV2; }
    const char* get_family_name() const override { return "DateTimeV2"; }
    std::string do_get_name() const override { return "DateTimeV2"; }

    bool can_be_used_as_version() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    MutableColumnPtr create_column() const override;

    const UInt32 get_scale() const { return scale_; }

    static void cast_to_date(const UInt64 from, Int64& to);
    static void cast_to_date_time(const UInt64 from, Int64& to);
    static void cast_to_date_v2(const UInt64 from, UInt32& to);
    static void cast_from_date(const Int64 from, UInt64& to);
    static void cast_from_date_time(const Int64 from, UInt64& to);

private:
    UInt32 scale_;
};

template <typename DataType>
constexpr bool IsDataTypeDateTimeV2 = false;
template <>
inline constexpr bool IsDataTypeDateTimeV2<DataTypeDateTimeV2> = true;

} // namespace doris::vectorized
