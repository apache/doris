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

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <string>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_datetimev2_serde.h"
#include "vec/data_types/serde/data_type_datev2_serde.h"
#include "vec/data_types/serde/data_type_number_serde.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class ReadBuffer;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/**
 * Use UInt32 as underlying type to represent DateV2 type.
 * Specifically, a dateV2 type is represented as (YYYY (23 bits), MM (4 bits), dd (5 bits)).
 */
class DataTypeDateV2 final : public DataTypeNumberBase<UInt32> {
public:
    TypeIndex get_type_id() const override { return TypeIndex::DateV2; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_DATEV2);
    }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATEV2;
    }
    const char* get_family_name() const override { return "DateV2"; }
    std::string do_get_name() const override { return "DateV2"; }

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeDateV2SerDe>(nesting_level);
    }

    Field get_field(const TExprNode& node) const override {
        DateV2Value<DateV2ValueType> value;
        if (value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            return value.to_date_int_val();
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type DateV2", node.date_literal.value);
        }
    }
    bool equals(const IDataType& rhs) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<UInt32>::template to_string_batch_impl<DataTypeDateV2>(column,
                                                                                  column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const UInt32& num) const;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    std::string to_string(UInt32 int_val) const;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;

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

    DataTypeDateTimeV2(UInt32 scale = 0) : _scale(scale) {
        if (UNLIKELY(scale > 6)) {
            LOG(FATAL) << fmt::format("Scale {} is out of bounds", scale);
        }
    }

    DataTypeDateTimeV2(const DataTypeDateTimeV2& rhs) : _scale(rhs._scale) {}
    TypeIndex get_type_id() const override { return TypeIndex::DateTimeV2; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_DATETIMEV2);
    }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
    }
    const char* get_family_name() const override { return "DateTimeV2"; }
    std::string do_get_name() const override { return "DateTimeV2"; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<UInt64>::template to_string_batch_impl<DataTypeDateTimeV2>(column,
                                                                                      column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const UInt64& num) const;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    std::string to_string(UInt64 int_val) const;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeDateTimeV2SerDe>(_scale, nesting_level);
    };

    Field get_field(const TExprNode& node) const override {
        DateV2Value<DateTimeV2ValueType> value;
        const int32_t scale =
                node.type.types.empty() ? -1 : node.type.types.front().scalar_type.scale;
        if (value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size(),
                                scale)) {
            return value.to_date_int_val();
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type DateTimeV2",
                                   node.date_literal.value);
        }
    }
    MutableColumnPtr create_column() const override;

    UInt32 get_scale() const override { return _scale; }

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    static void cast_to_date(const UInt64 from, Int64& to);
    static void cast_to_date_time(const UInt64 from, Int64& to);
    static void cast_to_date_v2(const UInt64 from, UInt32& to);
    static void cast_from_date(const Int64 from, UInt64& to);
    static void cast_from_date_time(const Int64 from, UInt64& to);

private:
    UInt32 _scale;
};

DataTypePtr create_datetimev2(UInt64 scale);

template <typename DataType>
constexpr bool IsDataTypeDateTimeV2 = false;
template <>
inline constexpr bool IsDataTypeDateTimeV2<DataTypeDateTimeV2> = true;

} // namespace doris::vectorized
