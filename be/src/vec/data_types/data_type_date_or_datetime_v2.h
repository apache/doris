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

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <cstdint>
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
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
/**
 * Use UInt32 as underlying type to represent DateV2 type.
 * Specifically, a dateV2 type is represented as (YYYY (23 bits), MM (4 bits), dd (5 bits)).
 */
class DataTypeDateV2 final : public DataTypeNumberBase<PrimitiveType::TYPE_DATEV2> {
public:
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_DATEV2; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATEV2;
    }
    const std::string get_family_name() const override { return "DateV2"; }
    std::string do_get_name() const override { return "DateV2"; }

    using SerDeType = DataTypeDateV2SerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }

    Field get_field(const TExprNode& node) const override {
        DateV2Value<DateV2ValueType> value;
        if (value.from_date_str(node.date_literal.value.c_str(),
                                cast_set<Int32>(node.date_literal.value.size()))) {
            return Field::create_field<TYPE_DATEV2>(value.to_date_int_val());
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type DateV2", node.date_literal.value);
        }
    }
    bool equals(const IDataType& rhs) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<PrimitiveType::TYPE_DATEV2>::template to_string_batch_impl<
                DataTypeDateV2>(column, column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const UInt32& num) const;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    std::string to_string(UInt32 int_val) const;

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
class DataTypeDateTimeV2 final : public DataTypeNumberBase<PrimitiveType::TYPE_DATETIMEV2> {
public:
    static constexpr bool is_parametric = true;

    DataTypeDateTimeV2(UInt32 scale = 0) : _scale(scale) {
        if (UNLIKELY(scale > 6)) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Scale {} is out of bounds", scale);
        }
    }

    DataTypeDateTimeV2(const DataTypeDateTimeV2& rhs) : _scale(rhs._scale) {}
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_DATETIMEV2; }
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override {
        scalar_type->set_scale(_scale);
    }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
    }
    const std::string get_family_name() const override { return "DateTimeV2"; }
    std::string do_get_name() const override { return "DateTimeV2"; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<PrimitiveType::TYPE_DATETIMEV2>::template to_string_batch_impl<
                DataTypeDateTimeV2>(column, column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const UInt64& num) const;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    std::string to_string(UInt64 int_val) const;
    using SerDeType = DataTypeDateTimeV2SerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(_scale, nesting_level);
    };

    Field get_field(const TExprNode& node) const override {
        DateV2Value<DateTimeV2ValueType> value;
        const int32_t scale =
                node.type.types.empty() ? -1 : node.type.types.front().scalar_type.scale;
        if (value.from_date_str(node.date_literal.value.c_str(),
                                cast_set<int32_t>(node.date_literal.value.size()), scale)) {
            return Field::create_field<TYPE_DATETIMEV2>(value.to_date_int_val());
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type DateTimeV2({})",
                                   node.date_literal.value, _scale);
        }
    }
    MutableColumnPtr create_column() const override;

    UInt32 get_scale() const override { return _scale; }

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;

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
#include "common/compile_check_end.h"
} // namespace doris::vectorized
