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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeNumberBase.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "serde/data_type_number_serde.h"
#include "vec/columns/column_vector.h"
#include "vec/common/uint128.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class IColumn;
class ReadBuffer;
template <typename T>
struct TypeId;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** Implements part of the IDataType interface, common to all numbers and for Date and DateTime.
  */
template <typename T>
class DataTypeNumberBase : public IDataType {
    static_assert(IsNumber<T>);

public:
    static constexpr bool is_parametric = false;
    using ColumnType = ColumnVector<T>;
    using FieldType = T;

    const char* get_family_name() const override { return TypeName<T>::get(); }
    TypeIndex get_type_id() const override { return TypeId<T>::value; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        // Doris does not support uint8 at present, use uint8 as boolean type
        if constexpr (std::is_same_v<TypeId<T>, TypeId<UInt8>>) {
            return TypeDescriptor(TYPE_BOOLEAN);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int8>>) {
            return TypeDescriptor(TYPE_TINYINT);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int16>> ||
                      std::is_same_v<TypeId<T>, TypeId<UInt16>>) {
            return TypeDescriptor(TYPE_SMALLINT);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int32>> ||
                      std::is_same_v<TypeId<T>, TypeId<UInt32>>) {
            return TypeDescriptor(TYPE_INT);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int64>> ||
                      std::is_same_v<TypeId<T>, TypeId<UInt64>>) {
            return TypeDescriptor(TYPE_BIGINT);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int128>> ||
                      std::is_same_v<TypeId<T>, TypeId<Int128>>) {
            return TypeDescriptor(TYPE_LARGEINT);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Float32>>) {
            return TypeDescriptor(TYPE_FLOAT);
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Float64>>) {
            return TypeDescriptor(TYPE_DOUBLE);
        }
        return TypeDescriptor(INVALID_TYPE);
    }

    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int8>>) {
            return TPrimitiveType::TINYINT;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int16>>) {
            return TPrimitiveType::SMALLINT;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int32>>) {
            return TPrimitiveType::INT;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int64>>) {
            return TPrimitiveType::BIGINT;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Int128>>) {
            return TPrimitiveType::LARGEINT;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Float32>>) {
            return TPrimitiveType::FLOAT;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Float64>>) {
            return TPrimitiveType::DOUBLE;
        }
        LOG(FATAL) << "__builtin_unreachable";
        __builtin_unreachable();
    }
    Field get_default() const override;

    Field get_field(const TExprNode& node) const override;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    MutableColumnPtr create_column() const override;

    bool get_is_parametric() const override { return false; }
    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return true; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return true; }
    bool is_value_represented_by_number() const override { return true; }
    bool is_value_represented_by_integer() const override;
    bool is_value_represented_by_unsigned_integer() const override;
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override { return sizeof(T); }
    bool can_be_inside_low_cardinality() const override { return true; }

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;
    bool is_null_literal() const override { return _is_null_literal; }
    void set_null_literal(bool flag) { _is_null_literal = flag; }
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeNumberSerDe<T>>(nesting_level);
    };

private:
    bool _is_null_literal = false;
};

} // namespace doris::vectorized
