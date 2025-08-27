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

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "serde/data_type_number_serde.h"
#include "vec/columns/column_vector.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class BufferWritable;
class IColumn;

/** Implements part of the IDataType interface, common to all numbers and for Date and DateTime.
  */
template <PrimitiveType T>
class DataTypeNumberBase : public IDataType {
    static_assert(is_int_or_bool(T) || is_ip(T) || is_date_type(T) || is_float_or_double(T) ||
                  T == TYPE_TIME || T == TYPE_TIMEV2);

public:
    static constexpr bool is_parametric = false;
    static constexpr PrimitiveType PType = T;
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
    using FieldType = typename PrimitiveTypeTraits<T>::ColumnItemType;

    static std::string to_string(const typename PrimitiveTypeTraits<T>::ColumnItemType& value);

    const std::string get_family_name() const override { return type_to_string(T); }
    PrimitiveType get_primitive_type() const override {
        // Doris does not support uint8 at present, use uint8 as boolean type
        return T;
    }

    doris::FieldType get_storage_field_type() const override {
        // Doris does not support uint8 at present, use uint8 as boolean type
        if constexpr (T == TYPE_BOOLEAN) {
            return doris::FieldType::OLAP_FIELD_TYPE_BOOL;
        }
        if constexpr (T == TYPE_TINYINT) {
            return doris::FieldType::OLAP_FIELD_TYPE_TINYINT;
        }
        if constexpr (T == TYPE_SMALLINT) {
            return doris::FieldType::OLAP_FIELD_TYPE_SMALLINT;
        }
        if constexpr (T == TYPE_INT) {
            return doris::FieldType::OLAP_FIELD_TYPE_INT;
        }
        if constexpr (T == TYPE_BIGINT) {
            return doris::FieldType::OLAP_FIELD_TYPE_BIGINT;
        }
        if constexpr (T == TYPE_LARGEINT) {
            return doris::FieldType::OLAP_FIELD_TYPE_LARGEINT;
        }
        if constexpr (T == TYPE_FLOAT) {
            return doris::FieldType::OLAP_FIELD_TYPE_FLOAT;
        }
        if constexpr (T == TYPE_DOUBLE) {
            return doris::FieldType::OLAP_FIELD_TYPE_DOUBLE;
        }
        if constexpr (T == TYPE_DATE) {
            return doris::FieldType::OLAP_FIELD_TYPE_DATE;
        }
        if constexpr (T == TYPE_DATETIME) {
            return doris::FieldType::OLAP_FIELD_TYPE_DATETIME;
        }
        if constexpr (T == TYPE_DATEV2) {
            return doris::FieldType::OLAP_FIELD_TYPE_DATEV2;
        }
        if constexpr (T == TYPE_DATETIMEV2) {
            return doris::FieldType::OLAP_FIELD_TYPE_DATETIMEV2;
        }
        if constexpr (T == TYPE_IPV4) {
            return doris::FieldType::OLAP_FIELD_TYPE_IPV4;
        }
        if constexpr (T == TYPE_IPV6) {
            return doris::FieldType::OLAP_FIELD_TYPE_IPV6;
        }
        if constexpr (T == TYPE_TIMEV2) {
            return doris::FieldType::OLAP_FIELD_TYPE_TIMEV2;
        }
        throw Exception(Status::FatalError("__builtin_unreachable"));
    }

    Field get_default() const override;

    Field get_field(const TExprNode& node) const override;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return true; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return true; }
    bool is_value_represented_by_number() const override { return true; }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override {
        return sizeof(typename PrimitiveTypeTraits<T>::ColumnItemType);
    }
    bool can_be_inside_low_cardinality() const override { return true; }

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    bool is_null_literal() const override { return _is_null_literal; }
    void set_null_literal(bool flag) { _is_null_literal = flag; }
    using SerDeType = DataTypeNumberSerDe<T>;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    };

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;

protected:
    template <typename Derived>
    void to_string_batch_impl(const IColumn& column, ColumnString& column_to) const {
        // column may be column const
        const auto& col_ptr = column.get_ptr();
        const auto& [column_ptr, is_const] = unpack_if_const(col_ptr);
        if (is_const) {
            _to_string_batch_impl<Derived, true>(column_ptr, column_to);
        } else {
            _to_string_batch_impl<Derived, false>(column_ptr, column_to);
        }
    }

    template <typename Derived, bool is_const>
    void _to_string_batch_impl(const ColumnPtr& column_ptr, ColumnString& column_to) const {
        auto& col_vec = assert_cast<const ColumnType&>(*column_ptr);
        const auto size = col_vec.size();
        auto& chars = column_to.get_chars();
        auto& offsets = column_to.get_offsets();
        offsets.resize(size);
        chars.reserve(static_cast<const Derived*>(this)->number_length() * size);
        for (int row_num = 0; row_num < size; row_num++) {
            auto num = is_const ? col_vec.get_element(0) : col_vec.get_element(row_num);
            static_cast<const Derived*>(this)->push_number(chars, num);
            // push_number can check the chars is over uint32 so use static_cast here.
            offsets[row_num] = static_cast<UInt32>(chars.size());
        }
    }

private:
    bool _is_null_literal = false;
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
