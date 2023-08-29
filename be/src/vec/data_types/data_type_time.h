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
#include <memory>
#include <string>

#include "runtime/define_primitive_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_time_serde.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class DataTypeTime final : public DataTypeNumberBase<Float64> {
public:
    DataTypeTime() = default;

    bool equals(const IDataType& rhs) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_TIME);
    }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::TIME;
    }
    doris::FieldType get_type_as_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_TIMEV2;
    }

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    MutableColumnPtr create_column() const override;

    bool is_summable() const override { return true; }
    bool can_be_used_in_bit_operations() const override { return true; }
    bool can_be_used_in_boolean_context() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    DataTypeSerDeSPtr get_serde() const override { return std::make_shared<DataTypeTimeSerDe>(); };
    TypeIndex get_type_id() const override { return TypeIndex::Time; }
    const char* get_family_name() const override { return "time"; }
};

class DataTypeTimeV2 final : public DataTypeNumberBase<Float64> {
public:
    DataTypeTimeV2(int scale = 0) : _scale(scale) {
        if (UNLIKELY(scale > 6)) {
            LOG(FATAL) << fmt::format("Scale {} is out of bounds", scale);
        }
        if (scale == -1) {
            _scale = 0;
        }
    }
    bool equals(const IDataType& rhs) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_TIMEV2);
    }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::TIMEV2;
    }

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    MutableColumnPtr create_column() const override;

    bool is_summable() const override { return true; }
    bool can_be_used_in_bit_operations() const override { return true; }
    bool can_be_used_in_boolean_context() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    void to_pb_column_meta(PColumnMeta* col_meta) const override;
    DataTypeSerDeSPtr get_serde() const override {
        return std::make_shared<DataTypeTimeV2SerDe>(_scale);
    };
    TypeIndex get_type_id() const override { return TypeIndex::TimeV2; }
    const char* get_family_name() const override { return "timev2"; }
    UInt32 get_scale() const override { return _scale; }

private:
    UInt32 _scale;
};
} // namespace doris::vectorized
