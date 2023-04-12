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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeObject.h
// and modified by Doris

#pragma once
#include <vec/columns/column_object.h>
#include <vec/core/field.h>
#include <vec/data_types/data_type.h>
namespace doris::vectorized {
class DataTypeObject : public IDataType {
private:
    String schema_format;
    bool is_nullable;

public:
    DataTypeObject(const String& schema_format_, bool is_nullable_);
    const char* get_family_name() const override { return "Variant"; }
    TypeIndex get_type_id() const override { return TypeIndex::VARIANT; }
    PrimitiveType get_type_as_primitive_type() const override { return TYPE_VARIANT; }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::VARIANT;
    }
    MutableColumnPtr create_column() const override { return ColumnObject::create(is_nullable); }
    bool is_object() const override { return true; }
    bool equals(const IDataType& rhs) const override;
    bool hasNullableSubcolumns() const { return is_nullable; }
    bool get_is_parametric() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }
    bool have_subtypes() const override { return true; };
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override {
        const auto& column_object = assert_cast<const ColumnObject&>(column);
        return "Variant: " + column_object.get_keys_str();
    }
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;
    [[noreturn]] Field get_default() const override {
        LOG(FATAL) << "Method getDefault() is not implemented for data type " << get_name();
    }
};
} // namespace doris::vectorized
