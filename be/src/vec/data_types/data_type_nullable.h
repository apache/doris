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

#include "vec/data_types/data_type.h"

namespace doris::vectorized {

/// A nullable data type is an ordinary data type provided with a tag
/// indicating that it also contains the NULL value. The following class
/// embodies this concept.
class DataTypeNullable final : public IDataType {
public:
    explicit DataTypeNullable(const DataTypePtr& nested_data_type_);
    std::string do_get_name() const override {
        return "Nullable(" + nested_data_type->get_name() + ")";
    }
    const char* get_family_name() const override { return "Nullable"; }
    TypeIndex get_type_id() const override { return TypeIndex::Nullable; }

    size_t serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;
    MutableColumnPtr create_column() const override;

    Field get_default() const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return true; }
    bool cannot_be_stored_in_tables() const override {
        return nested_data_type->cannot_be_stored_in_tables();
    }
    bool should_align_right_in_pretty_formats() const override {
        return nested_data_type->should_align_right_in_pretty_formats();
    }
    bool text_can_contain_only_valid_utf8() const override {
        return nested_data_type->text_can_contain_only_valid_utf8();
    }
    bool is_comparable() const override { return nested_data_type->is_comparable(); }
    bool can_be_compared_with_collation() const override {
        return nested_data_type->can_be_compared_with_collation();
    }
    bool can_be_used_as_version() const override { return false; }
    bool is_summable() const override { return nested_data_type->is_summable(); }
    bool can_be_used_in_boolean_context() const override {
        return nested_data_type->can_be_used_in_boolean_context();
    }
    bool have_maximum_size_of_value() const override {
        return nested_data_type->have_maximum_size_of_value();
    }
    size_t get_maximum_size_of_value_in_memory() const override {
        return 1 + nested_data_type->get_maximum_size_of_value_in_memory();
    }
    bool is_nullable() const override { return true; }
    size_t get_size_of_value_in_memory() const override;
    bool only_null() const override;
    bool can_be_inside_low_cardinality() const override {
        return nested_data_type->can_be_inside_low_cardinality();
    }
    std::string to_string(const IColumn& column, size_t row_num) const;

    const DataTypePtr& get_nested_type() const { return nested_data_type; }

private:
    DataTypePtr nested_data_type;
};

DataTypePtr make_nullable(const DataTypePtr& type);
DataTypePtr remove_nullable(const DataTypePtr& type);

} // namespace doris::vectorized
