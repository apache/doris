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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeArray.h
// and modified by Doris

#pragma once

#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class DataTypeArray final : public IDataType {
private:
    /// The type of array elements.
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    DataTypeArray(const DataTypePtr& nested_);

    TypeIndex get_type_id() const override { return TypeIndex::Array; }

    std::string do_get_name() const override { return "Array(" + nested->get_name() + ")"; }

    const char* get_family_name() const override { return "Array"; }

    bool can_be_inside_nullable() const override { return true; }

    MutableColumnPtr create_column() const override;

    Field get_default() const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return true; }
    bool cannot_be_stored_in_tables() const override {
        return nested->cannot_be_stored_in_tables();
    }
    bool text_can_contain_only_valid_utf8() const override {
        return nested->text_can_contain_only_valid_utf8();
    }
    bool is_comparable() const override { return nested->is_comparable(); }
    bool can_be_compared_with_collation() const override {
        return nested->can_be_compared_with_collation();
    }

    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return nested->is_value_unambiguously_represented_in_contiguous_memory_region();
    }

    //SerializationPtr doGetDefaultSerialization() const override;

    const DataTypePtr& get_nested_type() const { return nested; }

    /// 1 for plain array, 2 for array of arrays and so on.
    size_t get_number_of_dimensions() const;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;
};

} // namespace doris::vectorized
