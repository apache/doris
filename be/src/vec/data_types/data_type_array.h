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

#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "serde/data_type_array_serde.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class DataTypeArray final : public IDataType {
private:
    /// The type of array elements.
    DataTypePtr nested;

public:
    static constexpr PrimitiveType PType = TYPE_ARRAY;
    static constexpr bool is_parametric = true;

    DataTypeArray(const DataTypePtr& nested_);

    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_ARRAY; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_ARRAY;
    }

    std::string do_get_name() const override { return "Array(" + nested->get_name() + ")"; }

    const std::string get_family_name() const override { return "Array"; }

    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;
    Field get_default() const override;

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Unimplemented get_field for array");
    }

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;

    bool equals(const IDataType& rhs) const override;

    bool have_subtypes() const override { return true; }
    bool text_can_contain_only_valid_utf8() const override {
        return nested->text_can_contain_only_valid_utf8();
    }
    bool is_comparable() const override { return nested->is_comparable(); }

    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return nested->is_value_unambiguously_represented_in_contiguous_memory_region();
    }

    const DataTypePtr& get_nested_type() const { return nested; }

    /// 1 for plain array, 2 for array of arrays and so on.
    size_t get_number_of_dimensions() const;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    using SerDeType = DataTypeArraySerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nested->get_serde(nesting_level + 1), nesting_level);
    };

    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override {
        node->set_type(TTypeNodeType::ARRAY);
        node->set_contains_null(nested->is_nullable());
        nested->to_protobuf(ptype);
    }

#ifdef BE_TEST
    void to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const override {
        node.type = TTypeNodeType::ARRAY;
        node.__isset.contains_nulls = true;
        node.contains_nulls.push_back(nested->is_nullable());
        nested->to_thrift(thrift_type);
    }
#endif
};

} // namespace doris::vectorized
