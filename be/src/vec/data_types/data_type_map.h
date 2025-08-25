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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeMap.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_map_serde.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
/** Map data type.
  */
class DataTypeMap final : public IDataType {
private:
    DataTypePtr key_type;
    DataTypePtr value_type;

public:
    static constexpr bool is_parametric = true;
    static constexpr PrimitiveType PType = TYPE_MAP;

    DataTypeMap(const DataTypePtr& key_type_, const DataTypePtr& value_type_);
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_MAP; }
    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_MAP;
    }

    std::string do_get_name() const override {
        return "Map(" + key_type->get_name() + ", " + value_type->get_name() + ")";
    }
    const std::string get_family_name() const override { return "Map"; }

    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;
    Field get_default() const override;

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Unimplemented get_field for map");
    }

    bool equals(const IDataType& rhs) const override;
    bool have_subtypes() const override { return true; }
    bool is_comparable() const override {
        return key_type->is_comparable() && value_type->is_comparable();
    }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }

    const DataTypePtr& get_key_type() const { return key_type; }
    const DataTypePtr& get_value_type() const { return value_type; }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    using SerDeType = DataTypeMapSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(key_type->get_serde(nesting_level + 1),
                                           value_type->get_serde(nesting_level + 1), nesting_level);
    };
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override {
        node->set_type(TTypeNodeType::MAP);
        node->add_contains_nulls(key_type->is_nullable());
        node->add_contains_nulls(value_type->is_nullable());
        key_type->to_protobuf(ptype);
        value_type->to_protobuf(ptype);
    }
#ifdef BE_TEST
    void to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const override {
        node.type = TTypeNodeType::MAP;
        node.contains_nulls.push_back(key_type->is_nullable());
        node.contains_nulls.push_back(value_type->is_nullable());
        key_type->to_thrift(thrift_type);
        value_type->to_thrift(thrift_type);
    }
#endif
};

} // namespace doris::vectorized
