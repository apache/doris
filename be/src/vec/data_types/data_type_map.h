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
class ReadBuffer;
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

    DataTypeMap(const DataTypePtr& key_type_, const DataTypePtr& value_type_);

    TypeIndex get_type_id() const override { return TypeIndex::Map; }
    PrimitiveType get_type_as_primitive_type() const override { return TYPE_MAP; }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::MAP;
    }
    std::string do_get_name() const override {
        return "Map(" + key_type->get_name() + ", " + value_type->get_name() + ")";
    }
    const char* get_family_name() const override { return "Map"; }

    bool can_be_inside_nullable() const override { return true; }
    MutableColumnPtr create_column() const override;
    Field get_default() const override;

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        LOG(FATAL) << "Unimplemented get_field for map";
    }

    bool equals(const IDataType& rhs) const override;
    bool get_is_parametric() const override { return true; }
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
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;
    DataTypeSerDeSPtr get_serde() const override {
        return std::make_shared<DataTypeMapSerDe>(key_type->get_serde(), value_type->get_serde());
    };
};

} // namespace doris::vectorized
