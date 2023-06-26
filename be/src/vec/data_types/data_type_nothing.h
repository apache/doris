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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeNothing.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <ostream>
#include <string>

#include "runtime/define_primitive_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** Data type that cannot have any values.
  * Used to represent NULL of unknown type as Nullable(Nothing),
  * and possibly for empty array of unknown type as Array(Nothing).
  */
class DataTypeNothing final : public IDataType {
public:
    static constexpr bool is_parametric = false;

    const char* get_family_name() const override { return "Nothing"; }
    TypeIndex get_type_id() const override { return TypeIndex::Nothing; }
    PrimitiveType get_type_as_primitive_type() const override { return INVALID_TYPE; }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::INVALID_TYPE;
    }

    doris::FieldType get_type_as_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_NONE;
    }

    MutableColumnPtr create_column() const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return false; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override { return 0; }
    bool can_be_inside_nullable() const override { return true; }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override {
        return 0;
    }
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    [[noreturn]] Field get_default() const override {
        LOG(FATAL) << "Method get_default() is not implemented for data type " << get_name();
    }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        LOG(FATAL) << "Unimplemented get_field for Nothing";
    }

    void insert_default_into(IColumn&) const override {
        LOG(FATAL) << "Method insert_default_into() is not implemented for data type "
                   << get_name();
    }

    bool have_subtypes() const override { return false; }
    DataTypeSerDeSPtr get_serde() const override {
        LOG(FATAL) << get_name() << " not support serde";
    };
};

} // namespace doris::vectorized
