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
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <sstream>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "serde/data_type_object_serde.h"
#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
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
class DataTypeObject : public IDataType {
private:
    String schema_format;
    bool is_nullable;

public:
    DataTypeObject(const String& schema_format_ = "json", bool is_nullable_ = true);
    const char* get_family_name() const override { return "Variant"; }
    TypeIndex get_type_id() const override { return TypeIndex::VARIANT; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_VARIANT);
    }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_VARIANT;
    }
    MutableColumnPtr create_column() const override { return ColumnObject::create(is_nullable); }
    bool is_object() const override { return true; }
    bool equals(const IDataType& rhs) const override;
    bool hasNullableSubcolumns() const { return is_nullable; }
    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return true; };
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;
    Field get_default() const override { return VariantMap(); }

    Field get_field(const TExprNode& node) const override {
        if (node.__isset.string_literal) {
            return node.string_literal.value;
        }
        if (node.node_type == TExprNodeType::NULL_LITERAL) {
            return Field();
        }
        std::stringstream error_string;
        node.printTo(error_string);
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Unkown literal {}", error_string.str());
        return {};
    }

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeObjectSerDe>(nesting_level);
    };
};
} // namespace doris::vectorized
