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
#include "core/assert_cast.h"
#include "core/column/column_variant.h"
#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/data_type_serde/data_type_variant_serde.h"
#include "core/field.h"
#include "core/types.h"

namespace doris {
class IColumn;
} // namespace doris

namespace doris {
class DataTypeVariant : public IDataType {
private:
    int32_t _max_subcolumns_count = 0;
    bool _enable_doc_mode = false;
    std::string name = "Variant";

public:
    static constexpr PrimitiveType PType = TYPE_VARIANT;
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_VARIANT; }
    DataTypeVariant() = default;
    DataTypeVariant(int32_t max_subcolumns_count, bool enable_doc_mode);
    String do_get_name() const override { return name; }
    const std::string get_family_name() const override { return "Variant"; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_VARIANT;
    }
    Status check_column(const IColumn& column) const override {
        return check_column_non_nested_type<ColumnVariant>(column);
    }
    MutableColumnPtr create_column() const override;
    bool equals(const IDataType& rhs) const override;
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    Field get_default() const override { return Field::create_field<TYPE_VARIANT>(VariantMap()); }

    Field get_field(const TExprNode& node) const override;

    using SerDeType = DataTypeVariantSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    };
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override {
        node->set_type(TTypeNodeType::VARIANT);
        node->set_variant_max_subcolumns_count(_max_subcolumns_count);
        node->set_variant_enable_doc_mode(_enable_doc_mode);
    }
    void to_pb_column_meta(PColumnMeta* col_meta) const override;
    int32_t variant_max_subcolumns_count() const { return _max_subcolumns_count; }
    bool enable_doc_mode() const { return _enable_doc_mode; }
};
} // namespace doris
