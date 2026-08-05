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

#include "core/data_type/data_type_variant_v2.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>

#include <sstream>

#include "common/exception.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/field.h"
#include "core/typeid_cast.h"

namespace doris {

DataTypeVariantV2::DataTypeVariantV2(int32_t max_subcolumns_count)
        : DataTypeVariantV2(max_subcolumns_count, false) {}

DataTypeVariantV2::DataTypeVariantV2(int32_t max_subcolumns_count, bool enable_doc_mode)
        : _max_subcolumns_count(max_subcolumns_count), _enable_doc_mode(enable_doc_mode) {
    _name = fmt::format("Variant(max subcolumns count = {}, enable doc mode = {})",
                        max_subcolumns_count, enable_doc_mode);
}

Status DataTypeVariantV2::check_column(const IColumn& column) const {
    return check_column_non_nested_type<ColumnVariantV2>(column);
}

MutableColumnPtr DataTypeVariantV2::create_column() const {
    return ColumnVariantV2::create();
}

bool DataTypeVariantV2::equals(const IDataType& rhs) const {
    const auto* rhs_type = typeid_cast<const DataTypeVariantV2*>(&rhs);
    return rhs_type != nullptr &&
           _max_subcolumns_count == rhs_type->variant_max_subcolumns_count() &&
           _enable_doc_mode == rhs_type->enable_doc_mode();
}

int64_t DataTypeVariantV2::get_uncompressed_serialized_bytes(const IColumn& column,
                                                             int be_exec_version) const {
    return DataTypeVariantV2SerDe::get_uncompressed_serialized_bytes(column, be_exec_version);
}

char* DataTypeVariantV2::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    return DataTypeVariantV2SerDe::serialize(column, buf, be_exec_version);
}

const char* DataTypeVariantV2::deserialize(const char* buf, MutableColumnPtr* column,
                                           int be_exec_version) const {
    return DataTypeVariantV2SerDe::deserialize(buf, column, be_exec_version);
}

Field DataTypeVariantV2::get_field(const TExprNode& node) const {
    if (node.__isset.string_literal) {
        return Field::create_field<TYPE_STRING>(node.string_literal.value);
    }
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return {};
    }
    std::stringstream error_string;
    node.printTo(error_string);
    throw Exception(ErrorCode::INTERNAL_ERROR, "Unknown literal {}", error_string.str());
}

DataTypeSerDeSPtr DataTypeVariantV2::get_serde(int nesting_level) const {
    return std::make_shared<DataTypeVariantV2SerDe>(nesting_level);
}

void DataTypeVariantV2::to_protobuf(PTypeDesc*, PTypeNode* node, PScalarType*) const {
    node->set_type(TTypeNodeType::VARIANT);
    node->set_variant_max_subcolumns_count(_max_subcolumns_count);
    node->set_variant_enable_doc_mode(_enable_doc_mode);
    node->set_variant_is_v2(true);
}

void DataTypeVariantV2::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->set_variant_max_subcolumns_count(_max_subcolumns_count);
    col_meta->set_variant_enable_doc_mode(_enable_doc_mode);
    col_meta->set_variant_is_v2(true);
}

} // namespace doris
