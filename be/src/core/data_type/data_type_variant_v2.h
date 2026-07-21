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

#include <cstdint>
#include <string>

#include "core/data_type/data_type.h"
#include "core/data_type/define_primitive_type.h"

namespace doris {

class DataTypeVariantV2 final : public IDataType {
public:
    DataTypeVariantV2() = default;
    explicit DataTypeVariantV2(int32_t max_subcolumns_count);
    DataTypeVariantV2(int32_t max_subcolumns_count, bool enable_doc_mode);

    static constexpr PrimitiveType PType = TYPE_VARIANT;
    PrimitiveType get_primitive_type() const override { return TYPE_VARIANT; }
    const std::string get_family_name() const override { return "Variant"; }

    Status check_column(const IColumn& column) const override;
    MutableColumnPtr create_column() const override;
    bool equals(const IDataType& rhs) const override;
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    Field get_field(const TExprNode& node) const override;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override;
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override;
    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    int32_t variant_max_subcolumns_count() const { return _max_subcolumns_count; }
    bool enable_doc_mode() const { return _enable_doc_mode; }

protected:
    String do_get_name() const override { return _name; }

private:
    int32_t _max_subcolumns_count = 0;
    bool _enable_doc_mode = false;
    std::string _name = "Variant";
};

} // namespace doris
