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

#include <string>

#include "core/column/column_spatial.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_varbinary_serde.h"

namespace doris {

// The physical payload is WKB, but this DataType deliberately remains distinct
// from VARBINARY so that the external-format reader can enforce spatial semantics.
class DataTypeSpatial final : public IDataType {
public:
    DataTypeSpatial(PrimitiveType primitive_type, std::string crs = "OGC:CRS84",
                    std::string algorithm = "")
            : _primitive_type(primitive_type),
              _crs(std::move(crs)),
              _algorithm(std::move(algorithm)) {
        DCHECK(primitive_type == TYPE_GEOMETRY || primitive_type == TYPE_GEOGRAPHY);
        DCHECK(!_crs.empty());
        DCHECK(primitive_type == TYPE_GEOMETRY || !_algorithm.empty());
        DCHECK(primitive_type == TYPE_GEOGRAPHY || _algorithm.empty());
    }

    const std::string get_family_name() const override {
        return _primitive_type == TYPE_GEOMETRY ? "Geometry" : "Geography";
    }
    PrimitiveType get_primitive_type() const override { return _primitive_type; }
    const std::string& crs() const { return _crs; }
    const std::string& algorithm() const { return _algorithm; }

    doris::FieldType get_storage_field_type() const override;
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;
    Field get_field(const TExprNode& node) const override;
    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;
    bool equals(const IDataType& rhs) const override;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeVarbinarySerDe>(nesting_level);
    }
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override;
#ifdef BE_TEST
    void to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const override;
#endif

private:
    const PrimitiveType _primitive_type;
    const std::string _crs;
    const std::string _algorithm;
};

} // namespace doris
