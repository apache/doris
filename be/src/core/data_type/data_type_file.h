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

#include "core/column/column_file.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/file_schema_descriptor.h"
#include "core/data_type_serde/data_type_file_serde.h"

namespace doris {

class DataTypeFile final : public IDataType {
public:
    static constexpr PrimitiveType PType = TYPE_FILE;

    PrimitiveType get_primitive_type() const override { return TYPE_FILE; }
    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_FILE;
    }

    std::string do_get_name() const override { return "File"; }
    const std::string get_family_name() const override { return "File"; }

    MutableColumnPtr create_column() const override { return ColumnFile::create(_schema); }
    Status check_column(const IColumn& column) const override;
    Field get_default() const override { return _physical_type.get_default(); }
    Field get_field(const TExprNode& node) const override;
    bool equals(const IDataType& rhs) const override;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeFileSerDe>(_physical_type.get_serde(nesting_level),
                                                   nesting_level);
    }
    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    const FileSchemaDescriptor& schema() const { return _schema; }

private:
    const FileSchemaDescriptor& _schema = FileSchemaDescriptor::instance();
    DataTypeJsonb _physical_type;
};

} // namespace doris
