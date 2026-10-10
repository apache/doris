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

#include <optional>

#include "core/data_type/data_type.h"

namespace doris {

class ColumnFile;

bool contains_file_type(const DataTypePtr& type);
Status validate_file_column(const IColumn& column, const DataTypePtr& type);
Status validate_file_column(const IColumn& column, const DataTypePtr& type, size_t start,
                            size_t count);
// A decoded FILE row has no ancestor NULL mask and can be checked without extracting a Field.
Status validate_file_row(const ColumnFile& column, size_t row);

// A FILE always owns its canonical six-child schema. Scan selection is carried separately.
class DataTypeFile final : public IDataType {
public:
    static constexpr PrimitiveType PType = TYPE_FILE;
    static constexpr size_t FIELD_COUNT = 6;
    static constexpr size_t INLINE_FIELD_INDEX = 5;

    DataTypeFile();
    DataTypeFile(const DataTypes& elements, const Strings& names);

    PrimitiveType get_primitive_type() const override { return TYPE_FILE; }
    const std::string get_family_name() const override { return "File"; }
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;
    Field get_field(const TExprNode& node) const override;
    bool equals(const IDataType& rhs) const override;

    const DataTypes& get_elements() const { return _elements; }
    const DataTypePtr& get_element(size_t i) const { return _elements.at(i); }
    const Strings& get_element_names() const { return _names; }
    const String& get_element_name(size_t i) const { return _names.at(i); }
    size_t get_position_by_name(const String& name) const;
    std::optional<size_t> try_get_position_by_name(const String& name) const;
    String get_name_by_position(size_t i) const { return _names.at(i); }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    void to_pb_column_meta(PColumnMeta* col_meta) const override;
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override;
#ifdef BE_TEST
    using IDataType::to_thrift;
    void to_thrift(TTypeDesc& thrift_type, TTypeNode& node) const override;
#endif

private:
    DataTypes _elements;
    Strings _names;
};

} // namespace doris
