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

#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "serde/data_type_string_serde.h"
#include "vec/common/string_view.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_varbinary_serde.h"

namespace doris::vectorized {
class BufferWritable;
class IColumn;

class DataTypeVarbinary : public IDataType {
public:
    using ColumnType = ColumnVarbinary;
    using FieldType = doris::StringView;

    static constexpr PrimitiveType PType = TYPE_VARBINARY;

    const std::string get_family_name() const override { return "VarBinary"; }

    DataTypeVarbinary(int len = -1, PrimitiveType primitive_type = PrimitiveType::TYPE_VARBINARY)
            : _len(len), _primitive_type(primitive_type) {
        DCHECK(doris::is_varbinary(primitive_type)) << primitive_type;
    }
    PrimitiveType get_primitive_type() const override { return _primitive_type; }

    doris::FieldType get_storage_field_type() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Data type {} get_storage_field_type ostr not implement.",
                               get_name());
        return doris::FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    Field get_default() const override;

    Field get_field(const TExprNode& node) const override {
        DCHECK_EQ(node.node_type, TExprNodeType::VARBINARY_LITERAL);
        DCHECK(node.__isset.varbinary_literal);
        return Field::create_field<TYPE_VARBINARY>(doris::StringView(node.varbinary_literal.value));
    }

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;

    bool equals(const IDataType& rhs) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeVarbinarySerDe>(nesting_level);
    };

    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override {
        scalar_type->set_len(_len);
    }

    int len() const { return _len; }

private:
    const int _len;
    const PrimitiveType _primitive_type;
};

template <typename T>
constexpr static bool IsVarBinaryType = false;
template <>
inline constexpr bool IsVarBinaryType<DataTypeVarbinary> = true;

} // namespace doris::vectorized
