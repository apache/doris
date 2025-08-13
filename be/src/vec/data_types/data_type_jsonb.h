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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "common/cast_set.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column_string.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/serde/data_type_jsonb_serde.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_string_serde.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class BufferWritable;
class IColumn;

class DataTypeJsonb final : public IDataType {
public:
    using ColumnType = ColumnString;
    using FieldType = JsonbField;
    static constexpr PrimitiveType PType = TYPE_JSONB;
    static constexpr bool is_parametric = false;

    const std::string get_family_name() const override { return "JSONB"; }
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_JSONB; }
    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_JSONB;
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int data_version) const override;
    char* serialize(const IColumn& column, char* buf, int data_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int data_version) const override;

    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    Field get_default() const override;

    Field get_field(const TExprNode& node) const override;

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;

    bool equals(const IDataType& rhs) const override;

    bool have_subtypes() const override { return false; }
    bool is_comparable() const override { return false; }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    bool can_be_inside_low_cardinality() const override { return true; }
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    using SerDeType = DataTypeJsonbSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    };

private:
    DataTypeString data_type_string;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
