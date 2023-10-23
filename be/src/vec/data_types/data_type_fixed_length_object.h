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

#include <memory>
#include <typeinfo>

#include "runtime/define_primitive_type.h"
#include "serde/data_type_fixedlengthobject_serde.h"
#include "vec/columns/column_fixed_length_object.h"
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

class DataTypeFixedLengthObject final : public IDataType {
public:
    using ColumnType = ColumnFixedLengthObject;

    DataTypeFixedLengthObject() = default;

    DataTypeFixedLengthObject(const DataTypeFixedLengthObject& other) {}

    const char* get_family_name() const override { return "DataTypeFixedLengthObject"; }

    TypeIndex get_type_id() const override { return TypeIndex::FixedLengthObject; }

    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(INVALID_TYPE);
    }

    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::INVALID_TYPE;
    }

    Field get_default() const override { return String(); }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        LOG(FATAL) << "Unimplemented get_field for DataTypeFixedLengthObject";
    }

    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override {
        return static_cast<const ColumnType&>(column).byte_size() + sizeof(uint32_t) +
               sizeof(size_t);
    }

    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    MutableColumnPtr create_column() const override;

    bool get_is_parametric() const override { return false; }
    bool have_subtypes() const override { return false; }

    bool can_be_inside_low_cardinality() const override { return false; }
    DataTypeSerDeSPtr get_serde() const override {
        return std::make_shared<DataTypeFixedLengthObjectSerDe>();
    };
};

} // namespace doris::vectorized
