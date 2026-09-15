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

#include "common/exception.h"
#include "core/data_type/data_type_number_base.h"
#include "core/data_type_serde/data_type_uuid_serde.h"
#include "core/value/uuid_value.h"

namespace doris {

class DataTypeUUID final : public DataTypeNumberBase<PrimitiveType::TYPE_UUID> {
public:
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_UUID; }
    const std::string get_family_name() const override { return "UUID"; }
    std::string do_get_name() const override { return "UUID"; }

    bool equals(const IDataType& rhs) const override;

    Field get_field(const TExprNode& node) const override {
        UUIDValueType value;
        if (!UUIDValue::from_string(value, node.uuid_literal.value)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Invalid value: {} for type UUID",
                            node.uuid_literal.value);
        }
        return Field::create_field<TYPE_UUID>(value);
    }

    MutableColumnPtr create_column() const override;

    using SerDeType = DataTypeUUIDSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }
};

template <typename DataType>
constexpr bool IsUUIDType = false;
template <>
inline constexpr bool IsUUIDType<DataTypeUUID> = true;

} // namespace doris
