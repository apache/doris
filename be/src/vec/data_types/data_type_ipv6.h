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

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_ipv6_serde.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class ReadBuffer;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class DataTypeIPv6 final : public DataTypeNumberBase<IPv6> {
public:
    TypeIndex get_type_id() const override { return TypeIndex::IPv6; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_IPV6);
    }
    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_IPV6;
    }
    const char* get_family_name() const override { return "IPv6"; }
    std::string do_get_name() const override { return "IPv6"; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    static std::string to_string(const IPv6& value);
    Status from_string(ReadBuffer& rb, IColumn* column) const override;

    Field get_field(const TExprNode& node) const override {
        IPv6 value;
        if (!IPv6Value::from_string(value, node.ipv6_literal.value)) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type IPv6", node.ipv6_literal.value);
        }
        return value;
    }

    MutableColumnPtr create_column() const override;

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeIPv6SerDe>(nesting_level);
    }
};

template <typename DataType>
constexpr bool IsIPv6Type = false;
template <>
inline constexpr bool IsIPv6Type<DataTypeIPv6> = true;

template <typename DataType>
constexpr bool IsIPType = IsIPv4Type<DataType> || IsIPv6Type<DataType>;

} // namespace doris::vectorized
