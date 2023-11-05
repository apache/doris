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
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_ipv4_serde.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class ReadBuffer;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class DataTypeIPv4 final : public DataTypeNumberBase<IPv4> {
public:
    TypeIndex get_type_id() const override { return TypeIndex::IPv4; }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::IPV4;
    }
    const char* get_family_name() const override { return "IPv4"; }
    std::string do_get_name() const override { return "IPv4"; }

    bool can_be_inside_nullable() const override { return true; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;

    static std::string convert_ipv4_to_string(IPv4 ipv4);
    static bool convert_string_to_ipv4(IPv4& x, std::string ipv4);

    Field get_field(const TExprNode& node) const override { return (IPv4)node.ipv4_literal.value; }

    MutableColumnPtr create_column() const override;

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeIPv4SerDe>(nesting_level);
    }
};

template <typename DataType>
constexpr bool IsIPv4Type = false;
template <>
inline constexpr bool IsIPv4Type<DataTypeIPv4> = true;

} // namespace doris::vectorized
