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

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <string>

#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "vec/common/pod_array.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_ipv4_serde.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class DataTypeIPv4 final : public DataTypeNumberBase<PrimitiveType::TYPE_IPV4> {
public:
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_IPV4; }
    const std::string get_family_name() const override { return "IPv4"; }
    std::string do_get_name() const override { return "IPv4"; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_IPV4;
    }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<PrimitiveType::TYPE_IPV4>::template to_string_batch_impl<DataTypeIPv4>(
                column, column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const IPv4& num) const;
    std::string to_string(const IPv4& value) const;

    Field get_field(const TExprNode& node) const override;

    MutableColumnPtr create_column() const override;

    using SerDeType = DataTypeIPv4SerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }
};

template <typename DataType>
constexpr bool IsIPv4Type = false;
template <>
inline constexpr bool IsIPv4Type<DataTypeIPv4> = true;

} // namespace doris::vectorized
