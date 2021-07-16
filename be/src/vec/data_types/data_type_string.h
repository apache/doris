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

#include <ostream>

#include "vec/data_types/data_type.h"

namespace doris::vectorized {

class DataTypeString final : public IDataType {
public:
    using FieldType = String;
    static constexpr bool is_parametric = false;

    const char* get_family_name() const override { return "String"; }

    TypeIndex get_type_id() const override { return TypeIndex::String; }
    size_t serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;

    MutableColumnPtr create_column() const override;

    Field get_default() const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return false; }
    bool have_subtypes() const override { return false; }
    bool is_comparable() const override { return true; }
    bool can_be_compared_with_collation() const override { return true; }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override { return true; }
    bool is_categorial() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }
    bool can_be_inside_low_cardinality() const override { return true; }
    std::string to_string(const IColumn& column, size_t row_num) const;
};

} // namespace doris::vectorized
