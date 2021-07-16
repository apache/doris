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

#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

/** Implements part of the IDataType interface, common to all numbers and for Date and DateTime.
  */
template <typename T>
class DataTypeNumberBase : public IDataType {
    static_assert(IsNumber<T>);

public:
    static constexpr bool is_parametric = false;
    using FieldType = T;

    const char* get_family_name() const override { return TypeName<T>::get(); }
    TypeIndex get_type_id() const override { return TypeId<T>::value; }
    Field get_default() const override;

    size_t serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;
    MutableColumnPtr create_column() const override;

    bool get_is_parametric() const override { return false; }
    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return true; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return true; }
    bool is_value_represented_by_number() const override { return true; }
    bool is_value_represented_by_integer() const override;
    bool is_value_represented_by_unsigned_integer() const override;
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override { return true; }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override { return sizeof(T); }
    bool is_categorial() const override { return is_value_represented_by_integer(); }
    bool can_be_inside_low_cardinality() const override { return true; }

    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const;
    std::string to_string(const IColumn& column, size_t row_num) const;
};

} // namespace doris::vectorized
