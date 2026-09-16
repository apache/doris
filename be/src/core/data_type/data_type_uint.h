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

#include "core/data_type/data_type.h"

namespace doris {

// UINT32/UINT64 are physical storage types for internal columns such as ARRAY/MAP offsets. They
// need an IDataType so every physical ColumnReader has a valid type, but they are not SQL numeric
// types. For example, an ARRAY offset reader can create and validate ColumnOffset64, while attempts
// to serialize that physical column through a logical SerDe must fail explicitly.
template <PrimitiveType T>
class DataTypeUInt final : public IDataType {
    static_assert(T == TYPE_UINT32 || T == TYPE_UINT64);

public:
    static constexpr PrimitiveType PType = T;
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;

    const std::string get_family_name() const override;
    PrimitiveType get_primitive_type() const override { return PType; }

    [[noreturn]] Field get_field(const TExprNode& node) const override;
    bool equals(const IDataType& rhs) const override;

    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override {
        return sizeof(typename PrimitiveTypeTraits<T>::CppType);
    }

    [[noreturn]] int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                                           int be_exec_version) const override;
    [[noreturn]] char* serialize(const IColumn& column, char* buf,
                                 int be_exec_version) const override;
    [[noreturn]] const char* deserialize(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const override;

    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    [[noreturn]] DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override;
};

extern template class DataTypeUInt<TYPE_UINT32>;
extern template class DataTypeUInt<TYPE_UINT64>;

} // namespace doris
