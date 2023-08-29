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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeTuple.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_struct_serde.h"

namespace doris {
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class IColumn;
class ReadBuffer;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** Struct data type.
  * Used as an intermediate result when evaluating expressions.
  * Also can be used as a column - the result of the query execution.
  *
  * Struct elements can have names.
  * If an element is unnamed, it will have automatically assigned name like '1', '2', '3' corresponding to its position.
  * Manually assigned names must not begin with digit. Names must be unique.
  *
  * All structs with same size and types of elements are equivalent for expressions, regardless to names of elements.
  */
class DataTypeStruct final : public IDataType {
private:
    DataTypes elems;
    Strings names;
    bool have_explicit_names;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeStruct(const DataTypes& elems);
    DataTypeStruct(const DataTypes& elems, const Strings& names);

    TypeIndex get_type_id() const override { return TypeIndex::Struct; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_STRUCT);
    }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::STRUCT;
    }
    std::string do_get_name() const override;
    const char* get_family_name() const override { return "Struct"; }

    bool can_be_inside_nullable() const override { return true; }
    bool supports_sparse_serialization() const { return true; }

    MutableColumnPtr create_column() const override;

    Field get_default() const override;

    Field get_field(const TExprNode& node) const override {
        LOG(FATAL) << "Unimplemented get_field for struct";
    }

    void insert_default_into(IColumn& column) const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return !elems.empty(); }
    bool is_comparable() const override;
    bool text_can_contain_only_valid_utf8() const override;
    bool have_maximum_size_of_value() const override;
    size_t get_maximum_size_of_value_in_memory() const override;
    size_t get_size_of_value_in_memory() const override;

    const DataTypePtr& get_element(size_t i) const { return elems[i]; }
    const DataTypes& get_elements() const { return elems; }
    const Strings& get_element_names() const { return names; }

    size_t get_position_by_name(const String& name) const;
    std::optional<size_t> try_get_position_by_name(const String& name) const;
    String get_name_by_position(size_t i) const;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;
    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    Status from_string(ReadBuffer& rb, IColumn* column) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    bool get_have_explicit_names() const { return have_explicit_names; }
    DataTypeSerDeSPtr get_serde() const override {
        DataTypeSerDeSPtrs ptrs;
        for (auto iter = elems.begin(); iter < elems.end(); ++iter) {
            ptrs.push_back((*iter)->get_serde());
        }
        return std::make_shared<DataTypeStructSerDe>(ptrs, names);
    };
};

} // namespace doris::vectorized
