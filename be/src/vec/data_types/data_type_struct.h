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

#include <exception>

#include "gen_cpp/data.pb.h"
#include "util/stack_util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

/** Struct data type.
  * Used as an intermediate result when evaluating expressions.
  * Also can be used as a column - the result of the query execution.
  *
  * Struct elements can have names.
  * If an element is unnamed, it will have automatically assigned name like '1', '2', '3' corresponding to its position.
  * Manually assigned names must not begin with digit. Names must be unique.
  *
  * All tuples with same size and types of elements are equivalent for expressions, regardless to names of elements.
  */
class DataTypeStruct final : public IDataType {
private:
    // using DataTypePtr = std::shared_ptr<const IDataType>;
    // using DataTypes = std::vector<DataTypePtr>;
    // using Strings = std::vector<std::string>;

    DataTypes elems;
    Strings names;
    bool have_explicit_names;

public:
    // static constexpr bool is_parametric = true;

    explicit DataTypeStruct(const DataTypes& elems);
    DataTypeStruct(const DataTypes& elems, const Strings& names);

    TypeIndex get_type_id() const override { return TypeIndex::Struct; }
    std::string do_get_name() const override;
    const char* get_family_name() const override { return "Struct"; }

    bool can_be_inside_nullable() const override { return false; }
    bool supports_sparse_serialization() const { return true; }

    MutableColumnPtr create_column() const override;
    // MutableColumnPtr create_column(const ISerialization& serialization) const override;

    Field get_default() const override;
    void insert_default_into(IColumn& column) const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return !elems.empty(); }
    bool is_comparable() const override;
    bool text_can_contain_only_valid_utf8() const override;
    bool have_maximum_size_of_value() const override;
    bool has_dynamic_subcolumns() const;
    size_t get_maximum_size_of_value_in_memory() const override;
    size_t get_size_of_value_in_memory() const override;

    const DataTypePtr& get_element(size_t i) const { return elems[i]; }
    const DataTypes& get_elements() const { return elems; }
    const Strings& get_element_names() const { return names; }

    size_t get_position_by_name(const String& name) const;
    std::optional<size_t> try_get_position_by_name(const String& name) const;
    String get_name_by_position(size_t i) const;

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override {
        LOG(FATAL) << "get_uncompressed_serialized_bytes not implemented";
    }

    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override {
        LOG(FATAL) << "serialize not implemented";
    }

    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override {
        LOG(FATAL) << "serialize not implemented";
    }

    // bool is_parametric() const { return true; }
    // SerializationPtr do_get_default_serialization() const override;
    // SerializationPtr get_serialization(const SerializationInfo& info) const override;
    // MutableSerializationInfoPtr create_serialization_info(
    //         const SerializationInfo::Settings& settings) const override;
    // SerializationInfoPtr get_serialization_info(const IColumn& column) const override;
    // bool have_explicit_names() const { return have_explicit_names; }
};

} // namespace doris::vectorized
