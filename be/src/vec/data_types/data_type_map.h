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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeMap.h
// and modified by Doris

#pragma once

#include "vec/data_types/data_type.h"

namespace doris::vectorized {
/** Map data type.
  *
  * Map's key and value only have types.
  * If only one type is set, then key's type is "String" in default.
  */
class DataTypeMap final : public  IDataType
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;
    DataTypePtr keys; // array
    DataTypePtr values; // array

public:
    static constexpr bool is_parametric = true;

    DataTypeMap(const DataTypePtr& keys_, const DataTypePtr& values_);

    TypeIndex get_type_id() const override { return TypeIndex::Map; }
    std::string do_get_name() const override { return "Map(" + key_type->get_name() + ", " + value_type->get_name()+ ")"; }
    const char * get_family_name() const override { return "Map"; }

    bool can_be_inside_nullable() const override { return true; }
    MutableColumnPtr create_column() const override;
    Field get_default() const override { return Map(); };
    bool equals(const IDataType& rhs) const override;
    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return true; }
    bool is_comparable() const override { return key_type->is_comparable() && value_type->is_comparable(); }
    bool can_be_compared_with_collation() const override { return false; }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    

    const DataTypePtr& get_keys() const { return keys; }
    const DataTypePtr& get_values() const { return values; }

    const DataTypePtr & get_key_type() const { return key_type; }
    const DataTypePtr & get_value_type() const { return value_type; }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;

};

}