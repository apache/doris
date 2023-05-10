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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeString.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_string.h"

namespace doris {
namespace vectorized {
class BufferWritable;
class IColumn;
class ReadBuffer;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class DataTypeAggState : public DataTypeString {
public:
    const char* get_family_name() const override { return "AggState"; }

    TypeIndex get_type_id() const override { return TypeIndex::AggState; }

    PrimitiveType get_type_as_primitive_type() const override { return TYPE_AGG_STATE; }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::AGG_STATE;
    }

    const DataTypes& get_sub_types() { return sub_types; }

    void add_sub_type(DataTypePtr type) { sub_types.push_back(type); }

    void to_pb_column_meta(PColumnMeta* col_meta) const override {
        IDataType::to_pb_column_meta(col_meta);
        for (auto type : sub_types) {
            type->to_pb_column_meta(col_meta->add_children());
        }
    }

private:
    DataTypes sub_types;
};

} // namespace doris::vectorized
