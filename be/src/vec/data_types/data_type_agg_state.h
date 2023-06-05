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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
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
    DataTypeAggState(DataTypes sub_types, bool result_is_nullable, std::string function_name)
            : _sub_types(sub_types),
              _result_is_nullable(result_is_nullable),
              _function_name(function_name) {}

    const char* get_family_name() const override { return "AggState"; }

    std::string do_get_name() const override {
        std::string types;
        for (auto type : _sub_types) {
            if (!types.empty()) {
                types += ", ";
            }
            types += type->get_name();
        }
        return "AggState(" + types + ")";
    }

    TypeIndex get_type_id() const override { return TypeIndex::AggState; }

    PrimitiveType get_type_as_primitive_type() const override { return TYPE_AGG_STATE; }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::AGG_STATE;
    }

    std::string to_string(const IColumn& column, size_t row_num) const override {
        std::string res = "binary(";
        StringRef str = column.get_data_at(row_num);
        for (auto c : str.to_string()) {
            res += std::to_string(int(c));
            res += ' ';
        }
        res += ")";
        return res;
    }

    const DataTypes& get_sub_types() const { return _sub_types; }

    void to_pb_column_meta(PColumnMeta* col_meta) const override {
        IDataType::to_pb_column_meta(col_meta);
        for (auto type : _sub_types) {
            type->to_pb_column_meta(col_meta->add_children());
        }
        col_meta->set_function_name(_function_name);
        col_meta->set_result_is_nullable(_result_is_nullable);
    }

    AggregateFunctionPtr get_nested_function() const {
        return AggregateFunctionSimpleFactory::instance().get(_function_name, _sub_types,
                                                              _result_is_nullable);
    }

private:
    DataTypes _sub_types;
    bool _result_is_nullable;
    std::string _function_name;
};

} // namespace doris::vectorized
