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
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_fixed_length_object.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/serde/data_type_fixedlengthobject_serde.h"

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
            : _result_is_nullable(result_is_nullable),
              _sub_types(sub_types),
              _function_name(function_name) {
        _agg_function = AggregateFunctionSimpleFactory::instance().get(_function_name, _sub_types,
                                                                       _result_is_nullable);
        if (_agg_function == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "DataTypeAggState function get failed, type={}", do_get_name());
        }
        _agg_serialized_type = _agg_function->get_serialized_type();
    }

    const char* get_family_name() const override { return "AggState"; }

    std::string do_get_name() const override {
        return fmt::format("AggState(function_name={},result_is_nullable={},arguments=[{}])",
                           _function_name, _result_is_nullable, get_types_string());
    }

    TypeIndex get_type_id() const override { return TypeIndex::AggState; }

    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_AGG_STATE);
    }
    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        return TPrimitiveType::AGG_STATE;
    }

    std::string to_string(const IColumn& column, size_t row_num) const override {
        std::string res = "binary(";
        StringRef str = column.get_data_at(row_num);
        for (auto c : str.to_string()) {
            for (int i = 0; i < 8; i++) {
                res += (c & (1 << (7 - i))) ? "1" : "0";
            }
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

    AggregateFunctionPtr get_nested_function() const { return _agg_function; }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override {
        return _agg_serialized_type->get_uncompressed_serialized_bytes(column, be_exec_version);
    }

    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override {
        return _agg_serialized_type->serialize(column, buf, be_exec_version);
    }

    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override {
        return _agg_serialized_type->deserialize(buf, column, be_exec_version);
    }

    MutableColumnPtr create_column() const override {
        //need pass the agg sizeof data
        return _agg_function->create_serialize_column();
    }

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return _agg_serialized_type->get_serde(nesting_level);
    };

    DataTypePtr get_serialized_type() const { return _agg_serialized_type; }

private:
    std::string get_types_string() const {
        std::string types;
        for (auto type : _sub_types) {
            if (!types.empty()) {
                types += ", ";
            }
            types += type->get_name();
        }
        return types;
    }

    bool _result_is_nullable;
    //because the agg_state type maybe mapped to ColumnString or ColumnFixedLengthObject
    DataTypePtr _agg_serialized_type;
    AggregateFunctionPtr _agg_function;
    DataTypes _sub_types;
    std::string _function_name;
};

} // namespace doris::vectorized
