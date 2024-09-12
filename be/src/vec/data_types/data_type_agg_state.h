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

#include <gen_cpp/data.pb.h>

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
#include "vec/data_types/serde/data_type_string_serde.h"

namespace doris::vectorized {

class DataTypeAggState : public DataTypeString {
public:
    DataTypeAggState(DataTypes sub_types, bool result_is_nullable, std::string function_name,
                     int be_exec_version)
            : _result_is_nullable(result_is_nullable),
              _sub_types(std::move(sub_types)),
              _function_name(std::move(function_name)),
              _be_exec_version(be_exec_version) {
        _agg_function = AggregateFunctionSimpleFactory::instance().get(
                _function_name, _sub_types, _result_is_nullable, _be_exec_version);
        if (_agg_function == nullptr ||
            !BeExecVersionManager::check_be_exec_version(be_exec_version)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "DataTypeAggState function get failed, type={}", do_get_name());
        }
        _agg_function->set_version(be_exec_version);
        _agg_serialized_type = _agg_function->get_serialized_type();
    }

    const char* get_family_name() const override { return "AggState"; }

    std::string do_get_name() const override {
        return fmt::format(
                "AggState(function_name={},result_is_nullable={},arguments=[{}],be_exec_version={}"
                ")",
                _function_name, _result_is_nullable, get_types_string(), _be_exec_version);
    }

    std::string get_function_name() const { return _function_name; }

    TypeIndex get_type_id() const override { return TypeIndex::AggState; }

    TypeDescriptor get_type_as_type_descriptor() const override { return {TYPE_AGG_STATE}; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_AGG_STATE;
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
        col_meta->set_be_exec_version(_be_exec_version);
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

    void check_agg_state_compatibility(int read_be_exec_version) const {
        BeExecVersionManager::check_agg_state_compatibility(read_be_exec_version, _be_exec_version,
                                                            get_nested_function()->get_name());
    }

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
    int _be_exec_version;
};

} // namespace doris::vectorized
