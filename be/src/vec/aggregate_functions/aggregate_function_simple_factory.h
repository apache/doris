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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/AggregateFunctions/AggregateFunctionFactory.h
// and modified by Doris

#pragma once

#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
using AggregateFunctionCreator = std::function<AggregateFunctionPtr(
        const std::string&, const DataTypes&, const Array&, const bool)>;

class AggregateFunctionSimpleFactory {
public:
    using Creator = AggregateFunctionCreator;

private:
    using AggregateFunctions = std::unordered_map<std::string, Creator>;

    AggregateFunctions aggregate_functions;
    AggregateFunctions nullable_aggregate_functions;
    std::unordered_map<std::string, std::string> function_alias;

public:
    void register_nullable_function_combinator(const Creator& creator) {
        for (const auto& entity : aggregate_functions) {
            if (nullable_aggregate_functions.find(entity.first) ==
                nullable_aggregate_functions.end()) {
                nullable_aggregate_functions[entity.first] = creator;
            }
        }
    }

    void register_distinct_function_combinator(const Creator& creator, const std::string& prefix,
                                               bool nullable = false) {
        auto& functions = nullable ? nullable_aggregate_functions : aggregate_functions;
        std::vector<std::string> need_insert;
        for (const auto& entity : aggregate_functions) {
            std::string target_value = prefix + entity.first;
            if (functions.find(target_value) == functions.end()) {
                need_insert.emplace_back(std::move(target_value));
            }
        }
        for (const auto& function_name : need_insert) {
            register_function(function_name, creator, nullable);
        }
    }

    AggregateFunctionPtr get(const std::string& name, const DataTypes& argument_types,
                             const Array& parameters, const bool result_is_nullable = false) {
        bool nullable = false;
        for (const auto& type : argument_types) {
            if (type->is_nullable()) {
                nullable = true;
            }
        }

        std::string name_str = name;
        if (function_alias.count(name)) {
            name_str = function_alias[name];
        }

        if (nullable) {
            return nullable_aggregate_functions.find(name_str) == nullable_aggregate_functions.end()
                           ? nullptr
                           : nullable_aggregate_functions[name_str](name_str, argument_types,
                                                                    parameters, result_is_nullable);
        } else {
            return aggregate_functions.find(name_str) == aggregate_functions.end()
                           ? nullptr
                           : aggregate_functions[name_str](name_str, argument_types, parameters,
                                                           result_is_nullable);
        }
    }

    void register_function(const std::string& name, const Creator& creator, bool nullable = false) {
        if (nullable) {
            nullable_aggregate_functions[name] = creator;
        } else {
            aggregate_functions[name] = creator;
        }
    }

    void register_alias(const std::string& name, const std::string& alias) {
        function_alias[alias] = name;
    }

public:
    static AggregateFunctionSimpleFactory& instance();
};
}; // namespace doris::vectorized
