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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
using AggregateFunctionCreator =
        std::function<AggregateFunctionPtr(const std::string&, const DataTypes&, const bool)>;

class AggregateFunctionSimpleFactory {
public:
    using Creator = AggregateFunctionCreator;

private:
    using AggregateFunctions = std::unordered_map<std::string, Creator>;

    AggregateFunctions aggregate_functions;
    AggregateFunctions nullable_aggregate_functions;
    std::unordered_map<std::string, std::string> function_alias;
    /// @TEMPORARY: for be_exec_version=2
    /// in order to solve agg of sum/count is not compatibility during the upgrade process
    constexpr static int AGG_FUNCTION_NEW = 2;
    /// @TEMPORARY: for be_exec_version < AGG_FUNCTION_NEW. replace function to old version.
    std::unordered_map<std::string, std::string> function_to_replace;

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
                             const bool result_is_nullable = false,
                             int be_version = BeExecVersionManager::get_newest_version(),
                             bool enable_decima256 = false) {
        bool nullable = false;
        for (const auto& type : argument_types) {
            if (type->is_nullable()) {
                nullable = true;
            }
        }

        std::string name_str = name;
        if (enable_decima256) {
            if (name_str == "sum" || name_str == "avg") {
                name_str += "_decimal256";
            }
        }
        temporary_function_update(be_version, name_str);

        if (function_alias.count(name)) {
            name_str = function_alias[name];
        }

        if (nullable) {
            return nullable_aggregate_functions.find(name_str) == nullable_aggregate_functions.end()
                           ? nullptr
                           : nullable_aggregate_functions[name_str](name_str, argument_types,
                                                                    result_is_nullable);
        } else {
            return aggregate_functions.find(name_str) == aggregate_functions.end()
                           ? nullptr
                           : aggregate_functions[name_str](name_str, argument_types,
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

    void register_function_both(const std::string& name, const Creator& creator) {
        register_function(name, creator, false);
        register_function(name, creator, true);
    }

    void register_alias(const std::string& name, const std::string& alias) {
        function_alias[alias] = name;
    }

    /// @TEMPORARY: for be_exec_version < AGG_FUNCTION_NEW
    void register_alternative_function(const std::string& name, const Creator& creator,
                                       bool nullable = false) {
        static std::string suffix {"_old_for_version_before_2_0"};
        register_function(name + suffix, creator, nullable);
        function_to_replace[name] = name + suffix;
    }

    /// @TEMPORARY: for be_exec_version < AGG_FUNCTION_NEW
    void temporary_function_update(int fe_version_now, std::string& name) {
        // replace if fe is old version.
        if (fe_version_now < AGG_FUNCTION_NEW &&
            function_to_replace.find(name) != function_to_replace.end()) {
            name = function_to_replace[name];
        }
    }

public:
    static AggregateFunctionSimpleFactory& instance();
};
}; // namespace doris::vectorized
