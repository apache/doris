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
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
using DataTypePtr = std::shared_ptr<const IDataType>;
using DataTypes = std::vector<DataTypePtr>;
using AggregateFunctionCreator = std::function<AggregateFunctionPtr(
        const std::string&, const DataTypes&, const bool, const AggregateFunctionAttr&)>;

inline std::string types_name(const DataTypes& types) {
    std::string name;
    for (auto&& type : types) {
        name += type->get_name();
    }
    return name;
}

class AggregateFunctionSimpleFactory {
public:
    using Creator = AggregateFunctionCreator;

private:
    using AggregateFunctions = std::unordered_map<std::string, Creator>;
    constexpr static std::string_view combiner_names[] = {"_foreach"};
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

    static bool is_foreach(const std::string& name) {
        constexpr std::string_view suffix = "_foreach";
        if (name.length() < suffix.length()) {
            return false;
        }
        return name.substr(name.length() - suffix.length()) == suffix;
    }

    static bool result_nullable_by_foreach(DataTypePtr& data_type) {
        // The return value of the 'foreach' function is 'null' or 'array<type>'.
        // The internal function's nullable should depend on whether 'type' is nullable
        DCHECK(data_type->is_nullable());
        return assert_cast<const DataTypeArray*>(remove_nullable(data_type).get())
                ->get_nested_type()
                ->is_nullable();
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

    void register_foreach_function_combinator(const Creator& creator, const std::string& suffix,
                                              bool nullable = false) {
        auto& functions = nullable ? nullable_aggregate_functions : aggregate_functions;
        std::vector<std::string> need_insert;
        for (const auto& entity : aggregate_functions) {
            std::string target_value = entity.first + suffix;
            if (functions.find(target_value) == functions.end()) {
                need_insert.emplace_back(std::move(target_value));
            }
        }
        for (const auto& function_name : need_insert) {
            register_function(function_name, creator, nullable);
        }
    }

    AggregateFunctionPtr get(const std::string& name, const DataTypes& argument_types,
                             const bool result_is_nullable, int be_version,
                             AggregateFunctionAttr attr = {}) {
        bool nullable = false;
        for (const auto& type : argument_types) {
            if (type->is_nullable()) {
                nullable = true;
            }
        }

        std::string name_str = name;
        temporary_function_update(be_version, name_str);

        if (function_alias.contains(name)) {
            name_str = function_alias[name];
        }
        if (nullable) {
            return nullable_aggregate_functions.find(name_str) == nullable_aggregate_functions.end()
                           ? nullptr
                           : nullable_aggregate_functions[name_str](name_str, argument_types,
                                                                    result_is_nullable, attr);
        } else {
            return aggregate_functions.find(name_str) == aggregate_functions.end()
                           ? nullptr
                           : aggregate_functions[name_str](name_str, argument_types,
                                                           result_is_nullable, attr);
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
        for (const auto& s : combiner_names) {
            function_alias[alias + std::string(s)] = name + std::string(s);
        }
    }

    void register_alternative_function(const std::string& name, const Creator& creator,
                                       bool nullable, int old_be_exec_version) {
        auto new_name = name + BeExecVersionManager::get_function_suffix(old_be_exec_version);
        register_function(new_name, creator, nullable);
        BeExecVersionManager::registe_old_function_compatibility(old_be_exec_version, name);
    }

    void temporary_function_update(int fe_version_now, std::string& name) {
        int old_version = BeExecVersionManager::get_function_compatibility(fe_version_now, name);
        if (!old_version) {
            return;
        }
        name = name + BeExecVersionManager::get_function_suffix(old_version);
    }

    static AggregateFunctionSimpleFactory& instance();
};
}; // namespace doris::vectorized
