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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/registerFunctions.h
// and modified by Doris

#pragma once

#include <mutex>
#include <string>

#include "vec/functions/function.h"

namespace doris::vectorized {

class SimpleFunctionFactory;

void register_function_comparison(SimpleFunctionFactory& factory);
void register_function_comparison_eq_for_null(SimpleFunctionFactory& factory);
void register_function_hll_cardinality(SimpleFunctionFactory& factory);
void register_function_hll_empty(SimpleFunctionFactory& factory);
void register_function_hll_hash(SimpleFunctionFactory& factory);
void register_function_logical(SimpleFunctionFactory& factory);
void register_function_case(SimpleFunctionFactory& factory);
void register_function_cast(SimpleFunctionFactory& factory);
void register_function_conv(SimpleFunctionFactory& factory);
void register_function_plus(SimpleFunctionFactory& factory);
void register_function_minus(SimpleFunctionFactory& factory);
void register_function_multiply(SimpleFunctionFactory& factory);
void register_function_divide(SimpleFunctionFactory& factory);
void register_function_int_div(SimpleFunctionFactory& factory);
void register_function_bit(SimpleFunctionFactory& factory);
void register_function_math(SimpleFunctionFactory& factory);
void register_function_modulo(SimpleFunctionFactory& factory);
void register_function_bitmap(SimpleFunctionFactory& factory);
void register_function_bitmap_variadic(SimpleFunctionFactory& factory);
void register_function_is_null(SimpleFunctionFactory& factory);
void register_function_is_not_null(SimpleFunctionFactory& factory);
void register_function_to_time_function(SimpleFunctionFactory& factory);
void register_function_time_of_function(SimpleFunctionFactory& factory);
void register_function_string(SimpleFunctionFactory& factory);
void register_function_date_time_to_string(SimpleFunctionFactory& factory);
void register_function_date_time_string_to_string(SimpleFunctionFactory& factory);
void register_function_in(SimpleFunctionFactory& factory);
void register_function_if(SimpleFunctionFactory& factory);
void register_function_nullif(SimpleFunctionFactory& factory);
void register_function_date_time_computation(SimpleFunctionFactory& factory);
void register_function_timestamp(SimpleFunctionFactory& factory);
void register_function_utility(SimpleFunctionFactory& factory);
void register_function_json(SimpleFunctionFactory& factory);
void register_function_function_hash(SimpleFunctionFactory& factory);
void register_function_function_ifnull(SimpleFunctionFactory& factory);
void register_function_like(SimpleFunctionFactory& factory);
void register_function_regexp(SimpleFunctionFactory& factory);
void register_function_random(SimpleFunctionFactory& factory);
void register_function_coalesce(SimpleFunctionFactory& factory);
void register_function_grouping(SimpleFunctionFactory& factory);
void register_function_datetime_floor_ceil(SimpleFunctionFactory& factory);
void register_function_convert_tz(SimpleFunctionFactory& factory);
void register_function_least_greast(SimpleFunctionFactory& factory);
void register_function_fake(SimpleFunctionFactory& factory);
void register_geo_functions(SimpleFunctionFactory& factory);

void register_function_encryption(SimpleFunctionFactory& factory);
void register_function_regexp_extract(SimpleFunctionFactory& factory);
void register_function_hex_variadic(SimpleFunctionFactory& factory);
class SimpleFunctionFactory {
    using Creator = std::function<FunctionBuilderPtr()>;
    using FunctionCreators = phmap::flat_hash_map<std::string, Creator>;
    using FunctionIsVariadic = phmap::flat_hash_set<std::string>;

public:
    void register_function(const std::string& name, const Creator& ptr) {
        DataTypes types = ptr()->get_variadic_argument_types();
        // types.empty() means function is not variadic
        if (!types.empty()) {
            function_variadic_set.insert(name);
        }

        std::string key_str = name;
        if (!types.empty()) {
            for (const auto& type : types) {
                key_str.append(type->get_family_name());
            }
        }
        function_creators[key_str] = ptr;
    }

    template <class Function>
    void register_function() {
        if constexpr (std::is_base_of<IFunction, Function>::value)
            register_function(Function::name, &createDefaultFunction<Function>);
        else
            register_function(Function::name, &Function::create);
    }

    void register_alias(const std::string& name, const std::string& alias) {
        function_alias[alias] = name;
    }

    FunctionBasePtr get_function(const std::string& name, const ColumnsWithTypeAndName& arguments,
                                 const DataTypePtr& return_type) {
        std::string key_str = name;

        if (function_alias.count(name)) {
            key_str = function_alias[name];
        }

        // if function is variadic, added types_str as key
        if (function_variadic_set.count(key_str)) {
            for (auto& arg : arguments) {
                key_str.append(arg.type->is_nullable()
                                       ? reinterpret_cast<const DataTypeNullable*>(arg.type.get())
                                                 ->get_nested_type()
                                                 ->get_family_name()
                                       : arg.type->get_family_name());
            }
        }

        auto iter = function_creators.find(key_str);
        if (iter != function_creators.end()) {
            return iter->second()->build(arguments, return_type);
        }

        return nullptr;
    }

private:
    FunctionCreators function_creators;
    FunctionIsVariadic function_variadic_set;
    std::unordered_map<std::string, std::string> function_alias;

    template <typename Function>
    static FunctionBuilderPtr createDefaultFunction() {
        return std::make_shared<DefaultFunctionBuilder>(Function::create());
    }

public:
    static SimpleFunctionFactory& instance() {
        static std::once_flag oc;
        static SimpleFunctionFactory instance;
        std::call_once(oc, [&]() {
            register_function_bitmap(instance);
            register_function_bitmap_variadic(instance);
            register_function_hll_cardinality(instance);
            register_function_hll_empty(instance);
            register_function_hll_hash(instance);
            register_function_comparison(instance);
            register_function_logical(instance);
            register_function_case(instance);
            register_function_cast(instance);
            register_function_conv(instance);
            register_function_plus(instance);
            register_function_minus(instance);
            register_function_math(instance);
            register_function_multiply(instance);
            register_function_divide(instance);
            register_function_int_div(instance);
            register_function_modulo(instance);
            register_function_bit(instance);
            register_function_is_null(instance);
            register_function_is_not_null(instance);
            register_function_to_time_function(instance);
            register_function_time_of_function(instance);
            register_function_string(instance);
            register_function_in(instance);
            register_function_if(instance);
            register_function_nullif(instance);
            register_function_date_time_computation(instance);
            register_function_timestamp(instance);
            register_function_utility(instance);
            register_function_date_time_to_string(instance);
            register_function_date_time_string_to_string(instance);
            register_function_json(instance);
            register_function_function_hash(instance);
            register_function_function_ifnull(instance);
            register_function_comparison_eq_for_null(instance);
            register_function_like(instance);
            register_function_regexp(instance);
            register_function_random(instance);
            register_function_coalesce(instance);
            register_function_grouping(instance);
            register_function_datetime_floor_ceil(instance);
            register_function_convert_tz(instance);
            register_function_least_greast(instance);
            register_function_fake(instance);
            register_function_encryption(instance);
            register_function_regexp_extract(instance);
            register_function_hex_variadic(instance);
            register_geo_functions(instance);
        });
        return instance;
    }
};
} // namespace doris::vectorized
