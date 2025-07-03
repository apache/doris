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

#include "agent/be_exec_version_manager.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

constexpr auto DECIMAL256_FUNCTION_SUFFIX {"_decimal256"};

class SimpleFunctionFactory;

void register_function_size(SimpleFunctionFactory& factory);
void register_function_comparison(SimpleFunctionFactory& factory);
void register_function_comparison_eq_for_null(SimpleFunctionFactory& factory);
void register_function_hll(SimpleFunctionFactory& factory);
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
void register_function_bit_count(SimpleFunctionFactory& factory);
void register_function_bit_shift(SimpleFunctionFactory& factory);
void register_function_round(SimpleFunctionFactory& factory);
void register_function_math(SimpleFunctionFactory& factory);
void register_function_modulo(SimpleFunctionFactory& factory);
void register_function_bitmap(SimpleFunctionFactory& factory);
void register_function_bitmap_variadic(SimpleFunctionFactory& factory);
void register_function_quantile_state(SimpleFunctionFactory& factory);
void register_function_is_null(SimpleFunctionFactory& factory);
void register_function_is_not_null(SimpleFunctionFactory& factory);
void register_function_nullables(SimpleFunctionFactory& factory);
void register_function_to_time_function(SimpleFunctionFactory& factory);
void register_function_time_value_field(SimpleFunctionFactory& factory);
void register_function_time_of_function(SimpleFunctionFactory& factory);
void register_function_string(SimpleFunctionFactory& factory);
void register_function_running_difference(SimpleFunctionFactory& factory);
void register_function_date_time_to_string(SimpleFunctionFactory& factory);
void register_function_date_time_string_to_string(SimpleFunctionFactory& factory);
void register_function_in(SimpleFunctionFactory& factory);
void register_function_collection_in(SimpleFunctionFactory& factory);
void register_function_if(SimpleFunctionFactory& factory);
void register_function_nullif(SimpleFunctionFactory& factory);
void register_function_date_time_computation(SimpleFunctionFactory& factory);
void register_function_date_time_computation_v2(SimpleFunctionFactory& factory);
void register_function_timestamp(SimpleFunctionFactory& factory);
void register_function_utility(SimpleFunctionFactory& factory);
void register_function_json(SimpleFunctionFactory& factory);
void register_function_jsonb(SimpleFunctionFactory& factory);
void register_function_hash(SimpleFunctionFactory& factory);
void register_function_ifnull(SimpleFunctionFactory& factory);
void register_function_like(SimpleFunctionFactory& factory);
void register_function_regexp(SimpleFunctionFactory& factory);
void register_function_random(SimpleFunctionFactory& factory);
void register_function_uuid(SimpleFunctionFactory& factory);
void register_function_uuid_numeric(SimpleFunctionFactory& factory);
void register_function_uuid_transforms(SimpleFunctionFactory& factory);
void register_function_coalesce(SimpleFunctionFactory& factory);
void register_function_grouping(SimpleFunctionFactory& factory);
void register_function_datetime_floor_ceil(SimpleFunctionFactory& factory);
void register_function_convert_tz(SimpleFunctionFactory& factory);
void register_function_least_greast(SimpleFunctionFactory& factory);
void register_function_fake(SimpleFunctionFactory& factory);
void register_function_array(SimpleFunctionFactory& factory);
void register_function_map(SimpleFunctionFactory& factory);
void register_function_struct(SimpleFunctionFactory& factory);
void register_function_struct_element(SimpleFunctionFactory& factory);
void register_function_variant_element(SimpleFunctionFactory& factory);
void register_function_geo(SimpleFunctionFactory& factory);
void register_function_multi_string_position(SimpleFunctionFactory& factory);
void register_function_multi_string_search(SimpleFunctionFactory& factory);
void register_function_width_bucket(SimpleFunctionFactory& factory);
void register_function_ignore(SimpleFunctionFactory& factory);
void register_function_encryption(SimpleFunctionFactory& factory);
void register_function_regexp_extract(SimpleFunctionFactory& factory);
void register_function_hex_variadic(SimpleFunctionFactory& factory);
void register_function_match(SimpleFunctionFactory& factory);
void register_function_tokenize(SimpleFunctionFactory& factory);
void register_function_url(SimpleFunctionFactory& factory);
void register_function_ip(SimpleFunctionFactory& factory);
void register_function_multi_match(SimpleFunctionFactory& factory);
void register_function_bit_test(SimpleFunctionFactory& factory);

class SimpleFunctionFactory {
    using Creator = std::function<FunctionBuilderPtr()>;
    using FunctionCreators = phmap::flat_hash_map<std::string, Creator>;
    using FunctionIsVariadic = phmap::flat_hash_set<std::string>;
    /// @TEMPORARY: for be_exec_version=4
    constexpr static int NEWEST_VERSION_FUNCTION_SUBSTITUTE = 4;

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
        if constexpr (std::is_base_of<IFunction, Function>::value) {
            register_function(Function::name, &createDefaultFunction<Function>);
        } else {
            register_function(Function::name, &Function::create);
        }
    }

    template <class Function>
    void register_function(std::string name) {
        register_function(name, &createDefaultFunction<Function>);
    }

    void register_alias(const std::string& name, const std::string& alias) {
        function_alias[alias] = name;
    }

    /// @TEMPORARY: for be_exec_version=3
    template <class Function>
    void register_alternative_function() {
        static std::string suffix {"_old_for_version_before_4_0"};
        function_to_replace[Function::name] = Function::name + suffix;
        register_function(Function::name + suffix, &createDefaultFunction<Function>);
    }

    FunctionBasePtr get_function(const std::string& name, const ColumnsWithTypeAndName& arguments,
                                 const DataTypePtr& return_type, const FunctionAttr& attr = {},
                                 int be_version = BeExecVersionManager::get_newest_version()) {
        std::string key_str = name;

        // special function replacement
        if (key_str == "is_ip_address_in_range" && !attr.new_is_ip_address_in_range) [[unlikely]] {
            key_str = "__is_ip_address_in_range_OLD__";
        }

        if (function_alias.contains(name)) {
            key_str = function_alias[name];
        }

        if (attr.enable_decimal256) {
            if (key_str == "array_sum" || key_str == "array_avg" || key_str == "array_product" ||
                key_str == "array_cum_sum") {
                key_str += DECIMAL256_FUNCTION_SUFFIX;
            }
        }

        temporary_function_update(be_version, key_str);

        // if function is variadic, added types_str as key
        if (function_variadic_set.count(key_str)) {
            for (const auto& arg : arguments) {
                key_str.append(arg.type->is_nullable()
                                       ? reinterpret_cast<const DataTypeNullable*>(arg.type.get())
                                                 ->get_nested_type()
                                                 ->get_family_name()
                                       : arg.type->get_family_name());
            }
        }

        auto iter = function_creators.find(key_str);
        if (iter == function_creators.end()) {
            // use original name as signature without variadic arguments
            iter = function_creators.find(name);
            if (iter == function_creators.end()) {
                LOG(WARNING) << fmt::format("Function signature {} is not found", key_str);
                return nullptr;
            }
        }

        return iter->second()->build(arguments, return_type);
    }

private:
    FunctionCreators function_creators;
    FunctionIsVariadic function_variadic_set;
    std::unordered_map<std::string, std::string> function_alias;
    /// @TEMPORARY: for be_exec_version=3. replace function to old version.
    std::unordered_map<std::string, std::string> function_to_replace;

    template <typename Function>
    static FunctionBuilderPtr createDefaultFunction() {
        return std::make_shared<DefaultFunctionBuilder>(Function::create());
    }

    /// @TEMPORARY: for be_exec_version=3
    void temporary_function_update(int fe_version_now, std::string& name) {
        // replace if fe is old version.
        if (fe_version_now < NEWEST_VERSION_FUNCTION_SUBSTITUTE &&
            function_to_replace.find(name) != function_to_replace.end()) {
            name = function_to_replace[name];
        }
    }

public:
    static SimpleFunctionFactory& instance() {
        static std::once_flag oc;
        static SimpleFunctionFactory instance;
        std::call_once(oc, []() {
            register_function_size(instance);
            register_function_bitmap(instance);
            register_function_quantile_state(instance);
            register_function_bitmap_variadic(instance);
            register_function_hll(instance);
            register_function_comparison(instance);
            register_function_logical(instance);
            register_function_case(instance);
            register_function_cast(instance);
            register_function_conv(instance);
            register_function_plus(instance);
            register_function_minus(instance);
            register_function_round(instance);
            register_function_math(instance);
            register_function_multiply(instance);
            register_function_divide(instance);
            register_function_int_div(instance);
            register_function_modulo(instance);
            register_function_bit(instance);
            register_function_bit_count(instance);
            register_function_bit_shift(instance);
            register_function_is_null(instance);
            register_function_is_not_null(instance);
            register_function_nullables(instance);
            register_function_to_time_function(instance);
            register_function_time_value_field(instance);
            register_function_time_of_function(instance);
            register_function_string(instance);
            register_function_in(instance);
            register_function_collection_in(instance);
            register_function_if(instance);
            register_function_nullif(instance);
            register_function_date_time_computation(instance);
            register_function_date_time_computation_v2(instance);
            register_function_timestamp(instance);
            register_function_utility(instance);
            register_function_date_time_to_string(instance);
            register_function_date_time_string_to_string(instance);
            register_function_json(instance);
            register_function_jsonb(instance);
            register_function_hash(instance);
            register_function_ifnull(instance);
            register_function_comparison_eq_for_null(instance);
            register_function_like(instance);
            register_function_regexp(instance);
            register_function_random(instance);
            register_function_uuid(instance);
            register_function_uuid_numeric(instance);
            register_function_uuid_transforms(instance);
            register_function_coalesce(instance);
            register_function_grouping(instance);
            register_function_datetime_floor_ceil(instance);
            register_function_convert_tz(instance);
            register_function_least_greast(instance);
            register_function_fake(instance);
            register_function_encryption(instance);
            register_function_regexp_extract(instance);
            register_function_hex_variadic(instance);
            register_function_array(instance);
            register_function_map(instance);
            register_function_struct(instance);
            register_function_struct_element(instance);
            register_function_geo(instance);
            register_function_url(instance);
            register_function_multi_string_position(instance);
            register_function_multi_string_search(instance);
            register_function_width_bucket(instance);
            register_function_match(instance);
            register_function_ip(instance);
            register_function_tokenize(instance);
            register_function_ignore(instance);
            register_function_variant_element(instance);
            register_function_multi_match(instance);
            register_function_bit_test(instance);
        });
        return instance;
    }
};
} // namespace doris::vectorized
