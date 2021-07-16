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
#include <mutex>
#include <string>

#include "vec/functions/function.h"

namespace doris::vectorized {

class SimpleFunctionFactory;

void register_function_comparison(SimpleFunctionFactory& factory);
void register_function_hll_cardinality(SimpleFunctionFactory& factory);
void register_function_hll_empty(SimpleFunctionFactory& factory);
void register_function_hll_hash(SimpleFunctionFactory& factory);
void register_function_logical(SimpleFunctionFactory& factory);
void register_function_cast(SimpleFunctionFactory& factory);
void register_function_plus(SimpleFunctionFactory& factory);
void register_function_minus(SimpleFunctionFactory& factory);
void register_function_multiply(SimpleFunctionFactory& factory);
void register_function_divide(SimpleFunctionFactory& factory);
void register_function_int_div(SimpleFunctionFactory& factory);
void register_function_math(SimpleFunctionFactory& factory);
void register_function_modulo(SimpleFunctionFactory& factory);
void register_function_bitmap(SimpleFunctionFactory& factory);
void register_function_is_null(SimpleFunctionFactory& factory);
void register_function_is_not_null(SimpleFunctionFactory& factory);
void register_function_to_time_fuction(SimpleFunctionFactory& factory);
void register_function_time_of_fuction(SimpleFunctionFactory& factory);
void register_function_string(SimpleFunctionFactory& factory);
void register_function_date_time_to_string(SimpleFunctionFactory& factory);
void register_function_date_time_string_to_string(SimpleFunctionFactory& factory);
void register_function_in(SimpleFunctionFactory& factory);
void register_function_if(SimpleFunctionFactory& factory);
void register_function_date_time_computation(SimpleFunctionFactory& factory);
void register_function_str_to_date(SimpleFunctionFactory& factory);
void register_function_json(SimpleFunctionFactory& factory);

class SimpleFunctionFactory {
    using Creator = std::function<FunctionBuilderPtr()>;
    using FunctionCreators = std::unordered_map<std::string, Creator>;

public:
    void register_function(const std::string& name, Creator ptr) { function_creators[name] = ptr; }

    template <class Function>
    void register_function() {
        if constexpr (std::is_base_of<IFunction, Function>::value)
            register_function(Function::name, &createDefaultFunction<Function>);
        else
            register_function(Function::name, &Function::create);
    }

    void register_alias(const std::string& name, const std::string& alias) {
        function_creators[alias] = function_creators[name];
    }

    FunctionBasePtr get_function(const std::string& name, const ColumnsWithTypeAndName& arguments,
                                 const DataTypePtr& return_type) {
        auto iter = function_creators.find(name);
        if (iter != function_creators.end()) {
            return iter->second()->build(arguments, return_type);
        }
        return nullptr;
    }

private:
    FunctionCreators function_creators;

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
            register_function_hll_cardinality(instance);
            register_function_hll_empty(instance);
            register_function_hll_hash(instance);
            register_function_comparison(instance);
            register_function_logical(instance);
            register_function_cast(instance);
            register_function_plus(instance);
            register_function_minus(instance);
            register_function_math(instance);
            register_function_multiply(instance);
            register_function_divide(instance);
            register_function_int_div(instance);
            register_function_modulo(instance);
            register_function_is_null(instance);
            register_function_is_not_null(instance);
            register_function_to_time_fuction(instance);
            register_function_time_of_fuction(instance);
            register_function_string(instance);
            register_function_in(instance);
            register_function_if(instance);
            register_function_date_time_computation(instance);
            register_function_str_to_date(instance);
            register_function_date_time_to_string(instance);
            register_function_date_time_string_to_string(instance);
            register_function_json(instance);
        });
        return instance;
    }
};
} // namespace doris::vectorized
