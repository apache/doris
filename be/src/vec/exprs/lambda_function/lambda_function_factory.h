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

#include "vec/exprs/lambda_function/lambda_function.h"

namespace doris::vectorized {

class LambdaFunctionFactory;

void register_function_array_map(LambdaFunctionFactory& factory);
void register_function_array_filter(LambdaFunctionFactory& factory);

class LambdaFunctionFactory {
    using Creator = std::function<LambdaFunctionPtr()>;
    using FunctionCreators = phmap::flat_hash_map<std::string, Creator>;

public:
    void register_function(const std::string& name, const Creator& ptr) {
        function_creators[name] = ptr;
    }

    template <class Function>
    void register_function() {
        register_function(Function::name, &Function::create);
    }

    LambdaFunctionPtr get_function(const std::string& name) {
        auto iter = function_creators.find(name);
        if (iter != function_creators.end()) {
            return iter->second();
        }
        LOG(WARNING) << fmt::format("Function signature {} is not found", name);
        return nullptr;
    }

private:
    FunctionCreators function_creators;

public:
    static LambdaFunctionFactory& instance() {
        static std::once_flag oc;
        static LambdaFunctionFactory instance;
        std::call_once(oc, []() {
            register_function_array_map(instance);
            register_function_array_filter(instance);
        });
        return instance;
    }
};
} // namespace doris::vectorized
