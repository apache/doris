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

#include "wasm_manager.h"
namespace doris {

WasmFunctionManager::WasmFunctionManager() {
    engine = new wasmtime::Engine;
    store = new wasmtime::Store(*engine);
}

WasmFunctionManager::~WasmFunctionManager() {
    delete (store);
    delete (engine);
}

bool WasmFunctionManager::RegisterFunction(std::string functionName, std::string functionHandler,
                                           const std::string& watString) {
    auto funcBody = funcMap.find(functionName);
    if (funcBody != funcMap.end()) {
        return false;
    }
    auto wasmRuntime = createInstanceAndFunction(watString, functionHandler);
    modules.emplace(functionName, wasmRuntime);
    funcMap.emplace(functionName, watString);
    return true;
}

WasmtimeRunInstance WasmFunctionManager::createInstanceAndFunction(
        const std::string& watString, const std::string& functionHandler) {
    auto module = wasmtime::Module::compile(*engine, watString).unwrap();
    auto instance = wasmtime::Instance::create(store, module, {}).unwrap();
    auto function_obj = instance.get(store, functionHandler);
    wasmtime::Func* func = std::get_if<wasmtime::Func>(&*function_obj);
    return WasmtimeRunInstance(*func, instance);
}

std::vector<wasmtime::Val> WasmFunctionManager::runElemFunc(const std::string functionName,
                                                            std::vector<wasmtime::Val> args) {
    auto module = modules.at(functionName);
    auto results = module.func.call(store, args).unwrap();
    return results;
}

bool WasmFunctionManager::DeleteFunction(std::string functionName) {
    auto funcBody = funcMap.find(functionName);
    if (funcBody == funcMap.end()) {
        return false;
    }
    modules.erase(functionName);
    return true;
}

void WasmFunctionManager::runWat(const std::string& watString) const {
    auto module = wasmtime::Module::compile(*engine, watString).unwrap();
    auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

    auto gcd = std::get<wasmtime::Func>(*instance.get(store, "gcd"));
    auto results = gcd.call(store, {6, 27}).unwrap();

    std::cout << results[0].i32() << "\n";
}

} // namespace doris