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

#include "util/wasm_manager.h"

namespace doris {

WasmFunctionManager::WasmFunctionManager() {
    engine = std::unique_ptr<wasmtime::Engine>(new wasmtime::Engine);
    store = std::unique_ptr<wasmtime::Store>(new wasmtime::Store(*engine));
}

WasmFunctionManager::~WasmFunctionManager() {
    engine.reset();
    store.reset();
}

bool WasmFunctionManager::RegisterFunction(std::string functionName, std::string functionHandler,
                                           const std::string& watString) {
    auto funcBody = funcs.find(functionName);
    if (funcBody != funcs.end()) {
        return false;
    }
    auto wasmRuntime = createInstanceAndFunction(watString, functionHandler);
    funcs.emplace(functionName, wasmRuntime);
    return true;
}

bool WasmFunctionManager::RegisterFunction(std::string functionName, std::string functionHandler,
                                           const wasmtime::Span<uint8_t>& wasm) {
    auto funcBody = funcs.find(functionName);
    if (funcBody != funcs.end()) {
        return false;
    }
    auto wasmRuntime = createInstanceAndFunction(wasm, functionHandler);
    funcs.emplace(functionName, wasmRuntime);
    return true;
}

WasmtimeRunInstance WasmFunctionManager::createInstanceAndFunction(
        const std::string& watString, const std::string& functionHandler) {
    auto module = wasmtime::Module::compile(*engine, watString).unwrap();
    auto instance = wasmtime::Instance::create(store.get(), module, {}).unwrap();
    auto function_obj = instance.get(store.get(), functionHandler);
    wasmtime::Func* func = std::get_if<wasmtime::Func>(&*function_obj);
    return WasmtimeRunInstance(*func, instance);
}

WasmtimeRunInstance WasmFunctionManager::createInstanceAndFunction(
        const wasmtime::Span<uint8_t> wasm, const std::string& functionHandler) {
    auto module = wasmtime::Module::compile(*engine, wasm).unwrap();
    auto instance = wasmtime::Instance::create(store.get(), module, {}).unwrap();
    auto function_obj = instance.get(store.get(), functionHandler);
    wasmtime::Func* func = std::get_if<wasmtime::Func>(&*function_obj);
    return WasmtimeRunInstance(*func, instance);
}

std::vector<wasmtime::Val> WasmFunctionManager::runElemFunc(const std::string functionName,
                                                            std::vector<wasmtime::Val> args) {
    auto module = funcs.at(functionName);
    auto results = module.func.call(store.get(), args).unwrap();
    return results;
}

bool WasmFunctionManager::DeleteFunction(std::string functionName) {
    auto funcBody = funcs.find(functionName);
    if (funcBody == funcs.end()) {
        return false;
    }
    funcs.erase(functionName);
    return true;
}

} // namespace doris