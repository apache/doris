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

#include <cassert>
#include <fstream>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include <wasmtime.hh>

namespace doris {

struct WasmtimeRunInstance {
    wasmtime::Func func;
    wasmtime::Instance instance;
    WasmtimeRunInstance(const wasmtime::Func& func, const wasmtime::Instance& instance)
            : func(func), instance(instance) {}
};

class WasmFunctionManager {
private:
    // wasmtime
    wasmtime::Engine* engine;
    wasmtime::Store* store;
    std::unordered_map<std::string, WasmtimeRunInstance> funcs;

    WasmFunctionManager(const WasmFunctionManager&);
    WasmFunctionManager& operator=(const WasmFunctionManager&);

public:
    WasmFunctionManager();
    ~WasmFunctionManager();
    WasmtimeRunInstance createInstanceAndFunction(const std::string& watString,
                                                  const std::string& functionHandler);
    WasmtimeRunInstance createInstanceAndFunction(const wasmtime::Span<uint8_t> wasm,
                                                  const std::string& functionHandler);
    bool RegisterFunction(std::string functionName, std::string functionHandler,
                          const std::string& watString);
    bool RegisterFunction(std::string functionName, std::string functionHandler,
                          const wasmtime::Span<uint8_t>& wasm);
    std::vector<wasmtime::Val> runElemFunc(const std::string functionName,
                                           std::vector<wasmtime::Val> args);
    void runWat(const std::string& watString) const;
    bool DeleteFunction(std::string functionName);
};

} // namespace doris
