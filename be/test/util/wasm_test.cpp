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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <wasmtime.hh>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"

namespace doris {
class WasmtimeTest : public ::testing::Test {
    std::string readFile(const char* name) {
        std::ifstream watFile;
        watFile.open(name);
        std::stringstream strStream;
        strStream << watFile.rdbuf();
        return strStream.str();
    }
};

TEST_F(WasmtimeTest, TestLoadWasm) {
    std::string dir_path = GetCurrentRunningDir();
    std::string stat_path(dir_path);
    stat_path += "/util/test_data/add.wat";
    wasmtime::Engine engine;
    wasmtime::Store store(engine);
    const std::string wasm_body = readFile(stat_path.c_str());
    auto module = wasmtime::Module::compile(engine, wasm_body).unwrap();
    auto instance = wasmtime::Instance::create(store, module, {}).unwrap();

    auto add = std::get<wasmtime::Func>(*instance.get(store, "add"));
    std::vector<wasmtime::Val> params;
    auto params_size = 2;
    params.reserve(params_size);
    params.emplace_back(10);
    params.emplace_back(20);
    auto results = add.call(store, params).unwrap();

    EXPECT_EQ(results[0].i32(), 30);
}
} // namespace doris
