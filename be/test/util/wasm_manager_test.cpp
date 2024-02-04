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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <fstream>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "testutil/test_util.h"

namespace doris {

class WasmManagerTest : public ::testing::Test {
    std::string readFile(const char* name) {
        std::ifstream wat_file;
        wat_file.open(name);
        std::stringstream str_stream;
        str_stream << wat_file.rdbuf();
        return str_stream.str();
    }
};

TEST_F(WasmManagerTest, TestExecAdd) {
    std::string dir_path = GetCurrentRunningDir();
    std::string stat_path(dir_path);
    stat_path += "/util/test_data/add.wat";
    WasmFunctionManager manager;
    const std::string wasm_body = readFile(stat_path.c_str());
    const std::string wasm_function_name = "add";
    manager.RegisterFunction(wasm_function_name, wasm_function_name, wasm_body);
    std::vector<wasmtime::Val> params;
    auto params_size = 2;
    params.reserve(params_size);
    params.emplace_back(10);
    params.emplace_back(20);
    auto results = manager.runElemFunc(wasm_function_name, params);
    EXPECT_EQ(results[0].i32(), 30);
}
} // namespace doris