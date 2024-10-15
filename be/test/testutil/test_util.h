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

#include <cstdlib>
#include <string>

#include "gtest/gtest.h"

struct TestCaseInfo {
    inline static int arg_const_info {};
    inline static int cur_cast_line {};
    inline static int arg_size {};
};

//"Rewrite the virtual functions of the gtest framework to enable listening to test events.
// If a test fails, output the constant conditions of the corresponding data case number and parameters
// that caused the failure."
struct TestListener : public ::testing::EmptyTestEventListener {
    void OnTestPartResult(const ::testing::TestPartResult& test_part_result) override {
        if (test_part_result.failed()) {
            std::cout << "\033[35m";
            if (TestCaseInfo::arg_const_info == -1) {
                std::cout << "other error" << std::endl;
            }
            for (int i = 0; i < TestCaseInfo::arg_size; i++) {
                if ((1 << i) & TestCaseInfo::arg_const_info) {
                    std::cout << "const arg" << i + 1 << " ";
                } else {
                    std::cout << "arg" << i + 1 << " ";
                }
            }
            std::cout << "\nError occurred in case" << TestCaseInfo::cur_cast_line << std::endl;
            std::cout << "\033[0m";
        }
    }
};

namespace doris {
enum class FieldType;

#define LOOP_LESS_OR_MORE(less, more) (AllowSlowTests() ? more : less)

// Get the value of an environment variable that has boolean semantics.
bool GetBooleanEnvironmentVariable(const char* env_var_name);

// Returns true if slow tests are runtime-enabled.
bool AllowSlowTests();

// Returns the path of the folder containing the currently running executable.
// Empty string if get errors.
std::string GetCurrentRunningDir();

// Initialize config file.
void InitConfig();

bool equal_ignore_case(std::string lhs, std::string rhs);

int rand_rng_int(int l, int r);
char rand_rng_char();
std::string rand_rng_string(size_t length = 8);
std::string rand_rng_by_type(FieldType fieldType);

} // namespace doris
