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

#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "vec/data_types/serde/data_type_serde.h"

DECLARE_bool(gen_out);
DECLARE_bool(gen_regression_case);
extern const std::string kApacheLicenseHeader;

struct TestCaseInfo {
    // index of call of check_function in one TEST or TEST_F
    inline static thread_local int func_call_index {};
    // bitmask of which args are const
    inline static thread_local int arg_const_info {};
    // line number of the test case that failed
    inline static thread_local int error_line_number {};
    // number of args. set by size of input_types in check_function
    inline static thread_local int arg_size {};
};

//"Rewrite the virtual functions of the gtest framework to enable listening to test events.
// If a test fails, output the constant conditions of the corresponding data case number and parameters
// that caused the failure."
struct TestListener : public ::testing::EmptyTestEventListener {
    void OnTestStart(const testing::TestInfo& test_info) override {
        TestCaseInfo::func_call_index = 0;
        TestCaseInfo::arg_const_info = 0;
        TestCaseInfo::error_line_number = -1;
        TestCaseInfo::arg_size = 0;
    }

    void OnTestPartResult(const ::testing::TestPartResult& test_part_result) override {
        if (test_part_result.failed()) {
            std::cout << "\033[35m"; // purple
            std::cout << "Error occurred in " << TestCaseInfo::func_call_index
                      << "-th test case's row " << TestCaseInfo::error_line_number << std::endl;
            if (TestCaseInfo::arg_const_info == -1) {
                std::cout << "other error" << std::endl;
            }
            for (int i = 0; i < TestCaseInfo::arg_size; i++) {
                if ((1 << i) & TestCaseInfo::arg_const_info) {
                    std::cout << "const arg" << i << " ";
                } else {
                    std::cout << "arg" << i << " ";
                }
            }
            std::cout << "\033[0m\n"; // reset color
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

void load_columns_data_from_file(vectorized::MutableColumns& columns,
                                 vectorized::DataTypeSerDeSPtrs serders, char col_spliter,
                                 std::set<int> idxes, const std::string& column_data_file);
void load_data_from_csv(const vectorized::DataTypeSerDeSPtrs serders,
                        vectorized::MutableColumns& columns, const std::string& file_path,
                        const char spliter = ';', const std::set<int> idxes = {0});
void check_or_generate_res_file(const std::string& res_file_path,
                                const std::vector<std::vector<std::string>>& res_columns);

} // namespace doris
