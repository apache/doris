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
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "function_test_util.h"
#include "vec/core/types.h"

namespace doris::vectorized {

TEST(function_levenshtein_test, test) {
    std::string func_name = "levenshtein";

    // Define input types: two strings
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};

    // Test data set: {{param1, param2}, expected result}
    DataSet data_set = {// Basic ASCII cases
                        {{std::string("kitten"), std::string("sitting")}, (int32_t)3},
                        {{std::string("rosettacode"), std::string("raisethysword")}, (int32_t)8},
                        {{std::string("hello"), std::string("hello")}, (int32_t)0},

                        // Empty string cases
                        {{std::string("abc"), std::string("")}, (int32_t)3},
                        {{std::string(""), std::string("abc")}, (int32_t)3},
                        {{std::string(""), std::string("")}, (int32_t)0},

                        // UTF-8 Chinese cases (Crucial Fix Verification)
                        // Expected distance is 1 (character level distance)
                        {{std::string("中国"), std::string("中")}, (int32_t)1},
                        {{std::string("测试"), std::string("测验")}, (int32_t)1},

                        // Null cases
                        {{Null(), std::string("abc")}, Null()},
                        {{std::string("abc"), Null()}, Null()},
                        {{Null(), Null()}, Null()}};

    // Run tests for Column vs Column (Vector vs Vector)
    check_function<DataTypeInt32, true>(func_name, input_types, data_set);

    // Note: The framework automatically handles Column vs Constant and Constant vs Column
    // when using check_function, verifying partial constant implementation.
}

} // namespace doris::vectorized