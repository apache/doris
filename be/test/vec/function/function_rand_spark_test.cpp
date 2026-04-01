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

#include "function_test_util.h"
#include "vec/core/types.h"

namespace doris::vectorized {

TEST(FunctionRandSparkTest, rand_spark_with_seed) {
    std::string func_name = "rand_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};

    // Test rand_spark(0) - deterministic result
    DataSet data_set = {{{(int64_t)0}, 0.1597933633704609}};
    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(FunctionRandSparkTest, rand_spark_with_null) {
    std::string func_name = "rand_spark";
    InputTypeSet input_types = {Nullable {PrimitiveType::TYPE_BIGINT}};

    // Test rand_spark(NULL) - should behave same as rand_spark(0)
    DataSet data_set = {{{Null()}, 0.1597933633704609}};
    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(FunctionRandSparkTest, rand_spark_with_constant_expressions) {
    std::string func_name = "rand_spark";
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};

    // Test rand_spark(10 + 5) -  should behave same as rand_spark(15)
    DataSet data_set = {
            {{(int64_t)10 + (int64_t)5}, 0.7827144101448653},
            {{(int64_t)15}, 0.7827144101448653},
    };
    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
