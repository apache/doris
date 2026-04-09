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

#include <cstdint>
#include <string>

#include "function_test_util.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

using namespace ut_type;

TEST(function_substr_spark_test, string_utf8) {
    std::string func_name = "substr_spark";

    // 3-arg
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_INT};
        DataSet data_set = {
                // Spark UTF8String#substringSQL: startCp = len + pos; take [startCp, startCp + len).
                {{std::string("spark"), std::int32_t(-7), std::int32_t(3)}, std::string("s")},
                {{std::string("spark"), std::int32_t(-6), std::int32_t(3)}, std::string("sp")},
                {{std::string("spark"), std::int32_t(-5), std::int32_t(3)}, std::string("spa")},
                {{std::string("spark"), std::int32_t(-4), std::int32_t(3)}, std::string("par")},
                {{std::string("Spark SQL"), std::int32_t(-10), std::int32_t(5)}, std::string("Spar")},
                {{std::string("spark"), std::int32_t(0), std::int32_t(3)}, std::string("spa")},
                // Negative length => empty (Spark).
                {{std::string("spark"), std::int32_t(-7), std::int32_t(-1)}, std::string("")},
                // Positive start out of range => empty.
                {{std::string("spark"), std::int32_t(10), std::int32_t(3)}, std::string("")},
                // UTF-8: `startCp` / `endCp` can be negative; substring becomes empty when `c < end` never holds.
                {{std::string("数据砖头"), std::int32_t(-10), std::int32_t(2)}, std::string("")},
                {{std::string("数据砖头"), std::int32_t(-2), std::int32_t(2)}, std::string("砖头")},
                {{std::string("数据砖头"), std::int32_t(-5), std::int32_t(2)}, std::string("数")},
                // Null propagation
                {{Null(), std::int32_t(1), std::int32_t(1)}, Null()},
                {{std::string("hi"), Null(), std::int32_t(1)}, Null()},
                {{std::string("hi"), std::int32_t(1), Null()}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    // 2-arg (len is completed with each row's byte length, same as Doris `substr`)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {{std::string("spark"), std::int32_t(-7)}, std::string("spark")},
                {{std::string("spark"), std::int32_t(-1)}, std::string("k")},
                {{std::string("spark"), std::int32_t(2)}, std::string("park")},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
