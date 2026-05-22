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

#include "function_test_util.h"
#include "testutil/any_type.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

using namespace ut_type;

TEST(DateTimeFunctionTest, year_and_week_test) {
    std::string func_name = "year_and_week";

    // Test with custom format string
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
        DataSet data_set = {
                {{std::string("2023-01-01"), std::string("%s-%s")}, std::string("2022-52")},
                {{std::string("2023-01-02"), std::string("%s-%s")}, std::string("2023-01")},
                {{std::string("2023-12-31"), std::string("%s-%s")}, std::string("2023-52")},
                {{std::string(""), std::string("%s-%s")}, std::string("0-00")},
                {{Null(), std::string("Year %s Week %s")}, Null()},
                {{std::string("2023-01-01"), Null()}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    // Test with different first day of week settings
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32};
        DataSet data_set = {
                {{std::string("2023-01-01"), std::string("%s-%s"), 1}, std::string("2023-01")},
                {{std::string("2023-01-02"), std::string("%s-%s"), 1}, std::string("2023-01")},
                {{std::string("2023-01-02"), std::string("%s-%s"), 2}, std::string("2023-01")},
                {{std::string("2023-01-01"), std::string("%s-%s"), 7}, std::string("2023-01")},
                {{Null(), std::string("%s-%s"), 1}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    // Test with different minimal days settings
    {
        InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::Int32,
                                    TypeIndex::Int32};
        DataSet data_set = {
                {{std::string("2023-01-01"), std::string("%s-%s"), 2, 1},
                 std::string("2023-01")},
                {{std::string("2023-01-01"), std::string("%s-%s"), 2, 4},
                 std::string("2022-52")},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

TEST(DateTimeFunctionTest, week_and_year_test) {
    std::string func_name = "week_and_year";

    // Test default format (Chinese)
    {
        InputTypeSet input_types = {TypeIndex::String};
        DataSet data_set = {
                {{std::string("2023-01-02")}, std::string("2023年第01周")},
                {{Null()}, Null()},
        };
        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }
}

} // namespace doris::vectorized
