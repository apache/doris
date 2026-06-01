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

#include <limits>
#include <string>

#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

TEST(FunctionHumanReadableSecondsTest, one_arg) {
    std::string func_name = "human_readable_seconds";

    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};

    DataSet data_set = {
            {{96.0}, std::string("1 minute, 36 seconds")},
            {{3762.0}, std::string("1 hour, 2 minutes, 42 seconds")},
            {{56363463.0}, std::string("93 weeks, 1 day, 8 hours, 31 minutes, 3 seconds")},
            {{0.0}, std::string("0 seconds")},
            {{-96.0}, std::string("-1 minute, 36 seconds")},
            {{0.9}, std::string("900 milliseconds")},
            {{0.001}, std::string("1 millisecond")},
            {{1.001}, std::string("1 second, 1 millisecond")},
            {{475.33}, std::string("7 minutes, 55 seconds, 330 milliseconds")},
            {{-0.5}, std::string("-500 milliseconds")},
            {{1.2}, std::string("1 second")},
            {{std::numeric_limits<double>::infinity()}, std::string("inf")},
            {{-std::numeric_limits<double>::infinity()}, std::string("-inf")},
            {{std::numeric_limits<double>::quiet_NaN()}, std::string("nan")},
            {{Null()}, Null()}};

    check_function_all_arg_comb<DataTypeString, true>(func_name, input_types, data_set);
}

} // namespace doris
