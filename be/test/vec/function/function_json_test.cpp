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

#include <iomanip>
#include <string>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(FunctionJsonTEST, GetJsonDoubleTest) {
    std::string func_name = "get_json_double";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {
            {{VARCHAR("{\"k1\":1.3, \"k2\":2}"), VARCHAR("$.k1")}, DOUBLE(1.3)},
            {{VARCHAR("{\"k1\":\"v1\", \"my.key\":[1.1, 2.2, 3.3]}"), VARCHAR("$.\"my.key\"[1]")},
             DOUBLE(2.2)},
            {{VARCHAR("{\"k1.key\":{\"k2\":[1.1, 2.2]}}"), VARCHAR("$.\"k1.key\".k2[0]")},
             DOUBLE(1.1)}};

    static_cast<void>(check_function<DataTypeFloat64, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonTEST, GetJsonIntTest) {
    std::string func_name = "get_json_int";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {
            {{VARCHAR("{\"k1\":1, \"k2\":2}"), VARCHAR("$.k1")}, INT(1)},
            {{VARCHAR("{\"k1\":\"v1\", \"my.key\":[1, 2, 3]}"), VARCHAR("$.\"my.key\"[1]")},
             INT(2)},
            {{VARCHAR("{\"k1.key\":{\"k2\":[1, 2]}}"), VARCHAR("$.\"k1.key\".k2[0]")}, INT(1)}};

    static_cast<void>(check_function<DataTypeInt32, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonTEST, GetJsonBigIntTest) {
    std::string func_name = "get_json_bigint";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {
            {{VARCHAR("{\"k1\":1, \"k2\":2}"), VARCHAR("$.k1")}, Int64(1)},
            {{VARCHAR("{\"k1\":1678708107000, \"k2\":2}"), VARCHAR("$.k1")}, Int64(1678708107000)},
            {{VARCHAR("{\"k1\":\"v1\", \"my.key\":[1, 2, 3]}"), VARCHAR("$.\"my.key\"[1]")},
             Int64(2)},
            {{VARCHAR("{\"k1.key\":{\"k2\":[1, 2]}}"), VARCHAR("$.\"k1.key\".k2[0]")}, Int64(1)}};

    static_cast<void>(check_function<DataTypeInt64, true>(func_name, input_types, data_set));
}

TEST(FunctionJsonTEST, GetJsonStringTest) {
    std::string func_name = "get_json_string";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {
            {{VARCHAR("{\"k1\":\"v1\", \"k2\":\"v2\"}"), VARCHAR("$.k1")}, VARCHAR("v1")},
            {{VARCHAR("{\"k1\":\"v1\", \"my.key\":[\"e1\", \"e2\", \"e3\"]}"),
              VARCHAR("$.\"my.key\"[1]")},
             VARCHAR("e2")},
            {{VARCHAR("{\"k1.key\":{\"k2\":[\"v1\", \"v2\"]}}"), VARCHAR("$.\"k1.key\".k2[0]")},
             VARCHAR("v1")},
            {{VARCHAR("[{\"k1\":\"v1\"}, {\"k2\":\"v2\"}, {\"k1\":\"v3\"}, {\"k1\":\"v4\"}]"),
              VARCHAR("$.k1")},
             VARCHAR("[\"v1\",\"v3\",\"v4\"]")}};

    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

} // namespace doris::vectorized
