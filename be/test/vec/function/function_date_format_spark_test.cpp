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
#include <string_view>

#include "common/status.h"
#include "function_test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
using namespace ut_type;

TEST(VTimestampFunctionsTest, date_format_spark_v2_test) {
    std::string func_name = "date_format_spark";

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2022-01-01"), std::string("yyyy-MM-dd")},
                  std::string("2022-01-01")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2020-02-29"), std::string("yyyy-MM-dd-D")},
                  std::string("2020-02-29-60")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2022-01-03"), std::string("Q-QQQ-QQQQ-E-EEEE")},
                  std::string("1-Q1-1st quarter-Mon-Monday")}}));
    }

    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DATETIMEV2, 6},
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2022-01-02 03:04:05.123456"),
                   std::string("G-y-D-d-a-K-h-k-H-m-s-S-SSSSSS-SSSSSSSS")},
                  std::string("AD-2022-2-2-AM-3-3-3-3-4-5-1-123456-12345600")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2022-01-01 00:00:00"), std::string("yyyy''MM''dd")},
                  std::string("2022'01'01")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2022-01-03 00:00:00"), std::string("[[yyyy]-MM]-dd")},
                  std::string("2022-01-03")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2022-01-01 00:00:00"),
                   std::string("VV-z-O-OOOO-x-xx-xxx-X-XX-XXX-Z-ZZZZ-ZZZZZ")},
                  std::string("UTC-UTC-GMT-GMT-+00-+0000-+00:00-Z-Z-Z-+0000-GMT-Z")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2020-12-31 00:00:00"), std::string("yyyy-MM-dd-D")},
                  std::string("2020-12-31-366")}}));
        static_cast<void>(check_function<DataTypeString, true>(
                func_name, input_types,
                {{{std::string("2021-01-01 00:00:00"), std::string("yyyy-MM-dd-D")},
                  std::string("2021-01-01-1")}}));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        for (std::string_view bad_pattern : {"Y", "W", "zzzzz", "XXXXXX", "'abcd", "]yyyy["}) {
            DataSet invalid_data_set = {
                    {{std::string("1970-01-01 00:00:00"), std::string(bad_pattern)},
                     std::string("")}};
            static_cast<void>(check_function<DataTypeString, true>(func_name, input_types,
                                                                   invalid_data_set, -1, -1, true));
        }
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet data_set = {
                {{std::string("2022-01-01 00:00:00"), std::string("G yyyy-MM-dd")},
                 std::string("AD 2022-01-01")},
                {{std::string("0001-01-01 00:00:00"), std::string("G yyyy-MM-dd")},
                 std::string("AD 0001-01-01")},
        };

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2,
                                    Consted {PrimitiveType::TYPE_VARCHAR}};
        DataSet invalid_data_set = {
                {{std::string("2022-01-01 00:00:00"), std::string("uuuu-MM-dd")}, std::string("")},
        };

        static_cast<void>(check_function<DataTypeString, true>(func_name, input_types,
                                                               invalid_data_set, -1, -1, true));
    }

    {
        InputTypeSet input_types = {PrimitiveType::TYPE_DATETIMEV2, PrimitiveType::TYPE_VARCHAR};
        DataSet data_set = {
                {{std::string("2022-01-01 00:00:00"), std::string("yyyy-MM-dd")},
                 std::string("2022-01-01")},
        };

        EXPECT_NE(Status::OK(),
                  (check_function<DataTypeString, true>(func_name, input_types, data_set)));
    }
}

} // namespace doris::vectorized
