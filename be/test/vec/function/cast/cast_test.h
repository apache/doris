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

#include <string>

#include "vec/function/function_test_util.h"

namespace doris::vectorized {
using namespace ut_type;
struct FunctionCastTest : public testing::Test {
    void SetUp() override { TimezoneUtils::load_timezones_to_cache(); }
    void TearDown() override {}

    // we always need return nullable=true for cast function because of its' get_return_type weird
    template <typename ResultDataType, int ResultScale = -1, int ResultPrecision = -1>
    void check_function_for_cast(InputTypeSet input_types, DataSet data_set,
                                 bool expect_execute_fail = false, bool expect_result_ne = false) {
        std::string func_name = "CAST";

        InputTypeSet add_input_types = input_types;
        add_input_types.emplace_back(ConstedNotnull {ResultDataType {}.get_primitive_type()});

        // the column-1(target type placeholder) must be const. so we must split the data_set into const_datasets with
        // only 1 row.
        for (const auto& row : data_set) {
            auto add_row = row;
            add_row.first.push_back(ut_type::ut_input_type_default_v<ResultDataType>);
            DataSet const_dataset = {add_row};

            static_cast<void>(check_function<ResultDataType, true, ResultScale, ResultPrecision>(
                    func_name, add_input_types, const_dataset, expect_execute_fail,
                    expect_result_ne));
        }
    }
};
} // namespace doris::vectorized
