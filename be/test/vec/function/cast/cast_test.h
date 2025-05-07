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
#include "common/status.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {
using namespace ut_type;
struct FunctionCastTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}

    template <typename T>
    using CppType = NativeType<typename T::FieldType>::Type;

    template <typename ToDataType>
    void check_function_for_cast(const InputTypeSet& input_types, const DataSet& data_set) {
        std::string func_name = "CAST";

        using CppType = CppType<ToDataType>;

        InputTypeSet add_input_types = input_types;
        add_input_types.push_back(ConstedNotnull {TypeId<CppType>::value});

        for (const auto& row : data_set) {
            auto add_row = row;
            add_row.first.push_back(CppType {});
            DataSet const_dataset = {add_row};

            auto st = check_function<ToDataType, true>(func_name, add_input_types, const_dataset);

            EXPECT_TRUE(st.ok()) << "check_function failed: " << st.msg();
        }
    }
};

} // namespace doris::vectorized