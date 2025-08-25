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

#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class MockFunctionThrowException : public IFunction {
public:
    static constexpr auto name = "mock_function_throw_exception";
    static FunctionPtr create() { return std::make_shared<MockFunctionThrowException>(); }
    String get_name() const override { return name; }
    bool skip_return_type_check() const override { return true; }
    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "BEUT TEST: MockFunctionThrowException");
    }
};

void register_function_throw_exception(SimpleFunctionFactory& factory) {
    factory.register_function<MockFunctionThrowException>();
}

TEST(FunctionThrowExceptionTest, test_throw_exception) {
    auto function = SimpleFunctionFactory::instance().get_function(
            "mock_function_throw_exception", {}, std::make_shared<DataTypeFloat64>(), {false},
            BeExecVersionManager::get_newest_version());

    Block block;
    auto st = function->execute(nullptr, block, {}, 0, 1);

    EXPECT_EQ(st.code(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(st.msg(), "BEUT TEST: MockFunctionThrowException");
}
} // namespace doris::vectorized