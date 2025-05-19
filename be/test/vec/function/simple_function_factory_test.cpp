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

#include "vec/functions/simple_function_factory.h"

#include <gtest/gtest.h>

#include <memory>

#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

class FunctionBeTestMock : public IFunction {
public:
    static constexpr auto name = "be_test_mock";

    static FunctionPtr create() { return std::make_shared<FunctionBeTestMock>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override { return nullptr; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        return Status::OK();
    }
};

class SimpleFunctionFactoryTest : public testing::Test {
    void SetUp() override {
        static std::once_flag oc;
        std::call_once(oc, []() {
            SimpleFunctionFactory::instance().register_function<FunctionBeTestMock>();
        });
    }

    ColumnsWithTypeAndName arguments(size_t size) {
        ColumnsWithTypeAndName args;
        for (size_t i = 0; i < size; ++i) {
            args.emplace_back(nullptr, std::make_shared<DataTypeInt64>(), "");
        }
        return args;
    }

    void TearDown() override {}
};

TEST_F(SimpleFunctionFactoryTest, test_return_type_check) {
    EXPECT_THROW(SimpleFunctionFactory::instance().get_function(
                         "be_test_mock", {}, std::make_shared<DataTypeInt64>(),
                         {.enable_decimal256 = false}, BeExecVersionManager::get_newest_version()),
                 doris::Exception);
}

TEST_F(SimpleFunctionFactoryTest, test_return_all) {
    auto factory = SimpleFunctionFactory::instance();

    for (auto [name, builder] : factory.function_creators) {
        auto function = builder();
        auto func_impl = std::dynamic_pointer_cast<FunctionBuilderImpl>(function);
        EXPECT_NE(func_impl, nullptr);
        if (func_impl->is_variadic()) {
            continue;
        }
        std::cout << func_impl->get_name() << std::endl;
        ///TODO: Currently, many DCHECK statements exist within get_return_type_impl.
        // In the future, after replacing all these DCHECK statements with exceptions, we will be able to enumerate all the functions.

        // try {
        //     auto return_type = func_impl->get_return_type_impl(
        //             arguments(func_impl->get_number_of_arguments()));
        //     EXPECT_NE(return_type, nullptr) << func_impl->get_name();
        // } catch (const doris::Exception& e) {
        //     std::cout << "Exception message: " << e.what() << std::endl; // 使用what()方法
        //     SUCCEED();
        // } catch (...) {
        // }
    }
}

} // namespace doris::vectorized