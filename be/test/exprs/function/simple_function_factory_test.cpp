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

#include "exprs/function/simple_function_factory.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_timestamp_ns.h"

namespace doris {

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

class FunctionNullLiteralBeTestMock : public IFunction {
public:
    static constexpr auto name = "null_literal_be_test_mock";

    static FunctionPtr create() { return std::make_shared<FunctionNullLiteralBeTestMock>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext*, Block&, const ColumnNumbers&, uint32_t,
                        size_t) const override {
        return Status::OK();
    }
};

class FunctionNestedDateTimeV2BeTestMock : public IFunction {
public:
    static constexpr auto name = "nested_datetimev2_be_test_mock";

    static FunctionPtr create() { return std::make_shared<FunctionNestedDateTimeV2BeTestMock>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeDateTimeV2>(6)));
    }

    Status execute_impl(FunctionContext*, Block&, const ColumnNumbers&, uint32_t,
                        size_t) const override {
        return Status::OK();
    }
};

class FunctionNestedTimeStampNsBeTestMock : public IFunction {
public:
    static constexpr auto name = "nested_timestamp_ns_be_test_mock";

    static FunctionPtr create() { return std::make_shared<FunctionNestedTimeStampNsBeTestMock>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeTimeStampNs>()));
    }

    Status execute_impl(FunctionContext*, Block&, const ColumnNumbers&, uint32_t,
                        size_t) const override {
        return Status::OK();
    }
};

class FunctionNestedStructTimeStampNsBeTestMock : public IFunction {
public:
    static constexpr auto name = "nested_struct_timestamp_ns_be_test_mock";

    static FunctionPtr create() {
        return std::make_shared<FunctionNestedStructTimeStampNsBeTestMock>();
    }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeStruct>(DataTypes {
                std::make_shared<DataTypeArray>(
                        make_nullable(std::make_shared<DataTypeTimeStampNs>())),
                std::make_shared<DataTypeDateTimeV2>(3), std::make_shared<DataTypeInt32>()});
    }

    Status execute_impl(FunctionContext*, Block&, const ColumnNumbers&, uint32_t,
                        size_t) const override {
        return Status::OK();
    }
};

class SimpleFunctionFactoryTest : public testing::Test {
    void SetUp() override {
        static std::once_flag oc;
        std::call_once(oc, []() {
            SimpleFunctionFactory::instance().register_function<FunctionBeTestMock>();
            SimpleFunctionFactory::instance().register_function<FunctionNullLiteralBeTestMock>();
            SimpleFunctionFactory::instance()
                    .register_function<FunctionNestedDateTimeV2BeTestMock>();
            SimpleFunctionFactory::instance()
                    .register_function<FunctionNestedTimeStampNsBeTestMock>();
            SimpleFunctionFactory::instance()
                    .register_function<FunctionNestedStructTimeStampNsBeTestMock>();
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
                         "be_test_mock", {}, std::make_shared<DataTypeInt64>(), {},
                         BeExecVersionManager::get_newest_version()),
                 doris::Exception);
}

TEST_F(SimpleFunctionFactoryTest, test_null_literal_skips_return_type_inference) {
    auto null_literal_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_NULL, true);
    ASSERT_TRUE(null_literal_type->is_nullable());
    ASSERT_TRUE(null_literal_type->is_null_literal());

    ColumnsWithTypeAndName arguments = {{nullptr, null_literal_type, "null"}};
    // The mock infers BIGINT, but a NULL literal should let the FE-provided nullable type win.
    auto expected_return_type = make_nullable(std::make_shared<DataTypeInt32>());
    FunctionBasePtr function;
    ASSERT_NO_THROW(function = SimpleFunctionFactory::instance().get_function(
                            FunctionNullLiteralBeTestMock::name, arguments, expected_return_type));
    ASSERT_NE(function, nullptr);
    EXPECT_TRUE(function->get_return_type()->equals(*expected_return_type));
}

TEST_F(SimpleFunctionFactoryTest, test_nested_timestamp_ns_return_type_check) {
    auto timestamp_ns_array =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeTimeStampNs>()));

    EXPECT_THROW(SimpleFunctionFactory::instance().get_function(
                         FunctionNestedDateTimeV2BeTestMock::name, {}, timestamp_ns_array),
                 doris::Exception);

    FunctionBasePtr function;
    ASSERT_NO_THROW(function = SimpleFunctionFactory::instance().get_function(
                            FunctionNestedTimeStampNsBeTestMock::name, {}, timestamp_ns_array));
    ASSERT_NE(function, nullptr);
    EXPECT_TRUE(function->get_return_type()->equals(*timestamp_ns_array));

    auto compatible_struct = std::make_shared<DataTypeStruct>(
            DataTypes {timestamp_ns_array, std::make_shared<DataTypeDateTimeV2>(6),
                       std::make_shared<DataTypeInt32>()});
    ASSERT_NO_THROW(
            function = SimpleFunctionFactory::instance().get_function(
                    FunctionNestedStructTimeStampNsBeTestMock::name, {}, compatible_struct));
    ASSERT_NE(function, nullptr);

    auto mismatched_struct = std::make_shared<DataTypeStruct>(DataTypes {
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeDateTimeV2>(6))),
            std::make_shared<DataTypeDateTimeV2>(6), std::make_shared<DataTypeInt32>()});
    EXPECT_THROW(SimpleFunctionFactory::instance().get_function(
                         FunctionNestedStructTimeStampNsBeTestMock::name, {}, mismatched_struct),
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

TEST_F(SimpleFunctionFactoryTest, test_bitmap_count_new_version_return_type) {
    ColumnsWithTypeAndName arguments = {
            {nullptr, make_nullable(std::make_shared<DataTypeBitMap>()), ""},
            {nullptr, make_nullable(std::make_shared<DataTypeBitMap>()), ""}};
    auto& factory = SimpleFunctionFactory::instance();

    for (const auto& [function_name, new_function_name] :
         std::vector<std::pair<std::string, std::string>> {
                 {"bitmap_and_count", "bitmap_and_count_v2"},
                 {"bitmap_or_count", "bitmap_or_count_v2"},
                 {"bitmap_xor_count", "bitmap_xor_count_v2"},
                 {"bitmap_and_not_count", "bitmap_and_not_count_v2"}}) {
        auto old_builder = std::dynamic_pointer_cast<FunctionBuilderImpl>(
                factory.function_creators.find(function_name)->second());
        ASSERT_NE(old_builder, nullptr);
        EXPECT_TRUE(old_builder->get_return_type(arguments)->is_nullable());

        auto new_builder = std::dynamic_pointer_cast<FunctionBuilderImpl>(
                factory.function_creators.find(new_function_name)->second());
        ASSERT_NE(new_builder, nullptr);
        EXPECT_FALSE(new_builder->get_return_type(arguments)->is_nullable());

        auto new_func = SimpleFunctionFactory::instance().get_function(
                function_name, arguments, std::make_shared<DataTypeInt64>(),
                {.new_version_bitmap_op_count = true}, BeExecVersionManager::get_newest_version());
        EXPECT_EQ(new_func->get_name(), new_function_name);
        EXPECT_FALSE(new_func->get_return_type()->is_nullable());
    }
}

} // namespace doris
