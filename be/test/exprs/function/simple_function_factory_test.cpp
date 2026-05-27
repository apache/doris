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
#include <vector>

#include "core/column/column_const.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function.h"
#include "exprs/function_context.h"
#include "testutil/column_helper.h"

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

class MockPreparedFunction final : public IPreparedFunction {
public:
    String get_name() const override { return "mock_const_execute_test"; }

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t input_rows_count) const override {
        return Status::OK();
    }
};

class MockConstRecordingFunction final : public IFunctionBase {
public:
    MockConstRecordingFunction()
            : _argument_types({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>(),
                               std::make_shared<DataTypeInt32>()}),
              _return_type(std::make_shared<DataTypeInt32>()),
              _prepared_function(std::make_shared<MockPreparedFunction>()) {}

    String get_name() const override { return "mock_const_recording_function"; }

    const DataTypes& get_argument_types() const override { return _argument_types; }

    const DataTypePtr& get_return_type() const override { return _return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, uint32_t result) const override {
        std::vector<bool> const_flags;
        const_flags.reserve(arguments.size());
        for (auto column_position : arguments) {
            const auto& column = sample_block.get_by_position(column_position).column;
            const_flags.push_back(column && is_column_const(*column));
        }
        observed_const_patterns.push_back(std::move(const_flags));
        return _prepared_function;
    }

    bool is_use_default_implementation_for_constants() const override { return false; }

    mutable std::vector<std::vector<bool>> observed_const_patterns;

private:
    DataTypes _argument_types;
    DataTypePtr _return_type;
    PreparedFunctionPtr _prepared_function;
};

class MockContextSyncPreparedFunction final : public IPreparedFunction {
public:
    explicit MockContextSyncPreparedFunction(std::vector<bool> expected_const_flags)
            : _expected_const_flags(std::move(expected_const_flags)) {}

    String get_name() const override { return "mock_const_context_sync_test"; }

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t input_rows_count) const override {
        auto* fragment_state = context->get_function_state(FunctionContext::FRAGMENT_LOCAL);
        auto* thread_state = context->get_function_state(FunctionContext::THREAD_LOCAL);

        if (_expected_const_flags[0] != (fragment_state != nullptr)) {
            return Status::InternalError("fragment-local const state mismatch");
        }
        if (_expected_const_flags[2] != (thread_state != nullptr)) {
            return Status::InternalError("thread-local const state mismatch");
        }
        return Status::OK();
    }

private:
    std::vector<bool> _expected_const_flags;
};

class MockConstContextSyncFunction final : public IFunctionBase {
public:
    MockConstContextSyncFunction()
            : _argument_types({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>(),
                               std::make_shared<DataTypeInt32>()}),
              _return_type(std::make_shared<DataTypeInt32>()) {}

    String get_name() const override { return "mock_const_context_sync_function"; }

    const DataTypes& get_argument_types() const override { return _argument_types; }

    const DataTypePtr& get_return_type() const override { return _return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, uint32_t result) const override {
        std::vector<bool> const_flags;
        const_flags.reserve(arguments.size());
        for (auto column_position : arguments) {
            const auto& column = sample_block.get_by_position(column_position).column;
            const_flags.push_back(column && is_column_const(*column));
        }
        return std::make_shared<MockContextSyncPreparedFunction>(std::move(const_flags));
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::FRAGMENT_LOCAL && context->is_col_constant(0)) {
            context->set_function_state(scope, std::make_shared<int>(1));
        }
        if (scope == FunctionContext::THREAD_LOCAL && context->is_col_constant(2)) {
            context->set_function_state(scope, std::make_shared<int>(1));
        }
        return Status::OK();
    }

    bool is_use_default_implementation_for_constants() const override { return false; }

private:
    DataTypes _argument_types;
    DataTypePtr _return_type;
};

class MockOmittedConstantArgsPreparedFunction final : public IPreparedFunction {
public:
    String get_name() const override { return "mock_omitted_constant_args_test"; }

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t input_rows_count) const override {
        return Status::OK();
    }
};

class MockOmittedConstantArgsFunction final : public IFunctionBase {
public:
    MockOmittedConstantArgsFunction()
            : _argument_types({std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt32>(),
                               std::make_shared<DataTypeInt32>()}),
              _return_type(std::make_shared<DataTypeInt32>()),
              _prepared_function(std::make_shared<MockOmittedConstantArgsPreparedFunction>()) {}

    String get_name() const override { return "mock_omitted_constant_args_function"; }

    const DataTypes& get_argument_types() const override { return _argument_types; }

    const DataTypePtr& get_return_type() const override { return _return_type; }

    PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                const ColumnNumbers& arguments, uint32_t result) const override {
        return _prepared_function;
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            if (!context->is_col_constant(0)) {
                return Status::InternalError("mocked execute argument was not constified");
            }
            if (context->get_constant_col(1) == nullptr ||
                context->get_constant_col(2) == nullptr) {
                return Status::InternalError("omitted constant arguments were dropped");
            }
        }
        return Status::OK();
    }

    bool is_use_default_implementation_for_constants() const override { return false; }

private:
    DataTypes _argument_types;
    DataTypePtr _return_type;
    PreparedFunctionPtr _prepared_function;
};

Block create_mock_const_test_block() {
    auto arg0 = ColumnHelper::create_column<DataTypeInt32>({1, 2, 3});
    auto arg1_data = ColumnHelper::create_column<DataTypeInt32>({11});
    ColumnPtr arg1 = ColumnConst::create(arg1_data, 3);
    auto arg2 = ColumnHelper::create_column<DataTypeInt32>({4, 5, 6});

    auto data_type = std::make_shared<DataTypeInt32>();
    Block block;
    block.insert({arg0, data_type, "arg0"});
    block.insert({arg1, data_type, "arg1"});
    block.insert({arg2, data_type, "arg2"});
    block.insert({data_type->create_column(), data_type, "result"});
    return block;
}

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
                         "be_test_mock", {}, std::make_shared<DataTypeInt64>(), {},
                         BeExecVersionManager::get_newest_version()),
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

TEST_F(SimpleFunctionFactoryTest, test_debug_mock_const_execute_modes) {
    MockConstRecordingFunction function;
    auto context = FunctionContext::create_context(nullptr, function.get_return_type(),
                                                   function.get_argument_types());
    auto block = create_mock_const_test_block();
    ColumnNumbers arguments {0, 1, 2};

    EXPECT_TRUE(function.execute(context.get(), block, arguments, 3, 3).ok());

#ifndef NDEBUG
    ASSERT_EQ(function.observed_const_patterns.size(), 4);
    EXPECT_EQ(function.observed_const_patterns[0], (std::vector<bool> {true, true, true}));
    EXPECT_EQ(function.observed_const_patterns[1], (std::vector<bool> {true, true, false}));
    EXPECT_EQ(function.observed_const_patterns[2], (std::vector<bool> {false, true, true}));
    EXPECT_EQ(function.observed_const_patterns[3], (std::vector<bool> {false, true, false}));
#else
    ASSERT_EQ(function.observed_const_patterns.size(), 1);
    EXPECT_EQ(function.observed_const_patterns[0], (std::vector<bool> {false, true, false}));
#endif
}

TEST_F(SimpleFunctionFactoryTest, test_debug_mock_const_execute_syncs_context_state) {
    MockConstContextSyncFunction function;
    auto context = FunctionContext::create_context(nullptr, function.get_return_type(),
                                                   function.get_argument_types());
    auto block = create_mock_const_test_block();
    ColumnNumbers arguments {0, 1, 2};

    EXPECT_TRUE(function.execute(context.get(), block, arguments, 3, 3).ok());
}

TEST_F(SimpleFunctionFactoryTest, test_debug_mock_const_execute_skips_probe_without_context) {
    MockConstRecordingFunction function;
    auto block = create_mock_const_test_block();
    ColumnNumbers arguments {0, 1, 2};

    EXPECT_TRUE(function.execute(nullptr, block, arguments, 3, 3).ok());

    ASSERT_EQ(function.observed_const_patterns.size(), 1);
    EXPECT_EQ(function.observed_const_patterns[0], (std::vector<bool> {false, true, false}));
}

TEST_F(SimpleFunctionFactoryTest,
       test_debug_mock_const_execute_preserves_context_constants_for_omitted_args) {
    MockOmittedConstantArgsFunction function;
    auto context = FunctionContext::create_context(nullptr, function.get_return_type(),
                                                   function.get_argument_types());

    auto data_type = std::make_shared<DataTypeInt32>();
    Block block;
    block.insert({ColumnHelper::create_column<DataTypeInt32>({1, 2, 3}), data_type, "arg0"});
    block.insert({data_type->create_column(), data_type, "result"});

    context->set_constant_cols(
            {nullptr,
             std::make_shared<ColumnPtrWrapper>(ColumnHelper::create_column<DataTypeInt32>({11})),
             std::make_shared<ColumnPtrWrapper>(ColumnHelper::create_column<DataTypeInt32>({22}))});

    EXPECT_TRUE(function.execute(context.get(), block, ColumnNumbers {0}, 1, 3).ok());
}

} // namespace doris