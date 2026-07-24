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

#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

namespace {

FunctionBasePtr create_function() {
    ColumnsWithTypeAndName arguments;
    auto int_type = std::make_shared<DataTypeInt32>();
    arguments.emplace_back(ColumnInt32::create(), int_type, "k1");
    arguments.emplace_back(ColumnInt32::create(), int_type, "bucket_num");
    return SimpleFunctionFactory::instance().get_function("paimon_bucket_id", arguments, int_type);
}

std::unique_ptr<FunctionContext> create_context(MockRuntimeState* state, int num_args = 2) {
    std::vector<DataTypePtr> arg_types(num_args, std::make_shared<DataTypeInt32>());
    auto ctx = FunctionContext::create_context(state, std::make_shared<DataTypeInt32>(), arg_types);
    return ctx;
}

std::shared_ptr<ColumnPtrWrapper> make_constant_int(int32_t value) {
    auto col = ColumnInt32::create();
    col->insert_value(value);
    return std::make_shared<ColumnPtrWrapper>(std::move(col));
}

Block build_execute_block() {
    Block block;
    auto int_type = std::make_shared<DataTypeInt32>();

    auto key_col = ColumnInt32::create();
    key_col->insert_value(1);
    key_col->insert_value(1);
    key_col->insert_value(2);
    block.insert(ColumnWithTypeAndName(std::move(key_col), int_type, "k1"));

    auto bucket_num_col = ColumnInt32::create();
    bucket_num_col->insert_value(4);
    bucket_num_col->insert_value(4);
    bucket_num_col->insert_value(4);
    block.insert(ColumnWithTypeAndName(std::move(bucket_num_col), int_type, "bucket_num"));

    block.insert(ColumnWithTypeAndName(ColumnInt32::create(), int_type, "result"));
    return block;
}

} // namespace

TEST(FunctionPaimonTest, OpenRejectsTooFewArguments) {
    auto function = create_function();
    ASSERT_TRUE(function != nullptr);

    MockRuntimeState state;
    auto ctx = create_context(&state, 1);

    Status st = function->open(ctx.get(), FunctionContext::THREAD_LOCAL);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(std::string::npos, st.to_string().find("requires at least 2 arguments"));
}

TEST(FunctionPaimonTest, OpenRejectsNonConstantBucketNum) {
    auto function = create_function();
    ASSERT_TRUE(function != nullptr);

    MockRuntimeState state;
    auto ctx = create_context(&state);

    Status st = function->open(ctx.get(), FunctionContext::THREAD_LOCAL);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(std::string::npos, st.to_string().find("requires constant bucket_num"));
}

TEST(FunctionPaimonTest, OpenRejectsInvalidBucketNum) {
    auto function = create_function();
    ASSERT_TRUE(function != nullptr);

    MockRuntimeState state;
    auto ctx = create_context(&state);
    ctx->set_constant_cols({nullptr, make_constant_int(0)});

    Status st = function->open(ctx.get(), FunctionContext::THREAD_LOCAL);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(std::string::npos, st.to_string().find("invalid paimon bucket_num"));
}

TEST(FunctionPaimonTest, OpenAndCloseManageThreadLocalState) {
    auto function = create_function();
    ASSERT_TRUE(function != nullptr);

    MockRuntimeState state;
    auto ctx = create_context(&state);
    ctx->set_constant_cols({nullptr, make_constant_int(4)});

    ASSERT_TRUE(function->open(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::THREAD_LOCAL));

    ASSERT_TRUE(function->open(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    ASSERT_NE(nullptr, ctx->get_function_state(FunctionContext::THREAD_LOCAL));

    ASSERT_TRUE(function->close(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    ASSERT_EQ(nullptr, ctx->get_function_state(FunctionContext::THREAD_LOCAL));
}

TEST(FunctionPaimonTest, ExecuteFailsWhenStateNotInitialized) {
    auto function = create_function();
    ASSERT_TRUE(function != nullptr);

    MockRuntimeState state;
    auto ctx = create_context(&state);
    Block block = build_execute_block();

    Status st = function->execute(ctx.get(), block, ColumnNumbers {0, 1}, 2, 3);
    ASSERT_FALSE(st.ok());
    ASSERT_NE(std::string::npos, st.to_string().find("state is not initialized"));
}

TEST(FunctionPaimonTest, ExecuteComputesBucketIds) {
    auto function = create_function();
    ASSERT_TRUE(function != nullptr);

    MockRuntimeState state;
    auto ctx = create_context(&state);
    ctx->set_constant_cols({nullptr, make_constant_int(4)});
    ASSERT_TRUE(function->open(ctx.get(), FunctionContext::THREAD_LOCAL).ok());

    Block block = build_execute_block();
    ASSERT_TRUE(function->execute(ctx.get(), block, ColumnNumbers {0, 1}, 2, 3).ok());

    const auto& result_col =
            assert_cast<const ColumnInt32&>(*block.get_by_position(2).column).get_data();
    ASSERT_EQ(3, result_col.size());
    EXPECT_EQ(result_col[0], result_col[1]);
    EXPECT_GE(result_col[0], 0);
    EXPECT_GE(result_col[2], 0);
    EXPECT_LT(result_col[0], 4);
    EXPECT_LT(result_col[2], 4);
}

} // namespace doris
