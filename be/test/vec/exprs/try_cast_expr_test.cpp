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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "runtime/primitive_type.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/exprs/function_context.h"
#include "vec/exprs/vcast_expr.h"

namespace doris::vectorized {

template <class Impl>
class try_cast_test_function : public IFunction {
public:
    static constexpr auto name = "";
    static FunctionPtr create() { return std::make_shared<try_cast_test_function>(); }
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
        return Impl::execute_impl(context, block, arguments, result, input_rows_count);
    }
};

struct TryCastTestNoErrorImpl {
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto column = block.get_by_position(arguments[0]).type->create_column();
        column->insert_many_defaults(input_rows_count);
        block.get_by_position(result).column = std::move(column);
        return Status::OK();
    }
};

struct TryCastTestReturnErrorImpl {
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        return Status::InternalError("try_cast test error");
    }
};

struct TryCastTestRowExecReturnNotNullImpl {
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto column = block.get_by_position(arguments[0]).column;
        if (column->size() > 1) {
            return Status::InvalidArgument("input column size > 1");
        }
        auto x = column->get_int(0);
        if (x == 1) {
            return Status::InvalidArgument("try_cast test error");
        }
        auto ret_col = ColumnInt32::create();
        ret_col->insert_value(x);
        block.get_by_position(result).column = std::move(ret_col);
        return Status::OK();
    }
};

struct TryCastTestRowExecReturnNullImpl {
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto column = block.get_by_position(arguments[0]).column;
        if (column->size() > 1) {
            return Status::InvalidArgument("input column size > 1");
        }
        auto x = column->get_int(0);
        if (x == 1) {
            return Status::InvalidArgument("try_cast test error");
        }
        auto ret_col = ColumnInt32::create();
        ret_col->insert_value(x);
        auto col =
                ColumnNullable::create(std::move(ret_col), ColumnUInt8::create(1, x == 0 ? 0 : 1));
        block.get_by_position(result).column = std::move(col);
        return Status::OK();
    }
};

struct TryCastTestRowExecReturnErrorImpl {
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        auto column = block.get_by_position(arguments[0]).column;
        if (column->size() > 1) {
            return Status::InvalidArgument("input column size > 1");
        }
        return Status::InternalError("try_cast test error");
    }
};

class MockVExprForTryCast : public VExpr {
public:
    MockVExprForTryCast() = default;
    MOCK_CONST_METHOD0(clone, VExprSPtr());
    const std::string& expr_name() const override { return _expr_name; }

    Status execute(VExprContext* context, Block* block, int* result_column_id) const override {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto int_column = int_type->create_column();
        for (int i = 0; i < 3; i++) {
            Int32 x = i;
            int_column->insert_data((const char*)&x, sizeof(Int32));
        }
        block->insert({std::move(int_column), int_type, "mock_input_column"});
        *result_column_id = 0;
        return Status::OK();
    }

    Status execute_column(VExprContext* context, const Block* block, Selector* selector,
                          size_t count, ColumnPtr& result_column) const override {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto int_column = int_type->create_column();
        for (int i = 0; i < 3; i++) {
            Int32 x = i;
            int_column->insert_data((const char*)&x, sizeof(Int32));
        }
        result_column = std::move(int_column);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* block) const override {
        return std::make_shared<DataTypeInt32>();
    }

    std::string _expr_name;
};

struct TryCastExprTest : public ::testing::Test {
    void SetUp() override {
        try_cast_expr._data_type =
                std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
        try_cast_expr._open_finished = true;

        try_cast_expr.add_child(std::make_shared<MockVExprForTryCast>());
        try_cast_expr._fn_context_index = 0;

        context = std::make_unique<VExprContext>(std::make_shared<MockVExprForTryCast>());
        context->_fn_contexts.push_back(nullptr);
    }

    TryCastExpr try_cast_expr;

    std::unique_ptr<VExprContext> context;
};

TEST_F(TryCastExprTest, BasicTest1) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestNoErrorImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    int result_column_id = -1;
    try_cast_expr._original_cast_return_is_nullable = true;
    auto st = try_cast_expr.execute(context.get(), &block, &result_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
}

TEST_F(TryCastExprTest, BasicTest2) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestNoErrorImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    int result_column_id = -1;
    try_cast_expr._original_cast_return_is_nullable = false;
    auto st = try_cast_expr.execute(context.get(), &block, &result_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();
}

TEST_F(TryCastExprTest, return_error) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestReturnErrorImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    int result_column_id = -1;
    try_cast_expr._original_cast_return_is_nullable = false;
    auto st = try_cast_expr.execute(context.get(), &block, &result_column_id);
    EXPECT_FALSE(st.ok()) << st.msg();
}

TEST_F(TryCastExprTest, row_exec1) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestRowExecReturnNotNullImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    int result_column_id = -1;
    try_cast_expr._original_cast_return_is_nullable = false;
    block.insert(ColumnWithTypeAndName {ColumnNothing::create(3), std::make_shared<DataTypeInt32>(),
                                        "mock_input_column"});
    auto st = try_cast_expr.execute(context.get(), &block, &result_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto result_col = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_col->size(), 3);

    EXPECT_EQ(result_col->is_null_at(0), false);
    EXPECT_EQ(result_col->is_null_at(1), true);
    EXPECT_EQ(result_col->is_null_at(2), false);
}

TEST_F(TryCastExprTest, row_exec2) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestRowExecReturnNullImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    int result_column_id = -1;
    try_cast_expr._original_cast_return_is_nullable = true;
    block.insert(ColumnWithTypeAndName {ColumnNothing::create(3), std::make_shared<DataTypeInt32>(),
                                        "mock_input_column"});
    auto st = try_cast_expr.execute(context.get(), &block, &result_column_id);
    EXPECT_TRUE(st.ok()) << st.msg();

    auto result_col = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_col->size(), 3);

    EXPECT_EQ(result_col->is_null_at(0), false);
    EXPECT_EQ(result_col->is_null_at(1), true);
    EXPECT_EQ(result_col->is_null_at(2), true);
}

TEST_F(TryCastExprTest, row_exec3) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestRowExecReturnErrorImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    int result_column_id = -1;
    try_cast_expr._original_cast_return_is_nullable = true;
    block.insert(ColumnWithTypeAndName {ColumnNothing::create(3), std::make_shared<DataTypeInt32>(),
                                        "mock_input_column"});
    auto st = try_cast_expr.execute(context.get(), &block, &result_column_id);
    EXPECT_FALSE(st.ok()) << st.msg();
}

} // namespace doris::vectorized