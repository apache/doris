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
#include <optional>
#include <string>
#include <vector>

#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/field.h"
#include "core/types.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "exprs/vcast_expr.h"
#include "exprs/vexpr_context.h"

namespace doris {

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

template <bool nullable>
struct TryCastTestOverflowImpl {
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
        const auto& column = block.get_by_position(arguments[0]).column;
        auto ret_col = ColumnInt32::create();
        for (size_t row = 0; row < input_rows_count; ++row) {
            auto value = column->get_int(row);
            if (value == 0) {
                return {ErrorCode::ARITHMETIC_OVERFLOW_ERRROR, "cast overflow"};
            }
            ret_col->insert_value(value);
        }
        if constexpr (nullable) {
            block.get_by_position(result).column = ColumnNullable::create(
                    std::move(ret_col), ColumnUInt8::create(input_rows_count, 0));
        } else {
            block.get_by_position(result).column = std::move(ret_col);
        }
        return Status::OK();
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

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        auto int_type = std::make_shared<DataTypeInt32>();
        auto int_column = int_type->create_column();
        for (size_t i = 0; i < count; i++) {
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

class MockBlockInputForTryCast : public MockVExprForTryCast {
public:
    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        result_column = block->get_by_position(0).column;
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* block) const override {
        return block->get_by_position(0).type;
    }
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

    void check_real_cast(const DataTypePtr& input_type, const DataTypePtr& nested_result_type,
                         const std::vector<std::optional<std::string>>& input_values,
                         const std::vector<std::optional<std::string>>& expected_values,
                         bool strict) {
        auto result_type = make_nullable(nested_result_type);
        auto input_column = input_type->create_column();
        auto expected_column = result_type->create_column();
        auto fill_column = [](const DataTypePtr& type, IColumn& column,
                              const std::vector<std::optional<std::string>>& values) {
            auto serde = type->get_serde();
            for (const auto& value : values) {
                if (value.has_value()) {
                    StringRef text {*value};
                    auto status = serde->from_string_strict_mode(text, column, {});
                    ASSERT_TRUE(status.ok()) << status;
                } else {
                    ASSERT_TRUE(type->is_nullable());
                    column.insert_default();
                }
            }
        };
        ASSERT_NO_FATAL_FAILURE(fill_column(input_type, *input_column, input_values));
        ASSERT_NO_FATAL_FAILURE(fill_column(result_type, *expected_column, expected_values));
        if (input_type->is_nullable()) {
            // Also exercise an outer NULL, independently of NULL elements in a complex value.
            input_column->insert_default();
            expected_column->insert_default();
        }

        try_cast_expr._data_type = result_type;
        try_cast_expr._original_cast_return_is_nullable = input_type->is_nullable();
        try_cast_expr._children[0] = std::make_shared<MockBlockInputForTryCast>();
        context->_fn_contexts[0] =
                FunctionContext::create_context(nullptr, result_type, {input_type, result_type});
        context->fn_context(0)->set_enable_strict_mode(strict);
        ColumnsWithTypeAndName arguments {{input_column->get_ptr(), input_type, "input"},
                                          {nullptr, result_type, "target"}};
        try_cast_expr._function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, result_type);
        ASSERT_NE(try_cast_expr._function, nullptr);

        Block block {arguments[0]};
        ColumnPtr result;
        auto status = try_cast_expr.execute_column(context.get(), &block, nullptr,
                                                   input_column->size(), result);
        ASSERT_TRUE(status.ok()) << status;
        ASSERT_EQ(result->size(), expected_column->size());
        for (size_t row = 0; row < result->size(); ++row) {
            EXPECT_EQ(result->compare_at(row, row, *expected_column, 1), 0)
                    << "row " << row << ", actual " << result_type->to_string(*result, row)
                    << ", expected " << result_type->to_string(*expected_column, row);
        }
    }

    TryCastExpr try_cast_expr;

    std::unique_ptr<VExprContext> context;
};

TEST_F(TryCastExprTest, BasicTest1) {
    try_cast_expr._function = std::make_shared<DefaultFunction>(
            try_cast_test_function<TryCastTestNoErrorImpl>::create(),
            DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());

    Block block;
    block.insert(ColumnWithTypeAndName {ColumnNothing::create(3), std::make_shared<DataTypeInt32>(),
                                        "mock_input_column"});
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
    block.insert(ColumnWithTypeAndName {ColumnNothing::create(3), std::make_shared<DataTypeInt32>(),
                                        "mock_input_column"});
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
    block.insert(ColumnWithTypeAndName {ColumnNothing::create(3), std::make_shared<DataTypeInt32>(),
                                        "mock_input_column"});
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

TEST_F(TryCastExprTest, arithmetic_overflow) {
    auto check_overflow = [&]<bool nullable>() {
        try_cast_expr._function = std::make_shared<DefaultFunction>(
                try_cast_test_function<TryCastTestOverflowImpl<nullable>>::create(),
                DataTypes {std::make_shared<DataTypeInt32>()}, std::make_shared<DataTypeInt32>());
        try_cast_expr._original_cast_return_is_nullable = nullable;
        for (size_t rows : {1, 3}) {
            ColumnPtr result;
            auto status = try_cast_expr.execute_column_impl(context.get(), nullptr, nullptr, rows,
                                                            result);
            ASSERT_TRUE(status.ok()) << status;
            const auto& nullable_result = assert_cast<const ColumnNullable&>(*result);
            ASSERT_EQ(nullable_result.size(), rows);
            EXPECT_TRUE(nullable_result.is_null_at(0));
            for (size_t row = 1; row < rows; ++row) {
                EXPECT_FALSE(nullable_result.is_null_at(row));
                EXPECT_EQ(nullable_result.get_nested_column().get_int(row), row);
            }
        }
    };
    check_overflow.template operator()<false>();
    check_overflow.template operator()<true>();
}

TEST_F(TryCastExprTest, child_arithmetic_overflow) {
    class OverflowChild : public MockVExprForTryCast {
        Status execute_column_impl(VExprContext* context, const Block* block,
                                   const Selector* selector, size_t count,
                                   ColumnPtr& result_column) const override {
            return {ErrorCode::ARITHMETIC_OVERFLOW_ERRROR, "child overflow"};
        }
    };
    try_cast_expr._children[0] = std::make_shared<OverflowChild>();
    ColumnPtr result;
    auto status = try_cast_expr.execute_column_impl(context.get(), nullptr, nullptr, 3, result);
    EXPECT_TRUE(status.is<ErrorCode::ARITHMETIC_OVERFLOW_ERRROR>()) << status;
}

TEST_F(TryCastExprTest, real_cast_array_decimal_overflow) {
    auto input_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDecimal32>(6, 3));
    auto result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDecimal32>(4, 2));
    for (bool nullable : {false, true}) {
        DataTypePtr from_type = nullable ? make_nullable(input_type) : input_type;
        check_real_cast(from_type, result_type, {"[12.340]", "[123.456]", "[null]", "[]"},
                        {"[12.34]", std::nullopt, "[null]", "[]"}, true);
        check_real_cast(from_type, result_type, {"[12.340]", "[123.456]", "[null]", "[]"},
                        {"[12.34]", "[null]", "[null]", "[]"}, false);
    }
}

TEST_F(TryCastExprTest, real_cast_map_integer_overflow) {
    auto input_type =
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeInt32>()),
                                          make_nullable(std::make_shared<DataTypeInt32>()));
    auto result_type =
            std::make_shared<DataTypeMap>(make_nullable(std::make_shared<DataTypeInt32>()),
                                          make_nullable(std::make_shared<DataTypeInt8>()));
    for (bool nullable : {false, true}) {
        DataTypePtr from_type = nullable ? make_nullable(input_type) : input_type;
        check_real_cast(from_type, result_type, {"{1:12}", "{1:128}", "{1:null}", "{}"},
                        {"{1:12}", std::nullopt, "{1:null}", "{}"}, true);
        check_real_cast(from_type, result_type, {"{1:12}", "{1:128}", "{1:null}", "{}"},
                        {"{1:12}", "{1:null}", "{1:null}", "{}"}, false);
    }
}

TEST_F(TryCastExprTest, real_cast_struct_integer_overflow) {
    auto input_type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt32>())}, Strings {"v"});
    auto result_type = std::make_shared<DataTypeStruct>(
            DataTypes {make_nullable(std::make_shared<DataTypeInt8>())}, Strings {"v"});
    for (bool nullable : {false, true}) {
        DataTypePtr from_type = nullable ? make_nullable(input_type) : input_type;
        check_real_cast(from_type, result_type, {"{v:12}", "{v:128}", "{v:null}"},
                        {"{v:12}", std::nullopt, "{v:null}"}, true);
        check_real_cast(from_type, result_type, {"{v:12}", "{v:128}", "{v:null}"},
                        {"{v:12}", "{v:null}", "{v:null}"}, false);
    }
}

TEST_F(TryCastExprTest, selected_row_safety) {
    VCastExpr cast_expr;
    cast_expr.add_child(std::make_shared<MockVExprForTryCast>());

    EXPECT_FALSE(cast_expr.is_safe_to_execute_on_selected_rows());
    EXPECT_TRUE(try_cast_expr.is_safe_to_execute_on_selected_rows());
}

} // namespace doris
