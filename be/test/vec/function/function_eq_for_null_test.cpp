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

#include <cassert>
#include <cstddef>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "gtest/gtest_pred_impl.h"
#include "udf/udf.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

TEST(EqForNullFunctionTest, both_only_null) {
    const size_t input_rows_count = 100;

    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(1, 1);

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone_resized(1),
                                   null_map_for_all_null->clone_resized(1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone_resized(1),
                                   null_map_for_all_null->clone_resized(1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 1);
    }
}

TEST(EqForNullFunctionTest, both_only_null_const) {
    const size_t input_rows_count = 100;

    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);

    ColumnWithTypeAndName left {
            ColumnConst::create(ColumnNullable::create(left_i32->clone_resized(1),
                                                       null_map_for_all_null->clone_resized(1)),
                                input_rows_count),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnConst::create(ColumnNullable::create(right_i32->clone_resized(1),
                                                       null_map_for_all_null->clone_resized(1)),
                                input_rows_count),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto col_holder = temporary_block.get_by_position(2).column->convert_to_full_column_if_const();
    auto result_column = assert_cast<const ColumnUInt8*>(col_holder.get());

    std::cout << "Output rows count " << result_column->size() << std::endl;
    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 1);
    }
}

TEST(EqForNullFunctionTest, left_only_null_right_const) {
    const size_t input_rows_count = 100;
    // Input [NULL, NULL, NULL, NULL] & [1, 1, 1, 1]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone_resized(1), ColumnUInt8::create(1, 1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};
    std::cout << "left only null " << left.column->only_null() << std::endl;
    ColumnWithTypeAndName right {
            ColumnConst::create(
                    ColumnNullable::create(right_i32->clone_resized(1), ColumnUInt8::create(1, 0)),
                    input_rows_count),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    std::cout << "Output rows count " << result_column->size() << std::endl;

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 0)
                << fmt::format("Data {}, i {}", result_column->get_data()[i], i);
    }
}

TEST(EqForNullFunctionTest, left_const_right_only_null) {
    const size_t input_rows_count = 100;

    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);

    ColumnWithTypeAndName left {
            ColumnConst::create(
                    ColumnNullable::create(left_i32->clone_resized(1), ColumnUInt8::create(1, 0)),
                    input_rows_count),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone_resized(1), ColumnUInt8::create(1, 1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});

    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    std::cout << "Output rows count " << result_column->size() << std::endl;

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 0)
                << fmt::format("Data {}, i {}", result_column->get_data()[i], i);
    }
}

TEST(EqForNullFunctionTest, left_only_null_right_nullable) {
    const size_t input_rows_count = 100;

    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone_resized(1), ColumnUInt8::create(1, 1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_column->get_data()[i], 0);
        } else {
            ASSERT_EQ(result_column->get_data()[i], 1);
        }
    }
}

TEST(EqForNullFunctionTest, left_only_null_right_not_nullable) {
    const size_t input_rows_count = 100;
    // left [NULL, NULL, NULL, NULL] & right [1, 1, 1, 1]
    // result [0, 0, 0, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone_resized(1), ColumnUInt8::create(1, 1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {right_i32->clone(), std::make_shared<DataTypeInt32>(), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 0);
    }
}

TEST(EqForNullFunctionTest, left_nullable_right_only_null) {
    const size_t input_rows_count = 100;
    // LEFT: [1, NULL, 1, NULL] & RIGHT: [NULL, NULL, NULL, NULL]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone_resized(1),
                                   null_map_for_all_null->clone_resized(1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_column->get_data()[i], 0);
        } else {
            ASSERT_EQ(result_column->get_data()[i], 1);
        }
    }
}

TEST(EqForNullFunctionTest, left_not_nullable_right_only_null) {
    const size_t input_rows_count = 100;
    // LEFT: [1, 1, 1, 1] & RIGHT: [NULL, NULL, NULL, NULL]
    // output [0, 0, 0, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto null_map_for_all_null = ColumnUInt8::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();
    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {left_i32->clone(), std::make_shared<DataTypeInt32>(), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone_resized(1),
                                   null_map_for_all_null->clone_resized(1)),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 0);
    }
}

TEST(EqForNullFunctionTest, left_nullable_right_nullable) {
    const size_t input_rows_count = 100;
    // input: [NULL, 1, NULL, 1] & [NULL, 1, NULL, 1]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        ASSERT_EQ(result_column->get_data()[i], 1);
    }
}

TEST(EqForNullFunctionTest, left_nullable_right_not_nullable) {
    const size_t input_rows_count = 100;
    // input        [1, NULL, 1, NULL] & [1, 1, 1, 1]
    // output       [1, 0, 1, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {right_i32->clone(), std::make_shared<DataTypeInt32>(), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_column->get_data()[i], 1);
        } else {
            ASSERT_EQ(result_column->get_data()[i], 0);
        }
    }
}

TEST(EqForNullFunctionTest, left_not_nullable_right_nullable) {
    const size_t input_rows_count = 100;
    // input        [1, 1, 1, 1] & [1, NULL, 1, NULL]
    // output       [1, 0, 1, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {left_i32->clone(), std::make_shared<DataTypeInt32>(), "right"};
    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_column->get_data()[i], 1);
        } else {
            ASSERT_EQ(result_column->get_data()[i], 0);
        }
    }
}

TEST(EqForNullFunctionTest, left_const_not_nullable_right_nullable) {
    const size_t input_rows_count = 10;
    // input        [1, 1, 1, 1] & [1, NULL, 1, NULL]
    // output       [1, 0, 1, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {ColumnConst::create(left_i32->clone_resized(1), input_rows_count),
                                std::make_shared<DataTypeInt32>(), "right"};
    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            EXPECT_EQ(result_column->get_data()[i], 1)
                    << fmt::format("i {} value {}", i, result_column->get_data()[i]);
        } else {
            EXPECT_EQ(result_column->get_data()[i], 0)
                    << fmt::format("i {} value {}", i, result_column->get_data()[i]);
        }
    }
}

TEST(EqForNullFunctionTest, left_const_nullable_right_nullable) {
    const size_t input_rows_count = 100;
    // input        [1, 1, 1, 1] & [1, NULL, 1, NULL]
    // output       [1, 0, 1, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnConst::create(
                    ColumnNullable::create(left_i32->clone_resized(1), ColumnUInt8::create(1, 0)),
                    input_rows_count),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnNullable::create(right_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_column->get_data()[i], 1);
        } else {
            ASSERT_EQ(result_column->get_data()[i], 0);
        }
    }
}

TEST(EqForNullFunctionTest, left_nullable_right_const_nullable) {
    const size_t input_rows_count = 100;
    // input        [1, NULL, 1, NULL] & [1, 1, 1, 1]
    // output       [1, 0, 1, 0]
    auto left_i32 = ColumnInt32::create(input_rows_count, 1);
    auto right_i32 = ColumnInt32::create(input_rows_count, 1);
    auto common_null_map = ColumnUInt8::create();

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            common_null_map->insert(0);
        } else {
            common_null_map->insert(1);
        }
    }

    ColumnWithTypeAndName left {
            ColumnNullable::create(left_i32->clone(), common_null_map->clone()),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "left"};

    ColumnWithTypeAndName right {
            ColumnConst::create(
                    ColumnNullable::create(right_i32->clone_resized(1), ColumnUInt8::create(1, 0)),
                    input_rows_count),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>()), "right"};

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto func_eq_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null", ColumnsWithTypeAndName {left, right}, return_type);

    Block temporary_block(ColumnsWithTypeAndName {left, right});
    temporary_block.insert(ColumnWithTypeAndName {nullptr, return_type, ""});
    FunctionContext* context = nullptr;
    auto status = func_eq_for_null->execute(context, temporary_block, {0, 1}, 2, input_rows_count);

    ASSERT_TRUE(status.ok());

    auto result_column =
            assert_cast<const ColumnUInt8*>(temporary_block.get_by_position(2).column.get());

    for (size_t i = 0; i < input_rows_count; ++i) {
        if (i % 2 == 0) {
            ASSERT_EQ(result_column->get_data()[i], 1);
        } else {
            ASSERT_EQ(result_column->get_data()[i], 0);
        }
    }
}

} // namespace doris::vectorized