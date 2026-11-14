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

#include "format_v2/expr/evolution_cast.h"

#include <gtest/gtest.h>

#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_spatial.h"
#include "core/data_type/data_type_timestamp_ns.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::format {

class EvolutionCastTest : public testing::Test {
protected:
    static DateV2Value<DateV2ValueType> date(int year, int month, int day) {
        DateV2Value<DateV2ValueType> value;
        value.unchecked_set_time(year, month, day, 0, 0, 0, 0);
        return value;
    }

    Status execute(const VExprSPtr& expr, Block* block, int* result) {
        auto context = VExprContext::create_shared(expr);
        auto status = context->prepare(&state, RowDescriptor());
        if (status.ok()) {
            status = context->open(&state);
        }
        if (status.ok()) {
            status = context->execute(block, result);
        }
        context->close();
        return status;
    }

    void check_spatial_binary_evolution(PrimitiveType primitive, bool nullable) {
        const std::string wkb(
                "\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@",
                21);
        DataTypePtr source = std::make_shared<DataTypeSpatial>(primitive);
        DataTypePtr target = std::make_shared<DataTypeVarbinary>();
        if (nullable) {
            source = make_nullable(source);
            target = make_nullable(target);
        }
        auto column = source->create_column();
        column->insert_data(wkb.data(), wkb.size());
        if (nullable) {
            column->insert_default();
        }
        Block block;
        block.insert({std::move(column), source, "shape"});
        auto expr = EvolutionCast::create_shared(target);
        expr->add_child(VSlotRef::create_shared(0, 0, -1, source, "shape"));
        int result = -1;
        ASSERT_TRUE(execute(expr, &block, &result).ok());
        const auto bytes = block.get_by_position(result).column;
        EXPECT_EQ(bytes->get_data_at(0).to_string(), wkb);
        if (nullable) {
            EXPECT_TRUE(bytes->is_null_at(1));
        }
        block.clear();
        EXPECT_EQ(bytes->get_data_at(0).to_string(), wkb);

        auto constant_value = source->create_column();
        constant_value->insert_data(wkb.data(), wkb.size());
        Block constant_block;
        constant_block.insert({ColumnConst::create(std::move(constant_value), 3), source, "shape"});
        auto constant_expr = EvolutionCast::create_shared(target);
        constant_expr->add_child(VSlotRef::create_shared(0, 0, -1, source, "shape"));
        ASSERT_TRUE(execute(constant_expr, &constant_block, &result).ok());
        EXPECT_EQ(constant_block.get_by_position(result).column->size(), 3);
        EXPECT_EQ(constant_block.get_by_position(result).column->get_data_at(2).to_string(), wkb);

        Block ordinary;
        auto native = source->create_column();
        native->insert_data(wkb.data(), wkb.size());
        ordinary.insert({std::move(native), source, "shape"});
        auto ordinary_cast = Cast::create_shared(target);
        ordinary_cast->add_child(VSlotRef::create_shared(0, 0, -1, source, "shape"));
        EXPECT_FALSE(execute(ordinary_cast, &ordinary, &result).ok());
    }

    DataTypePtr source_type = make_nullable(std::make_shared<DataTypeDateV2>());
    DataTypePtr target_type = make_nullable(std::make_shared<DataTypeTimeStampNs>());
    MockRuntimeState state;
};

TEST_F(EvolutionCastTest, OverflowIsIndependentOfSessionAndSurvivesClone) {
    for (bool strict : {false, true}) {
        state.set_enable_strict_cast(strict);
        for (auto value :
             {date(1000, 1, 1), date(1677, 9, 21), date(2262, 4, 12), date(9999, 12, 31)}) {
            for (bool clone : {false, true}) {
                SCOPED_TRACE(testing::Message() << "strict=" << strict << " clone=" << clone);
                VExprSPtr expr = EvolutionCast::create_shared(target_type);
                expr->add_child(VSlotRef::create_shared(0, 0, -1, source_type, "source"));
                if (clone) {
                    VExprSPtr cloned;
                    ASSERT_TRUE(expr->deep_clone(&cloned).ok());
                    expr = std::move(cloned);
                }
                Block block;
                block.insert({ColumnHelper::create_nullable_column<DataTypeDateV2>({value}, {0}),
                              source_type, "source"});
                int result = -1;
                auto status = execute(expr, &block, &result);
                EXPECT_FALSE(status.ok());
                EXPECT_NE(status.to_string().find("TIMESTAMP_NS overflow"), std::string::npos)
                        << status;
            }
        }
    }
}

TEST_F(EvolutionCastTest, ValidDatesAndNullRemainUnchanged) {
    state.set_enable_strict_cast(false);
    auto expr = EvolutionCast::create_shared(target_type);
    expr->add_child(VSlotRef::create_shared(0, 0, -1, source_type, "source"));
    Block block;
    // A NULL payload may be outside the target range and must not trigger overflow.
    block.insert({ColumnHelper::create_nullable_column<DataTypeDateV2>(
                          {date(1677, 9, 22), date(2262, 4, 11), date(1000, 1, 1)}, {0, 0, 1}),
                  source_type, "source"});
    int result = -1;
    ASSERT_TRUE(execute(expr, &block, &result).ok());
    const auto& column = *block.get_by_position(result).column;
    EXPECT_EQ(target_type->to_string(column, 0), "1677-09-22 00:00:00.000000000");
    EXPECT_EQ(target_type->to_string(column, 1), "2262-04-11 00:00:00.000000000");
    EXPECT_TRUE(column.is_null_at(2));
}

TEST_F(EvolutionCastTest, ConstantOverflowFailsDuringOpen) {
    state.set_enable_strict_cast(false);
    auto expr = EvolutionCast::create_shared(target_type);
    expr->add_child(VLiteral::create_shared(std::make_shared<DataTypeDateV2>(),
                                            Field::create_field<TYPE_DATEV2>(date(1000, 1, 1))));
    auto context = VExprContext::create_shared(expr);
    ASSERT_TRUE(context->prepare(&state, RowDescriptor()).ok());
    auto status = context->open(&state);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("TIMESTAMP_NS overflow"), std::string::npos) << status;
    context->close();
}

TEST_F(EvolutionCastTest, OrdinaryCastPreservesSessionBehavior) {
    for (bool strict : {false, true}) {
        state.set_enable_strict_cast(strict);
        auto expr = Cast::create_shared(target_type);
        expr->add_child(VSlotRef::create_shared(0, 0, -1, source_type, "source"));
        Block block;
        block.insert({ColumnHelper::create_nullable_column<DataTypeDateV2>({date(1000, 1, 1)}, {0}),
                      source_type, "source"});
        int result = -1;
        auto status = execute(expr, &block, &result);
        if (strict) {
            EXPECT_FALSE(status.ok());
        } else {
            ASSERT_TRUE(status.ok()) << status;
            EXPECT_TRUE(block.get_by_position(result).column->is_null_at(0));
        }
    }
}

TEST_F(EvolutionCastTest, RuntimeContextCloneRetainsOverflowCheck) {
    state.set_enable_strict_cast(false);
    auto expr = EvolutionCast::create_shared(target_type);
    expr->add_child(VSlotRef::create_shared(0, 0, -1, source_type, "source"));
    auto context = VExprContext::create_shared(expr);
    ASSERT_TRUE(context->prepare(&state, RowDescriptor()).ok());
    ASSERT_TRUE(context->open(&state).ok());
    VExprContextSPtr cloned;
    ASSERT_TRUE(context->clone(&state, cloned).ok());
    Block block;
    block.insert({ColumnHelper::create_nullable_column<DataTypeDateV2>({date(1000, 1, 1)}, {0}),
                  source_type, "source"});
    int result = -1;
    auto status = cloned->execute(&block, &result);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("TIMESTAMP_NS overflow"), std::string::npos) << status;
    cloned->close();
    context->close();
}

TEST_F(EvolutionCastTest, EmptyInputRemainsEmpty) {
    state.set_enable_strict_cast(false);
    auto expr = EvolutionCast::create_shared(target_type);
    expr->add_child(VSlotRef::create_shared(0, 0, -1, source_type, "source"));
    Block block;
    block.insert(
            {ColumnHelper::create_nullable_column<DataTypeDateV2>({}, {}), source_type, "source"});
    int result = -1;
    ASSERT_TRUE(execute(expr, &block, &result).ok());
    EXPECT_EQ(block.get_by_position(result).column->size(), 0);
}

TEST_F(EvolutionCastTest, NumericPromotionUsesExistingConversion) {
    auto expr = EvolutionCast::create_shared(std::make_shared<DataTypeInt64>());
    expr->add_child(VSlotRef::create_shared(0, 0, -1, std::make_shared<DataTypeInt32>(), "source"));
    Block block;
    block.insert(ColumnHelper::create_column_with_name<DataTypeInt32>({1, -2, 3}));
    int result = -1;
    ASSERT_TRUE(execute(expr, &block, &result).ok());
    const auto& column = assert_cast<const ColumnInt64&>(*block.get_by_position(result).column);
    EXPECT_EQ(column.get_data()[0], 1);
    EXPECT_EQ(column.get_data()[1], -2);
    EXPECT_EQ(column.get_data()[2], 3);
}

TEST_F(EvolutionCastTest, PaimonPrecisionNarrowingSurvivesCloneAndPreservesNull) {
    for (uint32_t scale : {0, 4, 5}) {
        for (bool clone : {false, true}) {
            const auto input_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(6));
            const auto output_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(scale));
            VExprSPtr expr = EvolutionCast::create_shared(output_type, true);
            expr->add_child(VSlotRef::create_shared(0, 0, -1, input_type, "source"));
            if (clone) {
                VExprSPtr cloned;
                ASSERT_TRUE(expr->deep_clone(&cloned).ok());
                expr = std::move(cloned);
            }
            DateV2Value<DateTimeV2ValueType> value;
            value.unchecked_set_time(1969, 12, 31, 23, 59, 59, 699999);
            Block block;
            block.insert({ColumnHelper::create_nullable_column<DataTypeDateTimeV2>({value, value},
                                                                                   {0, 1}),
                          input_type, "source"});
            int result = -1;
            ASSERT_TRUE(execute(expr, &block, &result).ok());
            const auto& column =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);
            EXPECT_TRUE(column.is_null_at(1));
            const auto& values = assert_cast<const ColumnDateTimeV2&>(column.get_nested_column());
            EXPECT_EQ(values.get_data()[0].second(), 59);
            EXPECT_EQ(values.get_data()[0].microsecond(),
                      scale == 0 ? 0 : (scale == 4 ? 699900 : 699990));
        }
    }
}

TEST_F(EvolutionCastTest, OrdinaryEvolutionDoesNotEnablePaimonTruncation) {
    const auto input_type = std::make_shared<DataTypeDateTimeV2>(6);
    const auto output_type = make_nullable(std::make_shared<DataTypeDateTimeV2>(0));
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(2025, 1, 1, 0, 0, 1, 600000);
    Block block;
    block.insert({ColumnHelper::create_column<DataTypeDateTimeV2>({value}), input_type, "source"});
    auto expr = EvolutionCast::create_shared(output_type);
    expr->add_child(VSlotRef::create_shared(0, 0, -1, input_type, "source"));
    int result = -1;
    const auto status = execute(expr, &block, &result);
    ASSERT_TRUE(status.ok()) << status;
    const auto& nullable_column =
            assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);
    EXPECT_FALSE(nullable_column.is_null_at(0));
    const auto& column = assert_cast<const ColumnDateTimeV2&>(nullable_column.get_nested_column());
    EXPECT_EQ(column.get_data()[0].second(), 2);
}

TEST_F(EvolutionCastTest, SpatialEvolutionPreservesExplicitBinaryBytesAndNulls) {
    for (auto primitive : {TYPE_GEOMETRY, TYPE_GEOGRAPHY}) {
        for (bool nullable : {false, true}) {
            check_spatial_binary_evolution(primitive, nullable);
        }
    }
}

} // namespace doris::format
