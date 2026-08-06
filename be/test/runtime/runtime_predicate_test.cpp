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

#include "runtime/runtime_predicate.h"

#include <gtest/gtest.h>

#include <array>

#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exec/pipeline/thrift_builder.h"
#include "exprs/expr_zonemap_filter.h"
#include "exprs/vexpr_context.h"
#include "exprs/vtopn_pred.h"
#include "runtime/descriptors.h"
#include "runtime/query_context.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

constexpr TPlanNodeId SOURCE_NODE_ID = 10;
constexpr TPlanNodeId TARGET_NODE_ID = 20;
constexpr SlotId SLOT_ID = 0;

TTopnFilterDesc create_topn_filter_desc(PrimitiveType type = TYPE_INT, bool is_asc = true,
                                        bool nulls_first = false) {
    auto target_expr = TRuntimeFilterDescBuilder::get_default_expr();
    target_expr.nodes[0].__set_type(create_type_desc(type));

    TTopnFilterDesc desc;
    desc.__set_source_node_id(SOURCE_NODE_ID);
    desc.__set_is_asc(is_asc);
    desc.__set_null_first(nulls_first);
    desc.__set_target_node_id_to_target_expr({{TARGET_NODE_ID, target_expr}});
    return desc;
}

SlotDescriptor create_int_slot_descriptor() {
    SlotDescriptor slot_desc;
    slot_desc._id = SLOT_ID;
    slot_desc._col_name = "k1";
    slot_desc._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    return slot_desc;
}

VExprContextSPtr create_prepared_topn_expr(MockRuntimeState* state, const DataTypePtr& data_type,
                                           const Field& top_value = Field(), bool is_asc = true,
                                           bool nulls_first = false) {
    DORIS_CHECK(state != nullptr);
    DORIS_CHECK(data_type != nullptr);
    auto desc = create_topn_filter_desc(data_type->get_primitive_type(), is_asc, nulls_first);
    state->get_query_ctx()->init_runtime_predicates({desc});
    auto& predicate = state->get_query_ctx()->get_runtime_predicate(SOURCE_NODE_ID);
    predicate.set_detected_source();
    DORIS_CHECK(predicate.init_target(TARGET_NODE_ID, {}, -1).ok());
    if (!top_value.is_null()) {
        DORIS_CHECK(predicate.update(top_value).ok());
    }

    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_is_nullable(false);
    auto expr = VTopNPred::create_shared(node, SOURCE_NODE_ID, nullptr);
    expr->add_child(VSlotRef::create_shared(SLOT_ID, 0, -1, data_type, "topn_column"));
    auto context = VExprContext::create_shared(std::move(expr));
    DORIS_CHECK(context->prepare(state, RowDescriptor()).ok());
    DORIS_CHECK(context->open(state).ok());
    return context;
}

} // namespace

TEST(RuntimePredicateTest, init_target_creates_column_predicate_for_valid_column_id) {
    RuntimePredicate predicate(create_topn_filter_desc());
    predicate.set_detected_source();

    auto slot_desc = create_int_slot_descriptor();
    phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc;
    slot_id_to_slot_desc[SLOT_ID] = &slot_desc;

    ASSERT_TRUE(predicate.init_target(TARGET_NODE_ID, slot_id_to_slot_desc, 0).ok());

    EXPECT_TRUE(predicate.enable());
    EXPECT_EQ("k1", predicate.get_col_name(TARGET_NODE_ID));
    EXPECT_NE(nullptr, predicate.get_predicate(TARGET_NODE_ID));
}

TEST(RuntimePredicateTest, init_target_without_column_predicate_still_enables_runtime_filter) {
    RuntimePredicate predicate(create_topn_filter_desc());
    predicate.set_detected_source();

    phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc;
    ASSERT_TRUE(predicate.init_target(TARGET_NODE_ID, slot_id_to_slot_desc, -1).ok());

    EXPECT_TRUE(predicate.enable());
    EXPECT_EQ(nullptr, predicate.get_predicate(TARGET_NODE_ID));

    auto top_value = Field::create_field<TYPE_INT>(10);
    ASSERT_TRUE(predicate.update(top_value).ok());
    EXPECT_TRUE(predicate.has_value());
    EXPECT_EQ(top_value, predicate.get_value());
}

TEST(RuntimePredicateTest, TopNPredicateFiltersRawFixedValuesUsingCurrentBound) {
    MockRuntimeState state;
    const auto type = std::make_shared<DataTypeInt32>();
    auto context = create_prepared_topn_expr(&state, type, Field::create_field<TYPE_INT>(3));
    const std::array<int32_t, 4> values {1, 3, 4, 2};
    IColumn::Filter matches(values.size(), 1);

    ASSERT_TRUE(context->root()->can_execute_on_raw_fixed_values(type, 0));
    ASSERT_TRUE(context->root()
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (IColumn::Filter {1, 1, 0, 1}));
    context->close();
}

TEST(RuntimePredicateTest, TopNPredicateEvaluatesDictionaryValuesUsingCurrentBound) {
    MockRuntimeState state;
    const auto type = std::make_shared<DataTypeInt32>();
    auto context = create_prepared_topn_expr(&state, type, Field::create_field<TYPE_INT>(3));
    DictionaryEvalContext dictionary_ctx;
    dictionary_ctx.slots.emplace(0, DictionaryEvalContext::SlotDictionary {
                                            .data_type = type,
                                            .values = {Field::create_field<TYPE_INT>(4),
                                                       Field::create_field<TYPE_INT>(5)}});

    ASSERT_TRUE(context->root()->can_evaluate_dictionary_filter());
    EXPECT_EQ(context->root()->evaluate_dictionary_filter(dictionary_ctx),
              ZoneMapFilterResult::kNoMatch);
    dictionary_ctx.slots.at(0).values.push_back(Field::create_field<TYPE_INT>(2));
    EXPECT_EQ(context->root()->evaluate_dictionary_filter(dictionary_ctx),
              ZoneMapFilterResult::kMayMatch);
    context->close();
}

TEST(RuntimePredicateTest, TopNPredicateFiltersRawBinaryValuesUsingCurrentBound) {
    MockRuntimeState state;
    const auto type = DataTypeFactory::instance().create_data_type(TYPE_STRING, false);
    auto context =
            create_prepared_topn_expr(&state, type, Field::create_field<TYPE_STRING>("middle"));
    const std::array<StringRef, 3> values {StringRef("alpha"), StringRef("middle"),
                                           StringRef("zulu")};
    IColumn::Filter matches(values.size(), 1);

    ASSERT_TRUE(context->root()->can_execute_on_raw_binary_values(type, 0));
    ASSERT_TRUE(context->root()
                        ->execute_on_raw_binary_values(values.data(), values.size(), type, 0,
                                                       matches.data())
                        .ok());
    EXPECT_EQ(matches, (IColumn::Filter {1, 1, 0}));
    context->close();
}

TEST(RuntimePredicateTest, TopNPredicateFiltersRawVarbinaryAndDictionaryValues) {
    MockRuntimeState state;
    const auto type = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, false);
    auto context = create_prepared_topn_expr(
            &state, type, Field::create_field<TYPE_VARBINARY>(StringView("middle")));
    const std::array<StringRef, 3> values {StringRef("alpha"), StringRef("middle"),
                                           StringRef("zulu")};
    IColumn::Filter matches(values.size(), 1);

    ASSERT_TRUE(context->root()
                        ->execute_on_raw_binary_values(values.data(), values.size(), type, 0,
                                                       matches.data())
                        .ok());
    EXPECT_EQ(matches, (IColumn::Filter {1, 1, 0}));

    DictionaryEvalContext dictionary_ctx;
    dictionary_ctx.slots.emplace(
            0, DictionaryEvalContext::SlotDictionary {
                       .data_type = type,
                       .values = {Field::create_field<TYPE_VARBINARY>(StringView("zulu"))}});
    EXPECT_EQ(context->root()->evaluate_dictionary_filter(dictionary_ctx),
              ZoneMapFilterResult::kNoMatch);
    dictionary_ctx.slots.at(0).values.push_back(
            Field::create_field<TYPE_VARBINARY>(StringView("alpha")));
    EXPECT_EQ(context->root()->evaluate_dictionary_filter(dictionary_ctx),
              ZoneMapFilterResult::kMayMatch);
    context->close();
}

TEST(RuntimePredicateTest, TopNPredicateKeepsDirectCapabilityBeforeFirstBound) {
    MockRuntimeState state;
    const auto type = std::make_shared<DataTypeInt32>();
    auto context = create_prepared_topn_expr(&state, type);
    const std::array<int32_t, 2> values {1, 4};
    IColumn::Filter matches(values.size(), 1);

    ASSERT_TRUE(context->root()->can_execute_on_raw_fixed_values(type, 0));
    EXPECT_TRUE(context->root()->raw_predicate_result_for_null());
    ASSERT_TRUE(context->root()
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (IColumn::Filter {1, 1}));

    auto& predicate = state.get_query_ctx()->get_runtime_predicate(SOURCE_NODE_ID);
    ASSERT_TRUE(predicate.update(Field::create_field<TYPE_INT>(2)).ok());
    EXPECT_FALSE(context->root()->raw_predicate_result_for_null());
    ASSERT_TRUE(context->root()
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (IColumn::Filter {1, 0}));
    context->close();
}

TEST(RuntimePredicateTest, TopNPredicateAdvertisesDirectCapabilityForEverySupportedType) {
    struct TypeSpec {
        PrimitiveType type;
        int precision = 0;
        int scale = 0;
        bool binary = false;
    };
    const std::array<TypeSpec, 23> supported_types {{
            {TYPE_BOOLEAN},
            {TYPE_TINYINT},
            {TYPE_SMALLINT},
            {TYPE_INT},
            {TYPE_BIGINT},
            {TYPE_LARGEINT},
            {TYPE_DATE},
            {TYPE_DATETIME},
            {TYPE_DATEV2},
            {TYPE_DATETIMEV2},
            {TYPE_TIMESTAMPTZ},
            {TYPE_TIMEV2},
            {TYPE_DECIMAL32, 9, 2},
            {TYPE_DECIMAL64, 18, 2},
            {TYPE_DECIMALV2, 27, 9},
            {TYPE_DECIMAL128I, 38, 2},
            {TYPE_DECIMAL256, 76, 2},
            {TYPE_IPV4},
            {TYPE_IPV6},
            {TYPE_CHAR, 0, 0, true},
            {TYPE_STRING, 0, 0, true},
            {TYPE_VARCHAR, 0, 0, true},
            {TYPE_VARBINARY, 0, 0, true},
    }};

    for (const auto& spec : supported_types) {
        SCOPED_TRACE(type_to_string(spec.type));
        MockRuntimeState state;
        const auto data_type = DataTypeFactory::instance().create_data_type(
                spec.type, false, spec.precision, spec.scale);
        auto context = create_prepared_topn_expr(&state, data_type);
        if (spec.binary) {
            EXPECT_TRUE(context->root()->can_execute_on_raw_binary_values(data_type, 0));
        } else {
            EXPECT_TRUE(context->root()->can_execute_on_raw_fixed_values(data_type, 0));
        }
        EXPECT_TRUE(context->root()->can_evaluate_dictionary_filter());
        context->close();
    }
}

TEST(RuntimePredicateTest, DescTopNPredicateFiltersRawValuesUsingCurrentBound) {
    MockRuntimeState state;
    const auto type = std::make_shared<DataTypeInt32>();
    auto context = create_prepared_topn_expr(&state, type, Field::create_field<TYPE_INT>(3), false);
    const std::array<int32_t, 3> values {1, 3, 4};
    IColumn::Filter matches(values.size(), 1);

    ASSERT_TRUE(context->root()
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), type, 0, matches.data())
                        .ok());
    EXPECT_EQ(matches, (IColumn::Filter {0, 1, 1}));
    context->close();
}

TEST(RuntimePredicateTest, NullableNullsFirstTopNSupportsRawNullSemantics) {
    MockRuntimeState state;
    const auto type = make_nullable(std::make_shared<DataTypeInt32>());
    auto context =
            create_prepared_topn_expr(&state, type, Field::create_field<TYPE_INT>(3), true, true);

    EXPECT_TRUE(context->root()->can_execute_on_raw_fixed_values(type, 0));
    EXPECT_TRUE(context->root()->raw_predicate_result_for_null());
    EXPECT_FALSE(context->root()->can_evaluate_dictionary_filter());
    context->close();
}

} // namespace doris
