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

#include "exprs/expr_zonemap_filter.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/value/vdatetime_value.h"
#include "exprs/create_predicate_function.h"
#include "exprs/function/functions_comparison.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/hybrid_set.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/segment/segment_iterator.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vin_predicate.h"
#undef private
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris {
namespace {

Field int_field(int32_t value) {
    return Field::create_field<TYPE_INT>(value);
}

DataTypePtr int_type() {
    return std::make_shared<DataTypeInt32>();
}

VExprSPtr make_slot(int column_id, const DataTypePtr& data_type) {
    auto slot = std::make_shared<VSlotRef>();
    slot->set_node_type(TExprNodeType::SLOT_REF);
    slot->set_column_id(column_id);
    slot->data_type() = data_type;
    return slot;
}

VExprSPtr make_int_literal(int32_t value) {
    return std::make_shared<VLiteral>(create_texpr_node_from(int_field(value), TYPE_INT, 0, 0));
}

Field datetimev2_field(uint16_t year, uint8_t month, uint8_t day, uint8_t hour, uint8_t minute,
                       uint8_t second, uint32_t microsecond) {
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    return Field::create_field<TYPE_DATETIMEV2>(value);
}

VExprSPtr make_datetimev2_literal(int scale) {
    return std::make_shared<VLiteral>(create_texpr_node_from(
            datetimev2_field(2024, 1, 2, 0, 0, 0, 0), TYPE_DATETIMEV2, 0, scale));
}

VExprSPtr make_null_int_literal() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_INT));
    node.__set_is_nullable(true);
    return std::make_shared<VLiteral>(node);
}

VExprSPtr make_null_string_literal() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::NULL_LITERAL);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_STRING));
    node.__set_is_nullable(true);
    return std::make_shared<VLiteral>(node);
}

VExprSPtr make_string_literal(std::string value) {
    auto field = Field::create_field<TYPE_STRING>(std::move(value));
    return std::make_shared<VLiteral>(create_texpr_node_from(field, TYPE_STRING, 0, 0));
}

ZoneMapEvalContext make_context(segment_v2::ZoneMap zone_map, const DataTypePtr& data_type) {
    ZoneMapEvalContext ctx;
    ZoneMapEvalContext::SlotZoneMap slot_zone_map;
    slot_zone_map.data_type = data_type;
    slot_zone_map.zone_map = std::make_shared<segment_v2::ZoneMap>(std::move(zone_map));
    ctx.slots.emplace(0, std::move(slot_zone_map));
    return ctx;
}

segment_v2::ZoneMap make_int_zonemap(int32_t min_value, int32_t max_value) {
    segment_v2::ZoneMap zone_map;
    zone_map.min_value = int_field(min_value);
    zone_map.max_value = int_field(max_value);
    zone_map.has_not_null = true;
    return zone_map;
}

segment_v2::ZoneMap make_string_zonemap(std::string min_value, std::string max_value) {
    segment_v2::ZoneMap zone_map;
    zone_map.min_value = Field::create_field<TYPE_STRING>(std::move(min_value));
    zone_map.max_value = Field::create_field<TYPE_STRING>(std::move(max_value));
    zone_map.has_not_null = true;
    return zone_map;
}

TDescriptorTable make_k2_scan_desc_tbl() {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_builder;
    auto k2_slot = TSlotDescriptorBuilder()
                           .type(TYPE_INT)
                           .column_name("k2")
                           .column_pos(0)
                           .nullable(false)
                           .build();
    k2_slot.__set_col_unique_id(1);
    tuple_builder.add_slot(k2_slot);
    tuple_builder.build(&desc_tbl_builder);
    return desc_tbl_builder.desc_tbl();
}

TExprNode make_compound_node(TExprOpcode::type opcode, int num_children) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(opcode);
    node.__set_num_children(num_children);
    node.__set_is_nullable(false);
    return node;
}

TExprNode make_in_predicate_node(bool is_not_in, int num_children) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.__set_opcode(is_not_in ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN);
    node.__set_num_children(num_children);
    node.__set_is_nullable(false);
    node.in_predicate.__set_is_not_in(is_not_in);
    return node;
}

class FixedZonemapExpr final : public VExpr {
public:
    explicit FixedZonemapExpr(ZoneMapFilterResult result) : _result(result) {
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("FixedZonemapExpr is only used by zonemap tests");
    }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext&) const override {
        return _result;
    }

    bool can_evaluate_zonemap_filter() const override { return true; }

private:
    ZoneMapFilterResult _result;
    std::string _expr_name = "fixed_zonemap_expr";
};

class UnsupportedSingleSlotExpr final : public VExpr {
public:
    explicit UnsupportedSingleSlotExpr(const VExprSPtr& slot) {
        _data_type = std::make_shared<DataTypeUInt8>();
        add_child(slot);
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("UnsupportedSingleSlotExpr is only used by zonemap tests");
    }

private:
    std::string _expr_name = "unsupported_single_slot_expr";
};

VExprSPtr make_fixed_zonemap_expr(ZoneMapFilterResult result) {
    return std::make_shared<FixedZonemapExpr>(result);
}

} // namespace

TEST(ExprZonemapFilterTest, DataTypesCompatibleRequiresExactNonStringType) {
    EXPECT_TRUE(expr_zonemap::data_types_compatible(std::make_shared<DataTypeInt32>(),
                                                    std::make_shared<DataTypeInt32>()));
    EXPECT_TRUE(expr_zonemap::data_types_compatible(
            std::make_shared<DataTypeString>(10, TYPE_CHAR),
            std::make_shared<DataTypeString>(-1, TYPE_VARCHAR)));

    EXPECT_FALSE(expr_zonemap::data_types_compatible(std::make_shared<DataTypeInt32>(),
                                                     std::make_shared<DataTypeInt64>()));
    EXPECT_FALSE(expr_zonemap::data_types_compatible(std::make_shared<DataTypeDecimal64>(10, 2),
                                                     std::make_shared<DataTypeDecimal64>(10, 0)));
}

TEST(ExprZonemapFilterTest, ComparisonZonemapPrunesDisjointRanges) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    FunctionComparison<LessOp, NameLess> less;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              less.evaluate_zonemap_filter(ctx, {slot, make_int_literal(10)}));

    FunctionComparison<GreaterOp, NameGreater> greater;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              greater.evaluate_zonemap_filter(ctx, {slot, make_int_literal(20)}));

    FunctionComparison<EqualsOp, NameEquals> equals;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals.evaluate_zonemap_filter(ctx, {slot, make_int_literal(30)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals.evaluate_zonemap_filter(ctx, {slot, make_int_literal(15)}));
}

TEST(ExprZonemapFilterTest, ComparisonZonemapHandlesLiteralOnLeft) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    FunctionComparison<LessOp, NameLess> less;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              less.evaluate_zonemap_filter(ctx, {make_int_literal(25), slot}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              less.evaluate_zonemap_filter(ctx, {make_int_literal(15), slot}));
}

TEST(ExprZonemapFilterTest, ComparisonZonemapHandlesBoundariesAndAllOperators) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    FunctionComparison<LessOrEqualsOp, NameLessOrEquals> less_equal;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              less_equal.evaluate_zonemap_filter(ctx, {slot, make_int_literal(9)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              less_equal.evaluate_zonemap_filter(ctx, {slot, make_int_literal(10)}));

    FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals> greater_equal;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              greater_equal.evaluate_zonemap_filter(ctx, {slot, make_int_literal(21)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              greater_equal.evaluate_zonemap_filter(ctx, {slot, make_int_literal(20)}));

    FunctionComparison<NotEqualsOp, NameNotEquals> not_equals;
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              not_equals.evaluate_zonemap_filter(ctx, {slot, make_int_literal(10)}));

    auto single_value_ctx = make_context(make_int_zonemap(10, 10), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              not_equals.evaluate_zonemap_filter(single_value_ctx, {slot, make_int_literal(10)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              not_equals.evaluate_zonemap_filter(single_value_ctx, {slot, make_int_literal(11)}));

    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              greater_equal.evaluate_zonemap_filter(ctx, {make_int_literal(9), slot}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              greater_equal.evaluate_zonemap_filter(ctx, {make_int_literal(10), slot}));
}

TEST(ExprZonemapFilterTest, ComparisonZonemapHandlesNullAndUnsupportedInputs) {
    auto type = int_type();
    auto slot = make_slot(0, type);

    FunctionComparison<EqualsOp, NameEquals> equals;
    EXPECT_FALSE(equals.can_evaluate_zonemap_filter({slot, make_null_int_literal()}));

    auto string_type = std::make_shared<DataTypeString>();
    auto string_slot = make_slot(0, string_type);
    EXPECT_TRUE(equals.can_evaluate_zonemap_filter({string_slot, make_string_literal("10")}));

    auto datetimev2_slot = make_slot(
            0, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)));
    EXPECT_FALSE(equals.can_evaluate_zonemap_filter({datetimev2_slot, make_datetimev2_literal(0)}));

    ZoneMapEvalContext missing_zonemap_ctx;
    ZoneMapEvalContext::SlotZoneMap slot_without_zonemap;
    slot_without_zonemap.data_type = type;
    missing_zonemap_ctx.slots.emplace(0, std::move(slot_without_zonemap));
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
              equals.evaluate_zonemap_filter(missing_zonemap_ctx, {slot, make_int_literal(10)}));
    EXPECT_EQ(1, missing_zonemap_ctx.stats.unusable_zonemap_eval_count);

    auto pass_all_zonemap = make_int_zonemap(10, 20);
    pass_all_zonemap.pass_all = true;
    auto pass_all_ctx = make_context(std::move(pass_all_zonemap), type);
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
              equals.evaluate_zonemap_filter(pass_all_ctx, {slot, make_int_literal(10)}));
    EXPECT_EQ(1, pass_all_ctx.stats.unusable_zonemap_eval_count);
}

TEST(ExprZonemapFilterTest, MissingSlotTypeCountsUnsupportedZonemapEvalOnce) {
    auto type = int_type();
    auto slot = make_slot(0, type);

    FunctionComparison<EqualsOp, NameEquals> equals;
    ZoneMapEvalContext comparison_ctx;
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
              equals.evaluate_zonemap_filter(comparison_ctx, {slot, make_int_literal(10)}));
    EXPECT_EQ(1, comparison_ctx.stats.unusable_zonemap_eval_count);

    auto string_type = std::make_shared<DataTypeString>();
    auto string_slot = make_slot(0, string_type);
    auto starts_with = SimpleFunctionFactory::instance().get_function(
            "starts_with",
            ColumnsWithTypeAndName {{nullptr, string_type, "slot"},
                                    {nullptr, string_type, "prefix"}},
            std::make_shared<DataTypeUInt8>());
    ASSERT_NE(starts_with, nullptr);
    ZoneMapEvalContext starts_with_ctx;
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
              starts_with->evaluate_zonemap_filter(starts_with_ctx,
                                                   {string_slot, make_string_literal("ab")}));
    EXPECT_EQ(1, starts_with_ctx.stats.unusable_zonemap_eval_count);

    std::vector<Field> values {int_field(10)};
    ZoneMapEvalContext in_ctx;
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
              expr_zonemap::eval_in_zonemap(in_ctx, slot, false, values, int_field(10),
                                            int_field(10)));
    EXPECT_EQ(1, in_ctx.stats.unusable_zonemap_eval_count);
}

TEST(ExprZonemapFilterTest, NullZonemapUsesNullFlagsOnly) {
    auto type = int_type();
    auto slot = make_slot(0, type);

    struct NullFlagCase {
        bool has_null;
        bool has_not_null;
        ZoneMapFilterResult is_null_result;
        ZoneMapFilterResult is_not_null_result;
    };
    const std::vector<NullFlagCase> cases {
            {false, false, ZoneMapFilterResult::kNoMatch, ZoneMapFilterResult::kNoMatch},
            {true, false, ZoneMapFilterResult::kMayMatch, ZoneMapFilterResult::kNoMatch},
            {false, true, ZoneMapFilterResult::kNoMatch, ZoneMapFilterResult::kMayMatch},
            {true, true, ZoneMapFilterResult::kMayMatch, ZoneMapFilterResult::kMayMatch}};

    for (const auto& c : cases) {
        segment_v2::ZoneMap zone_map;
        zone_map.has_null = c.has_null;
        zone_map.has_not_null = c.has_not_null;
        auto ctx = make_context(zone_map, type);

        EXPECT_EQ(c.is_null_result, expr_zonemap::eval_null_zonemap(ctx, {slot}, true))
                << "has_null=" << c.has_null << ", has_not_null=" << c.has_not_null;
        EXPECT_EQ(c.is_not_null_result, expr_zonemap::eval_null_zonemap(ctx, {slot}, false))
                << "has_null=" << c.has_null << ", has_not_null=" << c.has_not_null;
    }
}

TEST(ExprZonemapFilterTest, RangeStatsUnusableFlagsFallback) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    FunctionComparison<EqualsOp, NameEquals> equals;

    for (int flag = 0; flag < 4; ++flag) {
        auto zone_map = make_int_zonemap(10, 20);
        zone_map.pass_all = flag == 0;
        zone_map.has_nan = flag == 1;
        zone_map.has_positive_inf = flag == 2;
        zone_map.has_negative_inf = flag == 3;

        auto ctx = make_context(std::move(zone_map), type);

        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  equals.evaluate_zonemap_filter(ctx, {slot, make_int_literal(10)}))
                << "flag=" << flag;
        EXPECT_EQ(1, ctx.stats.unusable_zonemap_eval_count) << "flag=" << flag;
    }
}

TEST(ExprZonemapFilterTest, InZonemapSkipsZonesWithoutNonNullValues) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    std::vector<Field> values {int_field(10)};

    segment_v2::ZoneMap empty_zone;
    auto empty_ctx = make_context(empty_zone, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(empty_ctx, slot, false, values, int_field(10),
                                            int_field(10)));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(empty_ctx, slot, true, values, int_field(10),
                                            int_field(10)));

    segment_v2::ZoneMap only_null_zone;
    only_null_zone.has_null = true;
    auto only_null_ctx = make_context(only_null_zone, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(only_null_ctx, slot, false, values, int_field(10),
                                            int_field(10)));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(only_null_ctx, slot, true, values, int_field(10),
                                            int_field(10)));
}

TEST(ExprZonemapFilterTest, FunctionStringStartsWithZonemapUsesPrefixRange) {
    auto type = std::make_shared<DataTypeString>();
    auto slot = make_slot(0, type);
    auto starts_with = SimpleFunctionFactory::instance().get_function(
            "starts_with",
            ColumnsWithTypeAndName {{nullptr, type, "slot"}, {nullptr, type, "prefix"}},
            std::make_shared<DataTypeUInt8>());
    ASSERT_NE(starts_with, nullptr);
    EXPECT_EQ("starts_with", starts_with->get_name());

    EXPECT_TRUE(starts_with->can_evaluate_zonemap_filter({slot, make_string_literal("ab")}));
    EXPECT_FALSE(starts_with->can_evaluate_zonemap_filter({slot, make_null_string_literal()}));
    EXPECT_FALSE(starts_with->can_evaluate_zonemap_filter({slot, make_string_literal("")}));
    EXPECT_FALSE(starts_with->can_evaluate_zonemap_filter({make_string_literal("ab"), slot}));

    auto below_prefix_ctx = make_context(make_string_zonemap("aa", "aa"), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              starts_with->evaluate_zonemap_filter(below_prefix_ctx,
                                                   {slot, make_string_literal("ab")}));

    auto above_prefix_ctx = make_context(make_string_zonemap("ac", "ad"), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              starts_with->evaluate_zonemap_filter(above_prefix_ctx,
                                                   {slot, make_string_literal("ab")}));

    auto overlap_ctx = make_context(make_string_zonemap("aa", "abz"), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              starts_with->evaluate_zonemap_filter(overlap_ctx, {slot, make_string_literal("ab")}));

    segment_v2::ZoneMap only_null;
    only_null.has_null = true;
    auto null_ctx = make_context(only_null, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              starts_with->evaluate_zonemap_filter(null_ctx, {slot, make_string_literal("ab")}));

    std::string max_byte_prefix(1, static_cast<char>(0xff));
    EXPECT_TRUE(
            starts_with->can_evaluate_zonemap_filter({slot, make_string_literal(max_byte_prefix)}));
    auto max_prefix_ctx =
            make_context(make_string_zonemap(max_byte_prefix, max_byte_prefix + "z"), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              starts_with->evaluate_zonemap_filter(max_prefix_ctx,
                                                   {slot, make_string_literal(max_byte_prefix)}));
}

TEST(ExprZonemapFilterTest, CharZonemapUsesTrimmedLogicalBounds) {
    auto char_type = std::make_shared<DataTypeString>(10, TYPE_CHAR);
    auto slot = make_slot(0, char_type);

    std::string padded_alpha = "alpha";
    padded_alpha.resize(10, '\0');
    std::string padded_beta = "beta";
    padded_beta.resize(10, '\0');

    segment_v2::ZoneMapPB zone_map_pb;
    zone_map_pb.set_has_not_null(true);
    zone_map_pb.set_min(padded_alpha);
    zone_map_pb.set_max(padded_beta);
    segment_v2::ZoneMap zone_map;
    ASSERT_TRUE(segment_v2::ZoneMap::from_proto(zone_map_pb, char_type, zone_map).ok());
    EXPECT_EQ(Field::create_field<TYPE_STRING>("alpha"), zone_map.min_value);
    EXPECT_EQ(Field::create_field<TYPE_STRING>("beta"), zone_map.max_value);

    FunctionComparison<EqualsOp, NameEquals> equals;
    EXPECT_TRUE(equals.can_evaluate_zonemap_filter({slot, make_string_literal("gamma")}));
    auto comparison_ctx = make_context(zone_map, char_type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals.evaluate_zonemap_filter(comparison_ctx, {slot, make_string_literal("gamma")}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals.evaluate_zonemap_filter(comparison_ctx, {slot, make_string_literal("alpha")}));

    auto string_type = std::make_shared<DataTypeString>();
    auto starts_with = SimpleFunctionFactory::instance().get_function(
            "starts_with",
            ColumnsWithTypeAndName {{nullptr, char_type, "slot"}, {nullptr, string_type, "prefix"}},
            std::make_shared<DataTypeUInt8>());
    ASSERT_NE(starts_with, nullptr);
    EXPECT_TRUE(starts_with->can_evaluate_zonemap_filter({slot, make_string_literal("ga")}));
    auto starts_with_ctx = make_context(zone_map, char_type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              starts_with->evaluate_zonemap_filter(starts_with_ctx,
                                                   {slot, make_string_literal("ga")}));

    auto in_value = Field::create_field<TYPE_STRING>("gamma");
    std::vector<Field> values {in_value};
    auto in_ctx = make_context(zone_map, char_type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(in_ctx, slot, false, values, in_value, in_value));
}

TEST(ExprZonemapFilterTest, InZonemapFallsBackToRangeWhenPointListIsLarge) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    std::vector<Field> values;
    for (int value = 1; value <= 65; ++value) {
        values.emplace_back(int_field(value));
    }
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, values, int_field(1), int_field(65)));

    EXPECT_EQ(0, ctx.stats.in_zonemap_point_check_count);
    EXPECT_EQ(1, ctx.stats.in_zonemap_range_only_count);
}

TEST(ExprZonemapFilterTest, InZonemapUsesPointChecksUnderThreshold) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    std::vector<Field> values {int_field(1), int_field(30)};
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, values, int_field(1), int_field(30)));
    EXPECT_EQ(1, ctx.stats.in_zonemap_point_check_count);
}

TEST(ExprZonemapFilterTest, InZonemapHandlesEmptyListAndNotInSingleValueRange) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    std::vector<Field> empty_values;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, empty_values, {}, {}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, true, empty_values, {}, {}));

    auto single_value_ctx = make_context(make_int_zonemap(10, 10), type);
    std::vector<Field> values {int_field(10)};
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(single_value_ctx, slot, true, values, int_field(10),
                                            int_field(10)));

    std::vector<Field> other_values {int_field(11)};
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_zonemap(single_value_ctx, slot, true, other_values,
                                            int_field(11), int_field(11)));
}

TEST(ExprZonemapFilterTest, UnsupportedSingleSlotExprDoesNotAdvertiseZonemapCapability) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto unsupported_expr = std::make_shared<UnsupportedSingleSlotExpr>(slot);

    std::set<int> slot_indexes;
    unsupported_expr->collect_slot_column_ids(slot_indexes);
    EXPECT_EQ(std::set<int>({0}), slot_indexes);
    EXPECT_FALSE(unsupported_expr->can_evaluate_zonemap_filter());

    FunctionComparison<EqualsOp, NameEquals> equals;
    EXPECT_TRUE(equals.can_evaluate_zonemap_filter({slot, make_int_literal(10)}));
    EXPECT_FALSE(equals.can_evaluate_zonemap_filter({unsupported_expr, make_int_literal(10)}));
}

TEST(ExprZonemapFilterTest, VInPredicateMaterializesZonemapValues) {
    auto type = int_type();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    auto thrift_desc_tbl = make_k2_scan_desc_tbl();
    ASSERT_TRUE(DescriptorTbl::create(&obj_pool, thrift_desc_tbl, &desc_tbl).ok());

    RuntimeState runtime_state;
    runtime_state.set_desc_tbl(desc_tbl);
    RowDescriptor row_desc(runtime_state.desc_tbl(), {0});

    auto in_predicate = std::make_shared<VInPredicate>(make_in_predicate_node(false, 3));
    auto in_slot = make_slot(0, type);
    std::static_pointer_cast<VSlotRef>(in_slot)->set_slot_id(0);
    in_predicate->add_child(in_slot);
    in_predicate->add_child(make_int_literal(1));
    in_predicate->add_child(make_int_literal(30));
    VExprContext in_context(in_predicate);
    ASSERT_TRUE(in_context.prepare(&runtime_state, row_desc).ok());
    ASSERT_TRUE(in_context.open(&runtime_state).ok());

    auto ctx = make_context(make_int_zonemap(10, 20), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, in_predicate->evaluate_zonemap_filter(ctx));
    EXPECT_TRUE(in_predicate->_zonemap_materialized);
    EXPECT_EQ(int_field(1), in_predicate->_seg_filter_min);
    EXPECT_EQ(int_field(30), in_predicate->_seg_filter_max);

    auto not_in_with_null = std::make_shared<VInPredicate>(make_in_predicate_node(true, 3));
    auto not_in_slot = make_slot(0, type);
    std::static_pointer_cast<VSlotRef>(not_in_slot)->set_slot_id(0);
    not_in_with_null->add_child(not_in_slot);
    not_in_with_null->add_child(make_int_literal(10));
    not_in_with_null->add_child(make_null_int_literal());
    VExprContext not_in_context(not_in_with_null);
    ASSERT_TRUE(not_in_context.prepare(&runtime_state, row_desc).ok());
    ASSERT_TRUE(not_in_context.open(&runtime_state).ok());

    auto may_match_ctx = make_context(make_int_zonemap(11, 11), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              not_in_with_null->evaluate_zonemap_filter(may_match_ctx));
    EXPECT_TRUE(not_in_with_null->_seg_filter_contains_null);
}

TEST(ExprZonemapFilterTest, DirectInPredicateMaterializesStringSetForZonemap) {
    auto type = std::make_shared<DataTypeString>();
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_STRING, false));
    StringRef aaa("aaa");
    filter->insert(&aaa);
    StringRef zzz("zzz");
    filter->insert(&zzz);

    auto slot = make_slot(0, type);
    VDirectInPredicate direct_in_expr(make_in_predicate_node(false, 2), filter, true);
    direct_in_expr.add_child(slot);
    ASSERT_TRUE(direct_in_expr._materialize_for_zonemap_filter().ok());

    EXPECT_TRUE(direct_in_expr._zonemap_materialized);
    EXPECT_EQ(2, direct_in_expr._seg_filter_values.size());
    EXPECT_EQ(Field::create_field<TYPE_STRING>("aaa"), direct_in_expr._seg_filter_min);
    EXPECT_EQ(Field::create_field<TYPE_STRING>("zzz"), direct_in_expr._seg_filter_max);
}

TEST(ExprZonemapFilterTest, DirectInPredicateMaterializesZonemapValuesDuringPrepare) {
    auto type = int_type();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    auto thrift_desc_tbl = make_k2_scan_desc_tbl();
    ASSERT_TRUE(DescriptorTbl::create(&obj_pool, thrift_desc_tbl, &desc_tbl).ok());

    RuntimeState runtime_state;
    runtime_state.set_desc_tbl(desc_tbl);
    RowDescriptor row_desc(runtime_state.desc_tbl(), {0});

    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, false));
    int32_t low_value = 1;
    int32_t high_value = 30;
    filter->insert(&low_value);
    filter->insert(&high_value);

    auto direct_in_expr =
            std::make_shared<VDirectInPredicate>(make_in_predicate_node(false, 1), filter, true);
    auto slot = make_slot(0, type);
    std::static_pointer_cast<VSlotRef>(slot)->set_slot_id(0);
    direct_in_expr->add_child(slot);

    VExprContext context(direct_in_expr);
    ASSERT_TRUE(context.prepare(&runtime_state, row_desc).ok());

    EXPECT_TRUE(direct_in_expr->_zonemap_materialized);
    EXPECT_TRUE(direct_in_expr->can_evaluate_zonemap_filter());
    EXPECT_EQ(2, direct_in_expr->_seg_filter_values.size());
    EXPECT_EQ(int_field(1), direct_in_expr->_seg_filter_min);
    EXPECT_EQ(int_field(30), direct_in_expr->_seg_filter_max);
}

TEST(ExprZonemapFilterTest, DirectInPredicateRewritesStringSetToInPredicate) {
    auto type = std::make_shared<DataTypeString>();
    auto slot = make_slot(0, type);
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_STRING, false));
    StringRef value("iceberg");
    filter->insert(&value);

    VDirectInPredicate direct_in_expr(make_in_predicate_node(false, 1), filter, true);
    direct_in_expr.add_child(slot);

    VExprSPtr in_expr;
    ASSERT_TRUE(direct_in_expr.get_slot_in_expr(in_expr));
    EXPECT_NE(std::string::npos, in_expr->debug_string().find("iceberg"));
}

TEST(ExprZonemapFilterTest, DirectInPredicateSkipsMaterializationWhenSetTypeDiffersFromChild) {
    auto string_type = std::make_shared<DataTypeString>();
    auto slot = make_slot(0, string_type);
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, false));
    int dict_code = 1;
    filter->insert(&dict_code);

    VDirectInPredicate direct_in_expr(make_in_predicate_node(false, 1), filter, false);
    direct_in_expr.add_child(slot);

    ASSERT_TRUE(direct_in_expr._materialize_for_zonemap_filter().ok());
    EXPECT_FALSE(direct_in_expr._zonemap_materialized);
    VExprSPtr in_expr;
    EXPECT_FALSE(direct_in_expr.get_slot_in_expr(in_expr));
}

TEST(ExprZonemapFilterTest, RuntimeFilterExprNullAwareZonemapKeepsZonesWithNull) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, true));
    int32_t low_value = 1;
    int32_t high_value = 30;
    filter->insert(&low_value);
    filter->insert(&high_value);

    auto direct_in_expr =
            std::make_shared<VDirectInPredicate>(make_in_predicate_node(false, 1), filter, true);
    direct_in_expr->add_child(slot);
    ASSERT_TRUE(direct_in_expr->_materialize_for_zonemap_filter().ok());

    auto runtime_filter = RuntimeFilterExpr::create_shared(make_in_predicate_node(false, 1),
                                                           direct_in_expr, 0.0, true, 7);
    EXPECT_TRUE(runtime_filter->can_evaluate_zonemap_filter());

    auto no_null_ctx = make_context(make_int_zonemap(10, 20), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, runtime_filter->evaluate_zonemap_filter(no_null_ctx));

    auto with_null_zonemap = make_int_zonemap(10, 20);
    with_null_zonemap.has_null = true;
    auto with_null_ctx = make_context(std::move(with_null_zonemap), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              runtime_filter->evaluate_zonemap_filter(with_null_ctx));

    segment_v2::ZoneMap only_null_zonemap;
    only_null_zonemap.has_null = true;
    auto only_null_ctx = make_context(std::move(only_null_zonemap), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              runtime_filter->evaluate_zonemap_filter(only_null_ctx));
}

TEST(ExprZonemapFilterTest, CompoundPredicateEvaluatesChildrenForZonemap) {
    ZoneMapEvalContext ctx;

    VCompoundPred and_pred(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    and_pred.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kMayMatch));
    and_pred.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, and_pred.evaluate_zonemap_filter(ctx));

    auto type = int_type();
    auto slot = make_slot(0, type);
    VCompoundPred and_with_unsupported(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    and_with_unsupported.add_child(std::make_shared<UnsupportedSingleSlotExpr>(slot));
    and_with_unsupported.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, and_with_unsupported.evaluate_zonemap_filter(ctx));

    VCompoundPred or_pred(make_compound_node(TExprOpcode::COMPOUND_OR, 2));
    or_pred.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    or_pred.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kMayMatch));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, or_pred.evaluate_zonemap_filter(ctx));

    VCompoundPred or_with_unsupported(make_compound_node(TExprOpcode::COMPOUND_OR, 2));
    or_with_unsupported.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    or_with_unsupported.add_child(std::make_shared<UnsupportedSingleSlotExpr>(slot));
    EXPECT_FALSE(or_with_unsupported.can_evaluate_zonemap_filter());

    VCompoundPred or_no_match(make_compound_node(TExprOpcode::COMPOUND_OR, 2));
    or_no_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    or_no_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, or_no_match.evaluate_zonemap_filter(ctx));

    VCompoundPred not_pred(make_compound_node(TExprOpcode::COMPOUND_NOT, 1));
    not_pred.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported, not_pred.evaluate_zonemap_filter(ctx));
    EXPECT_EQ(1, ctx.stats.unusable_zonemap_eval_count);
}

TEST(ExprZonemapFilterTest, ExprContextZonemapEvaluationShortCircuitsOnNoMatch) {
    auto may_match =
            VExprContext::create_shared(make_fixed_zonemap_expr(ZoneMapFilterResult::kMayMatch));
    auto no_match =
            VExprContext::create_shared(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));

    ZoneMapEvalContext ctx;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              VExprContext::evaluate_zonemap_filter({may_match, no_match}, ctx));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              VExprContext::evaluate_zonemap_filter({may_match}, ctx));

    auto type = int_type();
    auto slot = make_slot(0, type);
    auto unsupported =
            VExprContext::create_shared(std::make_shared<UnsupportedSingleSlotExpr>(slot));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              VExprContext::evaluate_zonemap_filter({unsupported, no_match}, ctx));
    EXPECT_EQ(0, ctx.stats.unusable_zonemap_eval_count);
}

} // namespace doris
