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

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_time.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/value/vdatetime_value.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/create_predicate_function.h"
#include "exprs/function/functions_comparison.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/hybrid_set.h"
#include "exprs/hybrid_set_min_max.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vbloom_predicate.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/index/bloom_filter/block_split_bloom_filter.h"
#include "storage/index/zone_map/zone_map_index.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/segment/segment_iterator.h"
#include "util/defer_op.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vin_predicate.h"
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

DictionaryEvalContext make_dictionary_context(std::vector<Field> values,
                                              const DataTypePtr& data_type) {
    DictionaryEvalContext ctx;
    ctx.slots.emplace(0, DictionaryEvalContext::SlotDictionary {
                                 .data_type = data_type,
                                 .values = std::move(values),
                         });
    return ctx;
}

std::unique_ptr<segment_v2::BlockSplitBloomFilter> make_int_bloom_filter(
        const std::vector<int32_t>& values) {
    auto bloom_filter = std::make_unique<segment_v2::BlockSplitBloomFilter>();
    EXPECT_TRUE(bloom_filter->init(segment_v2::BloomFilter::MINIMUM_BYTES).ok());
    for (const auto value : values) {
        bloom_filter->add_bytes(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    return bloom_filter;
}

std::unique_ptr<segment_v2::BlockSplitBloomFilter> make_string_bloom_filter(
        const std::vector<std::string>& values) {
    auto bloom_filter = std::make_unique<segment_v2::BlockSplitBloomFilter>();
    EXPECT_TRUE(bloom_filter->init(segment_v2::BloomFilter::MINIMUM_BYTES).ok());
    for (const auto& value : values) {
        bloom_filter->add_bytes(value.data(), value.size());
    }
    return bloom_filter;
}

BloomFilterEvalContext make_bloom_filter_context(const segment_v2::BloomFilter* bloom_filter,
                                                 const DataTypePtr& data_type) {
    BloomFilterEvalContext ctx;
    ctx.slots.emplace(0, BloomFilterEvalContext::SlotBloomFilter {
                                 .data_type = data_type,
                                 .bloom_filter = bloom_filter,
                         });
    return ctx;
}

struct MinMaxTestSet {
    std::shared_ptr<HybridSetBase> set;
    HybridSetMinMax min_max;
};

MinMaxTestSet make_int_set_with_min_max(const std::vector<int32_t>& values, bool null_aware = false,
                                        bool contains_null = false) {
    MinMaxTestSet result;
    result.set.reset(create_set(TYPE_INT, null_aware));
    for (const auto value : values) {
        result.set->insert(&value);
    }
    if (contains_null) {
        result.set->insert(static_cast<const void*>(nullptr));
    }
    expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(result.set, int_type(), result.min_max);
    return result;
}

template <PrimitiveType T>
MinMaxTestSet make_typed_set_with_min_max(
        const std::vector<typename PrimitiveTypeTraits<T>::CppType>& values,
        const DataTypePtr& data_type, bool null_aware = false, bool contains_null = false) {
    MinMaxTestSet result;
    result.set.reset(create_set(T, null_aware));
    for (const auto& value : values) {
        result.set->insert(&value);
    }
    if (contains_null) {
        result.set->insert(static_cast<const void*>(nullptr));
    }
    expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(result.set, data_type, result.min_max);
    return result;
}

MinMaxTestSet make_string_set_with_min_max(const std::vector<std::string>& values,
                                           const DataTypePtr& data_type) {
    MinMaxTestSet result;
    result.set.reset(create_set(TYPE_STRING, false));
    for (const auto& value : values) {
        StringRef string_value(value);
        result.set->insert(&string_value);
    }
    expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(result.set, data_type, result.min_max);
    return result;
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

TDescriptorTable make_k2_scan_desc_tbl(PrimitiveType primitive_type = TYPE_INT) {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_builder;
    auto k2_slot = TSlotDescriptorBuilder()
                           .type(primitive_type)
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

class MetadataAccessorExpr final : public VExpr {
public:
    MetadataAccessorExpr(std::string function_name, DataTypePtr result_type, VExprSPtr parent,
                         VExprSPtr selector)
            : VExpr(std::move(result_type), false), _expr_name(std::move(function_name)) {
        _fn.name.function_name = _expr_name;
        add_child(std::move(parent));
        add_child(std::move(selector));
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataAccessorExpr is metadata-only");
    }

private:
    std::string _expr_name;
};

class MetadataBloomPredicateExpr final : public VExpr {
public:
    explicit MetadataBloomPredicateExpr(VExprSPtr probe)
            : VExpr(std::make_shared<DataTypeUInt8>(), false) {
        add_child(std::move(probe));
    }

    const std::string& expr_name() const override { return _expr_name; }
    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("MetadataBloomPredicateExpr is metadata-only");
    }
    bool can_evaluate_bloom_filter() const override { return true; }
    ZoneMapFilterResult evaluate_bloom_filter(const BloomFilterEvalContext&) const override {
        return ZoneMapFilterResult::kMayMatch;
    }

private:
    const std::string _expr_name = "MetadataBloomPredicateExpr";
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
    // The zone holds only 10, so every row differs from 11.
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
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

TEST(ExprZonemapFilterTest, ComparisonDictionarySupportsTypedRangesWhileBloomUsesEquality) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    FunctionComparison<EqualsOp, NameEquals> equals;

    EXPECT_TRUE(equals.can_evaluate_dictionary_filter({slot, make_int_literal(2)}));
    auto dictionary_ctx = make_dictionary_context({int_field(1), int_field(3)}, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals.evaluate_dictionary_filter(dictionary_ctx, {slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals.evaluate_dictionary_filter(dictionary_ctx, {slot, make_int_literal(3)}));

    EXPECT_TRUE(equals.can_evaluate_bloom_filter({slot, make_int_literal(2)}));
    auto bloom_filter = make_int_bloom_filter({1, 3});
    auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals.evaluate_bloom_filter(bloom_ctx, {slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals.evaluate_bloom_filter(bloom_ctx, {slot, make_int_literal(3)}));

    FunctionComparison<NotEqualsOp, NameNotEquals> not_equals;
    FunctionComparison<LessOp, NameLess> less;
    FunctionComparison<LessOrEqualsOp, NameLessOrEquals> less_equal;
    FunctionComparison<GreaterOp, NameGreater> greater;
    FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals> greater_equal;
    EXPECT_TRUE(not_equals.can_evaluate_dictionary_filter({slot, make_int_literal(3)}));
    EXPECT_TRUE(less.can_evaluate_dictionary_filter({slot, make_int_literal(2)}));
    EXPECT_TRUE(less_equal.can_evaluate_dictionary_filter({slot, make_int_literal(1)}));
    EXPECT_TRUE(greater.can_evaluate_dictionary_filter({slot, make_int_literal(2)}));
    EXPECT_TRUE(greater_equal.can_evaluate_dictionary_filter({slot, make_int_literal(3)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              less.evaluate_dictionary_filter(dictionary_ctx, {slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              less.evaluate_dictionary_filter(dictionary_ctx, {slot, make_int_literal(1)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              less.evaluate_dictionary_filter(dictionary_ctx, {make_int_literal(2), slot}));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              less.evaluate_dictionary_filter(dictionary_ctx, {make_int_literal(4), slot}));
    EXPECT_FALSE(not_equals.can_evaluate_bloom_filter({slot, make_int_literal(3)}));

    auto string_type = std::make_shared<DataTypeString>();
    auto string_slot = make_slot(0, string_type);
    auto string_dictionary = make_dictionary_context({Field::create_field<TYPE_STRING>("alpha"),
                                                      Field::create_field<TYPE_STRING>("charlie")},
                                                     string_type);
    EXPECT_TRUE(less.can_evaluate_dictionary_filter({string_slot, make_string_literal("bravo")}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              less.evaluate_dictionary_filter(string_dictionary,
                                              {string_slot, make_string_literal("bravo")}));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              greater.evaluate_dictionary_filter(string_dictionary,
                                                 {string_slot, make_string_literal("delta")}));
}

TEST(ExprZonemapFilterTest, FloatingPointNanBloomProbeIsConservative) {
    auto bloom_filter = std::make_unique<segment_v2::BlockSplitBloomFilter>();
    ASSERT_TRUE(bloom_filter->init(segment_v2::BloomFilter::MINIMUM_BYTES).ok());
    const double finite_value = 1.0;
    bloom_filter->add_bytes(reinterpret_cast<const char*>(&finite_value), sizeof(finite_value));

    FunctionComparison<EqualsOp, NameEquals> equals;
    const auto check_type = [&](const DataTypePtr& type, Field nan_field) {
        auto slot = make_slot(0, type);
        auto literal = std::make_shared<VLiteral>(create_texpr_node_from(
                nan_field, remove_nullable(type)->get_primitive_type(), 0, 0));
        auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);

        EXPECT_FALSE(equals.can_evaluate_bloom_filter({slot, literal}));
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  equals.evaluate_bloom_filter(bloom_ctx, {slot, literal}));
        const auto primitive_type = remove_nullable(type)->get_primitive_type();
        std::shared_ptr<HybridSetBase> nan_values(create_set(primitive_type, false));
        if (primitive_type == TYPE_FLOAT) {
            nan_values->insert(&nan_field.get<TYPE_FLOAT>());
        } else {
            nan_values->insert(&nan_field.get<TYPE_DOUBLE>());
        }
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  expr_zonemap::eval_in_bloom_filter(bloom_ctx, slot, false, *nan_values));

        const Field absent_finite = primitive_type == TYPE_FLOAT
                                            ? Field::create_field<TYPE_FLOAT>(2.0F)
                                            : Field::create_field<TYPE_DOUBLE>(2.0);
        auto finite_literal = std::make_shared<VLiteral>(
                create_texpr_node_from(absent_finite, primitive_type, 0, 0));
        EXPECT_TRUE(equals.can_evaluate_bloom_filter({slot, finite_literal}));
        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  equals.evaluate_bloom_filter(bloom_ctx, {slot, finite_literal}));
    };

    check_type(std::make_shared<DataTypeFloat32>(),
               Field::create_field<TYPE_FLOAT>(std::numeric_limits<float>::quiet_NaN()));
    check_type(std::make_shared<DataTypeFloat64>(),
               Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()));
}

TEST(ExprZonemapFilterTest, FloatingPointInWithNanBloomProbeIsConservative) {
    auto type = std::make_shared<DataTypeFloat64>();
    auto predicate = std::make_shared<VInPredicate>(make_in_predicate_node(false, 2));
    predicate->add_child(make_slot(0, type));
    auto values = make_typed_set_with_min_max<TYPE_DOUBLE>(
            {1.0, std::numeric_limits<double>::quiet_NaN()}, type);
    predicate->_direct_filter_set = values.set;
    predicate->_zonemap_min_max = std::make_shared<HybridSetMinMax>(values.min_max);

    EXPECT_TRUE(predicate->can_evaluate_bloom_filter());
    auto bloom_filter = std::make_unique<segment_v2::BlockSplitBloomFilter>();
    ASSERT_TRUE(bloom_filter->init(segment_v2::BloomFilter::MINIMUM_BYTES).ok());
    EXPECT_EQ(
            ZoneMapFilterResult::kMayMatch,
            predicate->evaluate_bloom_filter(make_bloom_filter_context(bloom_filter.get(), type)));
}

TEST(ExprZonemapFilterTest, FloatingPointNanEqualityIgnoresFiniteOnlyRangeBounds) {
    const auto check_type = []<PrimitiveType Type, typename DataType, typename UInt>(
                                    UInt nan_bits) {
        using T = typename PrimitiveTypeTraits<Type>::CppType;
        auto type = std::make_shared<DataType>();
        auto slot = make_slot(0, type);
        const auto nan_field = Field::create_field<Type>(std::bit_cast<T>(nan_bits));
        auto nan_literal =
                std::make_shared<VLiteral>(create_texpr_node_from(nan_field, Type, 0, 0));

        segment_v2::ZoneMap zone_map;
        zone_map.min_value = Field::create_field<Type>(T {0});
        zone_map.max_value = Field::create_field<Type>(T {0});
        zone_map.has_not_null = true;
        auto ctx = make_context(std::move(zone_map), type);
        ctx.slots.at(0).floating_nan_count_unknown = true;

        FunctionComparison<EqualsOp, NameEquals> equals;
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  equals.evaluate_zonemap_filter(ctx, {slot, nan_literal}));

        const auto zero_field = Field::create_field<Type>(T {0});
        const auto one_field = Field::create_field<Type>(T {1});
        auto zero_literal =
                std::make_shared<VLiteral>(create_texpr_node_from(zero_field, Type, 0, 0));
        auto one_literal =
                std::make_shared<VLiteral>(create_texpr_node_from(one_field, Type, 0, 0));
        FunctionComparison<NotEqualsOp, NameNotEquals> not_equals;
        FunctionComparison<GreaterOp, NameGreater> greater;
        FunctionComparison<GreaterOrEqualsOp, NameGreaterOrEquals> greater_equal;
        FunctionComparison<LessOp, NameLess> less;
        FunctionComparison<LessOrEqualsOp, NameLessOrEquals> less_equal;
        auto in_values =
                make_typed_set_with_min_max<Type>({T {10}, nan_field.template get<Type>()}, type);
        auto not_in_values = make_typed_set_with_min_max<Type>({T {0}}, type);
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  not_equals.evaluate_zonemap_filter(ctx, {slot, zero_literal}));
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  greater.evaluate_zonemap_filter(ctx, {slot, one_literal}));
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  greater_equal.evaluate_zonemap_filter(ctx, {slot, one_literal}));
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  less.evaluate_zonemap_filter(ctx, {one_literal, slot}));
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  less_equal.evaluate_zonemap_filter(ctx, {one_literal, slot}));
        EXPECT_EQ(
                ZoneMapFilterResult::kUnsupported,
                expr_zonemap::eval_in_zonemap(ctx, slot, false, in_values.min_max, *in_values.set));
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  expr_zonemap::eval_in_zonemap(ctx, slot, true, not_in_values.min_max,
                                                *not_in_values.set));

        segment_v2::ZoneMap all_null_zone_map;
        all_null_zone_map.min_value = zero_field;
        all_null_zone_map.max_value = zero_field;
        auto all_null_ctx = make_context(std::move(all_null_zone_map), type);
        all_null_ctx.slots.at(0).floating_nan_count_unknown = true;
        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  expr_zonemap::eval_in_zonemap(all_null_ctx, slot, false, in_values.min_max,
                                                *in_values.set));

        ctx.slots.at(0).floating_nan_count_unknown = false;
        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  equals.evaluate_zonemap_filter(ctx, {slot, nan_literal}));
        EXPECT_EQ(
                ZoneMapFilterResult::kNoMatch,
                expr_zonemap::eval_in_zonemap(ctx, slot, false, in_values.min_max, *in_values.set));
    };

    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(uint32_t {0x7fc00002U});
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(uint64_t {0x7ff8000000000002ULL});
}

TEST(ExprZonemapFilterTest, FloatingPointNanOnlyInHasNoOrderedBounds) {
    const auto check_type = []<PrimitiveType Type, typename DataType>() {
        using T = typename PrimitiveTypeTraits<Type>::CppType;
        auto type = std::make_shared<DataType>();
        auto slot = make_slot(0, type);
        auto values =
                make_typed_set_with_min_max<Type>({std::numeric_limits<T>::quiet_NaN()}, type);
        EXPECT_TRUE(values.min_max.contains_nan);
        EXPECT_TRUE(values.min_max.min_value.is_null());
        EXPECT_TRUE(values.min_max.max_value.is_null());

        segment_v2::ZoneMap zone_map;
        zone_map.min_value = Field::create_field<Type>(T {0});
        zone_map.max_value = Field::create_field<Type>(T {1});
        zone_map.has_not_null = true;
        auto ctx = make_context(std::move(zone_map), type);
        ctx.slots.at(0).floating_nan_count_unknown = false;

        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  expr_zonemap::eval_in_zonemap(ctx, slot, false, values.min_max, *values.set));
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  expr_zonemap::eval_in_zonemap(ctx, slot, true, values.min_max, *values.set));

        auto nan_zone_map = *ctx.slots.at(0).zone_map;
        nan_zone_map.has_nan = true;
        auto nan_ctx = make_context(std::move(nan_zone_map), type);
        EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
                  expr_zonemap::eval_in_zonemap(nan_ctx, slot, false, values.min_max, *values.set));
    };

    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>();
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>();
}

TEST(ExprZonemapFilterTest, DirectInRawFixedKeepsEqualNanPayloadFromLargeSet) {
    const auto check_type = []<PrimitiveType Type, typename DataType, typename UInt>(
                                    UInt stored_bits, UInt probe_bits) {
        using T = typename PrimitiveTypeTraits<Type>::CppType;
        auto type = std::make_shared<DataType>();
        std::shared_ptr<HybridSetBase> filter(create_set(Type, false));
        for (int value = 0; value < FIXED_CONTAINER_MAX_SIZE; ++value) {
            T finite = static_cast<T>(value);
            filter->insert(&finite);
        }
        const T stored_nan = std::bit_cast<T>(stored_bits);
        filter->insert(&stored_nan);
        ASSERT_EQ(FIXED_CONTAINER_MAX_SIZE + 1, filter->size());

        VDirectInPredicate predicate(make_in_predicate_node(false, 1), filter, true);
        predicate.add_child(make_slot(0, type));
        ASSERT_TRUE(predicate.can_execute_on_raw_fixed_values(type, 0));

        const T probe_nan = std::bit_cast<T>(probe_bits);
        uint8_t match = 1;
        ASSERT_TRUE(
                predicate
                        .execute_on_raw_fixed_values(reinterpret_cast<const uint8_t*>(&probe_nan),
                                                     1, sizeof(T), type, 0, &match)
                        .ok());
        EXPECT_EQ(1, match);
    };

    check_type.template operator()<TYPE_FLOAT, DataTypeFloat32>(uint32_t {0x7fc00001U},
                                                                uint32_t {0x7fc00002U});
    check_type.template operator()<TYPE_DOUBLE, DataTypeFloat64>(uint64_t {0x7ff8000000000001ULL},
                                                                 uint64_t {0x7ff8000000000002ULL});
}

TEST(ExprZonemapFilterTest, FloatingPointSignedZeroBloomProbeChecksBothEncodings) {
    FunctionComparison<EqualsOp, NameEquals> equals;
    const auto check_type = [&]<PrimitiveType Type>(
                                    const DataTypePtr& type,
                                    typename PrimitiveTypeTraits<Type>::CppType stored_value,
                                    typename PrimitiveTypeTraits<Type>::CppType predicate_value) {
        auto bloom_filter = std::make_unique<segment_v2::BlockSplitBloomFilter>();
        ASSERT_TRUE(bloom_filter->init(segment_v2::BloomFilter::MINIMUM_BYTES).ok());
        bloom_filter->add_bytes(reinterpret_cast<const char*>(&stored_value), sizeof(stored_value));
        ASSERT_FALSE(bloom_filter->test_bytes(reinterpret_cast<const char*>(&predicate_value),
                                              sizeof(predicate_value)));

        auto slot = make_slot(0, type);
        const auto field = Field::create_field<Type>(predicate_value);
        auto literal = std::make_shared<VLiteral>(create_texpr_node_from(field, Type, 0, 0));
        auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);
        auto values = make_typed_set_with_min_max<Type>({predicate_value}, type);
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  equals.evaluate_bloom_filter(bloom_ctx, {slot, literal}));
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  expr_zonemap::eval_in_bloom_filter(bloom_ctx, slot, false, *values.set));
    };

    const auto float_type = std::make_shared<DataTypeFloat32>();
    check_type.template operator()<TYPE_FLOAT>(float_type, -0.0F, 0.0F);
    check_type.template operator()<TYPE_FLOAT>(float_type, 0.0F, -0.0F);
    const auto double_type = std::make_shared<DataTypeFloat64>();
    check_type.template operator()<TYPE_DOUBLE>(double_type, -0.0, 0.0);
    check_type.template operator()<TYPE_DOUBLE>(double_type, 0.0, -0.0);
}

TEST(ExprZonemapFilterTest, DefaultFunctionForwardsDictionaryAndBloomEvaluation) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto equals = SimpleFunctionFactory::instance().get_function(
            "eq", ColumnsWithTypeAndName {{nullptr, type, "slot"}, {nullptr, type, "literal"}},
            std::make_shared<DataTypeUInt8>());
    ASSERT_NE(equals, nullptr);

    EXPECT_TRUE(equals->can_evaluate_dictionary_filter({slot, make_int_literal(2)}));
    auto dictionary_ctx = make_dictionary_context({int_field(1), int_field(3)}, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals->evaluate_dictionary_filter(dictionary_ctx, {slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals->evaluate_dictionary_filter(dictionary_ctx, {slot, make_int_literal(3)}));

    EXPECT_TRUE(equals->can_evaluate_bloom_filter({slot, make_int_literal(2)}));
    auto bloom_filter = make_int_bloom_filter({1, 3});
    auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals->evaluate_bloom_filter(bloom_ctx, {slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals->evaluate_bloom_filter(bloom_ctx, {slot, make_int_literal(3)}));
}

TEST(ExprZonemapFilterTest, NullSafeEqualityUsesBloomOnlyForNonNullLiteral) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto equals_for_null = SimpleFunctionFactory::instance().get_function(
            "eq_for_null",
            ColumnsWithTypeAndName {{nullptr, type, "slot"}, {nullptr, type, "literal"}},
            std::make_shared<DataTypeUInt8>());
    ASSERT_NE(equals_for_null, nullptr);

    auto bloom_filter = make_int_bloom_filter({1, 3});
    auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);
    EXPECT_TRUE(equals_for_null->can_evaluate_bloom_filter({slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals_for_null->evaluate_bloom_filter(bloom_ctx, {slot, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals_for_null->evaluate_bloom_filter(bloom_ctx, {slot, make_int_literal(3)}));

    EXPECT_FALSE(equals_for_null->can_evaluate_bloom_filter({slot, make_null_int_literal()}));
}

TEST(ExprZonemapFilterTest, EqualityBloomAcceptsStructAndListLeafAccessors) {
    auto leaf_type = int_type();
    auto bloom_filter = make_int_bloom_filter({1, 3});
    auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), leaf_type);
    FunctionComparison<EqualsOp, NameEquals> equals;

    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"value"});
    auto struct_accessor = std::make_shared<MetadataAccessorExpr>(
            "element_at", leaf_type, make_slot(0, struct_type), make_string_literal("value"));
    EXPECT_TRUE(equals.can_evaluate_bloom_filter({struct_accessor, make_int_literal(2)}));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals.evaluate_bloom_filter(bloom_ctx, {struct_accessor, make_int_literal(2)}));

    auto list_type = std::make_shared<DataTypeArray>(leaf_type);
    auto list_accessor = std::make_shared<MetadataAccessorExpr>(
            "element_at", leaf_type, make_slot(0, list_type), make_int_literal(1));
    EXPECT_TRUE(equals.can_evaluate_bloom_filter({list_accessor, make_int_literal(3)}));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              equals.evaluate_bloom_filter(bloom_ctx, {list_accessor, make_int_literal(3)}));

    auto nested_type = std::make_shared<DataTypeStruct>(DataTypes {list_type}, Strings {"items"});
    auto nested_list = std::make_shared<MetadataAccessorExpr>(
            "element_at", list_type, make_slot(0, nested_type), make_string_literal("items"));
    auto nested_leaf = std::make_shared<MetadataAccessorExpr>(
            "element_at", leaf_type, std::move(nested_list), make_int_literal(1));
    auto nested_probe = expr_zonemap::extract_bloom_filter_probe(nested_leaf);
    ASSERT_TRUE(nested_probe.has_value());
    ASSERT_EQ(nested_probe->path.size(), 2);
    EXPECT_EQ(nested_probe->path[0].kind, expr_zonemap::BloomFilterPathKind::STRUCT_FIELD);
    EXPECT_EQ(nested_probe->path[1].kind, expr_zonemap::BloomFilterPathKind::LIST_ELEMENT);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              equals.evaluate_bloom_filter(bloom_ctx, {nested_leaf, make_int_literal(2)}));
}

TEST(ExprZonemapFilterTest, CompoundBloomProbeRequiresOneUniqueNestedLeaf) {
    const auto make_accessor = [](const DataTypePtr& struct_type, const DataTypePtr& leaf_type,
                                  std::string field_name) {
        return std::make_shared<MetadataAccessorExpr>("element_at", leaf_type,
                                                      make_slot(0, struct_type),
                                                      make_string_literal(std::move(field_name)));
    };
    const auto compound_probe = [](const VExprSPtr& first, const VExprSPtr& second,
                                   const VExprSPtr& outer) {
        auto inner =
                std::make_shared<VCompoundPred>(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
        inner->add_child(std::make_shared<MetadataBloomPredicateExpr>(first));
        inner->add_child(std::make_shared<MetadataBloomPredicateExpr>(second));
        auto root =
                std::make_shared<VCompoundPred>(make_compound_node(TExprOpcode::COMPOUND_OR, 2));
        root->add_child(std::move(inner));
        root->add_child(std::make_shared<MetadataBloomPredicateExpr>(outer));
        EXPECT_TRUE(root->can_evaluate_bloom_filter());
        return expr_zonemap::extract_bloom_filter_predicate_probe(root);
    };

    auto int_leaf = int_type();
    auto same_type_struct =
            std::make_shared<DataTypeStruct>(DataTypes {int_leaf, int_leaf}, Strings {"a", "b"});
    EXPECT_FALSE(compound_probe(make_accessor(same_type_struct, int_leaf, "a"),
                                make_accessor(same_type_struct, int_leaf, "b"),
                                make_accessor(same_type_struct, int_leaf, "a"))
                         .has_value());

    auto string_leaf = std::make_shared<DataTypeString>();
    auto mixed_type_struct =
            std::make_shared<DataTypeStruct>(DataTypes {int_leaf, string_leaf}, Strings {"a", "b"});
    EXPECT_FALSE(compound_probe(make_accessor(mixed_type_struct, int_leaf, "a"),
                                make_accessor(mixed_type_struct, string_leaf, "b"),
                                make_accessor(mixed_type_struct, int_leaf, "a"))
                         .has_value());

    auto same_leaf_probe = compound_probe(make_accessor(same_type_struct, int_leaf, "a"),
                                          make_accessor(same_type_struct, int_leaf, "a"),
                                          make_accessor(same_type_struct, int_leaf, "a"));
    ASSERT_TRUE(same_leaf_probe.has_value());
    ASSERT_EQ(same_leaf_probe->path.size(), 1);
    EXPECT_EQ(same_leaf_probe->path[0].field_name, "a");
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

    auto values = make_int_set_with_min_max({10});
    ZoneMapEvalContext in_ctx;
    EXPECT_EQ(ZoneMapFilterResult::kUnsupported,
              expr_zonemap::eval_in_zonemap(in_ctx, slot, false, values.min_max, *values.set));
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
            // Every row is NULL, so IS NULL holds for all of them and IS NOT NULL for none.
            {true, false, ZoneMapFilterResult::kAllMatch, ZoneMapFilterResult::kNoMatch},
            // Mirror of the row above.
            {false, true, ZoneMapFilterResult::kNoMatch, ZoneMapFilterResult::kAllMatch},
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
    auto values = make_int_set_with_min_max({10});

    segment_v2::ZoneMap empty_zone;
    auto empty_ctx = make_context(empty_zone, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(empty_ctx, slot, false, values.min_max, *values.set));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(empty_ctx, slot, true, values.min_max, *values.set));

    segment_v2::ZoneMap only_null_zone;
    only_null_zone.has_null = true;
    auto only_null_ctx = make_context(only_null_zone, type);
    EXPECT_EQ(
            ZoneMapFilterResult::kNoMatch,
            expr_zonemap::eval_in_zonemap(only_null_ctx, slot, false, values.min_max, *values.set));
    EXPECT_EQ(
            ZoneMapFilterResult::kNoMatch,
            expr_zonemap::eval_in_zonemap(only_null_ctx, slot, true, values.min_max, *values.set));
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
    // 0xff has no next prefix, so the range is [0xff, +inf) and the whole zone sits inside it.
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
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

    auto values = make_string_set_with_min_max({"gamma"}, char_type);
    auto in_ctx = make_context(zone_map, char_type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(in_ctx, slot, false, values.min_max, *values.set));
}

TEST(ExprZonemapFilterTest, InZonemapUsesConfiguredPointCheckThreshold) {
    const int32_t old_threshold = config::in_zonemap_point_check_threshold;
    Defer restore_threshold {
            [old_threshold]() { config::in_zonemap_point_check_threshold = old_threshold; }};
    config::in_zonemap_point_check_threshold = 64;

    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    std::vector<int32_t> values;
    for (int value = 1; value <= 65; ++value) {
        values.emplace_back(value);
    }
    auto values_with_min_max = make_int_set_with_min_max(values);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, values_with_min_max.min_max,
                                            *values_with_min_max.set));
    EXPECT_EQ(65, values_with_min_max.set->size());
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_dictionary(make_dictionary_context({int_field(65)}, type), slot,
                                               false, *values_with_min_max.set));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_dictionary(make_dictionary_context({int_field(100)}, type),
                                               slot, false, *values_with_min_max.set));

    auto singleton_ctx = make_context(make_int_zonemap(10, 10), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(singleton_ctx, slot, true, values_with_min_max.min_max,
                                            *values_with_min_max.set));

    EXPECT_EQ(0, ctx.stats.in_zonemap_point_check_count);
    EXPECT_EQ(1, ctx.stats.in_zonemap_range_only_count);

    config::in_zonemap_point_check_threshold = 65;
    auto point_ctx = make_context(make_int_zonemap(10, 20), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_zonemap(point_ctx, slot, false, values_with_min_max.min_max,
                                            *values_with_min_max.set));
    EXPECT_EQ(1, point_ctx.stats.in_zonemap_point_check_count);
    EXPECT_EQ(0, point_ctx.stats.in_zonemap_range_only_count);
}

TEST(ExprZonemapFilterTest, InZonemapUsesPointChecksUnderThreshold) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    auto values = make_int_set_with_min_max({1, 30});
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, values.min_max, *values.set));
    EXPECT_EQ(1, ctx.stats.in_zonemap_point_check_count);
}

TEST(ExprZonemapFilterTest, InZonemapUsesRangeOnlyForDenseBitSetContainer) {
    auto type = std::make_shared<DataTypeInt8>();
    auto slot = make_slot(0, type);
    segment_v2::ZoneMap zone_map;
    zone_map.min_value = Field::create_field<TYPE_TINYINT>(int8_t {-10});
    zone_map.max_value = Field::create_field<TYPE_TINYINT>(int8_t {10});
    zone_map.has_not_null = true;
    auto ctx = make_context(std::move(zone_map), type);

    auto values = make_typed_set_with_min_max<TYPE_TINYINT>({int8_t {-100}, int8_t {100}}, type);
    EXPECT_FALSE(values.set->supports_fast_range_lookup());
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, values.min_max, *values.set));
    EXPECT_EQ(1, ctx.stats.in_zonemap_point_check_count);
    EXPECT_EQ(0, ctx.stats.in_zonemap_range_only_count);
}

TEST(ExprZonemapFilterTest, InZonemapHandlesEmptyListAndNotInSingleValueRange) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    auto ctx = make_context(make_int_zonemap(10, 20), type);

    auto empty_values = make_int_set_with_min_max({});
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, false, empty_values.min_max,
                                            *empty_values.set));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_zonemap(ctx, slot, true, empty_values.min_max,
                                            *empty_values.set));

    auto single_value_ctx = make_context(make_int_zonemap(10, 10), type);
    auto values = make_int_set_with_min_max({10});
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_zonemap(single_value_ctx, slot, true, values.min_max,
                                            *values.set));

    // The zone holds only 10 and the list only 11, so every row satisfies NOT IN.
    auto other_values = make_int_set_with_min_max({11});
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch,
              expr_zonemap::eval_in_zonemap(single_value_ctx, slot, true, other_values.min_max,
                                            *other_values.set));
}

// GTest assertion macros dominate the reported cognitive complexity of this linear scenario.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(ExprZonemapFilterTest, GetMinMaxHandlesEmptyNullOnlyAndSignedBitSetBounds) {
    auto empty = make_int_set_with_min_max({});
    EXPECT_EQ(0, empty.set->size());
    EXPECT_FALSE(empty.set->contain_null());
    EXPECT_TRUE(empty.min_max.min_value.is_null());
    EXPECT_TRUE(empty.min_max.max_value.is_null());

    auto null_only = make_int_set_with_min_max({}, true, true);
    EXPECT_EQ(0, null_only.set->size());
    EXPECT_TRUE(null_only.set->contain_null());
    EXPECT_TRUE(null_only.min_max.min_value.is_null());
    EXPECT_TRUE(null_only.min_max.max_value.is_null());

    const std::vector<int8_t> values {127, -128, 0, -1};
    auto tinyint =
            make_typed_set_with_min_max<TYPE_TINYINT>(values, std::make_shared<DataTypeInt8>());
    EXPECT_EQ(values.size(), tinyint.set->size());
    EXPECT_EQ(Field::create_field<TYPE_TINYINT>(-128), tinyint.min_max.min_value);
    EXPECT_EQ(Field::create_field<TYPE_TINYINT>(127), tinyint.min_max.max_value);

    std::shared_ptr<HybridSetBase> smallint_set(create_set(TYPE_SMALLINT, false));
    const int16_t smallint_min = std::numeric_limits<int16_t>::min();
    const int16_t smallint_max = std::numeric_limits<int16_t>::max();
    smallint_set->insert(&smallint_min);
    smallint_set->insert(&smallint_max);
    for (int16_t value = -31; value <= 31; ++value) {
        smallint_set->insert(&value);
    }
    HybridSetMinMax smallint_min_max;
    expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(
            smallint_set, std::make_shared<DataTypeInt16>(), smallint_min_max);
    EXPECT_EQ(65, smallint_set->size());
    EXPECT_EQ(Field::create_field<TYPE_SMALLINT>(smallint_min), smallint_min_max.min_value);
    EXPECT_EQ(Field::create_field<TYPE_SMALLINT>(smallint_max), smallint_min_max.max_value);

    smallint_set->clear();
    for (int16_t value = 100; value <= 164; ++value) {
        smallint_set->insert(&value);
    }
    expr_zonemap::get_hybrid_set_min_max_for_zonemap_filter(
            smallint_set, std::make_shared<DataTypeInt16>(), smallint_min_max);
    EXPECT_EQ(65, smallint_set->size());
    EXPECT_EQ(Field::create_field<TYPE_SMALLINT>(100), smallint_min_max.min_value);
    EXPECT_EQ(Field::create_field<TYPE_SMALLINT>(164), smallint_min_max.max_value);
}

TEST(ExprZonemapFilterTest, StringSetHeterogeneousLookupPreservesBinaryValues) {
    StringSet<> set(false);
    const std::string empty;
    const std::string binary("a\0b", 3);
    StringRef empty_ref(empty);
    StringRef binary_ref(binary);
    set.insert(&empty_ref);
    set.insert(&binary_ref);

    EXPECT_TRUE(set.find(&empty_ref));
    EXPECT_TRUE(set.find(empty.data(), empty.size()));
    EXPECT_TRUE(set.find(Field::create_field<TYPE_STRING>(empty)));
    EXPECT_TRUE(set.find(&binary_ref));
    EXPECT_TRUE(set.find(binary.data(), binary.size()));
    EXPECT_TRUE(set.find(Field::create_field<TYPE_STRING>(binary)));

    const std::string prefix("a\0", 2);
    EXPECT_FALSE(set.find(prefix.data(), prefix.size()));
}

TEST(ExprZonemapFilterTest, GetMinMaxPreservesDecimalAndDatetimeV2Values) {
    const Decimal64 decimal_low(-1234);
    const Decimal64 decimal_high(5678);
    auto decimals = make_typed_set_with_min_max<TYPE_DECIMAL64>(
            {decimal_high, decimal_low}, std::make_shared<DataTypeDecimal64>(18, 2));
    EXPECT_EQ(Field::create_field<TYPE_DECIMAL64>(decimal_low), decimals.min_max.min_value);
    EXPECT_EQ(Field::create_field<TYPE_DECIMAL64>(decimal_high), decimals.min_max.max_value);
    EXPECT_TRUE(
            decimals.set->contains_any_in_range(Field::create_field<TYPE_DECIMAL64>(decimal_low),
                                                Field::create_field<TYPE_DECIMAL64>(decimal_low)));
    EXPECT_FALSE(decimals.set->contains_any_in_range(
            Field::create_field<TYPE_DECIMAL64>(Decimal64(-1000)),
            Field::create_field<TYPE_DECIMAL64>(Decimal64(5000))));

    DateV2Value<DateTimeV2ValueType> datetime_low;
    datetime_low.unchecked_set_time(2024, 1, 2, 3, 4, 5, 123456);
    DateV2Value<DateTimeV2ValueType> datetime_high;
    datetime_high.unchecked_set_time(2025, 6, 7, 8, 9, 10, 654321);
    auto datetimes = make_typed_set_with_min_max<TYPE_DATETIMEV2>(
            {datetime_high, datetime_low}, std::make_shared<DataTypeDateTimeV2>(6));
    EXPECT_EQ(Field::create_field<TYPE_DATETIMEV2>(datetime_low), datetimes.min_max.min_value);
    EXPECT_EQ(Field::create_field<TYPE_DATETIMEV2>(datetime_high), datetimes.min_max.max_value);
    EXPECT_TRUE(datetimes.set->contains_any_in_range(
            Field::create_field<TYPE_DATETIMEV2>(datetime_high),
            Field::create_field<TYPE_DATETIMEV2>(datetime_high)));
    DateV2Value<DateTimeV2ValueType> datetime_hole;
    datetime_hole.unchecked_set_time(2024, 6, 7, 8, 9, 10, 654321);
    EXPECT_FALSE(datetimes.set->contains_any_in_range(
            Field::create_field<TYPE_DATETIMEV2>(datetime_hole),
            Field::create_field<TYPE_DATETIMEV2>(datetime_hole)));
}

TEST(ExprZonemapFilterTest, InBloomFilterHandlesEmptyAndNullOnlySets) {
    auto type = int_type();
    auto bloom_filter = make_int_bloom_filter({7});
    auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);

    std::shared_ptr<HybridSetBase> empty_values(create_set(TYPE_INT, false));
    ASSERT_EQ(0, empty_values->size());
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_bloom_filter(bloom_ctx, make_slot(0, type), false,
                                                 *empty_values));

    std::shared_ptr<HybridSetBase> null_only_values(create_set(TYPE_INT, true));
    null_only_values->insert(static_cast<const void*>(nullptr));
    ASSERT_EQ(0, null_only_values->size());
    ASSERT_TRUE(null_only_values->contain_null());
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_bloom_filter(bloom_ctx, make_slot(0, type), false,
                                                 *null_only_values));
}

TEST(ExprZonemapFilterTest, InBloomFilterProbesInteriorNativeValues) {
    auto type = int_type();
    auto values = make_int_set_with_min_max({2, 4, 6});

    auto missing_bloom_filter = make_int_bloom_filter({1, 3, 5});
    auto missing_bloom_ctx = make_bloom_filter_context(missing_bloom_filter.get(), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              expr_zonemap::eval_in_bloom_filter(missing_bloom_ctx, make_slot(0, type), false,
                                                 *values.set));

    // 4 is neither the IN-set minimum nor maximum, so this requires probing native set values.
    auto matching_bloom_filter = make_int_bloom_filter({4});
    auto matching_bloom_ctx = make_bloom_filter_context(matching_bloom_filter.get(), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              expr_zonemap::eval_in_bloom_filter(matching_bloom_ctx, make_slot(0, type), false,
                                                 *values.set));
}

TEST(ExprZonemapFilterTest, InBloomFilterPreservesEmptyAndEmbeddedNullStrings) {
    auto type = std::make_shared<DataTypeString>();
    const std::string empty;
    const std::string binary("a\0b", 3);
    for (const bool borrowed_values : {false, true}) {
        SCOPED_TRACE(borrowed_values ? "StringValueSet" : "StringSet");
        std::shared_ptr<HybridSetBase> values(borrowed_values ? create_string_value_set(false)
                                                              : create_set(TYPE_STRING, false));
        StringRef empty_ref(empty);
        StringRef binary_ref(binary);
        values->insert(&empty_ref);
        values->insert(&binary_ref);

        auto missing_bloom_filter = make_string_bloom_filter({});
        auto missing_bloom_ctx = make_bloom_filter_context(missing_bloom_filter.get(), type);
        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  expr_zonemap::eval_in_bloom_filter(missing_bloom_ctx, make_slot(0, type), false,
                                                     *values));

        auto empty_bloom_filter = make_string_bloom_filter({empty});
        auto empty_bloom_ctx = make_bloom_filter_context(empty_bloom_filter.get(), type);
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  expr_zonemap::eval_in_bloom_filter(empty_bloom_ctx, make_slot(0, type), false,
                                                     *values));

        auto binary_bloom_filter = make_string_bloom_filter({binary});
        auto binary_bloom_ctx = make_bloom_filter_context(binary_bloom_filter.get(), type);
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  expr_zonemap::eval_in_bloom_filter(binary_bloom_ctx, make_slot(0, type), false,
                                                     *values));
    }
}

TEST(ExprZonemapFilterTest, InBloomFilterKeepsUnsupportedTypeConservative) {
    auto type = std::make_shared<DataTypeInt8>();
    auto values = make_typed_set_with_min_max<TYPE_TINYINT>({int8_t {7}}, type);
    auto bloom_filter = make_int_bloom_filter({});
    auto bloom_ctx = make_bloom_filter_context(bloom_filter.get(), type);
    EXPECT_EQ(
            ZoneMapFilterResult::kMayMatch,
            expr_zonemap::eval_in_bloom_filter(bloom_ctx, make_slot(0, type), false, *values.set));
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

TEST(ExprZonemapFilterTest, VInPredicatePreparesZonemapMinMax) {
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
    ASSERT_NE(nullptr, in_predicate->_zonemap_min_max);
    EXPECT_EQ(int_field(1), in_predicate->_zonemap_min_max->min_value);
    EXPECT_EQ(int_field(30), in_predicate->_zonemap_min_max->max_value);

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
    ASSERT_NE(nullptr, not_in_with_null->_direct_filter_set);
    EXPECT_TRUE(not_in_with_null->_direct_filter_set->contain_null());
}

TEST(ExprZonemapFilterTest, VInPredicateDictionaryAndBloomProbePreparedSet) {
    auto type = int_type();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    auto thrift_desc_tbl = make_k2_scan_desc_tbl();
    ASSERT_TRUE(DescriptorTbl::create(&obj_pool, thrift_desc_tbl, &desc_tbl).ok());

    RuntimeState runtime_state;
    runtime_state.set_desc_tbl(desc_tbl);
    RowDescriptor row_desc(runtime_state.desc_tbl(), {0});

    auto in_predicate = std::make_shared<VInPredicate>(make_in_predicate_node(false, 4));
    auto in_slot = make_slot(0, type);
    std::static_pointer_cast<VSlotRef>(in_slot)->set_slot_id(0);
    in_predicate->add_child(in_slot);
    in_predicate->add_child(make_int_literal(2));
    in_predicate->add_child(make_int_literal(4));
    in_predicate->add_child(make_int_literal(6));
    VExprContext in_context(in_predicate);
    ASSERT_TRUE(in_context.prepare(&runtime_state, row_desc).ok());
    ASSERT_TRUE(in_context.open(&runtime_state).ok());

    EXPECT_TRUE(in_predicate->can_evaluate_dictionary_filter());
    auto missing_dictionary_ctx = make_dictionary_context({int_field(1), int_field(3)}, type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              in_predicate->evaluate_dictionary_filter(missing_dictionary_ctx));
    auto matching_dictionary_ctx = make_dictionary_context({int_field(4), int_field(5)}, type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              in_predicate->evaluate_dictionary_filter(matching_dictionary_ctx));

    EXPECT_TRUE(in_predicate->can_evaluate_bloom_filter());
    auto missing_bloom_filter = make_int_bloom_filter({1, 3});
    auto missing_bloom_ctx = make_bloom_filter_context(missing_bloom_filter.get(), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              in_predicate->evaluate_bloom_filter(missing_bloom_ctx));
    // 4 is neither the IN-set minimum nor maximum. Bloom pruning must probe the full native set.
    auto matching_bloom_filter = make_int_bloom_filter({4});
    auto matching_bloom_ctx = make_bloom_filter_context(matching_bloom_filter.get(), type);
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              in_predicate->evaluate_bloom_filter(matching_bloom_ctx));
}

TEST(ExprZonemapFilterTest, VInPredicatePreparesNestedBloomValuesDuringOpen) {
    auto leaf_type = int_type();
    auto struct_type = std::make_shared<DataTypeStruct>(DataTypes {leaf_type}, Strings {"value"});
    auto slot = VSlotRef::create_shared(0, 0, -1, struct_type, "root");
    auto accessor = std::make_shared<MetadataAccessorExpr>("element_at", leaf_type, std::move(slot),
                                                           make_string_literal("value"));
    auto in_predicate = std::make_shared<VInPredicate>(make_in_predicate_node(false, 3));
    in_predicate->add_child(std::move(accessor));
    in_predicate->add_child(make_int_literal(2));
    in_predicate->add_child(make_int_literal(4));

    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    auto thrift_desc_tbl = make_k2_scan_desc_tbl();
    ASSERT_TRUE(DescriptorTbl::create(&obj_pool, thrift_desc_tbl, &desc_tbl).ok());
    RuntimeState runtime_state;
    runtime_state.set_desc_tbl(desc_tbl);
    RowDescriptor row_desc(runtime_state.desc_tbl(), {0});
    VExprContext in_context(in_predicate);
    ASSERT_TRUE(in_context.prepare(&runtime_state, row_desc).ok());
    ASSERT_TRUE(in_context.open(&runtime_state).ok());

    EXPECT_NE(nullptr, in_predicate->_zonemap_min_max);
    EXPECT_NE(nullptr, in_predicate->_direct_filter_set);
    EXPECT_TRUE(in_predicate->can_evaluate_bloom_filter());
    EXPECT_FALSE(in_predicate->can_evaluate_zonemap_filter());
    EXPECT_FALSE(in_predicate->can_evaluate_dictionary_filter());
    EXPECT_FALSE(in_predicate->can_execute_on_raw_fixed_values(leaf_type, 0));

    auto missing_bloom_filter = make_int_bloom_filter({1, 3});
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              in_predicate->evaluate_bloom_filter(
                      make_bloom_filter_context(missing_bloom_filter.get(), leaf_type)));
    auto matching_bloom_filter = make_int_bloom_filter({4});
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              in_predicate->evaluate_bloom_filter(
                      make_bloom_filter_context(matching_bloom_filter.get(), leaf_type)));
}

// GoogleTest assertions inflate this linear ownership-lifetime test's complexity metric.
TEST(ExprZonemapFilterTest, // NOLINT(readability-function-cognitive-complexity)
     VInPredicatePreparesOwningStringZonemapMinMax) {
    std::shared_ptr<const HybridSetMinMax> snapshot;
    std::weak_ptr<HybridSetBase> borrowed_set;
    {
        auto type = std::make_shared<DataTypeString>();
        ObjectPool obj_pool;
        DescriptorTbl* desc_tbl = nullptr;
        auto thrift_desc_tbl = make_k2_scan_desc_tbl(TYPE_STRING);
        ASSERT_TRUE(DescriptorTbl::create(&obj_pool, thrift_desc_tbl, &desc_tbl).ok());

        RuntimeState runtime_state;
        runtime_state.set_desc_tbl(desc_tbl);
        RowDescriptor row_desc(runtime_state.desc_tbl(), {0});

        auto in_predicate = std::make_shared<VInPredicate>(make_in_predicate_node(false, 5));
        auto in_slot = make_slot(0, type);
        std::static_pointer_cast<VSlotRef>(in_slot)->set_slot_id(0);
        in_predicate->add_child(in_slot);
        in_predicate->add_child(make_string_literal("zzz"));
        in_predicate->add_child(make_string_literal("aaa"));
        in_predicate->add_child(make_string_literal("aaa"));
        in_predicate->add_child(make_null_string_literal());
        VExprContext in_context(in_predicate);
        ASSERT_TRUE(in_context.prepare(&runtime_state, row_desc).ok());
        ASSERT_TRUE(in_context.open(&runtime_state).ok());

        ASSERT_NE(nullptr, in_predicate->_zonemap_min_max);
        ASSERT_NE(nullptr, dynamic_cast<StringValueSet<>*>(in_predicate->_direct_filter_set.get()));
        borrowed_set = in_predicate->_direct_filter_set;
        EXPECT_EQ(2, in_predicate->_direct_filter_set->size());
        EXPECT_TRUE(in_predicate->_direct_filter_set->contain_null());
        EXPECT_EQ(Field::create_field<TYPE_STRING>("aaa"),
                  in_predicate->_zonemap_min_max->min_value);
        EXPECT_EQ(Field::create_field<TYPE_STRING>("zzz"),
                  in_predicate->_zonemap_min_max->max_value);

        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  in_predicate->evaluate_dictionary_filter(make_dictionary_context(
                          {Field::create_field<TYPE_STRING>("aaa")}, type)));
        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  in_predicate->evaluate_dictionary_filter(make_dictionary_context(
                          {Field::create_field<TYPE_STRING>("mmm")}, type)));

        auto missing_bloom_filter = make_string_bloom_filter({});
        auto missing_bloom_ctx = make_bloom_filter_context(missing_bloom_filter.get(), type);
        EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
                  in_predicate->evaluate_bloom_filter(missing_bloom_ctx));
        auto matching_bloom_filter = make_string_bloom_filter({"aaa"});
        auto matching_bloom_ctx = make_bloom_filter_context(matching_bloom_filter.get(), type);
        EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
                  in_predicate->evaluate_bloom_filter(matching_bloom_ctx));

        snapshot = in_predicate->_zonemap_min_max;
    }

    ASSERT_NE(nullptr, snapshot);
    EXPECT_TRUE(borrowed_set.expired());
    EXPECT_EQ(Field::create_field<TYPE_STRING>("aaa"), snapshot->min_value);
    EXPECT_EQ(Field::create_field<TYPE_STRING>("zzz"), snapshot->max_value);
}

TEST(ExprZonemapFilterTest, DirectInPredicatePreparesStringMinMaxForZonemap) {
    auto type = std::make_shared<DataTypeString>();
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_STRING, false));
    StringRef aaa("aaa");
    filter->insert(&aaa);
    StringRef zzz("zzz");
    filter->insert(&zzz);

    auto slot = make_slot(0, type);
    VDirectInPredicate direct_in_expr(make_in_predicate_node(false, 2), filter, true);
    direct_in_expr.add_child(slot);
    direct_in_expr._prepare_zonemap_min_max();

    ASSERT_NE(nullptr, direct_in_expr._zonemap_min_max);
    EXPECT_EQ(Field::create_field<TYPE_STRING>("aaa"), direct_in_expr._zonemap_min_max->min_value);
    EXPECT_EQ(Field::create_field<TYPE_STRING>("zzz"), direct_in_expr._zonemap_min_max->max_value);
}

TEST(ExprZonemapFilterTest, DirectInPredicatePreparesZonemapMinMax) {
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

    EXPECT_TRUE(direct_in_expr->can_evaluate_zonemap_filter());
    ASSERT_NE(nullptr, direct_in_expr->_zonemap_min_max);
    EXPECT_EQ(int_field(1), direct_in_expr->_zonemap_min_max->min_value);
    EXPECT_EQ(int_field(30), direct_in_expr->_zonemap_min_max->max_value);
}

TEST(ExprZonemapFilterTest, DirectInDeepCloneAfterMinMaxPreparationReusesSnapshot) {
    auto type = int_type();
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, false));
    int32_t low_value = 1;
    int32_t high_value = 30;
    filter->insert(&low_value);
    filter->insert(&high_value);

    auto direct_in_expr =
            std::make_shared<VDirectInPredicate>(make_in_predicate_node(false, 1), filter, true);
    direct_in_expr->add_child(make_slot(0, type));
    direct_in_expr->_prepare_zonemap_min_max();

    VExprSPtr cloned_expr;
    ASSERT_TRUE(direct_in_expr->deep_clone(&cloned_expr).ok());
    auto cloned_direct_in = std::dynamic_pointer_cast<VDirectInPredicate>(cloned_expr);
    ASSERT_NE(cloned_direct_in, nullptr);
    EXPECT_EQ(direct_in_expr->_zonemap_min_max.get(), cloned_direct_in->_zonemap_min_max.get());
    EXPECT_TRUE(cloned_direct_in->can_evaluate_zonemap_filter());
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, cloned_direct_in->evaluate_zonemap_filter(
                                                     make_context(make_int_zonemap(10, 20), type)));
}

TEST(ExprZonemapFilterTest, DirectInDeepCloneBeforeMinMaxPreparationBuildsIndependentSnapshots) {
    auto type = int_type();
    std::shared_ptr<HybridSetBase> filter(create_set(TYPE_INT, false));
    int32_t low_value = 1;
    int32_t high_value = 30;
    filter->insert(&low_value);
    filter->insert(&high_value);

    auto direct_in_expr =
            std::make_shared<VDirectInPredicate>(make_in_predicate_node(false, 1), filter, true);
    direct_in_expr->add_child(make_slot(0, type));

    VExprSPtr cloned_expr;
    ASSERT_TRUE(direct_in_expr->deep_clone(&cloned_expr).ok());
    auto cloned_direct_in = std::dynamic_pointer_cast<VDirectInPredicate>(cloned_expr);
    ASSERT_NE(nullptr, cloned_direct_in);
    EXPECT_EQ(nullptr, direct_in_expr->_zonemap_min_max);
    EXPECT_EQ(nullptr, cloned_direct_in->_zonemap_min_max);

    direct_in_expr->_prepare_zonemap_min_max();
    cloned_direct_in->_prepare_zonemap_min_max();
    ASSERT_NE(nullptr, direct_in_expr->_zonemap_min_max);
    ASSERT_NE(nullptr, cloned_direct_in->_zonemap_min_max);
    EXPECT_NE(direct_in_expr->_zonemap_min_max.get(), cloned_direct_in->_zonemap_min_max.get());
    EXPECT_EQ(direct_in_expr->_zonemap_min_max->min_value,
              cloned_direct_in->_zonemap_min_max->min_value);
    EXPECT_EQ(direct_in_expr->_zonemap_min_max->max_value,
              cloned_direct_in->_zonemap_min_max->max_value);

    auto ctx = make_context(make_int_zonemap(10, 20), type);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch, cloned_direct_in->evaluate_zonemap_filter(ctx));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              cloned_direct_in->evaluate_dictionary_filter(
                      make_dictionary_context({int_field(30)}, type)));
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

TEST(ExprZonemapFilterTest, DirectInPredicateRewritePreservesEmbeddedNullString) {
    auto type = std::make_shared<DataTypeString>();
    auto slot = make_slot(0, type);
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_STRING, false));
    const std::string binary_value("a\0b", 3);
    StringRef value(binary_value);
    filter->insert(&value);

    VDirectInPredicate direct_in_expr(make_in_predicate_node(false, 1), filter, true);
    direct_in_expr.add_child(slot);

    VExprSPtr in_expr;
    ASSERT_TRUE(direct_in_expr.get_slot_in_expr(in_expr));
    ASSERT_EQ(2, in_expr->get_num_children());
    auto literal = std::dynamic_pointer_cast<VLiteral>(in_expr->get_child(1));
    ASSERT_NE(nullptr, literal);
    Field materialized_value;
    literal->get_column_ptr()->get(0, materialized_value);
    EXPECT_EQ(binary_value, std::string(materialized_value.as_string_view()));
}

TEST(ExprZonemapFilterTest, DirectInPredicateSkipsMinMaxWhenSetTypeDiffersFromChild) {
    auto string_type = std::make_shared<DataTypeString>();
    auto slot = make_slot(0, string_type);
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, false));
    int dict_code = 1;
    filter->insert(&dict_code);

    VDirectInPredicate direct_in_expr(make_in_predicate_node(false, 1), filter, false);
    direct_in_expr.add_child(slot);

    direct_in_expr._prepare_zonemap_min_max();
    EXPECT_EQ(nullptr, direct_in_expr._zonemap_min_max);
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
    direct_in_expr->_prepare_zonemap_min_max();

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

TEST(ExprZonemapFilterTest, RuntimeFilterExprDelegatesDirectInDictionaryAndRawEvaluation) {
    auto type = int_type();
    auto slot = make_slot(0, type);
    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, false));
    int32_t two = 2;
    int32_t four = 4;
    filter->insert(&two);
    filter->insert(&four);

    auto direct_in_expr =
            std::make_shared<VDirectInPredicate>(make_in_predicate_node(false, 1), filter, true);
    direct_in_expr->add_child(slot);
    direct_in_expr->_prepare_zonemap_min_max();

    auto runtime_filter = RuntimeFilterExpr::create_shared(make_in_predicate_node(false, 1),
                                                           direct_in_expr, 0.0, false, 7);
    EXPECT_TRUE(runtime_filter->can_evaluate_dictionary_filter());
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              runtime_filter->evaluate_dictionary_filter(
                      make_dictionary_context({int_field(1), int_field(3)}, type)));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              runtime_filter->evaluate_dictionary_filter(
                      make_dictionary_context({int_field(3), int_field(4)}, type)));

    EXPECT_TRUE(runtime_filter->can_execute_on_raw_fixed_values(type, 0));
    const std::array<int32_t, 4> values {1, 2, 3, 4};
    std::array<uint8_t, 4> matches {1, 1, 1, 1};
    ASSERT_TRUE(runtime_filter
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), type, 0, matches.data())
                        .ok());
    EXPECT_EQ((std::array<uint8_t, 4> {0, 1, 0, 1}), matches);

    auto null_aware_runtime_filter = RuntimeFilterExpr::create_shared(
            make_in_predicate_node(false, 1), direct_in_expr, 0.0, true, 8);
    EXPECT_FALSE(null_aware_runtime_filter->can_evaluate_dictionary_filter());
    EXPECT_FALSE(null_aware_runtime_filter->can_execute_on_raw_fixed_values(type, 0));
}

TEST(ExprZonemapFilterTest, RuntimeFilterExprDelegatesBloomDictionaryAndRawEvaluation) {
    auto type = int_type();
    std::shared_ptr<BloomFilterFuncBase> filter(create_bloom_filter(TYPE_INT, false));
    RuntimeFilterParams params;
    params.filter_type = RuntimeFilterType::BLOOM_FILTER;
    params.column_return_type = TYPE_INT;
    params.bloom_filter_size = 1024;
    filter->init_params(&params);
    ASSERT_TRUE(filter->init_with_fixed_length(1024).ok());
    auto build_values = ColumnInt32::create();
    build_values->insert_value(2);
    build_values->insert_value(4);
    filter->insert_fixed_len(std::move(build_values), 0);

    auto node = make_in_predicate_node(false, 1);
    node.__set_node_type(TExprNodeType::BLOOM_PRED);
    node.__set_opcode(TExprOpcode::RT_FILTER);
    auto bloom = VBloomPredicate::create_shared(node);
    bloom->set_filter(filter);
    bloom->add_child(make_slot(0, type));
    auto runtime_filter = RuntimeFilterExpr::create_shared(node, bloom, 0.0, false, 9);

    EXPECT_TRUE(runtime_filter->can_evaluate_dictionary_filter());
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              runtime_filter->evaluate_dictionary_filter(
                      make_dictionary_context({int_field(2)}, type)));

    int32_t missing = 1;
    while (missing < 10000 && filter->test_field(int_field(missing))) {
        ++missing;
    }
    ASSERT_LT(missing, 10000);
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              runtime_filter->evaluate_dictionary_filter(
                      make_dictionary_context({int_field(missing)}, type)));

    EXPECT_TRUE(runtime_filter->can_execute_on_raw_fixed_values(type, 0));
    const std::array<int32_t, 3> values {missing, 2, 4};
    std::array<uint8_t, 3> matches {1, 1, 1};
    ASSERT_TRUE(runtime_filter
                        ->execute_on_raw_fixed_values(
                                reinterpret_cast<const uint8_t*>(values.data()), values.size(),
                                sizeof(int32_t), type, 0, matches.data())
                        .ok());
    EXPECT_EQ((std::array<uint8_t, 3> {0, 1, 1}), matches);

    auto null_aware_runtime_filter = RuntimeFilterExpr::create_shared(node, bloom, 0.0, true, 10);
    EXPECT_FALSE(null_aware_runtime_filter->can_evaluate_dictionary_filter());
    EXPECT_FALSE(null_aware_runtime_filter->can_execute_on_raw_fixed_values(type, 0));
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

    // A branch that cannot be read only blocks kNoMatch, so the group is still worth asking.
    VCompoundPred or_with_unsupported(make_compound_node(TExprOpcode::COMPOUND_OR, 2));
    or_with_unsupported.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kNoMatch));
    or_with_unsupported.add_child(std::make_shared<UnsupportedSingleSlotExpr>(slot));
    EXPECT_TRUE(or_with_unsupported.can_evaluate_zonemap_filter());
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, or_with_unsupported.evaluate_zonemap_filter(ctx));

    // One branch matching every row settles the group, whatever the other branch is.
    VCompoundPred or_all_match(make_compound_node(TExprOpcode::COMPOUND_OR, 2));
    or_all_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kAllMatch));
    or_all_match.add_child(std::make_shared<UnsupportedSingleSlotExpr>(slot));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch, or_all_match.evaluate_zonemap_filter(ctx));

    // AND matches every row only when every branch does.
    VCompoundPred and_all_match(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    and_all_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kAllMatch));
    and_all_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kAllMatch));
    EXPECT_EQ(ZoneMapFilterResult::kAllMatch, and_all_match.evaluate_zonemap_filter(ctx));

    VCompoundPred and_partly_all_match(make_compound_node(TExprOpcode::COMPOUND_AND, 2));
    and_partly_all_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kAllMatch));
    and_partly_all_match.add_child(make_fixed_zonemap_expr(ZoneMapFilterResult::kMayMatch));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch, and_partly_all_match.evaluate_zonemap_filter(ctx));

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
    std::vector<bool> always_true;
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              VExprContext::evaluate_zonemap_filter({may_match, no_match}, ctx, &always_true));
    EXPECT_EQ(ZoneMapFilterResult::kMayMatch,
              VExprContext::evaluate_zonemap_filter({may_match}, ctx, &always_true));
    // A conjunct that only may match is never marked for dropping.
    EXPECT_EQ(std::vector<bool> {false}, always_true);

    auto type = int_type();
    auto slot = make_slot(0, type);
    auto unsupported =
            VExprContext::create_shared(std::make_shared<UnsupportedSingleSlotExpr>(slot));
    EXPECT_EQ(ZoneMapFilterResult::kNoMatch,
              VExprContext::evaluate_zonemap_filter({unsupported, no_match}, ctx, &always_true));
    EXPECT_EQ(0, ctx.stats.unusable_zonemap_eval_count);
}

} // namespace doris
