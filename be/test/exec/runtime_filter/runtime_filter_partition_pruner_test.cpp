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

#include "exec/runtime_filter/runtime_filter_partition_pruner.h"

#include <gtest/gtest.h>

#include <memory>
#include <type_traits>
#include <vector>

#include "core/data_type/data_type_factory.hpp"
#include "core/string_ref.h"
#include "core/types.h"
#include "exec/runtime_filter/utils.h"
#include "exprs/create_predicate_function.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vruntimefilter_wrapper.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"

namespace doris {

class RuntimeFilterPartitionPrunerTest : public testing::Test {
protected:
    static constexpr SlotId SLOT_ID = 1;

    template <PrimitiveType PT>
    using CppType = typename PrimitiveTypeTraits<PT>::CppType;

    template <PrimitiveType PT>
    TExprNode literal_node(const CppType<PT>& value, int precision = 0, int scale = 0) {
        TExprNode node;
        EXPECT_TRUE(create_texpr_literal_node<PT>(&value, &node, precision, scale).ok());
        return node;
    }

    TExprNode null_node(PrimitiveType ptype, int precision = 0, int scale = 0) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::NULL_LITERAL);
        node.__set_type(create_type_desc(ptype, precision, scale));
        return node;
    }

    template <PrimitiveType PT>
    TPartitionBoundary list_boundary(int64_t partition_id, const std::vector<TExprNode>& values) {
        TPartitionBoundary boundary;
        boundary.__set_partition_id(partition_id);
        boundary.__set_slot_id(SLOT_ID);
        boundary.__set_list_values(values);
        return boundary;
    }

    template <PrimitiveType PT>
    TPartitionBoundary range_boundary(int64_t partition_id, const CppType<PT>& start,
                                      const CppType<PT>& end, int precision = 0, int scale = 0) {
        TPartitionBoundary boundary;
        boundary.__set_partition_id(partition_id);
        boundary.__set_slot_id(SLOT_ID);
        boundary.__set_range_start(literal_node<PT>(start, precision, scale));
        boundary.__set_range_end(literal_node<PT>(end, precision, scale));
        return boundary;
    }

    template <PrimitiveType PT>
    TPartitionBoundary lower_unbounded_range_boundary(int64_t partition_id, const CppType<PT>& end,
                                                      int precision = 0, int scale = 0) {
        TPartitionBoundary boundary;
        boundary.__set_partition_id(partition_id);
        boundary.__set_slot_id(SLOT_ID);
        boundary.__set_range_end(literal_node<PT>(end, precision, scale));
        return boundary;
    }

    template <PrimitiveType PT>
    TPartitionBoundary upper_unbounded_range_boundary(int64_t partition_id,
                                                      const CppType<PT>& start, int precision = 0,
                                                      int scale = 0) {
        TPartitionBoundary boundary;
        boundary.__set_partition_id(partition_id);
        boundary.__set_slot_id(SLOT_ID);
        boundary.__set_range_start(literal_node<PT>(start, precision, scale));
        return boundary;
    }

    SlotDescriptor slot_desc(PrimitiveType ptype, bool nullable, int precision = 0, int scale = 0) {
        SlotDescriptor slot;
        slot._id = SLOT_ID;
        slot._type = DataTypeFactory::instance().create_data_type(
                create_type_desc(ptype, precision, scale), nullable);
        slot._col_name = "part_col";
        return slot;
    }

    std::unique_ptr<ParsedPartitionBoundaries> parse_boundaries(
            PrimitiveType ptype, const std::vector<TPartitionBoundary>& boundaries,
            bool nullable = false, int precision = 0, int scale = 0) {
        auto slot = slot_desc(ptype, nullable, precision, scale);
        phmap::flat_hash_map<int, SlotDescriptor*> slots;
        slots[SLOT_ID] = &slot;
        auto parsed = std::make_unique<ParsedPartitionBoundaries>();
        parsed->parse(boundaries, slots);
        return parsed;
    }

    template <PrimitiveType PT>
    std::shared_ptr<HybridSetBase> in_filter(const CppType<PT>& value, bool contain_null = false) {
        std::shared_ptr<HybridSetBase> set(create_set(PT, contain_null));
        if constexpr (std::is_same_v<CppType<PT>, String>) {
            StringRef ref(value.data(), value.size());
            set->insert(&ref);
        } else {
            set->insert(&value);
        }
        if (contain_null) {
            set->insert(static_cast<const void*>(nullptr));
        }
        return set;
    }

    template <PrimitiveType PT>
    VExprSPtr in_predicate(const CppType<PT>& value, bool contain_null = false) {
        TExprNode node;
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_node_type(contain_null ? TExprNodeType::NULL_AWARE_IN_PRED
                                          : TExprNodeType::IN_PRED);
        node.in_predicate.__set_is_not_in(false);
        node.__set_opcode(TExprOpcode::FILTER_IN);
        node.__set_is_nullable(false);
        return VDirectInPredicate::create_shared(node, in_filter<PT>(value, contain_null));
    }

    template <PrimitiveType PT>
    VExprSPtr minmax_predicate_le(const CppType<PT>& value, const DataTypePtr& type) {
        VExprSPtr pred;
        TExprNode node;
        EXPECT_TRUE(create_vbin_predicate(type, TExprOpcode::LE, pred, &node, false).ok());
        VExprSPtr lhs;
        VExprSPtr rhs;
        EXPECT_TRUE(create_literal(type, &value, lhs).ok());
        EXPECT_TRUE(create_literal(type, &value, rhs).ok());
        pred->add_child(lhs);
        pred->add_child(rhs);
        return pred;
    }

    VExprSPtr slot_ref_expr(const DataTypePtr& type) {
        TSlotRef slot_ref_thrift;
        slot_ref_thrift.__set_slot_id(SLOT_ID);
        slot_ref_thrift.__set_tuple_id(0);
        TExprNode node;
        node.__set_type(create_type_desc(type->get_primitive_type(), type->get_precision(),
                                         type->get_scale()));
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_num_children(0);
        node.__set_slot_ref(slot_ref_thrift);
        auto slot_ref = VSlotRef::create_shared(node);
        slot_ref->set_slot_id(SLOT_ID);
        slot_ref->set_column_id(0);
        return slot_ref;
    }

    template <PrimitiveType PT>
    void assert_parse_and_prune_type(const CppType<PT>& keep_value, const CppType<PT>& prune_value,
                                     int precision = 0, int scale = 0) {
        auto boundaries = std::vector<TPartitionBoundary> {
                list_boundary<PT>(1, {literal_node<PT>(keep_value, precision, scale)}),
                list_boundary<PT>(2, {literal_node<PT>(prune_value, precision, scale)})};
        auto parsed = parse_boundaries(PT, boundaries, false, precision, scale);
        ASSERT_FALSE(parsed->empty());
        ASSERT_EQ(parsed->total_partitions(), 2);
        const auto& parsed_boundaries = parsed->slot_to_boundaries().at(SLOT_ID);

        RuntimeFilterPartitionPruner in_pruner;
        phmap::flat_hash_set<int64_t> in_pruned;
        in_pruner._try_prune_by_single_rf(parsed_boundaries, in_predicate<PT>(keep_value),
                                          in_pruned);
        EXPECT_FALSE(in_pruned.contains(1));
        EXPECT_TRUE(in_pruned.contains(2));

        auto slot = slot_desc(PT, false, precision, scale);
        RuntimeFilterPartitionPruner minmax_pruner;
        phmap::flat_hash_set<int64_t> minmax_pruned;
        minmax_pruner._try_prune_by_single_rf(
                parsed_boundaries, minmax_predicate_le<PT>(keep_value, slot.type()), minmax_pruned);
        EXPECT_FALSE(minmax_pruned.contains(1));
        EXPECT_TRUE(minmax_pruned.contains(2));

        auto range_boundaries = std::vector<TPartitionBoundary> {
                range_boundary<PT>(3, keep_value, prune_value, precision, scale)};
        auto parsed_range = parse_boundaries(PT, range_boundaries, false, precision, scale);
        EXPECT_FALSE(parsed_range->empty());
        EXPECT_EQ(parsed_range->total_partitions(), 1);
    }

    DateV2Value<DateV2ValueType> date_v2(uint16_t year, uint8_t month, uint8_t day) {
        DateV2Value<DateV2ValueType> value;
        value.unchecked_set_time(year, month, day, 0, 0, 0, 0);
        return value;
    }

    DateV2Value<DateTimeV2ValueType> datetime_v2(uint16_t year, uint8_t month, uint8_t day) {
        DateV2Value<DateTimeV2ValueType> value;
        value.unchecked_set_time(year, month, day, 12, 0, 0, 123000);
        return value;
    }

    TimestampTzValue timestamptz(uint16_t year, uint8_t month, uint8_t day) {
        auto local_dt = datetime_v2(year, month, day);
        TimestampTzValue value;
        EXPECT_TRUE(value.from_datetime(local_dt, cctz::utc_time_zone(), 6, 6));
        return value;
    }
};

TEST_F(RuntimeFilterPartitionPrunerTest, ProjectedBoundariesPreserveOpenRangeBounds) {
    int32_t ten = 10;
    int32_t twenty = 20;
    std::vector<TPartitionBoundary> boundaries {lower_unbounded_range_boundary<TYPE_INT>(1, ten),
                                                upper_unbounded_range_boundary<TYPE_INT>(2, ten),
                                                range_boundary<TYPE_INT>(3, ten, twenty)};
    auto parsed = parse_boundaries(TYPE_INT, boundaries);
    auto slot = slot_desc(TYPE_INT, false);
    auto target_expr = slot_ref_expr(slot.type());
    VExprContext ctx(target_expr);

    auto increasing = parsed->get_or_compute_projected_boundaries(
            /*filter_id=*/101, target_expr, SLOT_ID, /*leaf_column_id=*/0,
            TTargetExprMonotonicity::MONOTONIC_INCREASING, nullptr, &ctx);
    ASSERT_EQ(increasing->size(), 3);
    const auto& inc_lower_unbounded =
            std::get<ColumnValueRange<TYPE_INT>>(increasing->at(0).boundary_cvr);
    EXPECT_TRUE(inc_lower_unbounded.is_low_value_minimum());
    EXPECT_FALSE(inc_lower_unbounded.is_high_value_maximum());
    EXPECT_EQ(inc_lower_unbounded.get_range_max_value(), ten);
    const auto& inc_upper_unbounded =
            std::get<ColumnValueRange<TYPE_INT>>(increasing->at(1).boundary_cvr);
    EXPECT_FALSE(inc_upper_unbounded.is_low_value_minimum());
    EXPECT_EQ(inc_upper_unbounded.get_range_min_value(), ten);
    EXPECT_TRUE(inc_upper_unbounded.is_high_value_maximum());

    auto decreasing = parsed->get_or_compute_projected_boundaries(
            /*filter_id=*/102, target_expr, SLOT_ID, /*leaf_column_id=*/0,
            TTargetExprMonotonicity::MONOTONIC_DECREASING, nullptr, &ctx);
    ASSERT_EQ(decreasing->size(), 3);
    const auto& dec_lower_unbounded =
            std::get<ColumnValueRange<TYPE_INT>>(decreasing->at(0).boundary_cvr);
    EXPECT_FALSE(dec_lower_unbounded.is_low_value_minimum());
    EXPECT_EQ(dec_lower_unbounded.get_range_min_value(), ten);
    EXPECT_TRUE(dec_lower_unbounded.is_high_value_maximum());
    const auto& dec_upper_unbounded =
            std::get<ColumnValueRange<TYPE_INT>>(decreasing->at(1).boundary_cvr);
    EXPECT_TRUE(dec_upper_unbounded.is_low_value_minimum());
    EXPECT_FALSE(dec_upper_unbounded.is_high_value_maximum());
    EXPECT_EQ(dec_upper_unbounded.get_range_max_value(), ten);
}

TEST_F(RuntimeFilterPartitionPrunerTest, ParseAndPrunePrimitiveTypeMatrix) {
    assert_parse_and_prune_type<TYPE_BOOLEAN>(false, true);
    assert_parse_and_prune_type<TYPE_TINYINT>(int8_t {1}, int8_t {2});
    assert_parse_and_prune_type<TYPE_SMALLINT>(int16_t {10}, int16_t {20});
    assert_parse_and_prune_type<TYPE_INT>(int32_t {100}, int32_t {200});
    assert_parse_and_prune_type<TYPE_BIGINT>(int64_t {1000}, int64_t {2000});
    assert_parse_and_prune_type<TYPE_LARGEINT>(int128_t {10000}, int128_t {20000});
    assert_parse_and_prune_type<TYPE_FLOAT>(1.5F, 2.5F);
    assert_parse_and_prune_type<TYPE_DOUBLE>(1.5, 2.5);
    assert_parse_and_prune_type<TYPE_CHAR>(String("aa"), String("bb"));
    assert_parse_and_prune_type<TYPE_VARCHAR>(String("alpha"), String("bravo"));
    assert_parse_and_prune_type<TYPE_STRING>(String("left"), String("right"));
    assert_parse_and_prune_type<TYPE_DATE>(VecDateTimeValue(0, 2, 0, 0, 0, 2024, 1, 1),
                                           VecDateTimeValue(0, 2, 0, 0, 0, 2024, 2, 1));
    assert_parse_and_prune_type<TYPE_DATETIME>(VecDateTimeValue(0, 3, 0, 0, 0, 2024, 1, 1),
                                               VecDateTimeValue(0, 3, 0, 0, 0, 2024, 2, 1));
    assert_parse_and_prune_type<TYPE_DATEV2>(date_v2(2024, 1, 1), date_v2(2024, 2, 1));
    assert_parse_and_prune_type<TYPE_DATETIMEV2>(datetime_v2(2024, 1, 1), datetime_v2(2024, 2, 1),
                                                 0, 6);
    assert_parse_and_prune_type<TYPE_TIMESTAMPTZ>(timestamptz(2024, 1, 1), timestamptz(2024, 2, 1),
                                                  0, 6);
    assert_parse_and_prune_type<TYPE_DECIMAL32>(Decimal32(100), Decimal32(200), 9, 2);
    assert_parse_and_prune_type<TYPE_DECIMAL64>(Decimal64(100), Decimal64(200), 18, 2);
    assert_parse_and_prune_type<TYPE_DECIMAL128I>(Decimal128V3(100), Decimal128V3(200), 38, 2);
    assert_parse_and_prune_type<TYPE_DECIMAL256>(Decimal256(100), Decimal256(200), 76, 2);
    assert_parse_and_prune_type<TYPE_DECIMALV2>(DecimalV2Value(1, 0), DecimalV2Value(2, 0), 27, 9);
    assert_parse_and_prune_type<TYPE_IPV4>(IPv4(1), IPv4(2));
    assert_parse_and_prune_type<TYPE_IPV6>(IPv6(1), IPv6(2));
}

TEST_F(RuntimeFilterPartitionPrunerTest, NullPartitionSemantics) {
    int32_t one = 1;
    int32_t two = 2;
    std::vector<TPartitionBoundary> boundaries {
            list_boundary<TYPE_INT>(1, {null_node(TYPE_INT)}),
            list_boundary<TYPE_INT>(2, {null_node(TYPE_INT), literal_node<TYPE_INT>(one)}),
            list_boundary<TYPE_INT>(3, {literal_node<TYPE_INT>(two)})};
    auto parsed = parse_boundaries(TYPE_INT, boundaries, true);
    const auto& parsed_boundaries = parsed->slot_to_boundaries().at(SLOT_ID);

    RuntimeFilterPartitionPruner non_null_pruner;
    phmap::flat_hash_set<int64_t> non_null_pruned;
    non_null_pruner._try_prune_by_single_rf(parsed_boundaries, in_predicate<TYPE_INT>(one),
                                            non_null_pruned);
    EXPECT_TRUE(non_null_pruned.contains(1));
    EXPECT_FALSE(non_null_pruned.contains(2));
    EXPECT_TRUE(non_null_pruned.contains(3));

    RuntimeFilterPartitionPruner null_aware_pruner;
    phmap::flat_hash_set<int64_t> null_aware_pruned;
    null_aware_pruner._try_prune_by_single_rf(parsed_boundaries, in_predicate<TYPE_INT>(one, true),
                                              null_aware_pruned);
    EXPECT_FALSE(null_aware_pruned.contains(1));
    EXPECT_FALSE(null_aware_pruned.contains(2));
    EXPECT_TRUE(null_aware_pruned.contains(3));
}

TEST_F(RuntimeFilterPartitionPrunerTest, PublicPruneByRuntimeFiltersIdentitySlot) {
    int32_t keep = 10;
    int32_t prune = 20;
    std::vector<TPartitionBoundary> boundaries {
            list_boundary<TYPE_INT>(1, {literal_node<TYPE_INT>(keep)}),
            list_boundary<TYPE_INT>(2, {literal_node<TYPE_INT>(prune)})};
    auto parsed = parse_boundaries(TYPE_INT, boundaries);
    auto slot = slot_desc(TYPE_INT, false);

    auto impl = in_predicate<TYPE_INT>(keep);
    impl->add_child(slot_ref_expr(slot.type()));
    TExprNode wrapper_node;
    wrapper_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    wrapper_node.__set_node_type(TExprNodeType::IN_PRED);
    wrapper_node.__set_opcode(TExprOpcode::FILTER_IN);
    auto wrapper =
            VRuntimeFilterWrapper::create_shared(wrapper_node, impl, 0, false, /*filter_id=*/7);
    VExprContextSPtrs conjuncts {std::make_shared<VExprContext>(wrapper)};

    RuntimeFilterPartitionPruner pruner;
    EXPECT_EQ(pruner.prune_by_runtime_filters(*parsed, conjuncts, {}, /*scan_node_id=*/0), 1);
    EXPECT_FALSE(pruner.is_partition_pruned(1));
    EXPECT_TRUE(pruner.is_partition_pruned(2));
    EXPECT_EQ(pruner.pruned_partition_count(), 1);
    EXPECT_EQ(pruner.prune_by_runtime_filters(*parsed, conjuncts, {}, /*scan_node_id=*/0), 0);
}

} // namespace doris
