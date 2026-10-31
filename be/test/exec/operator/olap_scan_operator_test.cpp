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

#include "exec/operator/olap_scan_operator.h"

#include <gtest/gtest.h>

#include <array>
#include <memory>
#include <optional>
#include <tuple>

#include "common/object_pool.h"
#include "core/column/column_const.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/pipeline/thrift_builder.h"
#include "exprs/vexpr_context.h"
#include "exprs/vtopn_pred.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryCache_types.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "storage/predicate/predicate_creator.h"
#include "storage/rowset/beta_rowset.h"
#include "testutil/column_helper.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_fn_call.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"

namespace doris {

class OlapScanOperatorChangelogPushDownTest : public testing::Test {
protected:
    void SetUp() override {
        _state = std::make_shared<MockRuntimeState>();

        DescriptorTblBuilder desc_builder(&_pool);
        desc_builder.declare_tuple()
                << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(), "k1"}
                << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(), "v1"};
        _descs = desc_builder.build();
        ASSERT_NE(_descs, nullptr);

        auto* tuple_desc = _descs->get_tuple_descriptor(0);
        ASSERT_NE(tuple_desc, nullptr);
        ASSERT_EQ(tuple_desc->slots().size(), 2);
        _key_slot = tuple_desc->slots()[0];
        _value_slot = tuple_desc->slots()[1];

        TOlapScanNode olap_scan_node;
        olap_scan_node.__set_tuple_id(0);
        olap_scan_node.__set_keyType(TKeysType::UNIQUE_KEYS);
        olap_scan_node.__set_key_column_name({"k1"});
        olap_scan_node.__set_key_column_type({TPrimitiveType::INT});

        TPlanNode plan_node;
        plan_node.__set_node_id(0);
        plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        plan_node.__set_num_children(0);
        plan_node.__set_row_tuples({0});
        plan_node.__set_olap_scan_node(olap_scan_node);

        _parent = std::make_shared<OlapScanOperatorX>(&_pool, plan_node, 0, *_descs, 1,
                                                      TQueryCacheParam {});
        _local_state = OlapScanLocalState::create_shared(_state.get(), _parent.get());

        auto range = std::make_unique<TPaloScanRange>();
        range->__set_changelog_scan_type(TChangelogScanType::MIN_DELTA);
        _local_state->_scan_ranges.push_back(std::move(range));
        ASSERT_TRUE(_local_state->_is_changelog_merge_scan());
    }

    ObjectPool _pool;
    DescriptorTbl* _descs = nullptr;
    SlotDescriptor* _key_slot = nullptr;
    SlotDescriptor* _value_slot = nullptr;
    std::shared_ptr<MockRuntimeState> _state;
    std::shared_ptr<OlapScanOperatorX> _parent;
    std::shared_ptr<OlapScanLocalState> _local_state;
};

TEST_F(OlapScanOperatorChangelogPushDownTest, MergeScanPushesDownOnlyKeyColumns) {
    EXPECT_TRUE(_local_state->can_push_down_column_predicate(_key_slot));
    EXPECT_FALSE(_local_state->can_push_down_column_predicate(_value_slot));
}

TEST_F(OlapScanOperatorChangelogPushDownTest, AppendOnlyKeepsValuePredicatePushDown) {
    _local_state->_scan_ranges[0]->__set_changelog_scan_type(TChangelogScanType::APPEND_ONLY);
    ASSERT_FALSE(_local_state->_is_changelog_merge_scan());

    EXPECT_TRUE(_local_state->can_push_down_column_predicate(_key_slot));
    EXPECT_TRUE(_local_state->can_push_down_column_predicate(_value_slot));
}

class OlapScanLikeConstantPatternTest : public OlapScanOperatorChangelogPushDownTest {
protected:
    void check_pattern(bool wrap_const, uint8_t is_null, const std::string& value, bool opposite) {
        ColumnPtr column = ColumnHelper::create_nullable_column<DataTypeString>({value}, {is_null});
        if (wrap_const) {
            column = ColumnConst::create(column, 1);
        }
        auto pattern = MockFnCall::create("pattern");
        pattern->_mock_is_constant = true;
        pattern->_data_type = make_nullable(std::make_shared<DataTypeString>());
        pattern->set_const_expr_col(std::move(column));
        auto like = MockFnCall::create("like");
        like->add_child(std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeString>()));
        like->add_child(pattern);
        VExprSPtr root = like;
        if (opposite) {
            auto not_expr = MockFnCall::create("not");
            not_expr->_node_type = TExprNodeType::COMPOUND_PRED;
            not_expr->add_child(like);
            root = not_expr;
        }
        auto ctx = VExprContext::create_shared(root);
        like->_fn_context_index = ctx->register_function_context(
                _state.get(), make_nullable(std::make_shared<DataTypeBool>()),
                {std::make_shared<DataTypeString>(), pattern->data_type()});
        auto pdt = PushDownType::ACCEPTABLE;
        StringRef constant_str;
        FunctionContext* fn_ctx = nullptr;
        auto st = _local_state->_should_push_down_function_filter(like.get(), ctx.get(),
                                                                  &constant_str, &fn_ctx, pdt);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(pdt, is_null ? PushDownType::UNACCEPTABLE : PushDownType::ACCEPTABLE);
        if (is_null) {
            check_expression_fallback(ctx.get());
        } else {
            EXPECT_EQ(constant_str.to_string(), value);
            EXPECT_EQ(fn_ctx, ctx->fn_context(like->_fn_context_index));
        }
    }

    void check_expression_fallback(VExprContext* ctx) {
        // Rejecting pushdown must leave both LIKE and NOT LIKE for expression evaluation.
        std::vector<std::shared_ptr<ColumnPredicate>> predicates;
        auto pdt = PushDownType::UNACCEPTABLE;
        auto st = _local_state->_normalize_function_filters(ctx, _key_slot, predicates, &pdt);
        ASSERT_TRUE(st.ok()) << st.to_string();
        EXPECT_TRUE(predicates.empty());
        EXPECT_EQ(pdt, PushDownType::UNACCEPTABLE);
    }
};

TEST_F(OlapScanLikeConstantPatternTest, NullPatternsRetainExpressionEvaluation) {
    for (bool wrap_const : {false, true}) {
        for (bool opposite : {false, true}) {
            check_pattern(wrap_const, 1, "", opposite);
            // A nonempty NULL payload must never be read as a LIKE pattern.
            check_pattern(wrap_const, 1, "ignored%", opposite);
        }
    }
}

TEST_F(OlapScanLikeConstantPatternTest, EmptyAndNonemptyPatternsRemainPushable) {
    for (bool wrap_const : {false, true}) {
        check_pattern(wrap_const, 0, "", false);
        check_pattern(wrap_const, 0, "prefix%", false);
    }
}

TEST_F(OlapScanLikeConstantPatternTest, BinaryPredicatePreservesNullConstant) {
    for (bool wrap_const : {false, true}) {
        ColumnPtr column = ColumnHelper::create_nullable_column<DataTypeInt32>({42}, {1});
        if (wrap_const) {
            column = ColumnConst::create(column, 1);
        }
        auto constant = MockFnCall::create("constant");
        constant->_mock_is_constant = true;
        constant->set_const_expr_col(std::move(column));
        auto eq = MockFnCall::create("eq");
        eq->add_child(std::make_shared<MockSlotRef>(0, std::make_shared<DataTypeInt32>()));
        eq->add_child(constant);
        auto ctx = VExprContext::create_shared(eq);
        Field value;
        EXPECT_EQ(_local_state->_should_push_down_binary_predicate(eq.get(), ctx.get(), value,
                                                                   {"eq"}),
                  PushDownType::ACCEPTABLE);
        EXPECT_TRUE(value.is_null());
    }
}

class OlapScanOperatorTsoPruningTest : public OlapScanOperatorChangelogPushDownTest {
protected:
    void SetUp() override {
        OlapScanOperatorChangelogPushDownTest::SetUp();
        // Match prepare()'s tablet slots, but leave the tablets null so the empty-source
        // tests still verify that TSO pruning finishes without accessing a tablet.
        _local_state->_tablets.resize(_local_state->_scan_ranges.size());
        _parent->_olap_scan_node.__set_read_changelog(true);
        _profile = std::make_unique<RuntimeProfile>("TsoPruningTest");
        _local_state->_scanner_init_timer = ADD_TIMER(_profile, "ScannerInitTime");
        _local_state->_rowset_tso_prune_timer = ADD_TIMER(_profile, "RowsetTsoPruneTime");
        _local_state->_rowsets_pruned_by_tso_counter =
                ADD_COUNTER(_profile, "RowsetsPrunedByTso", TUnit::UNIT);
        _local_state->_segments_pruned_by_tso_counter =
                ADD_COUNTER(_profile, "SegmentsPrunedByTso", TUnit::UNIT);
        _local_state->_tablets_pruned_by_tso_counter =
                ADD_COUNTER(_profile, "TabletsPrunedByTso", TUnit::UNIT);
        _local_state->_scan_dependency = Dependency::create_shared(0, 0, "Scan", false);
    }

    RowsetReaderSharedPtr make_reader(std::optional<TsoRange> tso, int64_t segments = 1,
                                      int64_t rows = 1) {
        auto meta = std::make_shared<RowsetMeta>();
        meta->set_version(tso && tso->start_tso() == tso->end_tso() ? Version(2, 2)
                                                                    : Version(2, 4));
        meta->set_num_segments(segments);
        meta->set_num_rows(rows);
        if (tso) {
            meta->set_commit_tso(*tso);
        }
        auto rowset = std::make_shared<BetaRowset>(std::make_shared<TabletSchema>(), meta,
                                                   "/nonexistent/tso_pruning_test");
        RowsetReaderSharedPtr reader;
        EXPECT_TRUE(rowset->create_reader(&reader).ok());
        return reader;
    }

    std::unique_ptr<RuntimeProfile> _profile;
};

TEST_F(OlapScanOperatorTsoPruningTest, HalfOpenBoundsPreserveOverlappingAndUnknownRowsets) {
    TPaloScanRange range;
    range.__set_start_tso(100);
    range.__set_end_tso(200);

    auto before = make_reader(TsoRange {99, 99}, 2);
    auto lower = make_reader(TsoRange {100, 100});
    auto inside = make_reader(TsoRange {199, 199}, 3);
    auto upper = make_reader(TsoRange {200, 200}, 4);
    auto compacted_before = make_reader(TsoRange {10, 99}, 2);
    auto touches_lower = make_reader(TsoRange {90, 100}, 3);
    auto compacted_inside = make_reader(TsoRange {120, 170}, 2);
    auto spans_window = make_reader(TsoRange {0, 250}, 6);
    auto compacted_after = make_reader(TsoRange {200, 250}, 5);
    auto missing = make_reader(std::nullopt, 2);
    auto unknown_lower = make_reader(TsoRange {-1, 170});
    auto unknown_upper = make_reader(TsoRange {100, -1});

    TabletReadSource source;
    for (const auto& reader :
         {before, lower, inside, upper, compacted_before, touches_lower, compacted_inside,
          spans_window, compacted_after, missing, unknown_lower, unknown_upper}) {
        source.rs_splits.emplace_back(reader);
    }
    // Pruning the data source must not discard separately captured delete predicates.
    source.delete_predicates.push_back(before->rowset()->rowset_meta());
    _local_state->_prune_rowsets_by_tso(range, source);

    const std::vector<RowsetReaderSharedPtr> expected {
            lower,        inside,  touches_lower, compacted_inside,
            spans_window, missing, unknown_lower, unknown_upper};
    ASSERT_EQ(source.rs_splits.size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(source.rs_splits[i].rs_reader, expected[i]);
    }
    ASSERT_EQ(source.delete_predicates.size(), 1);
    EXPECT_EQ(source.delete_predicates.front(), before->rowset()->rowset_meta());
    EXPECT_EQ(_local_state->_rowsets_pruned_by_tso_counter->value(), 4);
    EXPECT_EQ(_local_state->_segments_pruned_by_tso_counter->value(), 13);
}

TEST_F(OlapScanOperatorTsoPruningTest, LowerBoundOnly) {
    TPaloScanRange range;
    range.__set_start_tso(100);
    TabletReadSource source;
    auto at_lower = make_reader(TsoRange {100, 100});
    auto later = make_reader(TsoRange {200, 200});
    source.rs_splits.emplace_back(make_reader(TsoRange {99, 99}));
    source.rs_splits.emplace_back(at_lower);
    source.rs_splits.emplace_back(later);

    _local_state->_prune_rowsets_by_tso(range, source);

    ASSERT_EQ(source.rs_splits.size(), 2);
    EXPECT_EQ(source.rs_splits[0].rs_reader, at_lower);
    EXPECT_EQ(source.rs_splits[1].rs_reader, later);
    EXPECT_EQ(_local_state->_rowsets_pruned_by_tso_counter->value(), 1);
}

TEST_F(OlapScanOperatorTsoPruningTest, UpperBoundOnly) {
    TPaloScanRange range;
    range.__set_end_tso(100);
    TabletReadSource source;
    auto earlier = make_reader(TsoRange {99, 99});
    source.rs_splits.emplace_back(earlier);
    source.rs_splits.emplace_back(make_reader(TsoRange {100, 100}));
    source.rs_splits.emplace_back(make_reader(TsoRange {200, 200}));

    _local_state->_prune_rowsets_by_tso(range, source);

    ASSERT_EQ(source.rs_splits.size(), 1);
    EXPECT_EQ(source.rs_splits.front().rs_reader, earlier);
    EXPECT_EQ(_local_state->_rowsets_pruned_by_tso_counter->value(), 2);
}

TEST_F(OlapScanOperatorTsoPruningTest, NoBoundsPreservesAllRowsets) {
    TPaloScanRange range;
    TabletReadSource source;
    source.rs_splits.emplace_back(make_reader(TsoRange {100, 100}));
    source.rs_splits.emplace_back(make_reader(std::nullopt));

    _local_state->_prune_rowsets_by_tso(range, source);

    EXPECT_EQ(source.rs_splits.size(), 2);
    EXPECT_EQ(_local_state->_rowsets_pruned_by_tso_counter->value(), 0);
    EXPECT_EQ(_local_state->_segments_pruned_by_tso_counter->value(), 0);
}

TEST_F(OlapScanOperatorTsoPruningTest, EmptyWindowSkipsScannersAndSignalsEos) {
    auto& range = *_local_state->_scan_ranges.front();
    range.__set_changelog_scan_type(TChangelogScanType::DETAIL);
    range.__set_start_tso(100);
    range.__set_end_tso(200);
    _local_state->_read_sources.resize(1);
    auto& source = _local_state->_read_sources.front();
    source.rs_splits.emplace_back(make_reader(TsoRange {99, 99}, 2));
    source.rs_splits.emplace_back(make_reader(TsoRange {200, 200}, 3));

    // There are no tablet or segment files. Successful preparation proves that the
    // fully pruned source is not recaptured or passed to a scanner for initialization.
    EXPECT_TRUE(_local_state->_prepare_scanners().ok());

    EXPECT_TRUE(_local_state->_scanners.empty());
    EXPECT_TRUE(_local_state->_eos);
    EXPECT_EQ(_local_state->_scan_dependency->is_blocked_by(nullptr), nullptr);
    EXPECT_EQ(_local_state->_rowsets_pruned_by_tso_counter->value(), 2);
    EXPECT_EQ(_local_state->_segments_pruned_by_tso_counter->value(), 5);
    EXPECT_EQ(_local_state->_tablets_pruned_by_tso_counter->value(), 1);
}

TEST_F(OlapScanOperatorTsoPruningTest, EmptyBootstrapDoesNotStartMinDeltaScanner) {
    auto& range = *_local_state->_scan_ranges.front();
    range.__set_start_tso(100);
    range.__set_end_tso(200);
    _local_state->_read_sources.resize(1);
    auto& source = _local_state->_read_sources.front();
    source.rs_splits.emplace_back(make_reader(std::nullopt, 0, 0));
    source.rs_splits.emplace_back(make_reader(TsoRange {99, 99}, 2));

    EXPECT_TRUE(_local_state->_prepare_scanners().ok());

    EXPECT_TRUE(_local_state->_scanners.empty());
    EXPECT_TRUE(_local_state->_eos);
    EXPECT_EQ(_local_state->_scan_dependency->is_blocked_by(nullptr), nullptr);
    EXPECT_EQ(_local_state->_rowsets_pruned_by_tso_counter->value(), 1);
    EXPECT_EQ(_local_state->_segments_pruned_by_tso_counter->value(), 2);
    EXPECT_EQ(_local_state->_tablets_pruned_by_tso_counter->value(), 1);
}

class OlapScanRuntimePredicateTest : public testing::TestWithParam<std::tuple<bool, bool, bool>> {
protected:
    void SetUp() override {
        _state = std::make_shared<MockRuntimeState>();
        DescriptorTblBuilder desc_builder(&_pool);
        desc_builder.declare_tuple()
                << TupleDescBuilder::SlotType {std::make_shared<DataTypeInt32>(), "k1"};
        _descs = desc_builder.build();
        _slot = _descs->get_tuple_descriptor(0)->slots()[0];

        TOlapScanNode scan_node;
        scan_node.__set_tuple_id(0);
        scan_node.__set_keyType(TKeysType::DUP_KEYS);
        scan_node.__set_key_column_name({"k1"});
        scan_node.__set_key_column_type({TPrimitiveType::INT});
        TPlanNode plan_node;
        plan_node.__set_node_id(0);
        plan_node.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
        plan_node.__set_num_children(0);
        plan_node.__set_row_tuples({0});
        plan_node.__set_limit(-1);
        plan_node.__set_olap_scan_node(scan_node);
        _parent = std::make_shared<OlapScanOperatorX>(&_pool, plan_node, 0, *_descs, 1,
                                                      TQueryCacheParam {});
        _parent->_push_down_agg_type = TPushAggOp::NONE;
        _parent->_colname_to_slot_id = {{"k1", _slot->id()}};
        // The normalization capability check needs a physical column UID, without opening tablets.
        TColumn column;
        column.__set_col_unique_id(0);
        _parent->_olap_scan_node.__set_columns_desc({column});
        _local_state = OlapScanLocalState::create_shared(_state.get(), _parent.get());
        _local_state->_custom_profile = std::make_unique<RuntimeProfile>("RuntimePredicateTest");

        TTopnFilterDesc desc;
        desc.__set_source_node_id(10);
        desc.__set_is_asc(std::get<0>(GetParam()));
        desc.__set_is_strict(std::get<1>(GetParam()));
        desc.__set_null_first(false);
        desc.__set_target_node_id_to_target_expr(
                {{0, TRuntimeFilterDescBuilder::get_default_expr()}});
        _state->get_query_ctx()->init_runtime_predicates({desc});
        _runtime_predicate = &_state->get_query_ctx()->get_runtime_predicate(10);
        _runtime_predicate->set_detected_source();
        ASSERT_TRUE(_runtime_predicate->init_target(0, {{_slot->id(), _slot}}, 0).ok());
        _shared_predicate = _runtime_predicate->get_predicate(0);
        if (std::get<2>(GetParam())) {
            ASSERT_TRUE(_runtime_predicate
                                ->update(Field::create_field<TYPE_INT>(
                                        std::get<0>(GetParam()) ? 60 : 40))
                                .ok());
        }
    }

    void check_retained(ColumnValueRange<TYPE_INT> range,
                        std::shared_ptr<ColumnPredicate> static_predicate = nullptr) {
        auto& predicates = _local_state->_slot_id_to_predicates[_slot->id()];
        if (static_predicate) {
            predicates.push_back(std::move(static_predicate));
        }
        _local_state->_slot_id_to_value_range[_slot->id()] = std::move(range);
        VExprSPtr expr;
        ASSERT_TRUE(VTopNPred::create_vtopn_pred(_runtime_predicate->get_texpr(0), 10, expr).ok());
        PushDownType push_down = PushDownType::UNACCEPTABLE;
        ASSERT_TRUE(
                _local_state->_normalize_topn_filter(nullptr, expr, _slot, predicates, &push_down)
                        .ok());
        ASSERT_EQ(push_down, PushDownType::ACCEPTABLE);
        ASSERT_EQ(predicates.back(), _shared_predicate);

        ASSERT_TRUE(_local_state->_build_key_ranges_and_filters().ok());
        // Reaching an exact scan key must still remove its static range and static predicate.
        EXPECT_EQ(_local_state->_slot_id_to_value_range.count(_slot->id()), 0);
        ASSERT_EQ(_local_state->_slot_id_to_predicates.count(_slot->id()), 1);
        const auto& retained = _local_state->_slot_id_to_predicates.at(_slot->id());
        ASSERT_EQ(retained.size(), 1);
        EXPECT_EQ(retained.front(), _shared_predicate);
        EXPECT_TRUE(retained.front()->is_runtime_filter());
        EXPECT_EQ(retained.front()->clone(0), _shared_predicate);

        auto column = ColumnInt32::create();
        column->get_data().assign({49, 50, 51});
        std::array<bool, 3> matches {};
        retained.front()->evaluate_vec(*column, matches.size(), matches.data());
        EXPECT_EQ(matches, (std::array<bool, 3> {true, true, true}));
        ASSERT_TRUE(_runtime_predicate->update(Field::create_field<TYPE_INT>(50)).ok());
        retained.front()->evaluate_vec(*column, matches.size(), matches.data());
        const bool ascending = std::get<0>(GetParam());
        const bool strict = std::get<1>(GetParam());
        EXPECT_EQ(matches, (std::array<bool, 3> {ascending, !strict, !ascending}));
    }

    ObjectPool _pool;
    DescriptorTbl* _descs = nullptr;
    SlotDescriptor* _slot = nullptr;
    std::shared_ptr<MockRuntimeState> _state;
    std::shared_ptr<OlapScanOperatorX> _parent;
    std::shared_ptr<OlapScanLocalState> _local_state;
    RuntimePredicate* _runtime_predicate = nullptr;
    std::shared_ptr<ColumnPredicate> _shared_predicate;
};

TEST_P(OlapScanRuntimePredicateTest, WholeRangePreservesRuntimePredicate) {
    check_retained(ColumnValueRange<TYPE_INT>("k1", false, 0, 0));
}

TEST_P(OlapScanRuntimePredicateTest, ScopeRangeRemovesOnlyStaticPredicate) {
    ColumnValueRange<TYPE_INT> range("k1", false, 0, 0);
    ASSERT_TRUE(range.add_range(SQLFilterOp::FILTER_LESS_OR_EQUAL, 100).ok());
    check_retained(std::move(range),
                   create_comparison_predicate<PredicateType::LE>(
                           0, "k1", _slot->type(), Field::create_field<TYPE_INT>(100), false));
}

TEST_P(OlapScanRuntimePredicateTest, FixedRangeRemovesOnlyStaticPredicate) {
    ColumnValueRange<TYPE_INT> range("k1", false, 0, 0);
    ASSERT_TRUE(range.add_fixed_value(50).ok());
    check_retained(std::move(range),
                   create_comparison_predicate<PredicateType::EQ>(
                           0, "k1", _slot->type(), Field::create_field<TYPE_INT>(50), false));
}

INSTANTIATE_TEST_SUITE_P(Bounds, OlapScanRuntimePredicateTest,
                         testing::Combine(testing::Bool(), testing::Bool(), testing::Bool()));

} // namespace doris
