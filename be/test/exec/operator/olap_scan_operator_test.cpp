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
#include <tuple>

#include "common/object_pool.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exec/pipeline/thrift_builder.h"
#include "exprs/vtopn_pred.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryCache_types.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "storage/predicate/predicate_creator.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class OlapScanRuntimePredicateTest : public testing::TestWithParam<std::tuple<bool, bool>> {
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
        plan_node.__set_nullable_tuples({false});
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
        desc.__set_null_first(false);
        desc.__set_target_node_id_to_target_expr(
                {{0, TRuntimeFilterDescBuilder::get_default_expr()}});
        _state->get_query_ctx()->init_runtime_predicates({desc});
        _runtime_predicate = &_state->get_query_ctx()->get_runtime_predicate(10);
        _runtime_predicate->set_detected_source();
        ASSERT_TRUE(_runtime_predicate->init_target(0, {{_slot->id(), _slot}}, 0).ok());
        _shared_predicate = _runtime_predicate->get_predicate(0);
        if (std::get<1>(GetParam())) {
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
        EXPECT_EQ(matches, (std::array<bool, 3> {ascending, true, !ascending}));
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
                         testing::Combine(testing::Bool(), testing::Bool()));

} // namespace doris
