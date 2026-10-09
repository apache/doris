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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "exprs/vtopn_pred.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"

// White-box access to SegmentIterator lazy materialization planning. This mirrors the
// existing segment_iterator_* white-box tests.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/segment/segment_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {
namespace {

// Stands in for a STRUCT column iterator whose predicate access paths cover only one
// nested field while another nested field is a lazy materialization target.
class SplitStructColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t ord) override { return Status::OK(); }

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          MutableColumnPtr& dst) override {
        return Status::OK();
    }

    ordinal_t get_current_ordinal() const override { return 0; }

    bool has_lazy_read_target() const override { return true; }
};

constexpr ColumnId kKeyOrdinal = 0;
constexpr ColumnId kStructOrdinal = 1;

ReadSchemaSPtr make_read_schema() {
    auto key = std::make_shared<TabletColumn>();
    key->set_unique_id(0);
    key->set_name("k");
    key->set_type(FieldType::OLAP_FIELD_TYPE_INT);
    key->set_is_key(true);
    key->set_is_nullable(false);

    auto s = std::make_shared<TabletColumn>();
    s->set_unique_id(1);
    s->set_name("s");
    s->set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    s->set_is_nullable(true);
    for (const auto* name : {"a", "b"}) {
        TabletColumn sub;
        sub.set_name(name);
        sub.set_type(FieldType::OLAP_FIELD_TYPE_INT);
        sub.set_is_nullable(true);
        s->add_sub_column(sub);
    }

    return std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {key, s});
}

VExprSPtr make_struct_slot_ref() {
    return VSlotRef::create_shared(/*slot_id=*/1, /*column_id=*/kStructOrdinal,
                                   /*column_uniq_id=*/1, std::make_shared<DataTypeInt32>(), "s");
}

// A planner-visible common expression: its nested accesses are covered by the FE
// predicate access paths.
VExprContextSPtr make_planner_expr_ctx() {
    return VExprContext::create_shared(make_struct_slot_ref());
}

// A TopN filter is created on BE at runtime, after the FE computed access paths.
VExprContextSPtr make_topn_filter_ctx() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_is_nullable(true);
    auto topn_pred = VTopNPred::create_shared(node, /*source_node_id=*/0, nullptr);
    topn_pred->add_child(make_struct_slot_ref());
    return VExprContext::create_shared(topn_pred);
}

} // namespace

class SegmentIteratorRuntimeCommonExprTest : public ::testing::Test {
protected:
    void SetUp() override {
        _read_schema = make_read_schema();
        _iter = std::make_unique<SegmentIterator>(nullptr, _read_schema);
        _iter->_opts.stats = &_stats;
        _iter->_enable_prune_nested_column = true;

        auto struct_iter = std::make_unique<SplitStructColumnIterator>();
        struct_iter->set_read_requirement(ColumnIterator::ReadRequirement::PREDICATE);
        _iter->_column_iterators[kStructOrdinal] = std::move(struct_iter);
    }

    ReadSchemaSPtr _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
    OlapReaderStatistics _stats;
};

TEST_F(SegmentIteratorRuntimeCommonExprTest, plannerExprKeepsLazyNestedRecovery) {
    _iter->_common_expr_ctxs_push_down = {make_planner_expr_ctx()};

    auto st = _iter->_vec_init_lazy_materialization();
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(_iter->_column_states[kStructOrdinal].has_common_expr);
    EXPECT_FALSE(_iter->_column_states[kStructOrdinal].has_runtime_common_expr);
    EXPECT_EQ(_iter->_lazy_pruned_ordinals, (std::vector<ColumnId> {kStructOrdinal}));
    EXPECT_EQ(_iter->_common_expr_ordinals, (std::vector<ColumnId> {kStructOrdinal}));
    EXPECT_EQ(_iter->_output_ordinals, (std::vector<ColumnId> {kKeyOrdinal}));
}

TEST_F(SegmentIteratorRuntimeCommonExprTest, topnFilterDisablesLazyNestedRecovery) {
    _iter->_common_expr_ctxs_push_down = {make_topn_filter_ctx()};

    auto st = _iter->_vec_init_lazy_materialization();
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(_iter->_column_states[kStructOrdinal].has_common_expr);
    EXPECT_TRUE(_iter->_column_states[kStructOrdinal].has_runtime_common_expr);
    EXPECT_FALSE(_iter->_column_states[kKeyOrdinal].has_runtime_common_expr);
    // The TopN filter may read nested fields outside the predicate access paths, so the
    // struct must be fully materialized before the expression runs.
    EXPECT_TRUE(_iter->_lazy_pruned_ordinals.empty());
    EXPECT_EQ(_iter->_common_expr_ordinals, (std::vector<ColumnId> {kStructOrdinal}));
}

TEST_F(SegmentIteratorRuntimeCommonExprTest, topnFilterWinsOverPlannerExprOnSameColumn) {
    _iter->_common_expr_ctxs_push_down = {make_planner_expr_ctx(), make_topn_filter_ctx()};

    auto st = _iter->_vec_init_lazy_materialization();
    ASSERT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(_iter->_column_states[kStructOrdinal].has_runtime_common_expr);
    EXPECT_TRUE(_iter->_lazy_pruned_ordinals.empty());
}

} // namespace doris::segment_v2
