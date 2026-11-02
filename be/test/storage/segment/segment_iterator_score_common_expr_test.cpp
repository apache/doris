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

#include <memory>

#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/score_runtime.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/virtual_slot_ref.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {

namespace {

constexpr ColumnId kScoreColumn = 1;

// Like Score(), an expression that reads no column.
class NoColumnExpr : public VExpr {
public:
    NoColumnExpr() { _data_type = std::make_shared<DataTypeFloat32>(); }

    const std::string& expr_name() const override {
        static const std::string kName = "NoColumnExpr";
        return kName;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::OK();
    }
};

TabletSchemaSPtr make_tablet_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* key = schema_pb.add_column();
    key->set_unique_id(0);
    key->set_name("k0");
    key->set_type("INT");
    key->set_is_key(true);
    key->set_is_nullable(false);
    auto* score = schema_pb.add_column();
    score->set_unique_id(1);
    score->set_name("__score");
    score->set_type("FLOAT");
    score->set_is_key(false);
    score->set_is_nullable(true);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

VExprSPtr make_score_slot() {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_FLOAT));
    node.__set_node_type(TExprNodeType::VIRTUAL_SLOT_REF);
    node.__set_num_children(0);
    node.__set_is_nullable(true);
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(-1);
    slot_ref.__set_tuple_id(-1);
    node.__set_slot_ref(slot_ref);

    auto slot = VirtualSlotRef::create_shared(node);
    slot->set_virtual_column_expr(std::make_shared<NoColumnExpr>());
    slot->set_column_id(kScoreColumn);
    return slot;
}

} // namespace

class SegmentIteratorScoreCommonExprTest : public testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema();
        _segment =
                std::make_shared<Segment>(0, RowsetId(), _tablet_schema, InvertedIndexFileInfo());
        _segment->_num_rows = 100;
        std::vector<ColumnId> read_column_ids(_tablet_schema->num_columns());
        for (uint32_t cid = 0; cid < read_column_ids.size(); ++cid) {
            read_column_ids[cid] = cid;
        }
        _read_schema = std::make_shared<Schema>(_tablet_schema->columns(), read_column_ids);
        _iter = std::make_unique<SegmentIterator>(_segment, _read_schema);
        _iter->_is_common_expr_column.resize(_read_schema->columns().size(), false);
        _score_slot = make_score_slot();
    }

    TabletSchemaSPtr _tablet_schema;
    std::shared_ptr<Segment> _segment;
    SchemaSPtr _read_schema;
    std::unique_ptr<SegmentIterator> _iter;
    VExprSPtr _score_slot;
};

// The scan prepares the score of every row before the first batch, so an expression on the score,
// e.g. a TopN filter, reads the score column instead of evaluating Score().
TEST_F(SegmentIteratorScoreCommonExprTest, expression_on_score_reads_score_column) {
    _iter->_score_runtime =
            ScoreRuntime::create_shared(std::make_shared<VExprContext>(_score_slot), false, 0);
    _iter->_score_runtime->_dest_column_idx = kScoreColumn;

    EXPECT_TRUE(_iter->_extract_common_expr_columns(_score_slot).ok());

    EXPECT_TRUE(_iter->_common_expr_columns.contains(kScoreColumn));
    EXPECT_TRUE(_iter->_is_common_expr_column[kScoreColumn]);
    EXPECT_FALSE(_iter->_is_common_expr_column[0]);
}

// Any other virtual column is computed from its expression, which reads no column here.
TEST_F(SegmentIteratorScoreCommonExprTest, other_virtual_column_is_computed) {
    EXPECT_TRUE(_iter->_extract_common_expr_columns(_score_slot).ok());

    EXPECT_TRUE(_iter->_common_expr_columns.empty());
    EXPECT_FALSE(_iter->_is_common_expr_column[kScoreColumn]);
}

} // namespace doris::segment_v2
