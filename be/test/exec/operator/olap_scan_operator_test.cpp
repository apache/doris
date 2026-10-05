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

#include <memory>
#include <optional>

#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryCache_types.h"
#include "storage/rowset/beta_rowset.h"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class OlapScanOperatorBinlogPushDownTest : public testing::Test {
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
        range->__set_binlog_scan_type(TBinlogScanType::MIN_DELTA);
        _local_state->_scan_ranges.push_back(std::move(range));
        ASSERT_TRUE(_local_state->_is_binlog_merge_scan());
    }

    ObjectPool _pool;
    DescriptorTbl* _descs = nullptr;
    SlotDescriptor* _key_slot = nullptr;
    SlotDescriptor* _value_slot = nullptr;
    std::shared_ptr<MockRuntimeState> _state;
    std::shared_ptr<OlapScanOperatorX> _parent;
    std::shared_ptr<OlapScanLocalState> _local_state;
};

TEST_F(OlapScanOperatorBinlogPushDownTest, MergeScanPushesDownOnlyKeyColumns) {
    EXPECT_TRUE(_local_state->can_push_down_column_predicate(_key_slot));
    EXPECT_FALSE(_local_state->can_push_down_column_predicate(_value_slot));
}

TEST_F(OlapScanOperatorBinlogPushDownTest, AppendOnlyKeepsValuePredicatePushDown) {
    _local_state->_scan_ranges[0]->__set_binlog_scan_type(TBinlogScanType::APPEND_ONLY);
    ASSERT_FALSE(_local_state->_is_binlog_merge_scan());

    EXPECT_TRUE(_local_state->can_push_down_column_predicate(_key_slot));
    EXPECT_TRUE(_local_state->can_push_down_column_predicate(_value_slot));
}

class OlapScanOperatorTsoPruningTest : public OlapScanOperatorBinlogPushDownTest {
protected:
    void SetUp() override {
        OlapScanOperatorBinlogPushDownTest::SetUp();
        _parent->_olap_scan_node.__set_read_row_binlog(true);
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
    range.__set_binlog_scan_type(TBinlogScanType::DETAIL);
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

} // namespace doris
