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

#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/QueryCache_types.h"
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

} // namespace doris
