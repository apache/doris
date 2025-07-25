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

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "pipeline/exec/mock_operator.h"
#include "pipeline/exec/operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris::pipeline {

class ProfileSpecTest : public testing::Test {
public:
    ProfileSpecTest() = default;
    ~ProfileSpecTest() override = default;

    void SetUp() override {
        scalar_type.__set_type(TPrimitiveType::STRING);
        tbl_desc.tableType = TTableType::OLAP_TABLE;

        tuple_desc.id = 0;
        tuple_descs.push_back(tuple_desc);
        tuple_desc.id = 1;
        tuple_descs.push_back(tuple_desc);

        TTypeNode type_node;
        type_node.__set_scalar_type(scalar_type);
        slot_desc.slotType.types.push_back(type_node);
        slot_desc.id = 0;
        slot_desc.parent = 0;
        slot_descs.push_back(slot_desc);
        slot_desc.id = 1;
        slot_desc.parent = 1;
        slot_descs.push_back(slot_desc);
        thrift_tbl.tableDescriptors.push_back(tbl_desc);
        thrift_tbl.tupleDescriptors = tuple_descs;
        thrift_tbl.slotDescriptors = slot_descs;
        std::ignore = DescriptorTbl::create(obj_pool.get(), thrift_tbl, &descs);

        row_desc = RowDescriptor(*descs, {0, 1}, {true, true});
        sink.__set_dest_node_id(1);
    }

private:
    class MockOperatorX : public OperatorX<MockLocalState> {
    public:
        MockOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                      const DescriptorTbl& descs)
                : OperatorX<MockLocalState>(pool, tnode, operator_id, descs) {}
        ~MockOperatorX() override = default;

        Status prepare(RuntimeState* state) override { return Status::OK(); }
        Status open(RuntimeState* state) { return Status::OK(); }
        Status close(RuntimeState* state) override { return Status::OK(); }
        Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
            return Status::OK();
        }
    };
    class MockRuntimeState : public RuntimeState {
    public:
        MockRuntimeState() = default;

        MOCK_CONST_METHOD0(enable_local_merge_sort, bool());
    };

    std::unique_ptr<ObjectPool> obj_pool = std::make_unique<ObjectPool>();
    TTableDescriptor tbl_desc;
    TScalarType scalar_type;
    TTupleDescriptor tuple_desc;
    std::vector<TTupleDescriptor> tuple_descs;
    TSlotDescriptor slot_desc;
    std::vector<TSlotDescriptor> slot_descs;
    TDescriptorTable thrift_tbl;
    DescriptorTbl* descs = nullptr;

    RowDescriptor row_desc;
    TDataStreamSink sink;
};

TEST_F(ProfileSpecTest, SourceOperatorNameSuffixTest1) {
    TPlanNode tnode;
    tnode.__set_nereids_id(-1);
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));
    std::vector<bool> null_map {false, false};
    tnode.nullable_tuples = null_map;

    MockOperatorX op(obj_pool.get(), tnode, 1, *descs);

    RuntimeState* runtime_state = nullptr;
    auto local_state = std::make_unique<MockLocalState>(runtime_state, &op);
    ASSERT_EQ(local_state->name_suffix(), "(id=1)");
}

TEST_F(ProfileSpecTest, SourceOperatorNameSuffixTest2) {
    TPlanNode tnode;
    tnode.__set_nereids_id(100);
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));
    std::vector<bool> null_map {false, false};
    tnode.nullable_tuples = null_map;

    MockOperatorX op(obj_pool.get(), tnode, 1, *descs);
    op._nereids_id = 100;
    RuntimeState* runtime_state = nullptr;
    auto local_state = std::make_unique<MockLocalState>(runtime_state, &op);
    ASSERT_EQ(local_state->name_suffix(), "(nereids_id=100)(id=1)");
}

TEST_F(ProfileSpecTest, DataStreamSinkOperatorTest) {
    sink.__set_dest_node_id(101);
    TDataPartition partition;
    sink.__set_output_partition(partition);
    sink.__set_tablet_sink_schema({});
    sink.__set_tablet_sink_partition({});
    sink.__set_tablet_sink_location({});
    sink.__set_tablet_sink_tuple_id(1);
    sink.__set_tablet_sink_txn_id(1);
    sink.__set_tablet_sink_exprs({});
    std::unique_ptr<MockRuntimeState> state = std::make_unique<MockRuntimeState>();
    ExchangeSinkOperatorX sink_op(static_cast<RuntimeState*>(state.get()), row_desc, 1, sink, {},
                                  {});
    ExchangeSinkLocalState local_state(state.get());
    local_state._parent = &sink_op;

    ASSERT_EQ(local_state.name_suffix(), "(dest_id=101)");
}

TEST_F(ProfileSpecTest, CommonCountersCustomCounters) {
    TPlanNode tnode;
    tnode.__set_nereids_id(100);
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));
    std::vector<bool> null_map {false, false};
    tnode.nullable_tuples = null_map;

    MockOperatorX op(obj_pool.get(), tnode, 1, *descs);

    std::unique_ptr<MockRuntimeState> runtime_state = std::make_unique<MockRuntimeState>();
    std::unique_ptr<MockLocalState> local_state =
            MockLocalState::create_unique(runtime_state.get(), &op);

    LocalStateInfo info {nullptr, {}, nullptr, {}, 0};
    std::unique_ptr<RuntimeProfile> parent_profile = std::make_unique<RuntimeProfile>("parent");
    info.parent_profile = parent_profile.get();
    std::ignore = local_state->init(runtime_state.get(), info);

    ASSERT_TRUE(local_state->operator_profile() != nullptr);
    ASSERT_TRUE(local_state->operator_profile()->get_child("CustomCounters") != nullptr);
    ASSERT_TRUE(local_state->operator_profile()->get_child("CommonCounters") != nullptr);
}

} // namespace doris::pipeline
