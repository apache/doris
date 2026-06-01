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
#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/exchange_sink_operator.h"
#include "exec/operator/mock_operator.h"
#include "exec/operator/mock_scan_operator.h"
#include "exec/operator/operator.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile_counter_names.h"
#include "runtime/runtime_state.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

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

        row_desc = RowDescriptor(*descs, {0, 1});
        sink.__set_dest_node_id(1);
    }

protected:
    template <typename Operator>
    void init_source_local_state(MockRuntimeState* runtime_state, Operator* op,
                                 RuntimeProfile* parent_profile) {
        const auto max_operator_id = op->operator_id() - 1;
        runtime_state->resize_op_id_to_local_state(max_operator_id);
        runtime_state->set_max_operator_id(max_operator_id);
        LocalStateInfo info {parent_profile, {}, nullptr, {}, 0};
        ASSERT_TRUE(op->setup_local_state(runtime_state, info).ok());
    }

    Block make_string_block(std::string value) {
        auto col = ColumnString::create();
        col->insert_data(value.data(), value.size());
        Block block;
        block.insert(
                ColumnWithTypeAndName(std::move(col), std::make_shared<DataTypeString>(), "c0"));
        return block;
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
        Status get_block(RuntimeState* state, Block* block, bool* eos) override {
            return Status::OK();
        }
    };
    class ProducingMockOperatorX : public OperatorX<MockLocalState> {
    public:
        ProducingMockOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                               const DescriptorTbl& descs)
                : OperatorX<MockLocalState>(pool, tnode, operator_id, descs) {
            _op_name = "MOCK_OPERATOR";
        }

        void set_output_block(Block block) { _block = std::move(block); }

        Status prepare(RuntimeState* state) override { return Status::OK(); }
        Status close(RuntimeState* state) override { return Status::OK(); }
        Status get_block(RuntimeState* state, Block* block, bool* eos) override {
            *eos = true;
            block->swap(_block);
            return Status::OK();
        }

    private:
        Block _block;
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

    MockOperatorX op(obj_pool.get(), tnode, 1, *descs);

    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto local_state = std::make_unique<MockLocalState>(runtime_state.get(), &op);
    ASSERT_EQ(local_state->name_suffix(), "(id=1)");
}

TEST_F(ProfileSpecTest, SourceOperatorNameSuffixTest2) {
    TPlanNode tnode;
    tnode.__set_nereids_id(100);
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));

    MockOperatorX op(obj_pool.get(), tnode, 1, *descs);
    op._nereids_id = 100;
    auto runtime_state = std::make_unique<MockRuntimeState>();
    auto local_state = std::make_unique<MockLocalState>(runtime_state.get(), &op);
    ASSERT_EQ(local_state->name_suffix(), "(nereids_id=100, id=1)");
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

    ASSERT_EQ(local_state.name_suffix(), "(id=101, dest_id=101)");
}

TEST_F(ProfileSpecTest, CommonCountersCustomCounters) {
    TPlanNode tnode;
    tnode.__set_nereids_id(100);
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
    tnode.row_tuples.push_back(TTupleId(0));
    tnode.row_tuples.push_back(TTupleId(1));

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

TEST_F(ProfileSpecTest, GenericSourceOperatorUpdatesOutputBlockByteCounters) {
    TPlanNode tnode;
    tnode.__set_node_id(1);
    tnode.__set_node_type(TPlanNodeType::AGGREGATION_NODE);
    tnode.row_tuples.push_back(TTupleId(0));

    ProducingMockOperatorX op(obj_pool.get(), tnode, -1, *descs);
    std::unique_ptr<MockRuntimeState> runtime_state = std::make_unique<MockRuntimeState>();
    RuntimeProfile parent_profile("parent");
    init_source_local_state(runtime_state.get(), &op, &parent_profile);

    Block expected = make_string_block("generic-output");
    const auto expected_bytes = static_cast<int64_t>(expected.bytes());
    op.set_output_block(std::move(expected));

    Block output;
    bool eos = false;
    ASSERT_TRUE(op.get_block_after_projects(runtime_state.get(), &output, &eos).ok());
    ASSERT_TRUE(eos);

    auto* local_state = runtime_state->get_local_state(op.operator_id());
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::ROWS_PRODUCED)->value(), 1);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::BLOCKS_PRODUCED)->value(), 1);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::OUTPUT_BLOCK_BYTES)->value(),
              expected_bytes);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::MAX_OUTPUT_BLOCK_BYTES)->value(),
              expected_bytes);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::MIN_OUTPUT_BLOCK_BYTES)->value(),
              expected_bytes);
}

TEST_F(ProfileSpecTest, ScanSourceOperatorUpdatesOutputBlockByteCounters) {
    MockScanOperatorX op;
    std::unique_ptr<MockRuntimeState> runtime_state = std::make_unique<MockRuntimeState>();
    RuntimeProfile parent_profile("parent");
    init_source_local_state(runtime_state.get(), &op, &parent_profile);

    Block expected = make_string_block("scan-output");
    const auto expected_bytes = static_cast<int64_t>(expected.bytes());
    op.set_output_block(std::move(expected));

    Block output;
    bool eos = false;
    ASSERT_TRUE(op.get_block_after_projects(runtime_state.get(), &output, &eos).ok());
    ASSERT_TRUE(eos);

    auto* local_state = runtime_state->get_local_state(op.operator_id());
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::ROWS_PRODUCED)->value(), 1);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::BLOCKS_PRODUCED)->value(), 1);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::OUTPUT_BLOCK_BYTES)->value(),
              expected_bytes);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::MAX_OUTPUT_BLOCK_BYTES)->value(),
              expected_bytes);
    EXPECT_EQ(local_state->common_profile()->get_counter(profile::MIN_OUTPUT_BLOCK_BYTES)->value(),
              expected_bytes);
}

} // namespace doris
