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

#include "pipeline/pipeline.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/exception.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "thrift_builder.h"

namespace doris::pipeline {

static void empty_function(RuntimeState*, Status*) {}

class PipelineTest : public testing::Test {
public:
    PipelineTest() : _obj_pool(new ObjectPool()) {
        auto query_options = TQueryOptionsBuilder().build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx = QueryContext::create_shared(_query_id, ExecEnv::GetInstance(), query_options,
                                                 fe_address, true, fe_address,
                                                 QuerySource::INTERNAL_FRONTEND);
        _context = std::make_shared<PipelineFragmentContext>(
                _query_id, _fragment_id, _query_ctx, ExecEnv::GetInstance(), empty_function,
                std::bind<Status>(std::mem_fn(&FragmentMgr::trigger_pipeline_context_report),
                                  ExecEnv::GetInstance()->fragment_mgr(), std::placeholders::_1,
                                  std::placeholders::_2));
        _runtime_state = RuntimeState::create_unique(_query_id, _fragment_id, query_options,
                                                     _query_ctx->query_globals,
                                                     ExecEnv::GetInstance(), _query_ctx.get());
        _runtime_state->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(_context));
    }
    ~PipelineTest() override = default;

private:
    std::shared_ptr<Pipeline> _build_pipeline(int id, int num_instances,
                                              Pipeline* parent = nullptr) {
        return std::make_shared<Pipeline>(
                id, parent ? std::min(parent->num_tasks(), num_instances) : num_instances,
                parent ? parent->num_tasks() : num_instances);
    }
    TUniqueId _next_ins_id() {
        _ins_id.lo++;
        return _ins_id;
    }
    int _next_node_id() { return _next_node_idx++; }
    int _next_op_id() { return _next_op_idx++; }

    std::shared_ptr<ObjectPool> _obj_pool;
    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<QueryContext> _query_ctx;
    std::shared_ptr<PipelineFragmentContext> _context;
    TUniqueId _query_id = TUniqueId();
    TUniqueId _ins_id = TUniqueId();
    int _fragment_id = 0;
    int _next_node_idx = 0;
    int _next_op_idx = 0;

    static constexpr std::string_view LOCALHOST = "127.0.0.1";
    static constexpr int DUMMY_PORT = 8030;
};

TEST_F(PipelineTest, HAPPY_PATH) {
    // Pipeline(ExchangeOperator(id=0, UNPARTITIONED) -> ExchangeSinkOperatorX(id=1, UNPARTITIONED))
    auto cur_pipe = _build_pipeline(0, 1);
    auto tnode =
            TPlanNodeBuilder(_next_node_id(), TPlanNodeType::EXCHANGE_NODE)
                    .set_exchange_node(TExchangeNodeBuilder()
                                               .set_partition_type(TPartitionType::UNPARTITIONED)
                                               .append_input_row_tuples(0)
                                               .build())
                    .append_row_tuples(0, false)
                    .build();

    TTupleDescriptor tuple0 = TTupleDescriptorBuilder().set_id(0).build();
    TSlotDescriptor slot0 =
            TSlotDescriptorBuilder()
                    .set_id(0)
                    .set_parent(tuple0)
                    .set_slotType(TTypeDescBuilder()
                                          .set_types(TTypeNodeBuilder()
                                                             .set_type(TTypeNodeType::SCALAR)
                                                             .set_scalar_type(TPrimitiveType::INT)
                                                             .build())
                                          .build())
                    .set_nullIndicatorBit(0)
                    .set_byteOffset(0)
                    .set_slotIdx(0)
                    .set_isMaterialized(true)
                    .set_colName("test_column0")
                    .build();

    TDescriptorTable desc_table = TDescriptorTableBuilder()
                                          .append_slotDescriptors(slot0)
                                          .append_tupleDescriptors(tuple0)
                                          .build();

    DescriptorTbl* desc;
    EXPECT_EQ(DescriptorTbl::create(_obj_pool.get(), desc_table, &desc), Status::OK());
    OperatorPtr op;
    op.reset(new ExchangeSourceOperatorX(_obj_pool.get(), tnode, _next_op_id(), *desc, 1));
    EXPECT_EQ(op->init(tnode, _runtime_state.get()), Status::OK());
    auto& exchange_operator = op->cast<ExchangeSourceOperatorX>();
    EXPECT_EQ(exchange_operator._is_merging, false);
    EXPECT_EQ(cur_pipe->add_operator(op, 0), Status::OK());

    std::vector<TPlanFragmentDestination> destinations;
    auto source_ins = _next_ins_id();
    auto dest0 = _next_ins_id();
    dest0.lo = 1;
    auto dest0_address = TNetworkAddress();
    dest0_address.hostname = LOCALHOST;
    dest0_address.port = DUMMY_PORT;
    destinations.push_back(
            TPlanFragmentDestinationBuilder(dest0, dest0_address, dest0_address).build());
    auto stream_sink =
            TDataStreamSinkBuilder(_next_node_id(),
                                   TDataPartitionBuilder(TPartitionType::UNPARTITIONED).build())
                    .build();
    auto tsink =
            TDataSinkBuilder(TDataSinkType::DATA_STREAM_SINK).set_stream_sink(stream_sink).build();
    DataSinkOperatorPtr sink;

    std::vector<TUniqueId> ids;
    ids.push_back(source_ins);
    sink.reset(new ExchangeSinkOperatorX(_runtime_state.get(), op->row_desc(), _next_op_id(),
                                         stream_sink, destinations, ids));
    EXPECT_EQ(sink->init(tsink), Status::OK());
    EXPECT_EQ(cur_pipe->set_sink(sink), Status::OK());

    EXPECT_EQ(cur_pipe->sink()->set_child(cur_pipe->operators().back()), Status::OK());

    EXPECT_EQ(cur_pipe->prepare(_runtime_state.get()), Status::OK());
}

} // namespace doris::pipeline
