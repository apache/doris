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
#include "pipeline/task_queue.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "thrift_builder.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/runtime/vdata_stream_mgr.h"

namespace doris::pipeline {

static void empty_function(RuntimeState*, Status*) {}

class PipelineTest : public testing::Test {
public:
    PipelineTest() : _obj_pool(new ObjectPool()) {
        _query_options = TQueryOptionsBuilder().set_enable_local_exchange(true).build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx = QueryContext::create_shared(_query_id, ExecEnv::GetInstance(), _query_options,
                                                 fe_address, true, fe_address,
                                                 QuerySource::INTERNAL_FRONTEND);
        ExecEnv::GetInstance()->set_stream_mgr(new doris::vectorized::VDataStreamMgr());
        _task_queue = std::make_unique<MultiCoreTaskQueue>(1);
    }
    ~PipelineTest() override = default;

private:
    std::shared_ptr<Pipeline> _build_pipeline(int id, int num_instances,
                                              Pipeline* parent = nullptr) {
        auto pip = std::make_shared<Pipeline>(
                id, parent ? std::min(parent->num_tasks(), num_instances) : num_instances,
                parent ? parent->num_tasks() : num_instances);
        _pipelines.push_back(pip);
        _pipeline_tasks.push_back(std::vector<std::unique_ptr<PipelineTask>> {});
        _runtime_states.push_back(std::vector<std::unique_ptr<RuntimeState>> {});
        _pipeline_profiles.push_back(nullptr);
        return pip;
    }
    std::shared_ptr<PipelineFragmentContext> _build_fragment_context() {
        int fragment_id = _next_fragment_id();
        _context.push_back(std::make_shared<PipelineFragmentContext>(
                _query_id, fragment_id, _query_ctx, ExecEnv::GetInstance(), empty_function,
                std::bind<Status>(std::mem_fn(&FragmentMgr::trigger_pipeline_context_report),
                                  ExecEnv::GetInstance()->fragment_mgr(), std::placeholders::_1,
                                  std::placeholders::_2)));
        _runtime_state.push_back(RuntimeState::create_unique(
                _query_id, fragment_id, _query_options, _query_ctx->query_globals,
                ExecEnv::GetInstance(), _query_ctx.get()));
        _runtime_state.back()->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(_context.back()));
        return _context.back();
    }
    TUniqueId _next_ins_id() {
        _ins_id.lo++;
        return _ins_id;
    }
    int _next_fragment_id() { return _fragment_id++; }
    int _next_node_id() { return _next_node_idx++; }
    int _next_op_id() { return _next_op_idx++; }

    // Query level
    std::shared_ptr<ObjectPool> _obj_pool;
    std::shared_ptr<QueryContext> _query_ctx;
    TUniqueId _query_id = TUniqueId();
    TQueryOptions _query_options;
    std::unique_ptr<MultiCoreTaskQueue> _task_queue;

    // Fragment level
    // Fragment0 -> Fragment1
    std::vector<std::unique_ptr<RuntimeState>> _runtime_state;
    std::vector<std::shared_ptr<PipelineFragmentContext>> _context;
    int _fragment_id = 0;

    TUniqueId _ins_id = TUniqueId();
    int _next_node_idx = 0;
    int _next_op_idx = 0;

    // Pipeline Level
    // Fragment0[Pipeline0 -> Pipeline1] -> Fragment1[Pipeline2 -> Pipeline3]
    std::vector<std::shared_ptr<Pipeline>> _pipelines;
    std::vector<std::shared_ptr<RuntimeProfile>> _pipeline_profiles;

    // Task Level
    // Fragment0[Pipeline0[Task0] -> Pipeline1[Task0]] -> Fragment1[Pipeline2[Task0] -> Pipeline3[Task0]]
    std::vector<std::vector<std::unique_ptr<PipelineTask>>> _pipeline_tasks;
    std::vector<std::vector<std::unique_ptr<RuntimeState>>> _runtime_states;

    const std::string LOCALHOST = BackendOptions::get_localhost();
    const int DUMMY_PORT = config::brpc_port;
};

TEST_F(PipelineTest, HAPPY_PATH) {
    // Pipeline(ExchangeOperator(id=0, UNPARTITIONED) -> ExchangeSinkOperatorX(id=1, UNPARTITIONED))
    int parallelism = 1;

    // Build pipeline
    int pipeline_id = 0;
    DescriptorTbl* desc;
    OperatorPtr op;
    _build_fragment_context();
    auto cur_pipe = _build_pipeline(pipeline_id, parallelism);
    {
        auto tnode = TPlanNodeBuilder(_next_node_id(), TPlanNodeType::EXCHANGE_NODE)
                             .set_exchange_node(
                                     TExchangeNodeBuilder()
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
                        .set_slotType(
                                TTypeDescBuilder()
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

        EXPECT_EQ(DescriptorTbl::create(_obj_pool.get(), desc_table, &desc), Status::OK());
        op.reset(new ExchangeSourceOperatorX(_obj_pool.get(), tnode, _next_op_id(), *desc, 1));
        EXPECT_EQ(op->init(tnode, _runtime_state.back().get()), Status::OK());
        auto& exchange_operator = op->cast<ExchangeSourceOperatorX>();
        EXPECT_EQ(exchange_operator._is_merging, false);
        EXPECT_EQ(cur_pipe->add_operator(op, 0), Status::OK());
    }

    TDataSink tsink;
    // 0-1
    auto source_ins = _next_ins_id();
    // 0-2
    auto dest0 = _next_ins_id();
    {
        std::vector<TPlanFragmentDestination> destinations;
        auto dest0_address = TNetworkAddress();
        dest0_address.hostname = LOCALHOST;
        dest0_address.port = DUMMY_PORT;
        destinations.push_back(
                TPlanFragmentDestinationBuilder(dest0, dest0_address, dest0_address).build());
        auto stream_sink =
                TDataStreamSinkBuilder(_next_node_id(),
                                       TDataPartitionBuilder(TPartitionType::UNPARTITIONED).build())
                        .build();
        tsink = TDataSinkBuilder(TDataSinkType::DATA_STREAM_SINK)
                        .set_stream_sink(stream_sink)
                        .build();
        DataSinkOperatorPtr sink;

        std::vector<TUniqueId> ids;
        ids.push_back(source_ins);
        sink.reset(new ExchangeSinkOperatorX(_runtime_state.back().get(), op->row_desc(),
                                             _next_op_id(), stream_sink, destinations, ids));
        EXPECT_EQ(sink->init(tsink), Status::OK());
        EXPECT_EQ(cur_pipe->set_sink(sink), Status::OK());

        EXPECT_EQ(cur_pipe->sink()->set_child(cur_pipe->operators().back()), Status::OK());

        EXPECT_EQ(cur_pipe->prepare(_runtime_state.back().get()), Status::OK());
        EXPECT_EQ(cur_pipe->num_tasks(), parallelism);
    }

    {
        // Build pipeline task
        int task_id = 0;
        std::unique_ptr<RuntimeState> local_runtime_state = RuntimeState::create_unique(
                source_ins, _query_id, _context.back()->get_fragment_id(), _query_options,
                _query_ctx->query_globals, ExecEnv::GetInstance(), _query_ctx.get());
        local_runtime_state->set_desc_tbl(desc);
        local_runtime_state->set_per_fragment_instance_idx(0);
        local_runtime_state->set_num_per_fragment_instances(parallelism);
        local_runtime_state->resize_op_id_to_local_state(-1);
        local_runtime_state->set_max_operator_id(-1);
        local_runtime_state->set_load_stream_per_node(0);
        local_runtime_state->set_total_load_streams(0);
        local_runtime_state->set_num_local_sink(0);
        local_runtime_state->set_task_id(task_id);
        local_runtime_state->set_task_num(cur_pipe->num_tasks());
        local_runtime_state->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(_context.back()));

        _pipeline_profiles[pipeline_id] =
                std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pipeline_id));
        std::map<int,
                 std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
                le_state_map;
        auto task = std::make_unique<PipelineTask>(
                cur_pipe, task_id, local_runtime_state.get(), _context.back().get(),
                _pipeline_profiles[pipeline_id].get(), le_state_map, task_id);
        cur_pipe->incr_created_tasks(task_id, task.get());
        local_runtime_state->set_task(task.get());
        task->set_task_queue(_task_queue.get());
        _pipeline_tasks[pipeline_id].push_back(std::move(task));
        _runtime_states[pipeline_id].push_back(std::move(local_runtime_state));
    }

    auto context = _build_fragment_context();
    std::unique_ptr<RuntimeState> downstream_runtime_state = RuntimeState::create_unique(
            dest0, _query_id, _next_fragment_id(), _query_options, _query_ctx->query_globals,
            ExecEnv::GetInstance(), _query_ctx.get());
    downstream_runtime_state->set_task_execution_context(
            std::static_pointer_cast<TaskExecutionContext>(context));

    auto downstream_pipeline_profile =
            std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pipeline_id));
    auto* memory_used_counter = downstream_pipeline_profile->AddHighWaterMarkCounter(
            "MemoryUsage", TUnit::BYTES, "", 1);
    auto downstream_recvr = ExecEnv::GetInstance()->_vstream_mgr->create_recvr(
            downstream_runtime_state.get(), memory_used_counter, op->row_desc(), dest0, 1, 1,
            downstream_pipeline_profile.get(), false);
    std::vector<TScanRangeParams> scan_ranges;
    EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->prepare(scan_ranges, 0, tsink, _query_ctx.get()),
              Status::OK());

    auto& local_state = _runtime_states.back()
                                .front()
                                ->get_local_state(op->operator_id())
                                ->cast<ExchangeLocalState>();
    auto& sink_local_state =
            _runtime_states.back().front()->get_sink_local_state()->cast<ExchangeSinkLocalState>();
    EXPECT_EQ(sink_local_state.channels.size(), 1);
    EXPECT_EQ(sink_local_state.only_local_exchange, true);

    EXPECT_EQ(local_state.stream_recvr->sender_queues().size(), 1);

    // Blocked by execution dependency which is set by FE 2-phase trigger.
    EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->_wait_to_start(), true);
    _query_ctx->get_execution_dependency()->set_ready();
    // Task is ready and be push into runnable task queue.
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->_wait_to_start(), false);

    bool eos = false;
    auto read_deps = local_state.dependencies();
    {
        // Blocked by exchange read dependency due to no data reached.
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->execute(&eos), Status::OK());
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->_opened, true);
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->_wake_up_early ||
                          _pipeline_tasks[pipeline_id].back()->_eos || eos,
                  false);
    }
    EXPECT_EQ(sink_local_state.channels[0]->_local_recvr != nullptr, true);
    EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                          [](const auto& dep) { return dep->ready(); }),
              false);

    // Construct input block
    vectorized::Block block;
    {
        vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();

        auto int_col0 = vectorized::ColumnInt32::create();
        int_col0->insert_many_vals(1, 10);
        block.insert({std::move(int_col0), int_type, "test_int_col0"});
    }

    local_state.stream_recvr->_sender_queues[0]->add_block(&block, true);
    EXPECT_EQ(block.columns(), 0);

    // Task is ready since a new block reached.
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                          [](const auto& dep) { return dep->ready(); }),
              true);
    {
        // Blocked by exchange read dependency due to no data reached.
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->execute(&eos), Status::OK());
        EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  false);
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->_wake_up_early ||
                          _pipeline_tasks[pipeline_id].back()->_eos || eos,
                  false);
    }
    // Upstream task finished.
    local_state.stream_recvr->_sender_queues[0]->decrement_senders(0);
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    {
        // Blocked by exchange read dependency due to no data reached.
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->execute(&eos), Status::OK());
        EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  true);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->is_pending_finish(), false);
        EXPECT_EQ(_pipeline_tasks[pipeline_id].back()->close(Status::OK()), Status::OK());
    }
    {
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 1);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 0);
    }
}

} // namespace doris::pipeline
