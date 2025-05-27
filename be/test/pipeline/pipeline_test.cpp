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
#include "dummy_task_queue.h"
#include "exprs/bloom_filter_func.h"
#include "exprs/hybrid_set.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/exchange_source_operator.h"
#include "pipeline/exec/hashjoin_build_sink.h"
#include "pipeline/exec/hashjoin_probe_operator.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime_filter/runtime_filter_definitions.h"
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
    PipelineTest()
            : _obj_pool(new ObjectPool()),
              _mgr(std::make_unique<doris::vectorized::VDataStreamMgr>()) {}
    ~PipelineTest() override = default;
    void SetUp() override {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                TRuntimeFilterParamsBuilder().build());
        ExecEnv::GetInstance()->set_stream_mgr(_mgr.get());
        _task_queue = std::make_unique<DummyTaskQueue>(1);
    }
    void TearDown() override {}

private:
    std::shared_ptr<Pipeline> _build_pipeline(int num_instances, Pipeline* parent = nullptr) {
        auto pip = std::make_shared<Pipeline>(
                _next_pipeline_id(),
                parent ? std::min(parent->num_tasks(), num_instances) : num_instances,
                parent ? parent->num_tasks() : num_instances);
        _pipelines.push_back(pip);
        _pipeline_tasks.push_back(std::vector<std::shared_ptr<PipelineTask>> {});
        _runtime_states.push_back(std::vector<std::unique_ptr<RuntimeState>> {});
        _pipeline_profiles.push_back(nullptr);
        if (parent) {
            parent->set_children(pip);
        }
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
    void _reset() {
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _runtime_state.clear();
        _context.clear();
        _fragment_id = 0;
        _ins_id = TUniqueId();
        _next_node_idx = 0;
        _next_op_idx = 0;
        _pipelines.clear();
        _pipeline_profiles.clear();
        _pipeline_tasks.clear();
        _runtime_states.clear();
        _runtime_filter_mgrs.clear();
    }
    int _next_fragment_id() { return _fragment_id++; }
    int _next_node_id() { return _next_node_idx++; }
    int _next_op_id() { return _next_op_idx--; }
    int _next_sink_op_id() { return _next_sink_op_idx--; }
    int _next_pipeline_id() { return _next_pipeline_idx++; }
    int _max_operator_id() const { return _next_op_idx; }

    // Query level
    std::shared_ptr<ObjectPool> _obj_pool;
    std::unique_ptr<doris::vectorized::VDataStreamMgr> _mgr;
    std::shared_ptr<QueryContext> _query_ctx;
    TUniqueId _query_id = TUniqueId();
    TQueryOptions _query_options;
    std::unique_ptr<DummyTaskQueue> _task_queue;

    // Fragment level
    // Fragment0 -> Fragment1
    std::vector<std::unique_ptr<RuntimeState>> _runtime_state;
    std::vector<std::shared_ptr<PipelineFragmentContext>> _context;
    int _fragment_id = 0;
    int _next_pipeline_idx = 0;

    TUniqueId _ins_id = TUniqueId();
    int _next_node_idx = 0;
    int _next_op_idx = 0;
    int _next_sink_op_idx = 0;

    // Pipeline Level
    // Fragment0[Pipeline0 -> Pipeline1] -> Fragment1[Pipeline2 -> Pipeline3]
    std::vector<std::shared_ptr<Pipeline>> _pipelines;
    std::vector<std::shared_ptr<RuntimeProfile>> _pipeline_profiles;

    // Task Level
    // Fragment0[Pipeline0[Task0] -> Pipeline1[Task0]] -> Fragment1[Pipeline2[Task0] -> Pipeline3[Task0]]
    std::vector<std::vector<std::shared_ptr<PipelineTask>>> _pipeline_tasks;
    std::vector<std::vector<std::unique_ptr<RuntimeState>>> _runtime_states;

    // Instance level
    std::vector<std::unique_ptr<RuntimeFilterMgr>> _runtime_filter_mgrs;

    const std::string LOCALHOST = BackendOptions::get_localhost();
    const int DUMMY_PORT = config::brpc_port;
};

TEST_F(PipelineTest, HAPPY_PATH) {
    // Pipeline(ExchangeOperator(id=0, UNPARTITIONED) -> ExchangeSinkOperatorX(id=1, UNPARTITIONED))
    int parallelism = 1;

    // Build pipeline
    DescriptorTbl* desc;
    OperatorPtr op;
    _build_fragment_context();
    auto cur_pipe = _build_pipeline(parallelism);
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
                        .set_nullIndicatorBit(-1)
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

        _pipeline_profiles[cur_pipe->id()] =
                std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(cur_pipe->id()));
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        auto task = std::make_unique<PipelineTask>(
                cur_pipe, task_id, local_runtime_state.get(), _context.back(),
                _pipeline_profiles[cur_pipe->id()].get(), shared_state_map, task_id);
        cur_pipe->incr_created_tasks(task_id, task.get());
        task->set_task_queue(_task_queue.get());
        _pipeline_tasks[cur_pipe->id()].push_back(std::move(task));
        _runtime_states[cur_pipe->id()].push_back(std::move(local_runtime_state));
    }

    auto context = _build_fragment_context();
    std::unique_ptr<RuntimeState> downstream_runtime_state = RuntimeState::create_unique(
            dest0, _query_id, _next_fragment_id(), _query_options, _query_ctx->query_globals,
            ExecEnv::GetInstance(), _query_ctx.get());
    downstream_runtime_state->set_task_execution_context(
            std::static_pointer_cast<TaskExecutionContext>(context));

    auto downstream_pipeline_profile =
            std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(cur_pipe->id()));
    auto* memory_used_counter = downstream_pipeline_profile->AddHighWaterMarkCounter(
            "MemoryUsage", TUnit::BYTES, "", 1);

    // Construct input block
    vectorized::Block block;
    {
        vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();

        auto int_col0 = vectorized::ColumnInt32::create();
        int_col0->insert_many_vals(1, 10);
        block.insert({std::move(int_col0), int_type, "test_int_col0"});
    }
    auto block_mem_usage = block.allocated_bytes();
    EXPECT_GT(block_mem_usage - 1, 0);

    auto downstream_recvr = ExecEnv::GetInstance()->_vstream_mgr->create_recvr(
            downstream_runtime_state.get(), memory_used_counter, dest0, 1, 1,
            downstream_pipeline_profile.get(), false, block_mem_usage - 1);
    std::vector<TScanRangeParams> scan_ranges;
    EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->prepare(scan_ranges, 0, tsink), Status::OK());

    auto& local_state = _runtime_states.back()
                                .front()
                                ->get_local_state(op->operator_id())
                                ->cast<ExchangeLocalState>();
    auto& sink_local_state =
            _runtime_states.back().front()->get_sink_local_state()->cast<ExchangeSinkLocalState>();
    EXPECT_EQ(sink_local_state.channels.size(), 1);
    EXPECT_EQ(sink_local_state._only_local_exchange, true);

    EXPECT_EQ(local_state.stream_recvr->sender_queues().size(), 1);

    // Blocked by execution dependency which is set by FE 2-phase trigger.
    EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_wait_to_start(), true);
    _query_ctx->get_execution_dependency()->set_ready();
    // Task is ready and be push into runnable task queue.
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_wait_to_start(), false);

    bool eos = false;
    auto read_deps = local_state.dependencies();
    {
        // Blocked by exchange read dependency due to no data reached.
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->execute(&eos), Status::OK());
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_opened, true);
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_wake_up_early ||
                          _pipeline_tasks[cur_pipe->id()].back()->_eos || eos,
                  false);
    }
    EXPECT_EQ(sink_local_state.channels[0]->_local_recvr != nullptr, true);
    EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                          [](const auto& dep) { return dep->ready(); }),
              false);

    local_state.stream_recvr->_sender_queues[0]->add_block(&block, true);
    EXPECT_EQ(block.columns(), 0);

    // Task is ready since a new block reached.
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                          [](const auto& dep) { return dep->ready(); }),
              true);
    {
        // Blocked by exchange read dependency due to no data reached.
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->execute(&eos), Status::OK());
        EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  false);
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_wake_up_early ||
                          _pipeline_tasks[cur_pipe->id()].back()->_eos || eos,
                  false);
    }

    {
        vectorized::DataTypePtr int_type = std::make_shared<vectorized::DataTypeInt32>();

        auto int_col0 = vectorized::ColumnInt32::create();
        int_col0->insert_many_vals(1, 10);
        block.insert({std::move(int_col0), int_type, "test_int_col0"});
    }
    block_mem_usage = block.allocated_bytes();
    EXPECT_GT(block_mem_usage - 1, 0);
    local_state.stream_recvr->_sender_queues[0]->add_block(&block, true);
    EXPECT_EQ(block.columns(), 0);

    // Task is ready since a new block reached.
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                          [](const auto& dep) { return dep->ready(); }),
              true);
    auto write_dependencies = sink_local_state.dependencies();
    // Write dependency is blocked because each data queue is limited by block_mem_usage.
    EXPECT_EQ(std::all_of(write_dependencies.cbegin(), write_dependencies.cend(),
                          [](const auto& dep) { return dep->ready(); }),
              false);
    {
        // Blocked by exchange write dependency due to full data queue.
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->execute(&eos), Status::OK());
        EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  true);
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_wake_up_early ||
                          _pipeline_tasks[cur_pipe->id()].back()->_eos || eos,
                  false);
        EXPECT_EQ(std::all_of(write_dependencies.cbegin(), write_dependencies.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  false);
    }
    {
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 1);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 1);
    }
    {
        vectorized::Block tmp_block;
        bool tmp_eos = false;
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->get_batch(&tmp_block, &tmp_eos),
                  Status::OK());
        EXPECT_EQ(tmp_eos, false);
        EXPECT_EQ(tmp_block.rows(), 10);

        EXPECT_EQ(std::all_of(write_dependencies.cbegin(), write_dependencies.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  true);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 0);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 1);
    }
    {
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->execute(&eos), Status::OK());
        EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  false);
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_wake_up_early ||
                          _pipeline_tasks[cur_pipe->id()].back()->_eos || eos,
                  false);
        EXPECT_EQ(std::all_of(write_dependencies.cbegin(), write_dependencies.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  false);
    }
    {
        vectorized::Block tmp_block;
        bool tmp_eos = false;
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->get_batch(&tmp_block, &tmp_eos),
                  Status::OK());
        EXPECT_EQ(tmp_eos, false);
        EXPECT_EQ(tmp_block.rows(), 10);

        EXPECT_EQ(std::all_of(write_dependencies.cbegin(), write_dependencies.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  true);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 0);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 1);
    }

    // Upstream task finished.
    local_state.stream_recvr->_sender_queues[0]->decrement_senders(0);
    EXPECT_EQ(_task_queue->take(0) != nullptr, true);
    {
        // Blocked by exchange read dependency due to no data reached.
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->execute(&eos), Status::OK());
        EXPECT_EQ(std::all_of(read_deps.cbegin(), read_deps.cend(),
                              [](const auto& dep) { return dep->ready(); }),
                  true);
        EXPECT_EQ(eos, true);
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->_is_pending_finish(), false);
        EXPECT_EQ(_pipeline_tasks[cur_pipe->id()].back()->close(Status::OK()), Status::OK());
    }
    {
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 0);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 0);
    }
    downstream_recvr->close();
}

TEST_F(PipelineTest, PLAN_LOCAL_EXCHANGE) {
    _reset();
    // Pipeline(ExchangeOperator(id=0, HASH_PARTITIONED) -> ExchangeSinkOperatorX(id=1, UNPARTITIONED))
    int parallelism = 2;

    // Build pipeline
    DescriptorTbl* desc;
    OperatorPtr op;
    _build_fragment_context();
    EXPECT_EQ(_runtime_state.front()->enable_local_shuffle(), true);
    auto cur_pipe = _build_pipeline(parallelism);
    {
        auto tnode = TPlanNodeBuilder(_next_node_id(), TPlanNodeType::EXCHANGE_NODE)
                             .set_exchange_node(
                                     TExchangeNodeBuilder()
                                             .set_partition_type(TPartitionType::HASH_PARTITIONED)
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
                        .set_nullIndicatorBit(-1)
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
        EXPECT_EQ(cur_pipe->num_tasks(), parallelism);
    }
    {
        cur_pipe->init_data_distribution();
        EXPECT_EQ(cur_pipe->data_distribution().distribution_type, ExchangeType::HASH_SHUFFLE);
        EXPECT_EQ(cur_pipe->sink()->required_data_distribution().distribution_type,
                  ExchangeType::NOOP);
        EXPECT_EQ(
                cur_pipe->need_to_local_exchange(cur_pipe->sink()->required_data_distribution(), 1),
                false);
    }
    {
        cur_pipe->operators().front()->set_serial_operator();
        cur_pipe->init_data_distribution();
        EXPECT_EQ(cur_pipe->data_distribution().distribution_type, ExchangeType::NOOP);
        EXPECT_EQ(cur_pipe->sink()->required_data_distribution().distribution_type,
                  ExchangeType::PASSTHROUGH);
        EXPECT_EQ(
                cur_pipe->need_to_local_exchange(cur_pipe->sink()->required_data_distribution(), 1),
                true);
    }
}

TEST_F(PipelineTest, PLAN_HASH_JOIN) {
    _reset();
    /**
     * Pipeline(ExchangeOperator(id=1, HASH_PARTITIONED) -> HashJoinBuildOperator(id=0))
     * Pipeline(ExchangeOperator(id=2, HASH_PARTITIONED) -> HashJoinProbeOperator(id=0, UNPARTITIONED) -> ExchangeSinkOperatorX(id=3, UNPARTITIONED))
     */
    int parallelism = 2;

    // Build pipeline
    DescriptorTbl* desc;
    _build_fragment_context();
    EXPECT_EQ(_runtime_state.front()->enable_local_shuffle(), true);
    {
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
                        .set_nullIndicatorBit(-1)
                        .set_byteOffset(0)
                        .set_slotIdx(0)
                        .set_isMaterialized(true)
                        .set_colName("test_column0")
                        .build();

        TTupleDescriptor tuple1 = TTupleDescriptorBuilder().set_id(1).build();
        TSlotDescriptor slot1 =
                TSlotDescriptorBuilder()
                        .set_id(1)
                        .set_parent(tuple1)
                        .set_slotType(
                                TTypeDescBuilder()
                                        .set_types(TTypeNodeBuilder()
                                                           .set_type(TTypeNodeType::SCALAR)
                                                           .set_scalar_type(TPrimitiveType::INT)
                                                           .build())
                                        .build())
                        .set_nullIndicatorBit(-1)
                        .set_byteOffset(0)
                        .set_slotIdx(0)
                        .set_isMaterialized(true)
                        .set_colName("test_column1")
                        .build();

        TTupleDescriptor tuple2 = TTupleDescriptorBuilder().set_id(2).build();
        TSlotDescriptor slot2 =
                TSlotDescriptorBuilder()
                        .set_id(2)
                        .set_parent(tuple2)
                        .set_slotType(
                                TTypeDescBuilder()
                                        .set_types(TTypeNodeBuilder()
                                                           .set_type(TTypeNodeType::SCALAR)
                                                           .set_scalar_type(TPrimitiveType::INT)
                                                           .build())
                                        .build())
                        .set_nullIndicatorBit(-1)
                        .set_byteOffset(0)
                        .set_slotIdx(0)
                        .set_isMaterialized(true)
                        .set_colName("test_column0")
                        .build();
        TSlotDescriptor slot3 =
                TSlotDescriptorBuilder()
                        .set_id(3)
                        .set_parent(tuple2)
                        .set_slotType(
                                TTypeDescBuilder()
                                        .set_types(TTypeNodeBuilder()
                                                           .set_type(TTypeNodeType::SCALAR)
                                                           .set_scalar_type(TPrimitiveType::INT)
                                                           .build())
                                        .build())
                        .set_nullIndicatorBit(-1)
                        .set_byteOffset(4)
                        .set_slotIdx(1)
                        .set_isMaterialized(true)
                        .set_colName("test_column1")
                        .build();

        TDescriptorTable desc_table = TDescriptorTableBuilder()
                                              .append_slotDescriptors(slot0)
                                              .append_tupleDescriptors(tuple0)
                                              .append_slotDescriptors(slot1)
                                              .append_tupleDescriptors(tuple1)
                                              .append_slotDescriptors(slot2)
                                              .append_slotDescriptors(slot3)
                                              .append_tupleDescriptors(tuple2)
                                              .build();

        EXPECT_EQ(DescriptorTbl::create(_obj_pool.get(), desc_table, &desc), Status::OK());
        _runtime_state.back()->set_desc_tbl(desc);
    }

    auto join_node =
            TPlanNodeBuilder(_next_node_id(), TPlanNodeType::HASH_JOIN_NODE)
                    .set_output_tuple_id(2)
                    .set_hash_join_node(
                            THashJoinNodeBuilder(
                                    TJoinOp::INNER_JOIN,
                                    std::vector<TEqJoinCondition> {
                                            TEqJoinConditionBuilder(
                                                    TExprBuilder()
                                                            .append_nodes(
                                                                    TExprNodeBuilder(
                                                                            TExprNodeType::SLOT_REF,
                                                                            TTypeDescBuilder()
                                                                                    .set_types(
                                                                                            TTypeNodeBuilder()
                                                                                                    .set_type(
                                                                                                            TTypeNodeType::
                                                                                                                    SCALAR)
                                                                                                    .set_scalar_type(
                                                                                                            TPrimitiveType::
                                                                                                                    INT)
                                                                                                    .build())
                                                                                    .build(),
                                                                            0)
                                                                            .set_slot_ref(
                                                                                    TSlotRefBuilder(
                                                                                            0, 0)
                                                                                            .build())
                                                                            .build())
                                                            .build(),
                                                    TExprBuilder()
                                                            .append_nodes(
                                                                    TExprNodeBuilder(
                                                                            TExprNodeType::SLOT_REF,
                                                                            TTypeDescBuilder()
                                                                                    .set_types(
                                                                                            TTypeNodeBuilder()
                                                                                                    .set_type(
                                                                                                            TTypeNodeType::
                                                                                                                    SCALAR)
                                                                                                    .set_scalar_type(
                                                                                                            TPrimitiveType::
                                                                                                                    INT)
                                                                                                    .build())
                                                                                    .build(),
                                                                            0)
                                                                            .set_slot_ref(
                                                                                    TSlotRefBuilder(
                                                                                            1, 1)
                                                                                            .build())
                                                                            .build())
                                                            .build())
                                                    .build()})
                                    .set_is_broadcast_join(false)
                                    .set_dist_type(TJoinDistributionType::PARTITIONED)
                                    .append_vintermediate_tuple_id_list(0)
                                    .append_vintermediate_tuple_id_list(1)
                                    .build())
                    .append_row_tuples(2, false)
                    .append_projections(
                            TExprBuilder()
                                    .append_nodes(
                                            TExprNodeBuilder(
                                                    TExprNodeType::SLOT_REF,
                                                    TTypeDescBuilder()
                                                            .set_types(
                                                                    TTypeNodeBuilder()
                                                                            .set_type(
                                                                                    TTypeNodeType::
                                                                                            SCALAR)
                                                                            .set_scalar_type(
                                                                                    TPrimitiveType::
                                                                                            INT)
                                                                            .build())
                                                            .build(),
                                                    0)
                                                    .set_slot_ref(TSlotRefBuilder(0, 0).build())
                                                    .build())
                                    .build())
                    .append_projections(
                            TExprBuilder()
                                    .append_nodes(
                                            TExprNodeBuilder(
                                                    TExprNodeType::SLOT_REF,
                                                    TTypeDescBuilder()
                                                            .set_types(
                                                                    TTypeNodeBuilder()
                                                                            .set_type(
                                                                                    TTypeNodeType::
                                                                                            SCALAR)
                                                                            .set_scalar_type(
                                                                                    TPrimitiveType::
                                                                                            INT)
                                                                            .build())
                                                            .build(),
                                                    0)
                                                    .set_slot_ref(TSlotRefBuilder(1, 1).build())
                                                    .build())
                                    .build())
                    .append_runtime_filters(TRuntimeFilterDescBuilder()
                                                    .set_bloom_filter_size_bytes(1048576)
                                                    .set_build_bf_by_runtime_size(false)
                                                    .build())
                    .build();

    {
        auto probe_side_pipe = _build_pipeline(parallelism);

        OperatorPtr op;
        op.reset(new HashJoinProbeOperatorX(_obj_pool.get(), join_node, _next_op_id(), *desc));
        EXPECT_EQ(op->init(join_node, _runtime_state.back().get()), Status::OK());
        EXPECT_EQ(probe_side_pipe->add_operator(op, 0), Status::OK());

        auto tnode = TPlanNodeBuilder(_next_node_id(), TPlanNodeType::EXCHANGE_NODE)
                             .set_exchange_node(
                                     TExchangeNodeBuilder()
                                             .set_partition_type(TPartitionType::HASH_PARTITIONED)
                                             .append_input_row_tuples(0)
                                             .build())
                             .append_row_tuples(0, false)
                             .build();

        op.reset(new ExchangeSourceOperatorX(_obj_pool.get(), tnode, _next_op_id(), *desc, 1));
        EXPECT_EQ(op->init(tnode, _runtime_state.back().get()), Status::OK());
        auto& exchange_operator = op->cast<ExchangeSourceOperatorX>();
        EXPECT_EQ(exchange_operator._is_merging, false);
        EXPECT_EQ(probe_side_pipe->operators().front()->set_child(op), Status::OK());
        EXPECT_EQ(probe_side_pipe->add_operator(op, 0), Status::OK());
    }
    {
        auto build_side_pipe = _build_pipeline(parallelism, _pipelines.front().get());

        DataSinkOperatorPtr sink;
        sink.reset(new HashJoinBuildSinkOperatorX(
                _obj_pool.get(), _next_sink_op_id(),
                _pipelines.front()->operators().back()->operator_id(), join_node, *desc));
        EXPECT_EQ(sink->init(join_node, _runtime_state.back().get()), Status::OK());
        EXPECT_EQ(build_side_pipe->set_sink(sink), Status::OK());

        auto tnode = TPlanNodeBuilder(_next_node_id(), TPlanNodeType::EXCHANGE_NODE)
                             .set_exchange_node(
                                     TExchangeNodeBuilder()
                                             .set_partition_type(TPartitionType::HASH_PARTITIONED)
                                             .append_input_row_tuples(1)
                                             .build())
                             .append_row_tuples(1, false)
                             .build();

        OperatorPtr op;
        op.reset(new ExchangeSourceOperatorX(_obj_pool.get(), tnode, _next_op_id(), *desc, 1));
        EXPECT_EQ(op->init(tnode, _runtime_state.back().get()), Status::OK());
        auto& exchange_operator = op->cast<ExchangeSourceOperatorX>();
        EXPECT_EQ(exchange_operator._is_merging, false);
        EXPECT_EQ(build_side_pipe->add_operator(op, 0), Status::OK());
        EXPECT_EQ(build_side_pipe->num_tasks(), parallelism);
        EXPECT_EQ(_pipelines.front()->operators().back()->set_child(op), Status::OK());
        EXPECT_EQ(sink->set_child(op), Status::OK());
    }

    TDataSink tsink;
    std::vector<TUniqueId> ids;
    auto dest_ins_id = _next_ins_id();
    auto dest_node_id = _next_node_id();
    {
        auto cur_pipe = _pipelines.front();
        std::vector<TPlanFragmentDestination> destinations;
        auto dest0_address = TNetworkAddress();
        dest0_address.hostname = LOCALHOST;
        dest0_address.port = DUMMY_PORT;
        destinations.push_back(
                TPlanFragmentDestinationBuilder(dest_ins_id, dest0_address, dest0_address).build());
        auto stream_sink =
                TDataStreamSinkBuilder(dest_node_id,
                                       TDataPartitionBuilder(TPartitionType::UNPARTITIONED).build())
                        .build();
        tsink = TDataSinkBuilder(TDataSinkType::DATA_STREAM_SINK)
                        .set_stream_sink(stream_sink)
                        .build();
        DataSinkOperatorPtr sink;

        for (int i = 0; i < parallelism; i++) {
            ids.push_back(_next_ins_id());
        }
        sink.reset(new ExchangeSinkOperatorX(_runtime_state.back().get(),
                                             _pipelines.back()->operators().back()->row_desc(),
                                             _next_sink_op_id(), stream_sink, destinations, ids));
        EXPECT_EQ(sink->init(tsink), Status::OK());
        EXPECT_EQ(cur_pipe->set_sink(sink), Status::OK());
        EXPECT_EQ(cur_pipe->sink()->set_child(cur_pipe->operators().back()), Status::OK());
        EXPECT_EQ(cur_pipe->num_tasks(), parallelism);
    }

    for (int pip_idx = cast_set<int>(_pipelines.size()) - 1; pip_idx >= 0; pip_idx--) {
        _pipelines[pip_idx]->init_data_distribution();
        if (pip_idx == 1) {
            // Pipeline(ExchangeOperator(id=1, HASH_PARTITIONED) -> HashJoinBuildOperator(id=0))
            EXPECT_EQ(_pipelines[pip_idx]->data_distribution().distribution_type,
                      ExchangeType::HASH_SHUFFLE);
            EXPECT_EQ(_pipelines[pip_idx]->sink()->required_data_distribution().distribution_type,
                      ExchangeType::HASH_SHUFFLE);
            EXPECT_EQ(_pipelines[pip_idx]->need_to_local_exchange(
                              _pipelines[pip_idx]->sink()->required_data_distribution(), 1),
                      false);
        } else {
            // Pipeline(ExchangeOperator(id=2, HASH_PARTITIONED) -> HashJoinProbeOperator(id=0, UNPARTITIONED) -> ExchangeSinkOperatorX(id=3, UNPARTITIONED))
            _pipelines[pip_idx]->set_data_distribution(
                    _pipelines[pip_idx]->children().front()->data_distribution());
            EXPECT_EQ(_pipelines[pip_idx]->data_distribution().distribution_type,
                      ExchangeType::HASH_SHUFFLE);
            EXPECT_EQ(_pipelines[pip_idx]->need_to_local_exchange(
                              _pipelines[pip_idx]->sink()->required_data_distribution(), 2),
                      false);
            EXPECT_EQ(_pipelines[pip_idx]
                              ->operators()
                              .back()
                              ->required_data_distribution()
                              .distribution_type,
                      ExchangeType::HASH_SHUFFLE);
            EXPECT_EQ(_pipelines[pip_idx]->need_to_local_exchange(
                              _pipelines[pip_idx]->operators().back()->required_data_distribution(),
                              1),
                      false);
        }
    }

    for (PipelinePtr& pipeline : _pipelines) {
        pipeline->children().clear();
        EXPECT_EQ(pipeline->prepare(_runtime_state.front().get()), Status::OK());
    }

    {
        // Build pipeline task
        int task_id = 0;
        _runtime_filter_mgrs.resize(parallelism);
        for (int j = 0; j < parallelism; j++) {
            _runtime_filter_mgrs[j] = std::make_unique<RuntimeFilterMgr>(false);
        }
        for (size_t i = 0; i < _pipelines.size(); i++) {
            EXPECT_EQ(_pipelines[i]->id(), i);
            _pipeline_profiles[_pipelines[i]->id()] = std::make_shared<RuntimeProfile>(
                    "Pipeline : " + std::to_string(_pipelines[i]->id()));
            for (int j = 0; j < parallelism; j++) {
                std::unique_ptr<RuntimeState> local_runtime_state = RuntimeState::create_unique(
                        ids[j], _query_id, _context.back()->get_fragment_id(), _query_options,
                        _query_ctx->query_globals, ExecEnv::GetInstance(), _query_ctx.get());
                local_runtime_state->set_desc_tbl(desc);
                local_runtime_state->set_per_fragment_instance_idx(j);
                local_runtime_state->set_be_number(j);
                local_runtime_state->set_num_per_fragment_instances(parallelism);
                local_runtime_state->resize_op_id_to_local_state(_max_operator_id());
                local_runtime_state->set_max_operator_id(_max_operator_id());
                local_runtime_state->set_load_stream_per_node(0);
                local_runtime_state->set_total_load_streams(0);
                local_runtime_state->set_num_local_sink(0);
                local_runtime_state->set_task_id(task_id++);
                local_runtime_state->set_task_num(_pipelines[i]->num_tasks());
                local_runtime_state->set_task_execution_context(
                        std::static_pointer_cast<TaskExecutionContext>(_context.back()));
                local_runtime_state->set_runtime_filter_mgr(_runtime_filter_mgrs[j].get());
                std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                        std::vector<std::shared_ptr<Dependency>>>>
                        shared_state_map;
                auto task = std::make_unique<PipelineTask>(
                        _pipelines[i], task_id, local_runtime_state.get(), _context.back(),
                        _pipeline_profiles[_pipelines[i]->id()].get(), shared_state_map, j);
                _pipelines[i]->incr_created_tasks(j, task.get());
                task->set_task_queue(_task_queue.get());
                _pipeline_tasks[_pipelines[i]->id()].push_back(std::move(task));
                _runtime_states[_pipelines[i]->id()].push_back(std::move(local_runtime_state));
            }
        }
    }
    {
        _pipeline_tasks[0][0]->inject_shared_state(_pipeline_tasks[1][0]->get_sink_shared_state());
        _pipeline_tasks[0][1]->inject_shared_state(_pipeline_tasks[1][1]->get_sink_shared_state());
    }

    std::shared_ptr<vectorized::VDataStreamRecvr> downstream_recvr;
    auto downstream_pipeline_profile = std::make_shared<RuntimeProfile>("Downstream Pipeline");
    {
        // Build downstream recvr
        auto context = _build_fragment_context();
        std::unique_ptr<RuntimeState> downstream_runtime_state = RuntimeState::create_unique(
                dest_ins_id, _query_id, context->get_fragment_id(), _query_options,
                _query_ctx->query_globals, ExecEnv::GetInstance(), _query_ctx.get());
        downstream_runtime_state->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(context));

        auto* memory_used_counter = downstream_pipeline_profile->AddHighWaterMarkCounter(
                "MemoryUsage", TUnit::BYTES, "", 1);
        downstream_recvr = ExecEnv::GetInstance()->_vstream_mgr->create_recvr(
                downstream_runtime_state.get(), memory_used_counter, dest_ins_id, dest_node_id,
                parallelism, downstream_pipeline_profile.get(), false, 2048000);
    }
    for (size_t i = 0; i < _pipelines.size(); i++) {
        for (int j = 0; j < parallelism; j++) {
            std::vector<TScanRangeParams> scan_ranges;
            EXPECT_EQ(_pipeline_tasks[_pipelines[i]->id()][j]->prepare(scan_ranges, j, tsink),
                      Status::OK());
            if (i == 1) {
                auto& local_state = _runtime_states[i][j]
                                            ->get_sink_local_state()
                                            ->cast<HashJoinBuildSinkLocalState>();
                EXPECT_EQ(local_state._runtime_filter_producer_helper->_producers.size(), 1);
                EXPECT_EQ(local_state._should_build_hash_table, true);
            }
        }
    }

    {
        for (size_t i = 0; i < _pipelines.size(); i++) {
            for (int j = 0; j < parallelism; j++) {
                // Blocked by execution dependency which is set by FE 2-phase trigger.
                EXPECT_EQ(_pipeline_tasks[_pipelines[i]->id()][j]->_wait_to_start(), true);
            }
        }
        EXPECT_EQ(_query_ctx->get_execution_dependency()->_blocked_task.size(),
                  _pipelines.size() * parallelism);
        _query_ctx->get_execution_dependency()->set_ready();
        for (size_t i = 0; i < _pipelines.size(); i++) {
            for (int j = 0; j < parallelism; j++) {
                // Task is ready and be push into runnable task queue.
                EXPECT_EQ(_task_queue->take(0) != nullptr, true);
            }
        }
        for (size_t i = 0; i < _pipelines.size(); i++) {
            for (int j = 0; j < parallelism; j++) {
                EXPECT_EQ(_pipeline_tasks[_pipelines[i]->id()][j]->_wait_to_start(), false);
            }
        }
    }

    {
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 0);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 2);
    }

    {
        for (int i = _pipelines.size() - 1; i >= 0; i--) {
            for (int j = 0; j < parallelism; j++) {
                bool eos = false;
                EXPECT_EQ(_pipeline_tasks[i][j]->execute(&eos), Status::OK());
                EXPECT_EQ(_pipeline_tasks[i][j]->_opened, true);
                EXPECT_EQ(eos, false);
            }
        }
    }
    for (int i = _pipelines.size() - 1; i >= 0; i--) {
        for (int j = 0; j < parallelism; j++) {
            {
                vectorized::Block block;
                {
                    vectorized::DataTypePtr int_type =
                            std::make_shared<vectorized::DataTypeInt32>();

                    auto int_col0 = vectorized::ColumnInt32::create();
                    if (j == 0 || i == 0) {
                        int_col0->insert_many_vals(j, 10);
                    } else {
                        size_t ndv = 16;
                        for (size_t n = 0; n < ndv; n++) {
                            int_col0->insert_many_vals(n, 1);
                        }
                    }

                    block.insert({std::move(int_col0), int_type, "test_int_col0"});
                }
                auto& local_state =
                        _runtime_states[i][j]
                                ->get_local_state(_pipelines[i]->operators().front()->operator_id())
                                ->cast<ExchangeLocalState>();
                EXPECT_EQ(local_state.stream_recvr->_sender_queues[0]->_source_dependency->ready(),
                          false);
                EXPECT_EQ(local_state.stream_recvr->_sender_queues[0]
                                  ->_source_dependency->_blocked_task.size(),
                          i == 1 ? 1 : 0);
                local_state.stream_recvr->_sender_queues[0]->add_block(&block, true);
            }
        }
    }
    {
        // Pipeline 1 is blocked by exchange dependency so tasks are ready after data reached.
        // Pipeline 0 is blocked by hash join dependency and is still waiting for upstream tasks done.
        for (int j = 0; j < parallelism; j++) {
            // Task is ready and be push into runnable task queue.
            EXPECT_EQ(_task_queue->take(0) != nullptr, true);
        }
        EXPECT_EQ(_task_queue->take(0), nullptr);
        for (int j = 0; j < parallelism; j++) {
            EXPECT_EQ(_pipeline_tasks[1][j]->_is_blocked(), false);
        }
    }
    {
        // Pipeline 1 ran first and build hash table in join build operator.
        for (int j = 0; j < parallelism; j++) {
            bool eos = false;
            EXPECT_EQ(_pipeline_tasks[1][j]->execute(&eos), Status::OK());
            EXPECT_EQ(eos, false);
        }
        for (int j = 0; j < parallelism; j++) {
            auto& local_state =
                    _runtime_states[1][j]
                            ->get_local_state(_pipelines[1]->operators().front()->operator_id())
                            ->cast<ExchangeLocalState>();
            local_state.stream_recvr->_sender_queues[0]->decrement_senders(0);

            bool eos = false;
            EXPECT_EQ(_pipeline_tasks[1][j]->execute(&eos), Status::OK());
            EXPECT_EQ(_pipeline_tasks[1][j]->_is_blocked(), false);
            EXPECT_EQ(eos, true);
            auto& sink_local_state = _runtime_states[1][j]
                                             ->get_sink_local_state()
                                             ->cast<HashJoinBuildSinkLocalState>();
            EXPECT_EQ(
                    sink_local_state._runtime_filter_producer_helper->_skip_runtime_filters_process,
                    false);
            EXPECT_EQ(sink_local_state._runtime_filter_producer_helper->_producers.size(), 1);
            EXPECT_TRUE(
                    sink_local_state._runtime_filter_producer_helper->_producers[0]->_rf_state ==
                    RuntimeFilterProducer::State::WAITING_FOR_DATA);
            EXPECT_EQ(sink_local_state._runtime_filter_producer_helper->_producers[0]
                              ->_runtime_filter_type,
                      RuntimeFilterType::IN_OR_BLOOM_FILTER);
            EXPECT_EQ(_pipeline_tasks[1][j]->_is_pending_finish(), false);
            auto wrapper =
                    sink_local_state._runtime_filter_producer_helper->_producers[0]->_wrapper;
            EXPECT_EQ(_pipeline_tasks[1][j]->close(Status::OK()), Status::OK());
            EXPECT_EQ(wrapper->get_real_type(),
                      j == 0 ? RuntimeFilterType::IN_FILTER : RuntimeFilterType::BLOOM_FILTER)
                    << "  " << j << " "
                    << sink_local_state._runtime_filter_producer_helper->_producers[0]
                               ->debug_string();
            EXPECT_TRUE(wrapper->_state == RuntimeFilterWrapper::State::READY);

            if (j == 0) {
                EXPECT_EQ(wrapper->_hybrid_set->size(), 1);
            } else {
                EXPECT_EQ(wrapper->_bloom_filter_func->build_bf_by_runtime_size(), false);

                EXPECT_EQ(wrapper->_bloom_filter_func->_bloom_filter_length, 1048576);
            }
        }
    }
    {
        // Pipeline 0 ran once hash table is built.
        for (int j = 0; j < parallelism; j++) {
            EXPECT_EQ(_pipeline_tasks[0][j]->_is_blocked(), false);
        }
        for (int j = 0; j < parallelism; j++) {
            bool eos = false;
            EXPECT_EQ(_pipeline_tasks[0][j]->execute(&eos), Status::OK());
            EXPECT_EQ(eos, false);
        }
        for (int j = 0; j < parallelism; j++) {
            auto& local_state =
                    _runtime_states[0][j]
                            ->get_local_state(_pipelines[0]->operators().front()->operator_id())
                            ->cast<ExchangeLocalState>();
            local_state.stream_recvr->_sender_queues[0]->decrement_senders(0);

            bool eos = false;
            EXPECT_EQ(_pipeline_tasks[0][j]->execute(&eos), Status::OK());
            EXPECT_EQ(_pipeline_tasks[0][j]->_is_blocked(), false);
            EXPECT_EQ(eos, true);
            EXPECT_EQ(_pipeline_tasks[0][j]->_is_pending_finish(), false);
            EXPECT_EQ(_pipeline_tasks[0][j]->close(Status::OK()), Status::OK());
        }
    }
    {
        // [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] join [1, 1, 1, 1, 1, 1, 1, 1, 1, 1] produces 100 rows in instance 0.
        // [2, 2, 2, 2, 2, 2, 2, 2, 2, 2] join [2, 2, 2, 2, 2, 2, 2, 2, 2, 2] produces 100 rows in instance 1.
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.size(), 2);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.front()._block->rows(),
                  10 * 10);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_block_queue.back()._block->rows(), 10);
        EXPECT_EQ(downstream_recvr->_sender_queues[0]->_num_remaining_senders, 0);
    }
    downstream_recvr->close();
}

} // namespace doris::pipeline
