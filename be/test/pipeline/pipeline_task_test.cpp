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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "common/status.h"
#include "dummy_task_queue.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_thread_mem_tracker_mgr.h"
#include "testutil/mock/mock_workload_group_mgr.h"
#include "thrift_builder.h"

namespace doris::pipeline {

static void empty_function(RuntimeState*, Status*) {}

class PipelineTaskTest : public testing::Test {
public:
    PipelineTaskTest() : _obj_pool(new ObjectPool()) {}
    ~PipelineTaskTest() override = default;
    void SetUp() override {
        _thread_mem_tracker_mgr = std::move(thread_context()->thread_mem_tracker_mgr);
        thread_context()->thread_mem_tracker_mgr = std::make_unique<MockThreadMemTrackerMgr>();
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
        _task_queue = std::make_unique<DummyTaskQueue>(1);
        _build_fragment_context();
    }
    void TearDown() override {
        // Origin `thread_mem_tracker_mgr` must be restored otherwise `ThreadContextTest` will fail.
        thread_context()->thread_mem_tracker_mgr = std::move(_thread_mem_tracker_mgr);
    }

private:
    void _build_fragment_context() {
        int fragment_id = 0;
        _context = std::make_shared<PipelineFragmentContext>(
                _query_id, fragment_id, _query_ctx, ExecEnv::GetInstance(), empty_function,
                std::bind<Status>(std::mem_fn(&FragmentMgr::trigger_pipeline_context_report),
                                  ExecEnv::GetInstance()->fragment_mgr(), std::placeholders::_1,
                                  std::placeholders::_2));
        _runtime_state = std::make_unique<MockRuntimeState>(
                _query_id, fragment_id, _query_options, _query_ctx->query_globals,
                ExecEnv::GetInstance(), _query_ctx.get());
        _runtime_state->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(_context));
    }

    std::shared_ptr<ObjectPool> _obj_pool;
    std::shared_ptr<PipelineFragmentContext> _context;
    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<QueryContext> _query_ctx;
    TUniqueId _query_id = TUniqueId();
    std::unique_ptr<ThreadMemTrackerMgr> _thread_mem_tracker_mgr;
    TQueryOptions _query_options;
    std::unique_ptr<DummyTaskQueue> _task_queue;
    const std::string LOCALHOST = BackendOptions::get_localhost();
    const int DUMMY_PORT = config::brpc_port;
};

template class OperatorX<DummyOperatorLocalState>;
template class DataSinkOperatorX<DummySinkLocalState>;

TEST_F(PipelineTaskTest, TEST_CONSTRUCTOR) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));

    // shared state already exists
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    shared_state_map[3] = {std::make_shared<BasicSharedState>(), {}};
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    EXPECT_EQ(task->_sink_shared_state, nullptr);

    // shared state not exists
    shared_state_map.clear();
    task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                          profile.get(), shared_state_map, task_id);
    EXPECT_NE(task->_sink_shared_state, nullptr);
    EXPECT_EQ(task->_exec_state, PipelineTask::State::INITED);
}

TEST_F(PipelineTaskTest, TEST_PREPARE) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
}

TEST_F(PipelineTaskTest, TEST_PREPARE_ERROR) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    {
        _context.reset();
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_FALSE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::INITED);
    }
}

TEST_F(PipelineTaskTest, TEST_EXTRACT_DEPENDENCIES_ERROR) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    {
        EXPECT_FALSE(task->_extract_dependencies().ok());
        EXPECT_TRUE(task->_read_dependencies.empty());
        EXPECT_TRUE(task->_write_dependencies.empty());
        EXPECT_TRUE(task->_finish_dependencies.empty());
        EXPECT_TRUE(task->_spill_dependencies.empty());
    }
}

TEST_F(PipelineTaskTest, TEST_OPEN) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
    {
        EXPECT_TRUE(task->_open().ok());
        EXPECT_FALSE(task->_read_dependencies.empty());
        EXPECT_FALSE(task->_write_dependencies.empty());
        EXPECT_FALSE(task->_finish_dependencies.empty());
        EXPECT_FALSE(task->_spill_dependencies.empty());
        EXPECT_TRUE(task->_opened);
    }
}

TEST_F(PipelineTaskTest, TEST_EXECUTE) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    Dependency* read_dep;
    Dependency* write_dep;
    Dependency* source_finish_dep;
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    task->set_task_queue(_task_queue.get());
    {
        // `execute` should be called after `prepare`
        bool done = false;
        EXPECT_FALSE(task->execute(&done).ok());
    }
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
        read_dep = _runtime_state->get_local_state_result(task->_operators.front()->operator_id())
                           .value()
                           ->dependencies()
                           .front();
        write_dep = _runtime_state->get_sink_local_state()->dependencies().front();
    }
    {
        // task is blocked by execution dependency.
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_opened);
        EXPECT_FALSE(_query_ctx->get_execution_dependency()->ready());
        EXPECT_FALSE(_query_ctx->get_execution_dependency()->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::BLOCKED);
    }
    {
        // task is blocked by filter dependency.
        _query_ctx->get_execution_dependency()->set_ready();
        task->_execution_dependencies.back()->block();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_opened);
        EXPECT_FALSE(task->_execution_dependencies.back()->ready());
        EXPECT_FALSE(task->_execution_dependencies.back()->_blocked_task.empty());
        EXPECT_TRUE(task->_read_dependencies.empty());
        EXPECT_TRUE(task->_write_dependencies.empty());
        EXPECT_TRUE(task->_finish_dependencies.empty());
        EXPECT_TRUE(task->_spill_dependencies.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::BLOCKED);
    }
    {
        // `open` phase. And then task is blocked by read dependency.
        task->_execution_dependencies.back()->set_ready();
        read_dep->block();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_read_dependencies.empty());
        EXPECT_FALSE(task->_write_dependencies.empty());
        EXPECT_FALSE(task->_finish_dependencies.empty());
        EXPECT_FALSE(task->_spill_dependencies.empty());
        EXPECT_TRUE(task->_opened);
        EXPECT_FALSE(read_dep->ready());
        EXPECT_TRUE(write_dep->ready());
        EXPECT_FALSE(read_dep->_blocked_task.empty());
        source_finish_dep =
                _runtime_state->get_local_state_result(task->_operators.front()->operator_id())
                        .value()
                        ->finishdependency();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::BLOCKED);
    }
    {
        // `execute` phase. And then task is blocked by finish dependency.
        read_dep->set_ready();
        source_finish_dep->block();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        task->_operators.front()->cast<DummyOperator>()._eos = true;
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_TRUE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(source_finish_dep->ready());
        EXPECT_FALSE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::BLOCKED);
    }
    {
        // `execute` phase.
        source_finish_dep->set_ready();
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
    {
        EXPECT_TRUE(task->close(Status::OK()).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::FINISHED);
        EXPECT_TRUE(task->finalize().ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::FINALIZED);
    }
}

TEST_F(PipelineTaskTest, TEST_TERMINATE) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    task->set_task_queue(_task_queue.get());
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
    }
    _query_ctx->get_execution_dependency()->set_ready();
    {
        std::atomic_bool terminated = false;
        auto exec_func = [&]() {
            bool done = false;
            EXPECT_TRUE(task->execute(&done).ok());
            EXPECT_TRUE(terminated);
            EXPECT_TRUE(task->_eos);
            EXPECT_TRUE(done);
            EXPECT_TRUE(task->_wake_up_early);
            EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._terminated);
            EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._terminated);
            EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        };

        auto terminate_func = [&]() {
            // Sleep 0~5000ms randomly.
            std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 5000));
            terminated = true;
            task->set_wake_up_early();
            task->terminate();
        };

        std::thread exec_thread(exec_func);
        std::thread terminate_thread(terminate_func);
        exec_thread.join();
        terminate_thread.join();
    }
}

TEST_F(PipelineTaskTest, TEST_STATE_TRANSITION) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    task->set_task_queue(_task_queue.get());
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
    }
    for (int i = 0; i < task->LEGAL_STATE_TRANSITION.size(); i++) {
        auto target = (PipelineTask::State)i;
        for (int j = 0; j < task->LEGAL_STATE_TRANSITION.size(); j++) {
            task->_exec_state = (PipelineTask::State)j;
            EXPECT_EQ(task->_state_transition(target).ok(),
                      task->LEGAL_STATE_TRANSITION[i].contains((PipelineTask::State)j));
        }
    }
}

TEST_F(PipelineTaskTest, TEST_SINK_FINISHED) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    task->set_task_queue(_task_queue.get());
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
    }
    _query_ctx->get_execution_dependency()->set_ready();
    {
        auto& is_finished =
                _runtime_state->get_sink_local_state()->cast<DummySinkLocalState>()._is_finished;
        auto exec_func = [&]() {
            bool done = false;
            EXPECT_TRUE(task->execute(&done).ok());
            EXPECT_TRUE(is_finished);
            EXPECT_TRUE(task->_eos);
            EXPECT_TRUE(done);
            EXPECT_TRUE(task->_wake_up_early);
            EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._terminated);
            EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._terminated);
            EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        };

        auto finish_func = [&]() {
            // Sleep 0~5000ms randomly.
            std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() % 5000));
            is_finished = true;
        };

        auto finish_check_func = [&]() {
            while (!is_finished) {
                task->stop_if_finished();
            }
        };

        // Make sure `debug_string` will not be blocked.
        auto debug_string_func = [&]() {
            while (!is_finished) {
                static_cast<void>(task->debug_string());
            }
        };

        std::thread exec_thread(exec_func);
        std::thread finish_thread(finish_func);
        std::thread finish_check_thread(finish_check_func);
        std::thread debug_string_thread(debug_string_func);
        exec_thread.join();
        finish_thread.join();
        finish_check_thread.join();
        debug_string_thread.join();
    }
}

TEST_F(PipelineTaskTest, TEST_SINK_EOF) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->_exec_time_slice = 10'000'000'000ULL;
    task->set_task_queue(_task_queue.get());
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
    }
    _query_ctx->get_execution_dependency()->set_ready();
    {
        task->_operators.front()->cast<DummyOperator>()._eos = true;
        task->_sink->cast<DummySinkOperatorX>()._return_eof = true;
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._return_eof);
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(done);
        EXPECT_TRUE(task->_wake_up_early);
        EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._terminated);
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._terminated);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
}

TEST_F(PipelineTaskTest, TEST_RESERVE_MEMORY) {
    {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .set_enable_reserve_memory(true)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _task_queue = std::make_unique<DummyTaskQueue>(1);
        _build_fragment_context();

        TWorkloadGroupInfo twg_info;
        twg_info.__set_id(0);
        twg_info.__set_name("_dummpy_workload_group");
        twg_info.__set_version(0);

        WorkloadGroupInfo workload_group_info = WorkloadGroupInfo::parse_topic_info(twg_info);

        ((MockRuntimeState*)_runtime_state.get())->_workload_group =
                std::make_shared<WorkloadGroup>(workload_group_info);
        ((MockThreadMemTrackerMgr*)thread_context()->thread_mem_tracker_mgr.get())
                ->_test_low_memory = true;
    }
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    Dependency* read_dep;
    Dependency* write_dep;
    Dependency* source_finish_dep;
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->set_task_queue(_task_queue.get());
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
        read_dep = _runtime_state->get_local_state_result(task->_operators.front()->operator_id())
                           .value()
                           ->dependencies()
                           .front();
        write_dep = _runtime_state->get_sink_local_state()->dependencies().front();
    }
    {
        _query_ctx->get_execution_dependency()->set_ready();
        // Task is blocked by read dependency.
        read_dep->block();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_read_dependencies.empty());
        EXPECT_FALSE(task->_write_dependencies.empty());
        EXPECT_FALSE(task->_finish_dependencies.empty());
        EXPECT_FALSE(task->_spill_dependencies.empty());
        EXPECT_TRUE(task->_opened);
        EXPECT_FALSE(read_dep->ready());
        EXPECT_TRUE(write_dep->ready());
        EXPECT_FALSE(read_dep->_blocked_task.empty());
        source_finish_dep =
                _runtime_state->get_local_state_result(task->_operators.front()->operator_id())
                        .value()
                        ->finishdependency();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::BLOCKED);
    }
    {
        // set low memory mode and do not pause.
        read_dep->set_ready();
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_FALSE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
    {
        // set low memory mode and do not pause.
        task->_operators.front()->cast<DummyOperator>()._eos = true;
        _query_ctx->resource_ctx()->task_controller()->set_low_memory_mode(false);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_spilling);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
}

TEST_F(PipelineTaskTest, TEST_RESERVE_MEMORY_FAIL) {
    {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .set_enable_reserve_memory(true)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _task_queue = std::make_unique<DummyTaskQueue>(1);
        _build_fragment_context();

        TWorkloadGroupInfo twg_info;
        twg_info.__set_id(0);
        twg_info.__set_name("_dummpy_workload_group");
        twg_info.__set_version(0);

        WorkloadGroupInfo workload_group_info = WorkloadGroupInfo::parse_topic_info(twg_info);

        ((MockRuntimeState*)_runtime_state.get())->_workload_group =
                std::make_shared<WorkloadGroup>(workload_group_info);
        ((MockThreadMemTrackerMgr*)thread_context()->thread_mem_tracker_mgr.get())
                ->_test_low_memory = true;

        ExecEnv::GetInstance()->_workload_group_manager = new MockWorkloadGroupMgr();
    }
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    Dependency* read_dep;
    Dependency* write_dep;
    Dependency* source_finish_dep;
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        sink_op->_spillable = true;
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    task->set_task_queue(_task_queue.get());
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
        read_dep = _runtime_state->get_local_state_result(task->_operators.front()->operator_id())
                           .value()
                           ->dependencies()
                           .front();
        write_dep = _runtime_state->get_sink_local_state()->dependencies().front();
    }
    {
        _query_ctx->get_execution_dependency()->set_ready();
        // Task is blocked by read dependency.
        read_dep->block();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_read_dependencies.empty());
        EXPECT_FALSE(task->_write_dependencies.empty());
        EXPECT_FALSE(task->_finish_dependencies.empty());
        EXPECT_FALSE(task->_spill_dependencies.empty());
        EXPECT_TRUE(task->_opened);
        EXPECT_FALSE(read_dep->ready());
        EXPECT_TRUE(write_dep->ready());
        EXPECT_FALSE(read_dep->_blocked_task.empty());
        source_finish_dep =
                _runtime_state->get_local_state_result(task->_operators.front()->operator_id())
                        .value()
                        ->finishdependency();
        EXPECT_EQ(task->_exec_state, PipelineTask::State::BLOCKED);
    }
    {
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size =
                vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM + 1;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size =
                vectorized::SpillStream::MIN_SPILL_WRITE_BATCH_MEM + 1;
    }
    {
        // Reserve failed and paused.
        read_dep->set_ready();
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_FALSE(task->_spilling);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_FALSE(task->_eos);
        EXPECT_TRUE(task->_spilling);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_TRUE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    {
        // Reserve failed and paused.
        task->_operators.front()->cast<DummyOperator>()._disable_reserve_mem = true;
        task->_spilling = false;
        task->_operators.front()->cast<DummyOperator>()._eos = true;
        ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused = false;
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(task->_spilling);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_TRUE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    {
        // Reserve failed and paused.
        ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused = false;
        task->_sink->cast<DummySinkOperatorX>()._disable_reserve_mem = true;
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_TRUE(task->_eos);
        EXPECT_FALSE(task->_spilling);
        EXPECT_TRUE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_FALSE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    {
        EXPECT_FALSE(task->is_revoking());
        task->_spill_dependencies.front()->block();
        EXPECT_TRUE(task->is_revoking());
        EXPECT_FALSE(task->_spill_dependencies.front()->ready());
        EXPECT_TRUE(task->_spill_dependencies.front()->_blocked_task.empty());
        task->_spill_dependencies.front()->set_ready();
    }
    {
        EXPECT_FALSE(task->_is_blocked());
        task->_spill_dependencies.front()->block();
        EXPECT_TRUE(task->_is_blocked());
        EXPECT_FALSE(task->_spill_dependencies.front()->ready());
        EXPECT_FALSE(task->_spill_dependencies.front()->_blocked_task.empty());
        task->_spill_dependencies.front()->set_ready();
    }
    {
        EXPECT_FALSE(task->_is_pending_finish());
        task->_spill_dependencies.front()->block();
        EXPECT_TRUE(task->_is_pending_finish());
        EXPECT_FALSE(task->_spill_dependencies.front()->ready());
        EXPECT_FALSE(task->_spill_dependencies.front()->_blocked_task.empty());
        task->_spill_dependencies.front()->set_ready();
    }
    delete ExecEnv::GetInstance()->_workload_group_manager;
}

TEST_F(PipelineTaskTest, TEST_INJECT_SHARED_STATE) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        // 1. create and set the source operator of multi_cast_data_stream_source for new pipeline
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        int op_id = 1;
        int node_id = 2;
        int dest_id = 3;
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(op_id, node_id, dest_id));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);
    {
        // `_sink_shared_state` is created in constructor.
        EXPECT_NE(task->_sink_shared_state, nullptr);
        task->_sink_shared_state = nullptr;
    }
    {
        std::shared_ptr<BasicSharedState> shared_state = nullptr;
        EXPECT_FALSE(task->inject_shared_state(shared_state));
    }
    {
        auto shared_state = BasicSharedState::create_shared();
        shared_state->related_op_ids.insert(0);
        EXPECT_TRUE(task->inject_shared_state(shared_state));
    }
    {
        auto shared_state = BasicSharedState::create_shared();
        shared_state->related_op_ids.insert(3);
        EXPECT_TRUE(task->inject_shared_state(shared_state));
    }
    {
        auto shared_state = BasicSharedState::create_shared();
        shared_state->related_op_ids.insert(1);
        EXPECT_FALSE(task->inject_shared_state(shared_state));
    }
}

} // namespace doris::pipeline
