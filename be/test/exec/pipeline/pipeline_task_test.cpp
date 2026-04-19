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

#include "common/config.h"
#include "common/status.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/dummy_task_queue.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/pipeline/thrift_builder.h"
#include "exec/spill/spill_file.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_thread_mem_tracker_mgr.h"
#include "testutil/mock/mock_workload_group_mgr.h"
#include "util/debug_points.h"

namespace doris {

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
        _task_scheduler = std::make_unique<MockTaskScheduler>();
        _query_ctx->_task_scheduler = _task_scheduler.get();
        _build_fragment_context();
    }
    void TearDown() override {
        // Origin `thread_mem_tracker_mgr` must be restored otherwise `ThreadContextTest` will fail.
        thread_context()->thread_mem_tracker_mgr = std::move(_thread_mem_tracker_mgr);
    }

private:
    void _build_fragment_context() {
        int fragment_id = 0;
        _context = std::make_shared<PipelineFragmentContext>(_query_id, TPipelineFragmentParams(),
                                                             _query_ctx, ExecEnv::GetInstance(),
                                                             empty_function);
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
    std::unique_ptr<MockTaskScheduler> _task_scheduler;
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
            task->unblock_all_dependencies();
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
    {
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_GT(task->_execution_dependencies.size(), 1);
    }
    // Test normal LEGAL_STATE_TRANSITION table (with _wake_up_early = false).
    task->_wake_up_early = false;
    for (int i = 0; i < task->LEGAL_STATE_TRANSITION.size(); i++) {
        auto target = (PipelineTask::State)i;
        for (int j = 0; j < task->LEGAL_STATE_TRANSITION.size(); j++) {
            auto from = (PipelineTask::State)j;
            task->_exec_state = from;
            EXPECT_EQ(task->_state_transition(target).ok(),
                      task->LEGAL_STATE_TRANSITION[i].contains(from));
        }
    }

    // Test WAKE_UP_EARLY_LEGAL_STATE_TRANSITION table.
    task->_wake_up_early = true;
    for (int i = 0; i < task->WAKE_UP_EARLY_LEGAL_STATE_TRANSITION.size(); i++) {
        auto target = (PipelineTask::State)i;
        for (int j = 0; j < task->WAKE_UP_EARLY_LEGAL_STATE_TRANSITION.size(); j++) {
            auto from = (PipelineTask::State)j;
            task->_exec_state = from;
            EXPECT_EQ(task->_state_transition(target).ok(),
                      task->WAKE_UP_EARLY_LEGAL_STATE_TRANSITION[i].contains(from));
        }
    }

    // FINISHED→RUNNABLE under wake_up_early is legal but no-op: state stays FINISHED.
    task->_exec_state = PipelineTask::State::FINISHED;
    EXPECT_TRUE(task->_state_transition(PipelineTask::State::RUNNABLE).ok());
    EXPECT_EQ(task->_exec_state, PipelineTask::State::FINISHED);

    // FINALIZED→RUNNABLE under wake_up_early is legal but no-op: state stays FINALIZED.
    task->_exec_state = PipelineTask::State::FINALIZED;
    EXPECT_TRUE(task->_state_transition(PipelineTask::State::RUNNABLE).ok());
    EXPECT_EQ(task->_exec_state, PipelineTask::State::FINALIZED);

    // BLOCKED→FINISHED under wake_up_early does transition.
    task->_exec_state = PipelineTask::State::BLOCKED;
    EXPECT_TRUE(task->_state_transition(PipelineTask::State::FINISHED).ok());
    EXPECT_EQ(task->_exec_state, PipelineTask::State::FINISHED);
    task->_wake_up_early = false;

    // Test that wake_up() succeeds when the task has already finished (delayed wake_up race).
    // _state_transition(RUNNABLE) is a no-op, and wake_up() must NOT re-submit the task to the
    // scheduler. Submitting a finalized task causes SIGSEGV in is_blockable() because _sink is null.
    {
        task->_wake_up_early = true;
        _task_scheduler->reset_submit_count();
        std::mutex mtx;
        task->_exec_state = PipelineTask::State::FINISHED;
        auto dep = std::make_shared<Dependency>(0, 0, "test_dep", true);
        task->_blocked_dep = dep.get();
        std::unique_lock<std::mutex> lc(mtx);
        task->wake_up(dep.get(), lc);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::FINISHED);
        EXPECT_EQ(_task_scheduler->submit_count(), 0);
    }
    {
        _task_scheduler->reset_submit_count();
        std::mutex mtx;
        task->_exec_state = PipelineTask::State::FINALIZED;
        auto dep = std::make_shared<Dependency>(0, 0, "test_dep", true);
        task->_blocked_dep = dep.get();
        std::unique_lock<std::mutex> lc(mtx);
        task->wake_up(dep.get(), lc);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::FINALIZED);
        EXPECT_EQ(_task_scheduler->submit_count(), 0);
        task->_wake_up_early = false;
    }
    // Positive test: wake_up() on a BLOCKED task DOES submit to the scheduler.
    {
        _task_scheduler->reset_submit_count();
        std::mutex mtx;
        task->_exec_state = PipelineTask::State::BLOCKED;
        auto dep = std::make_shared<Dependency>(0, 0, "test_dep", true);
        task->_blocked_dep = dep.get();
        std::unique_lock<std::mutex> lc(mtx);
        task->wake_up(dep.get(), lc);
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_EQ(_task_scheduler->submit_count(), 1);
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
        _task_scheduler = std::make_unique<MockTaskScheduler>();
        _query_ctx->_task_scheduler = _task_scheduler.get();
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
        // Not check low memory mode here, because we temporary not use this feature, the
        // system buffer should be checked globally.
        // EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        // EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        // EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
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
        // EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->low_memory_mode());
        // EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._low_memory_mode);
        // EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._low_memory_mode);
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_FALSE(task->_spilling);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
    }
}

// Test for reserve memory fail for non-spillable task. It will not affect anything, the query
// will continue to run. And will disable reserve memory, so that the query will failed when allocated
// memory > limit.
TEST_F(PipelineTaskTest, TEST_RESERVE_MEMORY_FAIL) {
    {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .set_enable_reserve_memory(true)
                                 .set_enable_spill(false)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _task_scheduler = std::make_unique<MockTaskScheduler>();
        _query_ctx->_task_scheduler = _task_scheduler.get();
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
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size =
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
    }
    {
        // Reserve failed and but not enable spill disk, so that the query will continue to run.
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
        // Not enable spill disk, so that task will not be paused.
        EXPECT_FALSE(task->_spilling);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->is_enable_reserve_memory());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_FALSE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    {
        // Reserve failed .
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
        EXPECT_FALSE(task->_spilling);
        EXPECT_TRUE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->is_enable_reserve_memory());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_FALSE(
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
        EXPECT_FALSE(_query_ctx->resource_ctx()->task_controller()->is_enable_reserve_memory());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_FALSE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    delete ExecEnv::GetInstance()->_workload_group_manager;
    ExecEnv::GetInstance()->_workload_group_manager = nullptr;
}

// Test reserve memory fail for spillable pipeline task
TEST_F(PipelineTaskTest, TEST_RESERVE_MEMORY_FAIL_SPILLABLE) {
    {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .set_enable_reserve_memory(true)
                                 .set_enable_spill(true)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _task_scheduler = std::make_unique<MockTaskScheduler>();
        _query_ctx->_task_scheduler = _task_scheduler.get();
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
        EXPECT_TRUE(_runtime_state->enable_spill());
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
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size =
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
    }
    {
        // Reserve failed and enable spill disk, so that the query be paused.
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
        // Not enable spill disk, so that task will not be paused.
        EXPECT_TRUE(task->_spilling);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
        EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->is_enable_reserve_memory());
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
        EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->is_enable_reserve_memory());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_TRUE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    {
        ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused = false;
        // Disable reserve memory, so that the get_reserve_mem_size == 0, so that reserve will always success
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
        EXPECT_TRUE(_query_ctx->resource_ctx()->task_controller()->is_enable_reserve_memory());
        EXPECT_TRUE(source_finish_dep->ready());
        EXPECT_TRUE(source_finish_dep->_blocked_task.empty());
        EXPECT_EQ(task->_exec_state, PipelineTask::State::RUNNABLE);
        EXPECT_FALSE(
                ((MockWorkloadGroupMgr*)ExecEnv::GetInstance()->_workload_group_manager)->_paused);
    }
    delete ExecEnv::GetInstance()->_workload_group_manager;
    ExecEnv::GetInstance()->_workload_group_manager = nullptr;
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

TEST_F(PipelineTaskTest, TEST_SHOULD_TRIGGER_REVOKING) {
    {
        _query_options = TQueryOptionsBuilder()
                                 .set_enable_local_exchange(true)
                                 .set_enable_local_shuffle(true)
                                 .set_runtime_filter_max_in_num(15)
                                 .set_enable_reserve_memory(true)
                                 .set_enable_spill(true)
                                 .build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), _query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _task_scheduler = std::make_unique<MockTaskScheduler>();
        _query_ctx->_task_scheduler = _task_scheduler.get();
        _build_fragment_context();
    }
    TWorkloadGroupInfo twg_info;
    twg_info.__set_id(0);
    twg_info.__set_name("test_wg");
    twg_info.__set_version(0);
    auto wg = std::make_shared<WorkloadGroup>(WorkloadGroupInfo::parse_topic_info(twg_info));
    const int64_t wg_mem_limit = 1000LL * 1024 * 1024; // 1 GB
    wg->_memory_limit = wg_mem_limit;
    wg->_memory_low_watermark = 50;  // 50%
    wg->_memory_high_watermark = 80; // 80%
    wg->_total_mem_used = 0;
    _query_ctx->_resource_ctx->set_workload_group(wg);

    auto query_mem_tracker = _query_ctx->query_mem_tracker();
    query_mem_tracker->set_limit(wg_mem_limit);

    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(1, 2, 3));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());
    }
    auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            shared_state_map;
    _runtime_state->resize_op_id_to_local_state(-1);
    auto task = std::make_shared<PipelineTask>(pip, task_id, _runtime_state.get(), _context,
                                               profile.get(), shared_state_map, task_id);

    // reserve_size that passes the (reserve * parallelism > query_limit / 5) gate
    const size_t reserve_size = wg_mem_limit / 4; // 250MB > threshold of 200MB

    // Case 1: spill disabled -> false
    {
        ((MockRuntimeState*)_runtime_state.get())->set_enable_spill(false);
        EXPECT_FALSE(task->_should_trigger_revoking(reserve_size));
        ((MockRuntimeState*)_runtime_state.get())->set_enable_spill(true);
    }
    // Case 2: no workload group -> false
    {
        _query_ctx->_resource_ctx->set_workload_group(nullptr);
        EXPECT_FALSE(task->_should_trigger_revoking(reserve_size));
        _query_ctx->_resource_ctx->set_workload_group(wg);
    }
    // Case 3: effective query_limit = 0 (both tracker limit <= 0 and wg limit = 0) -> false
    {
        wg->_memory_limit = 0;
        query_mem_tracker->set_limit(-1);
        EXPECT_FALSE(task->_should_trigger_revoking(reserve_size));
        wg->_memory_limit = wg_mem_limit;
        query_mem_tracker->set_limit(wg_mem_limit);
    }
    // Case 4: reserve_size too small (reserve * parallelism <= query_limit / 5) -> false
    { EXPECT_FALSE(task->_should_trigger_revoking(wg_mem_limit / 5)); }
    // Case 5: no memory pressure (neither query tracker nor wg watermark) -> false
    {
        // consumption + reserve = 100MB + 250MB = 350MB < 90% of 1GB (900MB); wg not at watermark
        query_mem_tracker->consume(100LL * 1024 * 1024);
        EXPECT_FALSE(task->_should_trigger_revoking(reserve_size));
        query_mem_tracker->release(100LL * 1024 * 1024);
    }
    // Case 6: high memory pressure via query tracker, no revocable memory -> false
    {
        // consumption + reserve >= 90% of query_limit
        const int64_t consumption = int64_t(0.9 * wg_mem_limit) - int64_t(reserve_size) + 1;
        query_mem_tracker->consume(consumption);
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
        EXPECT_FALSE(task->_should_trigger_revoking(reserve_size));
        query_mem_tracker->release(consumption);
    }
    // Case 7: high pressure via query tracker, sufficient revocable -> true
    {
        const int64_t consumption = int64_t(0.9 * wg_mem_limit) - int64_t(reserve_size) + 1;
        query_mem_tracker->consume(consumption);
        // total revocable >= 20% of query_limit = 200MB (100MB each from op and sink)
        const size_t revocable = int64_t(0.2 * wg_mem_limit) / 2;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = revocable;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = revocable;
        EXPECT_TRUE(task->_should_trigger_revoking(reserve_size));
        query_mem_tracker->release(consumption);
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
    }
    // Case 8: high pressure via wg low watermark, sufficient revocable -> true
    {
        // wg total_mem_used > 50% of wg_limit -> low watermark triggered
        wg->_total_mem_used = int64_t(0.51 * wg_mem_limit);
        const size_t revocable = int64_t(0.2 * wg_mem_limit) / 2;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = revocable;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = revocable;
        EXPECT_TRUE(task->_should_trigger_revoking(reserve_size));
        wg->_total_mem_used = 0;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
    }
    // Case 9: high pressure via wg high watermark, sufficient revocable -> true
    {
        // wg total_mem_used > 80% of wg_limit -> high watermark triggered
        wg->_total_mem_used = int64_t(0.81 * wg_mem_limit);
        const size_t revocable = int64_t(0.2 * wg_mem_limit) / 2;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = revocable;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = revocable;
        EXPECT_TRUE(task->_should_trigger_revoking(reserve_size));
        wg->_total_mem_used = 0;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
    }
    // Case 10: query_limit capped to wg limit when tracker limit > wg limit -> no extra pressure
    {
        // effective limit = wg_mem_limit; reserve = wg_mem_limit/4 > threshold (wg_mem_limit/5)
        // but no consumption added, so no pressure -> false
        query_mem_tracker->set_limit(wg_mem_limit * 2);
        wg->_memory_limit = wg_mem_limit;
        EXPECT_FALSE(task->_should_trigger_revoking(reserve_size));
        query_mem_tracker->set_limit(wg_mem_limit);
    }
}

TEST_F(PipelineTaskTest, TEST_DO_REVOKE_MEMORY) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());

        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(1, 2, 3));
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
        std::vector<TScanRangeParams> scan_range;
        int sender_id = 0;
        TDataSink tsink;
        EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
        _query_ctx->get_execution_dependency()->set_ready();
    }
    // Case 1: fragment context expired -> InternalError
    {
        task->_fragment_context = std::weak_ptr<PipelineFragmentContext>();
        EXPECT_FALSE(task->do_revoke_memory(nullptr).ok());
        // Restore the fragment context
        task->_fragment_context = _context;
    }
    // Case 2: operators below MIN threshold, null spill_context -> no revoke_memory called
    {
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
        EXPECT_TRUE(task->do_revoke_memory(nullptr).ok());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._revoke_called);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._revoke_called);
    }
    // Case 3: operator has sufficient revocable memory -> operator revoke_memory called
    {
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size =
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
        task->_operators.front()->cast<DummyOperator>()._revoke_called = false;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revoke_called = false;
        EXPECT_TRUE(task->do_revoke_memory(nullptr).ok());
        EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._revoke_called);
        EXPECT_FALSE(task->_sink->cast<DummySinkOperatorX>()._revoke_called);
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_operators.front()->cast<DummyOperator>()._revoke_called = false;
    }
    // Case 4: sink has sufficient revocable memory -> sink revoke_memory called
    {
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_operators.front()->cast<DummyOperator>()._revoke_called = false;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size =
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
        task->_sink->cast<DummySinkOperatorX>()._revoke_called = false;
        EXPECT_TRUE(task->do_revoke_memory(nullptr).ok());
        EXPECT_FALSE(task->_operators.front()->cast<DummyOperator>()._revoke_called);
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._revoke_called);
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revoke_called = false;
    }
    // Case 5: non-null spill_context -> on_task_finished called, callback fires
    {
        bool callback_fired = false;
        auto spill_ctx = std::make_shared<SpillContext>(
                1, _query_id, [&callback_fired](SpillContext*) { callback_fired = true; });
        EXPECT_TRUE(task->do_revoke_memory(spill_ctx).ok());
        EXPECT_TRUE(callback_fired);
        EXPECT_EQ(spill_ctx->running_tasks_count.load(), 0);
    }
    // Case 6: wake_up_early -> operators terminated, eos set, callback fires
    {
        task->_wake_up_early = true;
        task->_eos = false;
        task->_operators.front()->cast<DummyOperator>()._terminated = false;
        task->_sink->cast<DummySinkOperatorX>()._terminated = false;
        bool callback_fired = false;
        auto spill_ctx = std::make_shared<SpillContext>(
                1, _query_id, [&callback_fired](SpillContext*) { callback_fired = true; });
        EXPECT_TRUE(task->do_revoke_memory(spill_ctx).ok());
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._terminated);
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._terminated);
        EXPECT_TRUE(callback_fired);
        task->_wake_up_early = false;
    }
}

TEST_F(PipelineTaskTest, TEST_REVOKE_MEMORY) {
    // Case 1: task is finalized -> on_task_finished called immediately
    {
        auto num_instances = 1;
        auto pip_id = 0;
        auto task_id = 0;
        auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
        OperatorPtr source_op;
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(1, 2, 3));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());

        auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        auto rs = std::make_unique<MockRuntimeState>(_query_id, 0, _query_options,
                                                     _query_ctx->query_globals,
                                                     ExecEnv::GetInstance(), _query_ctx.get());
        rs->set_task_execution_context(std::static_pointer_cast<TaskExecutionContext>(_context));
        rs->resize_op_id_to_local_state(-1);
        auto task = std::make_shared<PipelineTask>(pip, task_id, rs.get(), _context, profile.get(),
                                                   shared_state_map, task_id);
        {
            std::vector<TScanRangeParams> scan_range;
            int sender_id = 0;
            TDataSink tsink;
            EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
            _query_ctx->get_execution_dependency()->set_ready();
        }
        task->_exec_state = PipelineTask::State::FINALIZED;
        EXPECT_TRUE(task->is_finalized());
        bool callback_fired = false;
        auto spill_ctx = std::make_shared<SpillContext>(
                1, _query_id, [&callback_fired](SpillContext*) { callback_fired = true; });
        EXPECT_TRUE(task->revoke_memory(spill_ctx).ok());
        EXPECT_TRUE(callback_fired);
        EXPECT_EQ(spill_ctx->running_tasks_count.load(), 0);
    }
    // Case 2: _opened=true, revocable < MIN -> on_task_finished called immediately
    {
        auto num_instances = 1;
        auto pip_id = 0;
        auto task_id = 0;
        auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
        OperatorPtr source_op;
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(1, 2, 3));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());

        auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        auto rs = std::make_unique<MockRuntimeState>(_query_id, 0, _query_options,
                                                     _query_ctx->query_globals,
                                                     ExecEnv::GetInstance(), _query_ctx.get());
        rs->set_task_execution_context(std::static_pointer_cast<TaskExecutionContext>(_context));
        rs->resize_op_id_to_local_state(-1);
        auto task = std::make_shared<PipelineTask>(pip, task_id, rs.get(), _context, profile.get(),
                                                   shared_state_map, task_id);
        {
            std::vector<TScanRangeParams> scan_range;
            int sender_id = 0;
            TDataSink tsink;
            EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
            _query_ctx->get_execution_dependency()->set_ready();
        }
        task->_opened = true;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size = 0;
        task->_sink->cast<DummySinkOperatorX>()._revocable_mem_size = 0;
        bool callback_fired = false;
        auto spill_ctx = std::make_shared<SpillContext>(
                1, _query_id, [&callback_fired](SpillContext*) { callback_fired = true; });
        EXPECT_TRUE(task->revoke_memory(spill_ctx).ok());
        EXPECT_TRUE(callback_fired);
        EXPECT_EQ(spill_ctx->running_tasks_count.load(), 0);
    }
    // Case 3: _opened=true, sufficient revocable -> RevokableTask submitted; run it to fire callback
    {
        auto num_instances = 1;
        auto pip_id = 0;
        auto task_id = 0;
        auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
        OperatorPtr source_op;
        source_op.reset(new DummyOperator());
        EXPECT_TRUE(pip->add_operator(source_op, num_instances).ok());
        DataSinkOperatorPtr sink_op;
        sink_op.reset(new DummySinkOperatorX(1, 2, 3));
        EXPECT_TRUE(pip->set_sink(sink_op).ok());

        auto profile = std::make_shared<RuntimeProfile>("Pipeline : " + std::to_string(pip_id));
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        auto rs = std::make_unique<MockRuntimeState>(_query_id, 0, _query_options,
                                                     _query_ctx->query_globals,
                                                     ExecEnv::GetInstance(), _query_ctx.get());
        rs->set_task_execution_context(std::static_pointer_cast<TaskExecutionContext>(_context));
        rs->resize_op_id_to_local_state(-1);
        auto task = std::make_shared<PipelineTask>(pip, task_id, rs.get(), _context, profile.get(),
                                                   shared_state_map, task_id);
        {
            std::vector<TScanRangeParams> scan_range;
            int sender_id = 0;
            TDataSink tsink;
            EXPECT_TRUE(task->prepare(scan_range, sender_id, tsink).ok());
            _query_ctx->get_execution_dependency()->set_ready();
        }
        task->_opened = true;
        task->_operators.front()->cast<DummyOperator>()._revocable_mem_size =
                SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
        bool callback_fired = false;
        auto spill_ctx = std::make_shared<SpillContext>(
                1, _query_id, [&callback_fired](SpillContext*) { callback_fired = true; });
        EXPECT_TRUE(task->revoke_memory(spill_ctx).ok());
        // RevokableTask submitted but not yet executed, callback not fired
        EXPECT_FALSE(callback_fired);
        EXPECT_EQ(spill_ctx->running_tasks_count.load(), 1);

        // Take the submitted RevokableTask from the scheduler queue and run it
        auto revokable_task =
                ((MockTaskScheduler*)_query_ctx->_task_scheduler)->_task_queue->take(0);
        EXPECT_NE(revokable_task, nullptr);
        bool done = false;
        EXPECT_TRUE(revokable_task->execute(&done).ok());
        // After execution, spill_context->on_task_finished() was called inside do_revoke_memory
        EXPECT_TRUE(callback_fired);
        EXPECT_EQ(spill_ctx->running_tasks_count.load(), 0);
    }
}

// Test for the race condition fix between _wake_up_early and _is_pending_finish().
//
// The race: Pipeline::make_all_runnable() writes in order (A) set_wake_up_early -> (B) terminate()
// [sets finish_dep._always_ready]. In execute()'s Defer block, if Thread A reads _wake_up_early=false
// (A), then Thread B writes A and B, then Thread A reads _is_pending_finish()=false (due to
// _always_ready from B), Thread A would set *done=true without calling operator terminate().
//
// The fix: terminate() is called after _is_pending_finish() in the Defer. So if Thread A sees B's
// effect (_always_ready=true), it must also see A's effect (_wake_up_early=true) on the subsequent
// read, ensuring terminate() is always called.
//
// This test uses a debug point injected into the else-if branch to simulate the exact bad timing:
// the debug point fires set_wake_up_early() + terminate() after _is_pending_finish() returns false
// (due to finish_dep being naturally unblocked) but before the second _wake_up_early check.
TEST_F(PipelineTaskTest, TEST_TERMINATE_RACE_FIX) {
    auto num_instances = 1;
    auto pip_id = 0;
    auto task_id = 0;
    auto pip = std::make_shared<Pipeline>(pip_id, num_instances, num_instances);
    {
        OperatorPtr source_op;
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
    }
    _query_ctx->get_execution_dependency()->set_ready();

    // Get the sink's finish dependency and block it to simulate a pending async operation
    // (e.g. runtime filter size sync RPC in flight).
    auto* sink_finish_dep =
            _runtime_state->get_sink_local_state()->cast<DummySinkLocalState>().finishdependency();
    EXPECT_NE(sink_finish_dep, nullptr);
    sink_finish_dep->block();

    // Drive the task to EOS so it will enter the Defer's pending-finish check.
    task->_operators.front()->cast<DummyOperator>()._eos = true;
    {
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        // EOS reached but still blocked on finish dependency: not done yet.
        EXPECT_TRUE(task->_eos);
        EXPECT_FALSE(done);
        EXPECT_FALSE(task->_wake_up_early);
    }

    // Now unblock the finish dependency (simulates the async op completing) and activate the
    // debug point. The debug point fires inside the else-if branch — after _is_pending_finish()
    // returns false but before the second _wake_up_early read — and calls set_wake_up_early() +
    // terminate(). This precisely reproduces the race where Thread B's writes land between
    // Thread A's two reads of _wake_up_early.
    sink_finish_dep->set_ready();
    config::enable_debug_points = true;
    DebugPoints::instance()->add("PipelineTask::execute.wake_up_early_in_else_if");
    {
        bool done = false;
        EXPECT_TRUE(task->execute(&done).ok());
        EXPECT_TRUE(task->_eos);
        EXPECT_TRUE(done);
        // The key assertion: even though the task took the else-if path (not the
        // if(_wake_up_early) path), operator terminate() must have been called because the
        // second read of _wake_up_early correctly observed the value set by the debug point.
        EXPECT_TRUE(task->_wake_up_early);
        EXPECT_TRUE(task->_operators.front()->cast<DummyOperator>()._terminated);
        EXPECT_TRUE(task->_sink->cast<DummySinkOperatorX>()._terminated);
    }
    DebugPoints::instance()->clear();
    config::enable_debug_points = false;
}

} // namespace doris
