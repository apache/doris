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

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "exec/operator/operator.h"
#include "exec/operator/spill_utils.h"
#include "exec/pipeline/dependency.h"
#include "exec/pipeline/dummy_task_queue.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "exec/pipeline/pipeline_task.h"
#include "exec/pipeline/revokable_task.h"
#include "exec/pipeline/thrift_builder.h"
#include "exec/spill/spill_file.h"
#include "runtime/exec_env.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "runtime/workload_management/query_task_controller.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_thread_mem_tracker_mgr.h"
#include "util/defer_op.h"

namespace doris {
namespace {

class CompletionFailureSource final : public OperatorX<DummyOperatorLocalState> {
public:
    CompletionFailureSource() : OperatorX<DummyOperatorLocalState>(nullptr, 0, 0) {}
    bool is_source() const override { return true; }
    Status get_block_impl(RuntimeState*, Block*, bool* eos) override {
        *eos = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState*) const override { return revocable_size; }
    Status revoke_memory(RuntimeState*) override {
        ++revoke_calls;
        if (throw_on_revoke) {
            throw Exception(revoke_status);
        }
        return revoke_status;
    }
    Status terminate(RuntimeState*) override {
        ++terminate_calls;
        return terminate_status;
    }
    size_t revocable_size = SpillFile::MIN_SPILL_WRITE_BATCH_MEM + 1;
    int revoke_calls = 0;
    int terminate_calls = 0;
    bool throw_on_revoke = false;
    Status revoke_status = Status::OK();
    Status terminate_status = Status::OK();
};

class CompletionFailureSink final : public DataSinkOperatorX<DummySinkLocalState> {
public:
    CompletionFailureSink() : DataSinkOperatorX<DummySinkLocalState>(1, 2, 3) {}
    Status sink_impl(RuntimeState*, Block*, bool) override { return Status::OK(); }
    Status terminate(RuntimeState*) override {
        ++terminate_calls;
        return terminate_status;
    }
    int terminate_calls = 0;
    Status terminate_status = Status::OK();
};

class RejectingSpillScheduler final : public MockTaskScheduler {
public:
    Status submit(PipelineTaskSPtr task) override {
        auto spill_task = std::dynamic_pointer_cast<RevokableTask>(task);
        if (!spill_task) {
            ++waiter_submissions;
            waiter_status = task->runtime_state()->get_query_ctx()->exec_status();
            return MockTaskScheduler::submit(std::move(task));
        }
        ++attempts;
        if (last_context != spill_task->_spill_context) {
            last_context = spill_task->_spill_context;
            if (observe_completion) {
                auto original = last_context->all_tasks_finished_callback;
                last_context->all_tasks_finished_callback = [this,
                                                             original](SpillContext* context) {
                    ++completion_calls;
                    original(context);
                };
            }
        }
        if (attempts == reject_at) {
            if (throw_on_submit) {
                throw Exception(rejection);
            }
            return rejection;
        }
        if (execute_inline) {
            bool done = false;
            return spill_task->execute(&done);
        }
        return MockTaskScheduler::submit(std::move(task));
    }
    const Status rejection = Status::InternalError("injected spill scheduler rejection");
    std::shared_ptr<SpillContext> last_context;
    Status waiter_status = Status::OK();
    int attempts = 0;
    int reject_at = 1;
    int waiter_submissions = 0;
    int completion_calls = 0;
    bool observe_completion = false;
    bool throw_on_submit = false;
    bool execute_inline = false;
};

class ObservedSpillCompletionTask final : public PipelineTask {
public:
    using PipelineTask::PipelineTask;
    bool set_running(bool running) override {
        const auto previous = PipelineTask::set_running(running);
        if (!running && on_idle) {
            on_idle();
        }
        return previous;
    }
    std::function<void()> on_idle;
};

class SpillCompletionFailureTest : public testing::Test {
protected:
    void SetUp() override {
        _old_tracker = std::move(thread_context()->thread_mem_tracker_mgr);
        thread_context()->thread_mem_tracker_mgr = std::make_unique<MockThreadMemTrackerMgr>();
        _options = TQueryOptionsBuilder().set_runtime_filter_max_in_num(15).build();
        TNetworkAddress address;
        address.hostname = BackendOptions::get_localhost();
        address.port = config::brpc_port;
        _query = QueryContext::create(_query_id, ExecEnv::GetInstance(), _options, address, true,
                                      address, QuerySource::INTERNAL_FRONTEND);
        _scheduler = std::make_unique<RejectingSpillScheduler>();
        _query->_task_scheduler = _scheduler.get();
        _fragment = std::make_shared<PipelineFragmentContext>(_query_id, TPipelineFragmentParams(),
                                                              _query, ExecEnv::GetInstance(),
                                                              [](RuntimeState*, Status*) {});
        _state = std::make_unique<MockRuntimeState>(_query_id, 0, _options, _query->query_globals,
                                                    ExecEnv::GetInstance(), _query.get());
        _state->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(_fragment));
        _state->resize_op_id_to_local_state(-1);
        _pipeline = std::make_shared<Pipeline>(0, 1, 1);
        _source = std::make_shared<CompletionFailureSource>();
        _sink = std::make_shared<CompletionFailureSink>();
        OperatorPtr source = _source;
        DataSinkOperatorPtr sink = _sink;
        ASSERT_TRUE(_pipeline->add_operator(source, 1).ok());
        ASSERT_TRUE(_pipeline->set_sink(sink).ok());
        _profile = std::make_unique<RuntimeProfile>("SpillCompletionFailure");
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        _task = std::make_shared<ObservedSpillCompletionTask>(_pipeline, 0, _state.get(), _fragment,
                                                              _profile.get(), shared_state_map, 0);
        ASSERT_TRUE(_task->prepare({}, 0, TDataSink()).ok());
        _task->_opened = true;
        _query->get_execution_dependency()->set_ready();
        _query->set_memory_sufficient(false);
    }

    void TearDown() override {
        _scheduler.reset();
        _extra_states.clear();
        _task.reset();
        _state.reset();
        _fragment.reset();
        _query.reset();
        thread_context()->thread_mem_tracker_mgr = std::move(_old_tracker);
    }

    std::shared_ptr<SpillContext> context(int count = 1) {
        return std::make_shared<SpillContext>(count, _query_id, [this](SpillContext*) {
            ++_callbacks;
            _query->set_memory_sufficient(true);
        });
    }

    void expect_finished(const std::shared_ptr<SpillContext>& spill) {
        RecordProperty("remaining_spill_tasks", spill->running_tasks_count.load());
        RecordProperty("spill_callbacks", _callbacks);
        RecordProperty(
                "memory_dependency_blocked",
                _query->get_memory_sufficient_dependency()->is_blocked_by(nullptr) != nullptr);
        EXPECT_EQ(spill->running_tasks_count.load(), 0);
        EXPECT_EQ(_callbacks, 1);
        EXPECT_EQ(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    }

    void expect_batch_progress(const std::shared_ptr<SpillContext>& spill, int remaining,
                               int callbacks) {
        EXPECT_EQ(spill->running_tasks_count.load(), remaining);
        EXPECT_EQ(_scheduler->completion_calls, callbacks);
    }

    PipelineTaskSPtr make_task(int index) {
        auto state =
                std::make_unique<MockRuntimeState>(_query_id, 0, _options, _query->query_globals,
                                                   ExecEnv::GetInstance(), _query.get());
        state->set_task_execution_context(
                std::static_pointer_cast<TaskExecutionContext>(_fragment));
        state->resize_op_id_to_local_state(-1);
        std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                std::vector<std::shared_ptr<Dependency>>>>
                shared_state_map;
        auto task = std::make_shared<PipelineTask>(_pipeline, index, state.get(), _fragment,
                                                   _profile.get(), shared_state_map, index);
        EXPECT_TRUE(task->prepare({}, 0, TDataSink()).ok());
        task->_opened = true;
        _extra_states.emplace_back(std::move(state));
        return task;
    }

    PipelineTaskSPtr block_waiter() {
        auto waiter = make_task(10);
        auto* dep = _query->get_memory_sufficient_dependency();
        EXPECT_EQ(dep->is_blocked_by(waiter), dep);
        EXPECT_EQ(waiter->_exec_state, PipelineTask::State::BLOCKED);
        return waiter;
    }

    void select_tasks(int count) {
        _fragment->_tasks.resize(1);
        _fragment->_tasks[0].emplace_back(_task, std::move(_state));
        for (int i = 1; i < count; ++i) {
            auto task = make_task(i);
            _fragment->_tasks[0].emplace_back(std::move(task), std::move(_extra_states.back()));
            _extra_states.pop_back();
        }
        _query->_fragment_id_to_pipeline_ctx[0] = _fragment;
    }

    void expect_status(const Status& actual, const Status& expected) {
        EXPECT_EQ(actual.code(), expected.code());
        EXPECT_EQ(actual.msg(), expected.msg());
    }

    void expect_exception(const std::function<void()>& action, const Status& expected) {
        try {
            action();
            FAIL() << "Expected the original exception";
        } catch (const Exception& e) {
            expect_status(e.to_status(), expected);
        }
    }

    std::vector<std::unique_ptr<MockRuntimeState>> _extra_states;
    TUniqueId _query_id;
    TQueryOptions _options;
    std::unique_ptr<ThreadMemTrackerMgr> _old_tracker;
    std::shared_ptr<QueryContext> _query;
    std::unique_ptr<RejectingSpillScheduler> _scheduler;
    std::shared_ptr<PipelineFragmentContext> _fragment;
    std::unique_ptr<MockRuntimeState> _state;
    PipelinePtr _pipeline;
    std::shared_ptr<CompletionFailureSource> _source;
    std::shared_ptr<CompletionFailureSink> _sink;
    std::unique_ptr<RuntimeProfile> _profile;
    std::shared_ptr<ObservedSpillCompletionTask> _task;
    int _callbacks = 0;
};

TEST_F(SpillCompletionFailureTest, ExpiredFragmentStillFinishes) {
    auto spill = context();
    _task->_fragment_context.reset();
    const auto status = _task->do_revoke_memory(spill);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Fragment already finished"), std::string::npos);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, SchedulerRejectionStillFinishes) {
    auto spill = context();
    const auto status = _task->revoke_memory(spill);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("injected spill scheduler rejection"), std::string::npos);
    EXPECT_EQ(_scheduler->attempts, 1);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, RootTerminateFailureStillFinishes) {
    auto spill = context();
    _task->_wake_up_early = true;
    _source->terminate_status = Status::InternalError("injected root terminate failure");
    expect_exception([&]() { static_cast<void>(_task->do_revoke_memory(spill)); },
                     _source->terminate_status);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, SinkTerminateFailureStillFinishes) {
    auto spill = context();
    _task->_wake_up_early = true;
    _sink->terminate_status = Status::InternalError("injected sink terminate failure");
    expect_exception([&]() { static_cast<void>(_task->do_revoke_memory(spill)); },
                     _sink->terminate_status);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, RevokeStatusFailureFinishesOnce) {
    auto spill = context();
    _source->revoke_status = Status::InternalError("injected revoke failure");
    const auto status = _task->do_revoke_memory(spill);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("injected revoke failure"), std::string::npos);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, SuccessfulRevokeFinishesOnce) {
    auto spill = context();
    EXPECT_TRUE(_task->do_revoke_memory(spill).ok());
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, CancellationUnblocksMemoryWithoutSpillCallback) {
    auto spill = context();
    EXPECT_NE(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    _query->cancel(Status::Cancelled("injected cancel before spill execution"));
    EXPECT_EQ(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    EXPECT_EQ(spill->running_tasks_count.load(), 1);
    EXPECT_EQ(_callbacks, 0);
    // This control distinguishes the cancellation wake-up from spill completion.
    spill->on_task_finished();
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, PartialBatchSubmissionStillFinishesAllSelectedTasks) {
    _scheduler->reject_at = 2;
    _scheduler->observe_completion = true;
    select_tasks(3);
    auto tracker = _query->query_mem_tracker();
    const auto consumption = 100 * SpillFile::MIN_SPILL_WRITE_BATCH_MEM;
    tracker->consume(consumption);
    Defer release_consumption {[&]() { tracker->release(consumption); }};
    // The selected budget includes all three tasks; only the first reaches the queue.
    const auto status = _query->resource_ctx()->task_controller()->revoke_memory();
    EXPECT_FALSE(status.ok());
    ASSERT_EQ(_scheduler->attempts, 2);
    auto spill = _scheduler->last_context;
    ASSERT_NE(spill, nullptr);
    expect_batch_progress(spill, 1, 0);
    auto accepted = _scheduler->_task_queue->take(0);
    ASSERT_NE(accepted, nullptr);
    bool done = false;
    EXPECT_TRUE(accepted->execute(&done).ok());
    RecordProperty("remaining_spill_tasks", spill->running_tasks_count.load());
    RecordProperty("memory_dependency_blocked_before_cancel",
                   _query->get_memory_sufficient_dependency()->is_blocked_by(nullptr) != nullptr);
    expect_batch_progress(spill, 0, 1);
    EXPECT_NE(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    accepted.reset();
    expect_batch_progress(spill, 0, 1);
    // Failed submission must not resume the query before the caller cancels it.
    _query->cancel(status);
    RecordProperty("memory_dependency_blocked_after_cancel",
                   _query->get_memory_sufficient_dependency()->is_blocked_by(nullptr) != nullptr);
    EXPECT_EQ(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    EXPECT_NE(_query->exec_status().to_string().find("injected spill scheduler rejection"),
              std::string::npos);
}

TEST_F(SpillCompletionFailureTest, SchedulerCancellationBeforeExecuteStillFinishes) {
    auto spill = context();
    _scheduler->reject_at = 0;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    auto wrapper = _scheduler->_task_queue->take(0);
    ASSERT_NE(wrapper, nullptr);
    _pipeline->incr_created_tasks(0, _task.get());
    _fragment->_pip_id_to_pipeline[0] = _pipeline.get();
    _fragment->_total_tasks = 2;
    std::promise<void> idle;
    auto observed = idle.get_future();
    _task->on_idle = [&idle]() { idle.set_value(); };
    _query->cancel(Status::Cancelled("injected cancellation before scheduler execution"));
    HybridTaskScheduler scheduler(1, 1, "spill_completion_cancel_test", nullptr);
    Defer stop_scheduler {[&]() { scheduler.stop(); }};
    ASSERT_TRUE(scheduler.start().ok());
    ASSERT_TRUE(scheduler.submit(std::move(wrapper)).ok());
    ASSERT_EQ(observed.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    scheduler.stop();
    expect_finished(spill);
    EXPECT_NE(_query->exec_status().to_string().find("injected cancellation"), std::string::npos);
}

TEST_F(SpillCompletionFailureTest, SchedulerFinalizedBeforeExecuteStillFinishes) {
    auto spill = context();
    _scheduler->reject_at = 0;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    auto wrapper = _scheduler->_task_queue->take(0);
    ASSERT_NE(wrapper, nullptr);
    auto waiter = block_waiter();
    _task->_exec_state = PipelineTask::State::FINALIZED;
    std::promise<void> idle;
    auto observed = idle.get_future();
    _task->on_idle = [&idle]() { idle.set_value(); };
    HybridTaskScheduler scheduler(1, 1, "spill_completion_finalized_test", nullptr);
    Defer stop_scheduler {[&]() { scheduler.stop(); }};
    ASSERT_TRUE(scheduler.start().ok());
    ASSERT_TRUE(scheduler.submit(std::move(wrapper)).ok());
    ASSERT_EQ(observed.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    scheduler.stop();
    expect_finished(spill);
    EXPECT_EQ(waiter->_exec_state, PipelineTask::State::RUNNABLE);
    EXPECT_EQ(_scheduler->waiter_submissions, 1);
    EXPECT_EQ(_scheduler->_task_queue->take(0), waiter);
}

TEST_F(SpillCompletionFailureTest, SchedulerExpiredFragmentBeforeExecuteStillFinishes) {
    auto spill = context();
    _scheduler->reject_at = 0;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    auto wrapper = _scheduler->_task_queue->take(0);
    ASSERT_NE(wrapper, nullptr);
    _task->_fragment_context.reset();
    std::promise<void> idle;
    auto observed = idle.get_future();
    _task->on_idle = [&idle]() { idle.set_value(); };
    HybridTaskScheduler scheduler(1, 1, "spill_completion_expired_fragment_test", nullptr);
    Defer stop_scheduler {[&]() { scheduler.stop(); }};
    ASSERT_TRUE(scheduler.start().ok());
    ASSERT_TRUE(scheduler.submit(std::move(wrapper)).ok());
    ASSERT_EQ(observed.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    scheduler.stop();
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, RevokeStatusSurvivesRootTerminateFailure) {
    auto spill = context();
    _task->_wake_up_early = true;
    _source->revoke_status = Status::InvalidArgument("original revoke error");
    _source->terminate_status = Status::InternalError("secondary root terminate error");
    expect_status(_task->do_revoke_memory(spill), _source->revoke_status);
    EXPECT_EQ(_source->terminate_calls, 1);
    EXPECT_EQ(_sink->terminate_calls, 0);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, RevokeStatusSurvivesSinkTerminateFailure) {
    auto spill = context();
    _task->_wake_up_early = true;
    _source->revoke_status = Status::InvalidArgument("original revoke error");
    _sink->terminate_status = Status::InternalError("secondary sink terminate error");
    expect_status(_task->do_revoke_memory(spill), _source->revoke_status);
    EXPECT_EQ(_source->terminate_calls, 1);
    EXPECT_EQ(_sink->terminate_calls, 1);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, RevokeExceptionSurvivesTerminateFailure) {
    auto spill = context();
    _task->_wake_up_early = true;
    _source->revoke_status = Status::InvalidArgument("original revoke exception");
    _source->throw_on_revoke = true;
    _source->terminate_status = Status::InternalError("secondary terminate error");
    expect_exception([&]() { static_cast<void>(_task->do_revoke_memory(spill)); },
                     _source->revoke_status);
    EXPECT_EQ(_source->terminate_calls, 1);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, QueuedTaskDroppedOnStopFinishesOnce) {
    auto spill = context();
    _scheduler->reject_at = 0;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    auto wrapper = _scheduler->_task_queue->take(0);
    ASSERT_NE(wrapper, nullptr);
    HybridTaskScheduler scheduler(1, 1, "spill_completion_drain_test", nullptr);
    ASSERT_TRUE(scheduler.submit(std::move(wrapper)).ok());
    EXPECT_EQ(spill->running_tasks_count.load(), 1);
    EXPECT_EQ(_callbacks, 0);
    scheduler.stop();
    expect_finished(spill);
    EXPECT_EQ(_source->revoke_calls, 0);
    scheduler.stop();
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, OnlyLastTaskResumesAndDestructionDoesNotRepeatCompletion) {
    auto spill = context(2);
    _scheduler->reject_at = 0;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    auto wrapper = _scheduler->_task_queue->take(0);
    ASSERT_NE(wrapper, nullptr);
    bool done = false;
    ASSERT_TRUE(wrapper->execute(&done).ok());
    EXPECT_EQ(spill->running_tasks_count.load(), 1);
    EXPECT_EQ(_callbacks, 0);
    EXPECT_NE(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    wrapper.reset();
    EXPECT_EQ(spill->running_tasks_count.load(), 1);
    EXPECT_EQ(_callbacks, 0);
    auto last = std::make_shared<RevokableTask>(_task, spill);
    ASSERT_TRUE(last->execute(&done).ok());
    expect_finished(spill);
    last.reset();
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, FinalizedAtEntryFinishesOnce) {
    auto spill = context();
    _task->_exec_state = PipelineTask::State::FINALIZED;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    EXPECT_EQ(_scheduler->attempts, 0);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, InsufficientMemoryFinishesOnce) {
    auto spill = context();
    _source->revocable_size = 0;
    ASSERT_TRUE(_task->revoke_memory(spill).ok());
    EXPECT_EQ(_scheduler->attempts, 0);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, SubmissionExceptionStillFinishes) {
    auto spill = context();
    _scheduler->throw_on_submit = true;
    expect_exception([&]() { static_cast<void>(_task->revoke_memory(spill)); },
                     _scheduler->rejection);
    expect_finished(spill);
}

TEST_F(SpillCompletionFailureTest, BatchRejectionWakesWaiterOnlyAfterCancellation) {
    auto waiter = block_waiter();
    select_tasks(1);
    _scheduler->observe_completion = true;
    auto tracker = _query->query_mem_tracker();
    const auto consumption = 100 * SpillFile::MIN_SPILL_WRITE_BATCH_MEM;
    tracker->consume(consumption);
    Defer release_consumption {[&]() { tracker->release(consumption); }};
    WorkloadGroupMgr manager;
    manager.spill_query_(_query->resource_ctx());
    ASSERT_EQ(_scheduler->attempts, 1);
    ASSERT_NE(_scheduler->last_context, nullptr);
    EXPECT_EQ(_scheduler->last_context->running_tasks_count.load(), 0);
    EXPECT_EQ(_scheduler->completion_calls, 1);
    expect_status(_query->exec_status(), _scheduler->rejection);
    expect_status(_scheduler->waiter_status, _scheduler->rejection);
    EXPECT_EQ(_scheduler->waiter_submissions, 1);
    EXPECT_EQ(waiter->_exec_state, PipelineTask::State::RUNNABLE);
    EXPECT_EQ(_scheduler->_task_queue->take(0), waiter);
}

TEST_F(SpillCompletionFailureTest, BatchSubmissionExceptionCancelsWithOriginalError) {
    auto waiter = block_waiter();
    select_tasks(3);
    _scheduler->reject_at = 2;
    _scheduler->throw_on_submit = true;
    _scheduler->observe_completion = true;
    auto tracker = _query->query_mem_tracker();
    const auto consumption = 100 * SpillFile::MIN_SPILL_WRITE_BATCH_MEM;
    tracker->consume(consumption);
    Defer release_consumption {[&]() { tracker->release(consumption); }};
    WorkloadGroupMgr manager;
    EXPECT_NO_THROW(manager.spill_query_(_query->resource_ctx()));
    ASSERT_EQ(_scheduler->attempts, 2);
    auto spill = _scheduler->last_context;
    ASSERT_NE(spill, nullptr);
    expect_batch_progress(spill, 1, 0);
    auto accepted = _scheduler->_task_queue->take(0);
    ASSERT_NE(accepted, nullptr);
    accepted.reset();
    expect_batch_progress(spill, 0, 1);
    expect_status(_query->exec_status(), _scheduler->rejection);
    expect_status(_scheduler->waiter_status, _scheduler->rejection);
    EXPECT_EQ(_scheduler->waiter_submissions, 1);
    EXPECT_EQ(_scheduler->_task_queue->take(0), waiter);
}

TEST_F(SpillCompletionFailureTest, EarlierTasksCanFinishBeforeLastSubmissionFails) {
    select_tasks(3);
    _scheduler->reject_at = 3;
    _scheduler->execute_inline = true;
    _scheduler->observe_completion = true;
    auto tracker = _query->query_mem_tracker();
    const auto consumption = 100 * SpillFile::MIN_SPILL_WRITE_BATCH_MEM;
    tracker->consume(consumption);
    Defer release_consumption {[&]() { tracker->release(consumption); }};
    const auto status = _query->resource_ctx()->task_controller()->revoke_memory();
    expect_status(status, _scheduler->rejection);
    ASSERT_EQ(_scheduler->attempts, 3);
    EXPECT_EQ(_source->revoke_calls, 2);
    EXPECT_EQ(_scheduler->last_context->running_tasks_count.load(), 0);
    EXPECT_EQ(_scheduler->completion_calls, 1);
    EXPECT_NE(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
    _query->cancel(status);
    EXPECT_EQ(_query->get_memory_sufficient_dependency()->is_blocked_by(nullptr), nullptr);
}

} // namespace
} // namespace doris
