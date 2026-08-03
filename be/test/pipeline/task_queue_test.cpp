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

#include "pipeline/task_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "pipeline/pipeline.h"
#include "pipeline/task_scheduler.h"

namespace doris::pipeline {
namespace {

class TestPipelineTask : public PipelineTask {
public:
    explicit TestPipelineTask(PipelinePtr& pipeline)
            : PipelineTask(pipeline, 0, nullptr, nullptr, nullptr) {}
};

struct TaskOwner {
    explicit TaskOwner(std::atomic<bool>* destroyed)
            : pipeline(std::make_shared<Pipeline>(0, 1, std::weak_ptr<PipelineFragmentContext>())),
              task(pipeline),
              _destroyed(destroyed) {}

    ~TaskOwner() { _destroyed->store(true); }

    PipelinePtr pipeline;
    TestPipelineTask task;
    std::atomic<bool>* _destroyed;
};

TEST(TaskQueueTest, SubTaskQueueKeepsTaskOwnerAlive) {
    std::atomic<bool> destroyed = false;
    auto owner = std::make_shared<TaskOwner>(&destroyed);
    std::weak_ptr<TaskOwner> weak_owner = owner;
    PipelineTaskSPtr task_holder(owner, &owner->task);

    SubTaskQueue queue;
    queue.push_back(task_holder);
    task_holder.reset();
    owner.reset();
    EXPECT_FALSE(weak_owner.expired());

    auto taken = queue.try_take(false);
    ASSERT_NE(taken, nullptr);
    EXPECT_FALSE(weak_owner.expired());

    taken.reset();
    EXPECT_TRUE(weak_owner.expired());
    EXPECT_TRUE(destroyed.load());
}

TEST(TaskQueueTest, CloseReleasesPendingTaskOwner) {
    std::atomic<bool> destroyed = false;
    auto owner = std::make_shared<TaskOwner>(&destroyed);
    std::weak_ptr<TaskOwner> weak_owner = owner;
    PipelineTaskSPtr task_holder(owner, &owner->task);

    PriorityTaskQueue queue;
    ASSERT_TRUE(queue.push(task_holder).ok());
    task_holder.reset();
    owner.reset();
    EXPECT_FALSE(weak_owner.expired());

    queue.close();
    queue.close();
    EXPECT_TRUE(weak_owner.expired());
    EXPECT_TRUE(destroyed.load());
}

class FinishedPipelineTask final : public TestPipelineTask {
public:
    FinishedPipelineTask(PipelinePtr& pipeline, std::promise<void>* finished_checked,
                         std::atomic<int>* execute_calls)
            : TestPipelineTask(pipeline),
              _finished_checked(finished_checked),
              _execute_calls(execute_calls) {}

    bool is_pipelineX() const override { return true; }

    bool is_finished() const override {
        if (!_check_reported.exchange(true)) {
            _finished_checked->set_value();
        }
        return true;
    }

    Status execute(bool* /*eos*/) override {
        _execute_calls->fetch_add(1);
        return Status::OK();
    }

private:
    std::promise<void>* _finished_checked;
    std::atomic<int>* _execute_calls;
    mutable std::atomic<bool> _check_reported = false;
};

struct FinishedTaskOwner {
    FinishedTaskOwner(std::promise<void>* finished_checked, std::atomic<int>* execute_calls)
            : pipeline(std::make_shared<Pipeline>(0, 1, std::weak_ptr<PipelineFragmentContext>())),
              task(pipeline, finished_checked, execute_calls) {}

    PipelinePtr pipeline;
    FinishedPipelineTask task;
};

class OneShotTaskQueue final : public TaskQueue {
public:
    explicit OneShotTaskQueue(PipelineTaskSPtr task) : TaskQueue(1), _task(std::move(task)) {}

    void close() override {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _closed = true;
        }
        _closed_cv.notify_all();
    }

    PipelineTaskSPtr take(int /*core_id*/) override {
        std::unique_lock<std::mutex> lock(_mutex);
        if (_task) {
            return std::move(_task);
        }
        _closed_cv.wait(lock, [this] { return _closed; });
        return nullptr;
    }

    Status push_back(PipelineTask* /*task*/) override {
        return Status::InternalError("finished task should not be resubmitted");
    }

    Status push_back(PipelineTask* /*task*/, int /*core_id*/) override {
        return Status::InternalError("finished task should not be resubmitted");
    }

private:
    std::mutex _mutex;
    std::condition_variable _closed_cv;
    PipelineTaskSPtr _task;
    bool _closed = false;
};

TEST(TaskSchedulerTest, SkipsFinishedPipelineTask) {
    std::promise<void> finished_checked;
    auto checked_future = finished_checked.get_future();
    std::atomic<int> execute_calls = 0;
    auto owner = std::make_shared<FinishedTaskOwner>(&finished_checked, &execute_calls);
    PipelineTaskSPtr task_holder(owner, &owner->task);
    auto task_queue = std::make_shared<OneShotTaskQueue>(std::move(task_holder));

    TaskScheduler scheduler(nullptr, nullptr, task_queue, "terminal-task-test", nullptr);
    ASSERT_TRUE(scheduler.start().ok());
    ASSERT_EQ(checked_future.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    scheduler.stop();

    EXPECT_EQ(execute_calls.load(), 0);
    EXPECT_FALSE(owner->task.is_running());
}

} // namespace
} // namespace doris::pipeline
