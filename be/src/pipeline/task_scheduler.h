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

#pragma once

#include "common/status.h"
#include "pipeline.h"
#include "pipeline_task.h"
#include "task_queue.h"
#include "util/threadpool.h"

namespace doris::pipeline {

class BlockedTaskScheduler {
public:
    explicit BlockedTaskScheduler(std::shared_ptr<TaskQueue> task_queue);

    ~BlockedTaskScheduler() = default;

    Status start();
    void shutdown();
    Status add_blocked_task(PipelineTask* task);

private:
    std::shared_ptr<TaskQueue> _task_queue;

    std::mutex _task_mutex;
    std::condition_variable _task_cond;
    std::list<PipelineTask*> _blocked_tasks;

    scoped_refptr<Thread> _thread;
    std::atomic<bool> _started;
    std::atomic<bool> _shutdown;

    static constexpr auto EMPTY_TIMES_TO_YIELD = 64;

private:
    void _schedule();
    void _make_task_run(std::list<PipelineTask*>& local_tasks,
                        std::list<PipelineTask*>::iterator& task_itr,
                        std::vector<PipelineTask*>& ready_tasks,
                        PipelineTaskState state = PipelineTaskState::RUNNABLE);
};

class TaskScheduler {
public:
    TaskScheduler(ExecEnv* exec_env, std::shared_ptr<BlockedTaskScheduler> b_scheduler,
                  std::shared_ptr<TaskQueue> task_queue)
            : _task_queue(std::move(task_queue)),
              _exec_env(exec_env),
              _blocked_task_scheduler(std::move(b_scheduler)),
              _shutdown(false) {}

    ~TaskScheduler();

    Status schedule_task(PipelineTask* task);

    Status start();

    void shutdown();

    ExecEnv* exec_env() { return _exec_env; }

private:
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    std::shared_ptr<TaskQueue> _task_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> _markers;
    ExecEnv* _exec_env;
    std::shared_ptr<BlockedTaskScheduler> _blocked_task_scheduler;
    std::atomic<bool> _shutdown;

    void _do_work(size_t index);
    void _try_close_task(PipelineTask* task, PipelineTaskState state);
};
} // namespace doris::pipeline