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

#include <stddef.h>

#include <atomic>
#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/status.h"
#include "gutil/ref_counted.h"
#include "pipeline_task.h"
#include "runtime/task_group/task_group.h"
#include "util/thread.h"

namespace doris {
class ExecEnv;
class ThreadPool;

namespace pipeline {
class TaskQueue;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

class BlockedTaskScheduler {
public:
    explicit BlockedTaskScheduler(std::string name);

    ~BlockedTaskScheduler() = default;

    Status start();
    void shutdown();
    Status add_blocked_task(PipelineTask* task);

private:
    std::mutex _task_mutex;
    std::string _name;
    std::condition_variable _task_cond;
    std::list<PipelineTask*> _blocked_tasks;

    scoped_refptr<Thread> _thread;
    std::atomic<bool> _started;
    std::atomic<bool> _shutdown;

    static constexpr auto EMPTY_TIMES_TO_YIELD = 64;

    void _schedule();
    void _make_task_run(std::list<PipelineTask*>& local_tasks,
                        std::list<PipelineTask*>::iterator& task_itr,
                        PipelineTaskState state = PipelineTaskState::RUNNABLE);
};

class TaskScheduler {
public:
    TaskScheduler(ExecEnv* exec_env, std::shared_ptr<BlockedTaskScheduler> b_scheduler,
                  std::shared_ptr<TaskQueue> task_queue, std::string name,
                  CgroupCpuCtl* cgroup_cpu_ctl)
            : _task_queue(std::move(task_queue)),
              _blocked_task_scheduler(std::move(b_scheduler)),
              _shutdown(false),
              _name(name),
              _cgroup_cpu_ctl(cgroup_cpu_ctl) {}

    ~TaskScheduler();

    Status schedule_task(PipelineTask* task);

    Status start();

    void stop();

    TaskQueue* task_queue() const { return _task_queue.get(); }

private:
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    std::shared_ptr<TaskQueue> _task_queue;
    std::vector<std::unique_ptr<std::atomic<bool>>> _markers;
    std::shared_ptr<BlockedTaskScheduler> _blocked_task_scheduler;
    std::atomic<bool> _shutdown;
    std::string _name;
    CgroupCpuCtl* _cgroup_cpu_ctl = nullptr;

    void _do_work(size_t index);
    // after _try_close_task, task maybe destructed.
    void _try_close_task(PipelineTask* task, PipelineTaskState state,
                         Status exec_status = Status::OK());
};
} // namespace doris::pipeline