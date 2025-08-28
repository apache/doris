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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/status.h"
#include "pipeline_task.h"
#include "runtime/query_context.h"
#include "runtime/workload_group/workload_group.h"
#include "task_queue.h"
#include "util/thread.h"
#include "util/uid_util.h"

namespace doris {
class ExecEnv;
class ThreadPool;
} // namespace doris

namespace doris::pipeline {

class HybridTaskScheduler;
class TaskScheduler {
public:
    virtual ~TaskScheduler();

    virtual Status submit(PipelineTaskSPtr task);

    virtual Status start();

    virtual void stop();

    virtual std::vector<std::pair<std::string, std::vector<int>>> thread_debug_info() {
        return {{_name, _fix_thread_pool->debug_info()}};
    }

private:
    friend class HybridTaskScheduler;

    TaskScheduler(int core_num, std::string name, std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
            : _name(std::move(name)), _task_queue(core_num), _cgroup_cpu_ctl(cgroup_cpu_ctl) {}
    TaskScheduler() : _task_queue(0) {}
    std::string _name;
    std::unique_ptr<ThreadPool> _fix_thread_pool;

    MultiCoreTaskQueue _task_queue;
    bool _need_to_stop = false;
    bool _shutdown = false;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;

    void _do_work(int index);
};

class HybridTaskScheduler : public TaskScheduler {
public:
    HybridTaskScheduler(int core_num, std::string name,
                        std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
            : _blocking_scheduler(core_num * 2, name + "_blocking_scheduler", cgroup_cpu_ctl),
              _simple_scheduler(core_num, name + "_simple_scheduler", cgroup_cpu_ctl) {}

    Status submit(PipelineTaskSPtr task) override;

    Status start() override;

    void stop() override;

    std::vector<std::pair<std::string, std::vector<int>>> thread_debug_info() override {
        return {_blocking_scheduler.thread_debug_info()[0],
                _simple_scheduler.thread_debug_info()[0]};
    }

private:
    TaskScheduler _blocking_scheduler;
    TaskScheduler _simple_scheduler;
};
} // namespace doris::pipeline