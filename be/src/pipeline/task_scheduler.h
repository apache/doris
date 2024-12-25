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
#include "runtime/workload_group/workload_group.h"
#include "task_queue.h"
#include "util/thread.h"

namespace doris {
class ExecEnv;
class ThreadPool;
} // namespace doris

namespace doris::pipeline {

class TaskScheduler {
public:
    TaskScheduler(int core_num, std::string name, std::shared_ptr<CgroupCpuCtl> cgroup_cpu_ctl)
            : _task_queue(core_num),
              _shutdown(false),
              _name(std::move(name)),
              _cgroup_cpu_ctl(cgroup_cpu_ctl) {}

    ~TaskScheduler();

    Status schedule_task(PipelineTask* task);

    Status start();

    void stop();

    std::vector<int> thread_debug_info() { return _fix_thread_pool->debug_info(); }

private:
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    MultiCoreTaskQueue _task_queue;
    std::vector<bool> _markers;
    bool _shutdown;
    std::string _name;
    std::weak_ptr<CgroupCpuCtl> _cgroup_cpu_ctl;

    void _do_work(int index);
};
} // namespace doris::pipeline