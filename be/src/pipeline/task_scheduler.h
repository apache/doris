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
#include "gutil/ref_counted.h"
#include "pipeline_task.h"
#include "runtime/query_context.h"
#include "runtime/workload_group/workload_group.h"
#include "util/thread.h"
#include "util/uid_util.h"

namespace doris {
class ExecEnv;
class ThreadPool;

namespace pipeline {
class TaskQueue;
} // namespace pipeline
} // namespace doris

namespace doris::pipeline {

struct PausedQuery {
    std::shared_ptr<QueryContext> query_ctx;
    std::chrono::system_clock::time_point enqueue_at;
    size_t last_mem_usage {0};

    PausedQuery(std::shared_ptr<QueryContext> query_ctx_)
            : query_ctx(std::move(query_ctx_)), _query_id(print_id(query_ctx->query_id())) {
        enqueue_at = std::chrono::system_clock::now();
    }

    int64_t elapsed_time() const {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(now - enqueue_at).count();
    }

    std::string query_id() const { return _query_id; }

    bool operator<(const PausedQuery& other) const { return _query_id < other._query_id; }

    bool operator==(const PausedQuery& other) const { return _query_id == other._query_id; }

private:
    std::string _query_id;
};

class TaskScheduler {
public:
    TaskScheduler(ExecEnv* exec_env, std::shared_ptr<TaskQueue> task_queue, std::string name,
                  CgroupCpuCtl* cgroup_cpu_ctl)
            : _task_queue(std::move(task_queue)),
              _name(std::move(name)),
              _cgroup_cpu_ctl(cgroup_cpu_ctl) {}

    ~TaskScheduler();

    Status schedule_task(PipelineTask* task);

    Status start();

    void stop();

    std::vector<int> thread_debug_info() { return _fix_thread_pool->debug_info(); }

    void add_paused_task(PipelineTask* task);

private:
    std::unique_ptr<ThreadPool> _fix_thread_pool;
    std::shared_ptr<TaskQueue> _task_queue;
    bool _need_to_stop = false;
    bool _shutdown = false;
    std::string _name;
    CgroupCpuCtl* _cgroup_cpu_ctl = nullptr;

    std::map<WorkloadGroupPtr, std::set<PausedQuery>> _paused_queries_list;
    std::mutex _paused_queries_lock;
    std::condition_variable _paused_queries_cv;

    void _do_work(size_t index);

    void _paused_queries_handler();
};
} // namespace doris::pipeline