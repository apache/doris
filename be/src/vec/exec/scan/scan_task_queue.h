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

#include "olap/tablet.h"
#include "runtime/task_group/task_group.h"
#include "util/blocking_priority_queue.hpp"

namespace doris {
namespace vectorized {
class ScannerContext;
};

namespace taskgroup {

using WorkFunction = std::function<void()>;
static constexpr auto WAIT_CORE_TASK_TIMEOUT_MS = 100;

// Like PriorityThreadPool::Task
struct ScanTask {
    ScanTask();
    ScanTask(WorkFunction scan_func, vectorized::ScannerContext* scanner_context,
             TGSTEntityPtr scan_entity, int priority);
    bool operator<(const ScanTask& o) const { return priority < o.priority; }
    ScanTask& operator++() {
        priority += 2;
        return *this;
    }

    WorkFunction scan_func;
    vectorized::ScannerContext* scanner_context;
    TGSTEntityPtr scan_entity;
    int priority;
};

// Like pipeline::PriorityTaskQueue use BlockingPriorityQueue directly?
class ScanTaskQueue {
public:
    ScanTaskQueue();
    Status try_push_back(ScanTask);
    bool try_get(ScanTask* scan_task, uint32_t timeout_ms);
    int size() { return _queue.get_size(); }

private:
    BlockingPriorityQueue<ScanTask> _queue;
};

// Like TaskGroupTaskQueue
class ScanTaskTaskGroupQueue {
public:
    explicit ScanTaskTaskGroupQueue(size_t core_size);
    ~ScanTaskTaskGroupQueue();

    void close();
    bool take(ScanTask* scan_task);
    bool push_back(ScanTask);

    void update_statistics(ScanTask task, int64_t time_spent);

    void update_tg_cpu_share(const taskgroup::TaskGroupInfo&, taskgroup::TGSTEntityPtr);

private:
    TGSTEntityPtr _task_entity(ScanTask& scan_task);
    void _enqueue_task_group(TGSTEntityPtr);
    void _dequeue_task_group(TGSTEntityPtr);
    TGSTEntityPtr _next_tg_entity();
    uint64_t _ideal_runtime_ns(TGSTEntityPtr tg_entity) const;
    void _update_min_tg();

    // Like cfs rb tree in sched_entity
    struct TaskGroupSchedEntityComparator {
        bool operator()(const taskgroup::TGSTEntityPtr&, const taskgroup::TGSTEntityPtr&) const;
    };
    using ResouceGroupSet = std::set<taskgroup::TGSTEntityPtr, TaskGroupSchedEntityComparator>;
    ResouceGroupSet _group_entities;
    std::condition_variable _wait_task;
    std::mutex _rs_mutex;
    bool _closed = false;
    int _total_cpu_share = 0;
    std::atomic<taskgroup::TGSTEntityPtr> _min_tg_entity = nullptr;
    uint64_t _min_tg_v_runtime_ns = 0;
    size_t _core_size;
};

} // namespace taskgroup
} // namespace doris
