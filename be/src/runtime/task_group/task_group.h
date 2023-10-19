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
#include <stdint.h>

#include <atomic>
#include <memory>
#include <queue>
#include <shared_mutex>
#include <string>
#include <unordered_set>

#include "common/status.h"

namespace doris {

class TPipelineWorkloadGroup;
class MemTrackerLimiter;

namespace pipeline {
class PipelineTask;
} // namespace pipeline

namespace taskgroup {

class TaskGroup;
struct TaskGroupInfo;
class ScanTaskQueue;

template <typename QueueType>
class TaskGroupEntity {
public:
    explicit TaskGroupEntity(taskgroup::TaskGroup* tg, std::string type);
    ~TaskGroupEntity();

    TaskGroupEntity() = default; // used for empty group entity

    uint64_t vruntime_ns() const { return _vruntime_ns; }

    QueueType* task_queue();

    void incr_runtime_ns(uint64_t runtime_ns);

    void adjust_vruntime_ns(uint64_t vruntime_ns);

    size_t task_size() const;

    uint64_t cpu_share() const;

    std::string debug_string() const;

    uint64_t task_group_id() const;

    void check_and_update_cpu_share(const TaskGroupInfo& tg_info);

    void set_empty_group_entity(bool is_empty_group_entity);

    bool is_empty_group_entity();

    void update_empty_cpu_share(uint64_t empty_cpu_share);

private:
    QueueType* _task_queue;

    uint64_t _vruntime_ns = 0;
    taskgroup::TaskGroup* _tg;

    std::string _type;

    // Because updating cpu share of entity requires locking the task queue(pipeline task queue or
    // scan task queue) contains that entity, we kept version and cpu share in entity for
    // independent updates.
    int64_t _version;
    uint64_t _cpu_share;

    bool _is_empty_group_entity = false;
};

// TODO llj tg use PriorityTaskQueue to replace std::queue
using TaskGroupPipelineTaskEntity = TaskGroupEntity<std::queue<pipeline::PipelineTask*>>;
using TGPTEntityPtr = TaskGroupPipelineTaskEntity*;

using TaskGroupScanTaskEntity = TaskGroupEntity<ScanTaskQueue>;
using TGSTEntityPtr = TaskGroupScanTaskEntity*;

struct TgTrackerLimiterGroup {
    std::unordered_set<std::shared_ptr<MemTrackerLimiter>> trackers;
    std::mutex group_lock;
};

class TaskGroup : public std::enable_shared_from_this<TaskGroup> {
public:
    explicit TaskGroup(const TaskGroupInfo& tg_info);

    TaskGroupPipelineTaskEntity* task_entity() { return &_task_entity; }
    TGSTEntityPtr local_scan_task_entity() { return &_local_scan_entity; }

    int64_t version() const { return _version; }

    uint64_t cpu_share() const { return _cpu_share.load(); }

    uint64_t id() const { return _id; }

    std::string name() const { return _name; };

    bool enable_memory_overcommit() const {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _enable_memory_overcommit;
    };

    int64_t memory_limit() const {
        std::shared_lock<std::shared_mutex> r_lock(_mutex);
        return _memory_limit;
    };

    int64_t memory_used();

    std::string debug_string() const;

    void check_and_update(const TaskGroupInfo& tg_info);

    void add_mem_tracker_limiter(std::shared_ptr<MemTrackerLimiter> mem_tracker_ptr);

    void remove_mem_tracker_limiter(std::shared_ptr<MemTrackerLimiter> mem_tracker_ptr);

    void task_group_info(TaskGroupInfo* tg_info) const;

    std::vector<TgTrackerLimiterGroup>& mem_tracker_limiter_pool() {
        return _mem_tracker_limiter_pool;
    }

private:
    mutable std::shared_mutex _mutex; // lock _name, _version, _cpu_share, _memory_limit
    const uint64_t _id;
    std::string _name;
    int64_t _version;
    int64_t _memory_limit; // bytes
    bool _enable_memory_overcommit;
    std::atomic<uint64_t> _cpu_share;
    TaskGroupPipelineTaskEntity _task_entity;
    TaskGroupScanTaskEntity _local_scan_entity;
    std::vector<TgTrackerLimiterGroup> _mem_tracker_limiter_pool;
};

using TaskGroupPtr = std::shared_ptr<TaskGroup>;

struct TaskGroupInfo {
    uint64_t id;
    std::string name;
    uint64_t cpu_share;
    int64_t memory_limit;
    bool enable_memory_overcommit;
    int64_t version;
    int cpu_hard_limit;

    static Status parse_group_info(const TPipelineWorkloadGroup& resource_group,
                                   TaskGroupInfo* task_group_info);

private:
    static bool check_group_info(const TPipelineWorkloadGroup& resource_group);
};

} // namespace taskgroup
} // namespace doris
