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
#include <queue>
#include <shared_mutex>

#include "olap/olap_define.h"

namespace doris {

namespace pipeline {
class PipelineTask;
}

class QueryFragmentsCtx;
class TPipelineResourceGroup;

namespace taskgroup {

class TaskGroup;
struct TaskGroupInfo;

const static std::string CPU_SHARE = "cpu_share";

class TaskGroupEntity {
public:
    explicit TaskGroupEntity(taskgroup::TaskGroup* ts) : _tg(ts) {}
    void push_back(pipeline::PipelineTask* task);
    uint64_t vruntime_ns() const { return _vruntime_ns; }

    pipeline::PipelineTask* take();

    void incr_runtime_ns(uint64_t runtime_ns);

    void adjust_vruntime_ns(uint64_t vruntime_ns);

    size_t task_size() const { return _queue.size(); }

    uint64_t cpu_share() const;

    std::string debug_string() const;

private:
    // TODO pipeline use MLFQ
    std::queue<pipeline::PipelineTask*> _queue;
    taskgroup::TaskGroup* _tg;
    uint64_t _vruntime_ns = 0;
};

using TGEntityPtr = TaskGroupEntity*;

class TaskGroup {
public:
    TaskGroup(uint64_t id, std::string name, uint64_t cpu_share, int64_t version);

    TaskGroupEntity* task_entity() { return &_task_entity; }

    uint64_t cpu_share() const { return _cpu_share.load(); }

    uint64_t id() const { return _id; }

    std::string debug_string() const;

    bool check_version(int64_t version) const;

    void check_and_update(const TaskGroupInfo& tg_info);

private:
    mutable std::shared_mutex mutex;
    const uint64_t _id;
    std::string _name;
    std::atomic<uint64_t> _cpu_share;
    TaskGroupEntity _task_entity;
    int64_t _version;
};

using TaskGroupPtr = std::shared_ptr<TaskGroup>;

struct TaskGroupInfo {
    uint64_t _id;
    std::string _name;
    uint64_t _cpu_share;
    int64_t _version;

    static Status parse_group_info(const TPipelineResourceGroup& resource_group,
                                   TaskGroupInfo* task_group_info);

private:
    static bool check_group_info(const TPipelineResourceGroup& resource_group);
};

} // namespace taskgroup
} // namespace doris
