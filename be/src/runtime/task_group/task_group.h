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
#include <queue>

#include "olap/olap_define.h"

namespace doris {

namespace pipeline {
class PipelineTask;
}

class QueryFragmentsCtx;

namespace taskgroup {

class TaskGroup;

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
    TaskGroup(uint64_t id, std::string name, uint64_t cpu_share);

    TaskGroupEntity* task_entity() { return &_task_entity; }

    uint64_t share() const { return _share; }
    uint64_t id() const { return _id; }

    std::string debug_string() const;

private:
    uint64_t _id;
    std::string _name;
    uint64_t _share;
    TaskGroupEntity _task_entity;
};

using TaskGroupPtr = std::shared_ptr<TaskGroup>;

} // namespace taskgroup
} // namespace doris
