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

namespace resourcegroup {

class ResourceGroup;

class ResourceGroupEntry {
public:
    explicit ResourceGroupEntry(resourcegroup::ResourceGroup* rs) : _rs(rs) {}
    void push_back(pipeline::PipelineTask* task);
    int64_t vruntime_ns() const { return _vruntime_ns; }

    pipeline::PipelineTask* take();

    void incr_runtime_ns(int64_t runtime_ns);

    size_t task_size() { return _queue.size(); }

    int cpu_share() const;

private:
    // TODO rs poc 这里暂时不用多级反馈队列
    std::queue<pipeline::PipelineTask*> _queue;
    resourcegroup::ResourceGroup* _rs;
    int64_t _vruntime_ns = 0;
//    std::mutex _work_size_mutex;

//    int _num_queries = 0;
//    int _num_instances = 0;
//    int _num_tasks = 0;
};

using RSEntryPtr = ResourceGroupEntry*;

class ResourceGroup {
public:
    ResourceGroup(uint64_t id, std::string name, int cpu_share);

    // TODO rs
    Status check_big_query(const QueryFragmentsCtx& query_context) { return Status::OK(); }

//    void adjust_ns(int64_t runtime_ns) { _vruntime_ns += runtime_ns / _cpu_share; }

    ResourceGroupEntry* task_entity() { return &_task_entry; }

    int cpu_share() const { return _cpu_share; }
    uint64_t id() const { return _id; }

private:
    uint64_t _id;
    std::string _name;
    int _cpu_share;
    ResourceGroupEntry _task_entry;
};

using ResourceGroupPtr = std::shared_ptr<ResourceGroup>;

} // namespace resourcegroup
} // namespace doris
