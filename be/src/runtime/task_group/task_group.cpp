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

#include "task_group.h"

#include "pipeline/pipeline_task.h"

namespace doris {
namespace taskgroup {

pipeline::PipelineTask* TaskGroupEntity::take() {
    if (_queue.empty()) {
        return nullptr;
    }
    auto task = _queue.front();
    _queue.pop();
    return task;
}

void TaskGroupEntity::incr_runtime_ns(uint64_t runtime_ns) {
    auto v_time = runtime_ns / _tg->share();
    _vruntime_ns += v_time;
}

void TaskGroupEntity::adjust_vruntime_ns(uint64_t vruntime_ns) {
    VLOG_DEBUG << "adjust " << debug_string() << "vtime to " << vruntime_ns;
    _vruntime_ns = vruntime_ns;
}

void TaskGroupEntity::push_back(pipeline::PipelineTask* task) {
    _queue.emplace(task);
}

uint64_t TaskGroupEntity::cpu_share() const {
    return _tg->share();
}

std::string TaskGroupEntity::debug_string() const {
    return fmt::format("TGE[id = {}, cpu_share = {}, task size: {}, v_time:{}ns]", _tg->id(),
                       cpu_share(), _queue.size(), _vruntime_ns);
}

TaskGroup::TaskGroup(uint64_t id, std::string name, uint64_t share)
        : _id(id), _name(name), _share(share), _task_entity(this) {}

std::string TaskGroup::debug_string() const {
    return fmt::format("TG[id = {}, name = {}, share = {}", _id, _name, share());
}

} // namespace taskgroup
} // namespace doris
