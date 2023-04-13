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

#include <charconv>

#include "gen_cpp/PaloInternalService_types.h"
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
    auto v_time = runtime_ns / _tg->cpu_share();
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
    return _tg->cpu_share();
}

std::string TaskGroupEntity::debug_string() const {
    return fmt::format("TGE[id = {}, cpu_share = {}, task size: {}, v_time:{}ns]", _tg->id(),
                       cpu_share(), _queue.size(), _vruntime_ns);
}

TaskGroup::TaskGroup(uint64_t id, std::string name, uint64_t cpu_share, int64_t version)
        : _id(id), _name(name), _cpu_share(cpu_share), _task_entity(this), _version(version) {}

std::string TaskGroup::debug_string() const {
    std::shared_lock<std::shared_mutex> rl {mutex};
    return fmt::format("TG[id = {}, name = {}, cpu_share = {}, version = {}]", _id, _name,
                       cpu_share(), _version);
}

bool TaskGroup::check_version(int64_t version) const {
    std::shared_lock<std::shared_mutex> rl {mutex};
    return version > _version;
}

void TaskGroup::check_and_update(const TaskGroupInfo& tg_info) {
    if (tg_info._id != _id) {
        return;
    }

    std::lock_guard<std::shared_mutex> wl {mutex};
    if (tg_info._version > _version) {
        _name = tg_info._name;
        _cpu_share = tg_info._cpu_share;
        _version = tg_info._version;
    }
}

Status TaskGroupInfo::parse_group_info(const TPipelineResourceGroup& resource_group,
                                       TaskGroupInfo* task_group_info) {
    if (!check_group_info(resource_group)) {
        std::stringstream ss;
        ss << "incomplete resource group parameters: ";
        resource_group.printTo(ss);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    auto iter = resource_group.properties.find(CPU_SHARE);
    uint64_t share = 0;
    std::from_chars(iter->second.c_str(), iter->second.c_str() + iter->second.size(), share);

    task_group_info->_id = resource_group.id;
    task_group_info->_name = resource_group.name;
    task_group_info->_version = resource_group.version;
    task_group_info->_cpu_share = share;
    return Status::OK();
}

bool TaskGroupInfo::check_group_info(const TPipelineResourceGroup& resource_group) {
    return resource_group.__isset.id && resource_group.__isset.version &&
           resource_group.__isset.name && resource_group.__isset.properties &&
           resource_group.properties.count(CPU_SHARE) > 0;
}

} // namespace taskgroup
} // namespace doris
