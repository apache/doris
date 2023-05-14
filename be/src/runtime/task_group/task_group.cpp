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

#include <fmt/format.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <charconv>
#include <map>
#include <mutex>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "service/backend_options.h"
#include "util/mem_info.h"
#include "util/parse_util.h"

namespace doris {
namespace taskgroup {

const static std::string CPU_SHARE = "cpu_share";
const static std::string MEMORY_LIMIT = "memory_limit";

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

uint64_t TaskGroupEntity::task_group_id() const {
    return _tg->id();
}

std::string TaskGroupEntity::debug_string() const {
    return fmt::format("TGE[id = {}, cpu_share = {}, task size: {}, v_time:{}ns]", _tg->id(),
                       cpu_share(), _queue.size(), _vruntime_ns);
}

TaskGroup::TaskGroup(const TaskGroupInfo& tg_info)
        : _id(tg_info.id),
          _name(tg_info.name),
          _cpu_share(tg_info.cpu_share),
          _memory_limit(tg_info.memory_limit),
          _version(tg_info.version),
          _task_entity(this),
          _mem_tracker_limiter_pool(MEM_TRACKER_GROUP_NUM) {}

std::string TaskGroup::debug_string() const {
    std::shared_lock<std::shared_mutex> rl {_mutex};
    return fmt::format("TG[id = {}, name = {}, cpu_share = {}, memory_limit = {}, version = {}]",
                       _id, _name, cpu_share(), PrettyPrinter::print(_memory_limit, TUnit::BYTES),
                       _version);
}

void TaskGroup::check_and_update(const TaskGroupInfo& tg_info) {
    if (UNLIKELY(tg_info.id != _id)) {
        return;
    }
    {
        std::shared_lock<std::shared_mutex> rl {_mutex};
        if (LIKELY(tg_info.version <= _version)) {
            return;
        }
    }

    std::lock_guard<std::shared_mutex> wl {_mutex};
    if (tg_info.version > _version) {
        _name = tg_info.name;
        _version = tg_info.version;
        _memory_limit = tg_info.memory_limit;
        if (_cpu_share != tg_info.cpu_share) {
            ExecEnv::GetInstance()->pipeline_task_group_scheduler()->update_tg_cpu_share(
                    tg_info, shared_from_this());
        }
    }
}

void TaskGroup::update_cpu_share_unlock(const TaskGroupInfo& tg_info) {
    _cpu_share = tg_info.cpu_share;
}

void TaskGroup::add_mem_tracker_limiter(std::shared_ptr<MemTrackerLimiter> mem_tracker_ptr) {
    auto group_num = mem_tracker_ptr->group_num();
    std::lock_guard<std::mutex> l(_mem_tracker_limiter_pool[group_num].group_lock);
    _mem_tracker_limiter_pool[group_num].trackers.insert(mem_tracker_ptr);
}

void TaskGroup::remove_mem_tracker_limiter(std::shared_ptr<MemTrackerLimiter> mem_tracker_ptr) {
    auto group_num = mem_tracker_ptr->group_num();
    std::lock_guard<std::mutex> l(_mem_tracker_limiter_pool[group_num].group_lock);
    _mem_tracker_limiter_pool[group_num].trackers.erase(mem_tracker_ptr);
}

int64_t TaskGroup::memory_limit_gc() {
    std::string name;
    int64_t memory_limit;
    {
        std::shared_lock<std::shared_mutex> rl {_mutex};
        name = _name;
        memory_limit = _memory_limit;
    }
    return MemTrackerLimiter::tg_memory_limit_gc(_id, name, memory_limit,
                                                 _mem_tracker_limiter_pool);
}

Status TaskGroupInfo::parse_group_info(const TPipelineResourceGroup& resource_group,
                                       TaskGroupInfo* task_group_info) {
    if (UNLIKELY(!check_group_info(resource_group))) {
        std::stringstream ss;
        ss << "incomplete resource group parameters: ";
        resource_group.printTo(ss);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    auto iter = resource_group.properties.find(CPU_SHARE);
    uint64_t share = 0;
    std::from_chars(iter->second.c_str(), iter->second.c_str() + iter->second.size(), share);

    task_group_info->id = resource_group.id;
    task_group_info->name = resource_group.name;
    task_group_info->version = resource_group.version;
    task_group_info->cpu_share = share;

    bool is_percent = true;
    auto mem_limit_str = resource_group.properties.find(MEMORY_LIMIT)->second;
    auto mem_limit =
            ParseUtil::parse_mem_spec(mem_limit_str, -1, MemInfo::mem_limit(), &is_percent);
    if (UNLIKELY(mem_limit <= 0)) {
        std::stringstream ss;
        ss << "parse memory limit from TPipelineResourceGroup error, " << MEMORY_LIMIT << ": "
           << mem_limit_str;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    task_group_info->memory_limit = mem_limit;
    return Status::OK();
}

bool TaskGroupInfo::check_group_info(const TPipelineResourceGroup& resource_group) {
    return resource_group.__isset.id && resource_group.__isset.version &&
           resource_group.__isset.name && resource_group.__isset.properties &&
           resource_group.properties.count(CPU_SHARE) > 0 &&
           resource_group.properties.count(MEMORY_LIMIT) > 0;
}

} // namespace taskgroup
} // namespace doris
