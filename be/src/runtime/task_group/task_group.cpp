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
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/mem_info.h"
#include "util/parse_util.h"
#include "vec/exec/scan/scan_task_queue.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {
namespace taskgroup {

const static std::string CPU_SHARE = "cpu_share";
const static std::string MEMORY_LIMIT = "memory_limit";
const static std::string ENABLE_MEMORY_OVERCOMMIT = "enable_memory_overcommit";
const static std::string CPU_HARD_LIMIT = "cpu_hard_limit";

template <typename QueueType>
TaskGroupEntity<QueueType>::TaskGroupEntity(taskgroup::TaskGroup* tg, std::string type)
        : _tg(tg), _type(type), _version(tg->version()), _cpu_share(tg->cpu_share()) {
    _task_queue = new QueueType();
}

template <typename QueueType>
TaskGroupEntity<QueueType>::~TaskGroupEntity() {
    delete _task_queue;
}

template <typename QueueType>
QueueType* TaskGroupEntity<QueueType>::task_queue() {
    return _task_queue;
}

template <typename QueueType>
void TaskGroupEntity<QueueType>::incr_runtime_ns(uint64_t runtime_ns) {
    auto v_time = runtime_ns / _cpu_share;
    _vruntime_ns += v_time;
}

template <typename QueueType>
void TaskGroupEntity<QueueType>::adjust_vruntime_ns(uint64_t vruntime_ns) {
    VLOG_DEBUG << "adjust " << debug_string() << "vtime to " << vruntime_ns;
    _vruntime_ns = vruntime_ns;
}

template <typename QueueType>
size_t TaskGroupEntity<QueueType>::task_size() const {
    return _task_queue->size();
}

template <typename QueueType>
uint64_t TaskGroupEntity<QueueType>::cpu_share() const {
    return _cpu_share;
}

template <typename QueueType>
uint64_t TaskGroupEntity<QueueType>::task_group_id() const {
    return _tg->id();
}

template <typename QueueType>
void TaskGroupEntity<QueueType>::check_and_update_cpu_share(const TaskGroupInfo& tg_info) {
    if (tg_info.version > _version) {
        _cpu_share = tg_info.cpu_share;
        _version = tg_info.version;
    }
}

template <typename QueueType>
std::string TaskGroupEntity<QueueType>::debug_string() const {
    return fmt::format("TGE[id = {}, name = {}-{}, cpu_share = {}, task size: {}, v_time:{} ns]",
                       _tg->id(), _tg->name(), _type, cpu_share(), task_size(), _vruntime_ns);
}

template class TaskGroupEntity<std::queue<pipeline::PipelineTask*>>;
template class TaskGroupEntity<ScanTaskQueue>;

TaskGroup::TaskGroup(const TaskGroupInfo& tg_info)
        : _id(tg_info.id),
          _name(tg_info.name),
          _version(tg_info.version),
          _memory_limit(tg_info.memory_limit),
          _enable_memory_overcommit(tg_info.enable_memory_overcommit),
          _cpu_share(tg_info.cpu_share),
          _task_entity(this, "pipeline task entity"),
          _local_scan_entity(this, "local scan entity"),
          _mem_tracker_limiter_pool(MEM_TRACKER_GROUP_NUM) {}

std::string TaskGroup::debug_string() const {
    std::shared_lock<std::shared_mutex> rl {_mutex};
    return fmt::format(
            "TG[id = {}, name = {}, cpu_share = {}, memory_limit = {}, enable_memory_overcommit = "
            "{}, version = {}]",
            _id, _name, cpu_share(), PrettyPrinter::print(_memory_limit, TUnit::BYTES),
            _enable_memory_overcommit ? "true" : "false", _version);
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
    {
        std::lock_guard<std::shared_mutex> wl {_mutex};
        if (tg_info.version > _version) {
            _name = tg_info.name;
            _version = tg_info.version;
            _memory_limit = tg_info.memory_limit;
            _enable_memory_overcommit = tg_info.enable_memory_overcommit;
            _cpu_share = tg_info.cpu_share;
        } else {
            return;
        }
    }
    ExecEnv::GetInstance()->pipeline_task_group_scheduler()->task_queue()->update_tg_cpu_share(
            tg_info, &_task_entity);
    ExecEnv::GetInstance()->scanner_scheduler()->local_scan_task_queue()->update_tg_cpu_share(
            tg_info, &_local_scan_entity);
}

int64_t TaskGroup::memory_used() {
    int64_t used_memory = 0;
    for (auto& mem_tracker_group : _mem_tracker_limiter_pool) {
        std::lock_guard<std::mutex> l(mem_tracker_group.group_lock);
        for (const auto& tracker : mem_tracker_group.trackers) {
            used_memory += tracker->is_query_cancelled() ? 0 : tracker->consumption();
        }
    }
    return used_memory;
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

void TaskGroup::task_group_info(TaskGroupInfo* tg_info) const {
    std::shared_lock<std::shared_mutex> r_lock(_mutex);
    tg_info->id = _id;
    tg_info->name = _name;
    tg_info->cpu_share = _cpu_share;
    tg_info->memory_limit = _memory_limit;
    tg_info->enable_memory_overcommit = _enable_memory_overcommit;
    tg_info->version = _version;
}

Status TaskGroupInfo::parse_group_info(const TPipelineWorkloadGroup& resource_group,
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

    int cpu_hard_limit = 0;
    auto iter2 = resource_group.properties.find(CPU_HARD_LIMIT);
    std::from_chars(iter2->second.c_str(), iter2->second.c_str() + iter2->second.size(),
                    cpu_hard_limit);

    task_group_info->id = resource_group.id;
    task_group_info->name = resource_group.name;
    task_group_info->version = resource_group.version;
    task_group_info->cpu_share = share;
    task_group_info->cpu_hard_limit = cpu_hard_limit;

    bool is_percent = true;
    auto mem_limit_str = resource_group.properties.find(MEMORY_LIMIT)->second;
    auto mem_limit =
            ParseUtil::parse_mem_spec(mem_limit_str, -1, MemInfo::mem_limit(), &is_percent);
    if (UNLIKELY(mem_limit <= 0)) {
        std::stringstream ss;
        ss << "parse memory limit from TPipelineWorkloadGroup error, " << MEMORY_LIMIT << ": "
           << mem_limit_str;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    task_group_info->memory_limit = mem_limit;

    auto enable_memory_overcommit_iter = resource_group.properties.find(ENABLE_MEMORY_OVERCOMMIT);
    task_group_info->enable_memory_overcommit =
            enable_memory_overcommit_iter != resource_group.properties.end() &&
            enable_memory_overcommit_iter->second ==
                    "true" /* fe guarantees it is 'true' or 'false' */;
    return Status::OK();
}

bool TaskGroupInfo::check_group_info(const TPipelineWorkloadGroup& resource_group) {
    return resource_group.__isset.id && resource_group.__isset.version &&
           resource_group.__isset.name && resource_group.__isset.properties &&
           resource_group.properties.count(CPU_SHARE) > 0 &&
           resource_group.properties.count(MEMORY_LIMIT) > 0;
}

} // namespace taskgroup
} // namespace doris
