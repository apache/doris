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
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {
namespace taskgroup {

const static uint64_t CPU_SHARE_DEFAULT_VALUE = 1024;
const static std::string MEMORY_LIMIT_DEFAULT_VALUE = "0%";
const static bool ENABLE_MEMORY_OVERCOMMIT_DEFAULT_VALUE = true;
const static int CPU_HARD_LIMIT_DEFAULT_VALUE = -1;

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

TaskGroup::TaskGroup(const TaskGroupInfo& tg_info)
        : _id(tg_info.id),
          _name(tg_info.name),
          _version(tg_info.version),
          _memory_limit(tg_info.memory_limit),
          _enable_memory_overcommit(tg_info.enable_memory_overcommit),
          _cpu_share(tg_info.cpu_share),
          _task_entity(this, "pipeline task entity"),
          _mem_tracker_limiter_pool(MEM_TRACKER_GROUP_NUM),
          _cpu_hard_limit(tg_info.cpu_hard_limit) {}

std::string TaskGroup::debug_string() const {
    std::shared_lock<std::shared_mutex> rl {_mutex};
    return fmt::format(
            "TG[id = {}, name = {}, cpu_share = {}, memory_limit = {}, enable_memory_overcommit = "
            "{}, version = {}, cpu_hard_limit = {}]",
            _id, _name, cpu_share(), PrettyPrinter::print(_memory_limit, TUnit::BYTES),
            _enable_memory_overcommit ? "true" : "false", _version, cpu_hard_limit());
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
            _cpu_hard_limit = tg_info.cpu_hard_limit;
        } else {
            return;
        }
    }
    ExecEnv::GetInstance()->pipeline_task_group_scheduler()->task_queue()->update_tg_cpu_share(
            tg_info, &_task_entity);
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

Status TaskGroupInfo::parse_topic_info(const TWorkloadGroupInfo& workload_group_info,
                                       taskgroup::TaskGroupInfo* task_group_info) {
    // 1 id
    int tg_id = 0;
    if (workload_group_info.__isset.id) {
        tg_id = workload_group_info.id;
    } else {
        return Status::InternalError<false>("workload group id is required");
    }
    task_group_info->id = tg_id;

    // 2 name
    std::string name = "INVALID_NAME";
    if (workload_group_info.__isset.name) {
        name = workload_group_info.name;
    }
    task_group_info->name = name;

    // 3 version
    int version = 0;
    if (workload_group_info.__isset.version) {
        version = workload_group_info.version;
    } else {
        return Status::InternalError<false>("workload group version is required");
    }
    task_group_info->version = version;

    // 4 cpu_share
    uint64_t cpu_share = CPU_SHARE_DEFAULT_VALUE;
    if (workload_group_info.__isset.cpu_share) {
        cpu_share = workload_group_info.cpu_share;
    }
    task_group_info->cpu_share = cpu_share;

    // 5 cpu hard limit
    int cpu_hard_limit = CPU_HARD_LIMIT_DEFAULT_VALUE;
    if (workload_group_info.__isset.cpu_hard_limit) {
        cpu_hard_limit = workload_group_info.cpu_hard_limit;
    }
    task_group_info->cpu_hard_limit = cpu_hard_limit;

    // 6 mem_limit
    std::string mem_limit_str = MEMORY_LIMIT_DEFAULT_VALUE;
    if (workload_group_info.__isset.mem_limit) {
        mem_limit_str = workload_group_info.mem_limit;
    }
    bool is_percent = true;
    int64_t mem_limit =
            ParseUtil::parse_mem_spec(mem_limit_str, -1, MemInfo::mem_limit(), &is_percent);
    task_group_info->memory_limit = mem_limit;

    // 7 mem overcommit
    bool enable_memory_overcommit = ENABLE_MEMORY_OVERCOMMIT_DEFAULT_VALUE;
    if (workload_group_info.__isset.enable_memory_overcommit) {
        enable_memory_overcommit = workload_group_info.enable_memory_overcommit;
    }
    task_group_info->enable_memory_overcommit = enable_memory_overcommit;

    // 8 cpu soft limit or hard limit
    bool enable_cpu_hard_limit = false;
    if (workload_group_info.__isset.enable_cpu_hard_limit) {
        enable_cpu_hard_limit = workload_group_info.enable_cpu_hard_limit;
    }
    task_group_info->enable_cpu_hard_limit = enable_cpu_hard_limit;

    return Status::OK();
}

} // namespace taskgroup
} // namespace doris
