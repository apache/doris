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

#include "task_group_manager.h"

#include <memory>
#include <mutex>

#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/task_group/task_group.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris::taskgroup {

TaskGroupPtr TaskGroupManager::get_or_create_task_group(const TaskGroupInfo& task_group_info) {
    {
        std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
        if (LIKELY(_task_groups.count(task_group_info.id))) {
            auto task_group = _task_groups[task_group_info.id];
            task_group->check_and_update(task_group_info);
            return task_group;
        }
    }

    auto new_task_group = std::make_shared<TaskGroup>(task_group_info);
    std::lock_guard<std::shared_mutex> w_lock(_group_mutex);
    if (_task_groups.count(task_group_info.id)) {
        auto task_group = _task_groups[task_group_info.id];
        task_group->check_and_update(task_group_info);
        return task_group;
    }
    _task_groups[task_group_info.id] = new_task_group;
    return new_task_group;
}

void TaskGroupManager::get_resource_groups(const std::function<bool(const TaskGroupPtr& ptr)>& pred,
                                           std::vector<TaskGroupPtr>* task_groups) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    for (const auto& [id, task_group] : _task_groups) {
        if (pred(task_group)) {
            task_groups->push_back(task_group);
        }
    }
}

TaskGroupPtr TaskGroupManager::get_task_group_by_id(uint64_t tg_id) {
    std::shared_lock<std::shared_mutex> r_lock(_group_mutex);
    if (_task_groups.find(tg_id) != _task_groups.end()) {
        return _task_groups.at(tg_id);
    }
    return nullptr;
}

void TaskGroupManager::get_query_scheduler(uint64_t tg_id,
                                           doris::pipeline::TaskScheduler** exec_sched,
                                           vectorized::SimplifiedScanScheduler** scan_sched,
                                           ThreadPool** non_pipe_thread_pool) {
    std::shared_lock<std::shared_mutex> r_lock(_task_scheduler_lock);
    auto tg_sche_it = _tg_sche_map.find(tg_id);
    if (tg_sche_it != _tg_sche_map.end()) {
        *exec_sched = tg_sche_it->second.get();
    }

    auto tg_scan_sche_it = _tg_scan_sche_map.find(tg_id);
    if (tg_scan_sche_it != _tg_scan_sche_map.end()) {
        *scan_sched = tg_scan_sche_it->second.get();
    }

    auto non_pipe_thread_pool_iter = _non_pipe_thread_pool_map.find(tg_id);
    if (non_pipe_thread_pool_iter != _non_pipe_thread_pool_map.end()) {
        *non_pipe_thread_pool = non_pipe_thread_pool_iter->second.get();
    }
}

Status TaskGroupManager::upsert_cg_task_scheduler(taskgroup::TaskGroupInfo* tg_info,
                                                  ExecEnv* exec_env) {
    uint64_t tg_id = tg_info->id;
    std::string tg_name = tg_info->name;
    int cpu_hard_limit = tg_info->cpu_hard_limit;
    uint64_t cpu_shares = tg_info->cpu_share;
    bool enable_cpu_hard_limit = tg_info->enable_cpu_hard_limit;

    std::lock_guard<std::shared_mutex> write_lock(_task_scheduler_lock);
    // step 1: init cgroup cpu controller
    CgroupCpuCtl* cg_cu_ctl_ptr = nullptr;
    if (_cgroup_ctl_map.find(tg_id) == _cgroup_ctl_map.end()) {
        std::unique_ptr<CgroupCpuCtl> cgroup_cpu_ctl = std::make_unique<CgroupV1CpuCtl>(tg_id);
        Status ret = cgroup_cpu_ctl->init();
        if (ret.ok()) {
            cg_cu_ctl_ptr = cgroup_cpu_ctl.get();
            _cgroup_ctl_map.emplace(tg_id, std::move(cgroup_cpu_ctl));
        } else {
            return Status::InternalError<false>("cgroup init failed, gid={}, reason={}", tg_id,
                                                ret.to_string());
        }
    }

    // step 2: init task scheduler
    if (_tg_sche_map.find(tg_id) == _tg_sche_map.end()) {
        int32_t executors_size = config::pipeline_executor_size;
        if (executors_size <= 0) {
            executors_size = CpuInfo::num_cores();
        }
        auto task_queue = std::make_shared<pipeline::MultiCoreTaskQueue>(executors_size);

        auto pipeline_task_scheduler = std::make_unique<pipeline::TaskScheduler>(
                exec_env, exec_env->get_global_block_scheduler(), std::move(task_queue),
                "Exec_" + tg_name, cg_cu_ctl_ptr);
        Status ret = pipeline_task_scheduler->start();
        if (ret.ok()) {
            _tg_sche_map.emplace(tg_id, std::move(pipeline_task_scheduler));
        } else {
            return Status::InternalError<false>("task scheduler start failed, gid={}", tg_id);
        }
    }

    // step 3: init scan scheduler
    if (_tg_scan_sche_map.find(tg_id) == _tg_scan_sche_map.end()) {
        auto scan_scheduler =
                std::make_unique<vectorized::SimplifiedScanScheduler>(tg_name, cg_cu_ctl_ptr);
        Status ret = scan_scheduler->start();
        if (ret.ok()) {
            _tg_scan_sche_map.emplace(tg_id, std::move(scan_scheduler));
        } else {
            return Status::InternalError<false>("scan scheduler start failed, gid={}", tg_id);
        }
    }

    // step 4: init non-pipe scheduler
    if (_non_pipe_thread_pool_map.find(tg_id) == _non_pipe_thread_pool_map.end()) {
        std::unique_ptr<ThreadPool> thread_pool = nullptr;
        auto ret = ThreadPoolBuilder("nonPip_" + tg_name)
                           .set_min_threads(1)
                           .set_max_threads(config::fragment_pool_thread_num_max)
                           .set_max_queue_size(config::fragment_pool_queue_size)
                           .set_cgroup_cpu_ctl(cg_cu_ctl_ptr)
                           .build(&thread_pool);
        if (!ret.ok()) {
            LOG(INFO) << "create non-pipline thread pool failed";
        } else {
            _non_pipe_thread_pool_map.emplace(tg_id, std::move(thread_pool));
        }
    }

    // step 5: update cgroup cpu if needed
    if (enable_cpu_hard_limit) {
        if (cpu_hard_limit > 0) {
            _cgroup_ctl_map.at(tg_id)->update_cpu_hard_limit(cpu_hard_limit);
            _cgroup_ctl_map.at(tg_id)->update_cpu_soft_limit(CPU_SOFT_LIMIT_DEFAULT_VALUE);
        } else {
            return Status::InternalError<false>("enable cpu hard limit but value is illegal");
        }
    } else {
        if (config::enable_cgroup_cpu_soft_limit) {
            _cgroup_ctl_map.at(tg_id)->update_cpu_soft_limit(cpu_shares);
            _cgroup_ctl_map.at(tg_id)->update_cpu_hard_limit(
                    CPU_HARD_LIMIT_DEFAULT_VALUE); // disable cpu hard limit
        }
    }
    _cgroup_ctl_map.at(tg_id)->get_cgroup_cpu_info(&(tg_info->cgroup_cpu_shares),
                                                   &(tg_info->cgroup_cpu_hard_limit));

    return Status::OK();
}

void TaskGroupManager::delete_task_group_by_ids(std::set<uint64_t> used_wg_id) {
    // stop task sche may cost some time, so it should not be locked
    std::set<doris::pipeline::TaskScheduler*> task_sche_to_del;
    std::set<vectorized::SimplifiedScanScheduler*> scan_task_sche_to_del;
    std::set<ThreadPool*> non_pip_thread_pool_to_del;
    std::set<uint64_t> deleted_tg_ids;
    {
        std::shared_lock<std::shared_mutex> read_lock(_task_scheduler_lock);
        for (auto iter = _tg_sche_map.begin(); iter != _tg_sche_map.end(); iter++) {
            uint64_t tg_id = iter->first;
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                task_sche_to_del.insert(_tg_sche_map[tg_id].get());
                deleted_tg_ids.insert(tg_id);
            }
        }

        for (auto iter = _tg_scan_sche_map.begin(); iter != _tg_scan_sche_map.end(); iter++) {
            uint64_t tg_id = iter->first;
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                scan_task_sche_to_del.insert(_tg_scan_sche_map[tg_id].get());
            }
        }
        for (auto iter = _non_pipe_thread_pool_map.begin(); iter != _non_pipe_thread_pool_map.end();
             iter++) {
            uint64_t tg_id = iter->first;
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                non_pip_thread_pool_to_del.insert(_non_pipe_thread_pool_map[tg_id].get());
            }
        }
    }
    // 1 stop all threads
    for (auto* ptr1 : task_sche_to_del) {
        ptr1->stop();
    }
    for (auto* ptr2 : scan_task_sche_to_del) {
        ptr2->stop();
    }
    for (auto& ptr3 : non_pip_thread_pool_to_del) {
        ptr3->shutdown();
    }
    // 2 release resource in memory
    {
        std::lock_guard<std::shared_mutex> write_lock(_task_scheduler_lock);
        for (uint64_t tg_id : deleted_tg_ids) {
            _tg_sche_map.erase(tg_id);
            _tg_scan_sche_map.erase(tg_id);
            _cgroup_ctl_map.erase(tg_id);
        }
    }

    {
        std::lock_guard<std::shared_mutex> write_lock(_group_mutex);
        for (auto iter = _task_groups.begin(); iter != _task_groups.end();) {
            uint64_t tg_id = iter->first;
            if (used_wg_id.find(tg_id) == used_wg_id.end()) {
                iter = _task_groups.erase(iter);
            } else {
                iter++;
            }
        }
    }

    // 3 clear cgroup dir
    // NOTE(wb) currently we use rmdir to delete cgroup path,
    // this action may be failed until task file is cleared which means all thread are stopped.
    // So the first time to rmdir a cgroup path may failed.
    // Using cgdelete has no such issue.
    {
        std::lock_guard<std::shared_mutex> write_lock(_init_cg_ctl_lock);
        if (!_cg_cpu_ctl) {
            _cg_cpu_ctl = std::make_unique<CgroupV1CpuCtl>();
        }
        if (!_is_init_succ) {
            Status ret = _cg_cpu_ctl->init();
            if (ret.ok()) {
                _is_init_succ = true;
            } else {
                LOG(INFO) << "init task group mgr cpu ctl failed, " << ret.to_string();
            }
        }
        if (_is_init_succ) {
            Status ret = _cg_cpu_ctl->delete_unused_cgroup_path(used_wg_id);
            if (!ret.ok()) {
                LOG(WARNING) << ret.to_string();
            }
        }
    }
    LOG(INFO) << "finish clear unused cgroup path";
}

void TaskGroupManager::stop() {
    for (auto& task_sche : _tg_sche_map) {
        task_sche.second->stop();
    }
    for (auto& task_sche : _tg_scan_sche_map) {
        task_sche.second->stop();
    }
    for (auto& no_pip_sche : _non_pipe_thread_pool_map) {
        no_pip_sche.second->shutdown();
    }
}

} // namespace doris::taskgroup
