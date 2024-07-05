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

#include "agent/cgroup_cpu_ctl.h"

#include <fmt/format.h>
#include <sys/stat.h>

#include <filesystem>

#include "util/defer_op.h"

namespace doris {

Status CgroupCpuCtl::init() {
    _doris_cgroup_cpu_path = config::doris_cgroup_cpu_path;
    if (_doris_cgroup_cpu_path.empty()) {
        LOG(INFO) << "doris cgroup cpu path is not specify, path=" << _doris_cgroup_cpu_path;
        return Status::InvalidArgument<false>("doris cgroup cpu path {} is not specify.",
                                              _doris_cgroup_cpu_path);
    }

    if (access(_doris_cgroup_cpu_path.c_str(), F_OK) != 0) {
        LOG(INFO) << "doris cgroup cpu path not exists, path=" << _doris_cgroup_cpu_path;
        return Status::InvalidArgument<false>("doris cgroup cpu path {} not exists.",
                                              _doris_cgroup_cpu_path);
    }

    if (_doris_cgroup_cpu_path.back() != '/') {
        _doris_cgroup_cpu_path = _doris_cgroup_cpu_path + "/";
    }
    return Status::OK();
}

void CgroupCpuCtl::get_cgroup_cpu_info(uint64_t* cpu_shares, int* cpu_hard_limit) {
    std::lock_guard<std::shared_mutex> w_lock(_lock_mutex);
    *cpu_shares = this->_cpu_shares;
    *cpu_hard_limit = this->_cpu_hard_limit;
}

void CgroupCpuCtl::update_cpu_hard_limit(int cpu_hard_limit) {
    if (!_init_succ) {
        return;
    }
    std::lock_guard<std::shared_mutex> w_lock(_lock_mutex);
    if (_cpu_hard_limit != cpu_hard_limit) {
        Status ret = modify_cg_cpu_hard_limit_no_lock(cpu_hard_limit);
        if (ret.ok()) {
            _cpu_hard_limit = cpu_hard_limit;
        }
    }
}

void CgroupCpuCtl::update_cpu_soft_limit(int cpu_shares) {
    if (!_init_succ) {
        return;
    }
    std::lock_guard<std::shared_mutex> w_lock(_lock_mutex);
    if (_cpu_shares != cpu_shares) {
        Status ret = modify_cg_cpu_soft_limit_no_lock(cpu_shares);
        if (ret.ok()) {
            _cpu_shares = cpu_shares;
        }
    }
}

Status CgroupCpuCtl::write_cg_sys_file(std::string file_path, int value, std::string msg,
                                       bool is_append) {
    int fd = open(file_path.c_str(), is_append ? O_RDWR | O_APPEND : O_RDWR);
    if (fd == -1) {
        LOG(ERROR) << "open path failed, path=" << file_path;
        return Status::InternalError<false>("open path failed, path={}", file_path);
    }

    Defer defer {[&]() {
        if (-1 == ::close(fd)) {
            LOG(INFO) << "close file fd failed";
        }
    }};

    auto str = fmt::format("{}\n", value);
    int ret = write(fd, str.c_str(), str.size());
    if (ret == -1) {
        LOG(ERROR) << msg << " write sys file failed";
        return Status::InternalError<false>("{} write sys file failed", msg);
    }
    LOG(INFO) << msg << " success";
    return Status::OK();
}

Status CgroupV1CpuCtl::init() {
    RETURN_IF_ERROR(CgroupCpuCtl::init());

    // query path
    _cgroup_v1_cpu_query_path = _doris_cgroup_cpu_path + "query";
    if (access(_cgroup_v1_cpu_query_path.c_str(), F_OK) != 0) {
        int ret = mkdir(_cgroup_v1_cpu_query_path.c_str(), S_IRWXU);
        if (ret != 0) {
            LOG(ERROR) << "cgroup v1 mkdir query failed, path=" << _cgroup_v1_cpu_query_path;
            return Status::InternalError<false>("cgroup v1 mkdir query failed, path={}",
                                                _cgroup_v1_cpu_query_path);
        }
    }

    // check whether current user specified path is a valid cgroup path
    std::string query_path_tasks = _cgroup_v1_cpu_query_path + "/tasks";
    std::string query_path_cpu_shares = _cgroup_v1_cpu_query_path + "/cpu.shares";
    std::string query_path_quota = _cgroup_v1_cpu_query_path + "/cpu.cfs_quota_us";
    if (access(query_path_tasks.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("invalid cgroup path, not find task file");
    }
    if (access(query_path_cpu_shares.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("invalid cgroup path, not find cpu share file");
    }
    if (access(query_path_quota.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("invalid cgroup path, not find cpu quota file");
    }

    if (_wg_id == -1) {
        // means current cgroup cpu ctl is just used to clear dir,
        // it does not contains workload group.
        // todo(wb) rethinking whether need to refactor cgroup_cpu_ctl
        _init_succ = true;
        LOG(INFO) << "init cgroup cpu query path succ, path=" << _cgroup_v1_cpu_query_path;
        return Status::OK();
    }

    // workload group path
    _cgroup_v1_cpu_tg_path = _cgroup_v1_cpu_query_path + "/" + std::to_string(_wg_id);
    if (access(_cgroup_v1_cpu_tg_path.c_str(), F_OK) != 0) {
        int ret = mkdir(_cgroup_v1_cpu_tg_path.c_str(), S_IRWXU);
        if (ret != 0) {
            LOG(ERROR) << "cgroup v1 mkdir workload group failed, path=" << _cgroup_v1_cpu_tg_path;
            return Status::InternalError<false>("cgroup v1 mkdir workload group failed, path=",
                                                _cgroup_v1_cpu_tg_path);
        }
    }

    // quota file
    _cgroup_v1_cpu_tg_quota_file = _cgroup_v1_cpu_tg_path + "/cpu.cfs_quota_us";
    // cpu.shares file
    _cgroup_v1_cpu_tg_shares_file = _cgroup_v1_cpu_tg_path + "/cpu.shares";
    // task file
    _cgroup_v1_cpu_tg_task_file = _cgroup_v1_cpu_tg_path + "/tasks";
    LOG(INFO) << "cgroup v1 cpu path init success"
              << ", query tg path=" << _cgroup_v1_cpu_tg_path
              << ", query tg quota file path=" << _cgroup_v1_cpu_tg_quota_file
              << ", query tg tasks file path=" << _cgroup_v1_cpu_tg_task_file
              << ", core num=" << _cpu_core_num;
    _init_succ = true;
    return Status::OK();
}

Status CgroupV1CpuCtl::modify_cg_cpu_soft_limit_no_lock(int cpu_shares) {
    std::string msg = "modify cpu shares to " + std::to_string(cpu_shares);
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_tg_shares_file, cpu_shares, msg, false);
}

Status CgroupV1CpuCtl::modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) {
    int val = cpu_hard_limit > 0 ? (_cpu_cfs_period_us * _cpu_core_num * cpu_hard_limit / 100)
                                 : CGROUP_CPU_HARD_LIMIT_DEFAULT_VALUE;
    std::string msg = "modify cpu quota value to " + std::to_string(val);
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_tg_quota_file, val, msg, false);
}

Status CgroupV1CpuCtl::add_thread_to_cgroup() {
    if (!_init_succ) {
        return Status::OK();
    }
#if defined(__APPLE__)
    //unsupported now
    return Status::OK();
#else
    int tid = static_cast<int>(syscall(SYS_gettid));
    std::string msg =
            "add thread " + std::to_string(tid) + " to group" + " " + std::to_string(_wg_id);
    std::lock_guard<std::shared_mutex> w_lock(_lock_mutex);
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_tg_task_file, tid, msg, true);
#endif
}

Status CgroupV1CpuCtl::delete_unused_cgroup_path(std::set<uint64_t>& used_wg_ids) {
    if (!_init_succ) {
        return Status::InternalError<false>(
                "cgroup cpu ctl init failed, delete can not be executed");
    }
    // 1 get unused wg id
    std::set<std::string> unused_wg_ids;
    for (const auto& entry : std::filesystem::directory_iterator(_cgroup_v1_cpu_query_path)) {
        const std::string dir_name = entry.path().string();
        struct stat st;
        // == 0 means exists
        if (stat(dir_name.c_str(), &st) == 0 && (st.st_mode & S_IFDIR)) {
            int pos = dir_name.rfind("/");
            std::string wg_dir_name = dir_name.substr(pos + 1, dir_name.length());
            if (wg_dir_name.empty()) {
                return Status::InternalError<false>("find an empty workload group path, path={}",
                                                    dir_name);
            }
            if (std::all_of(wg_dir_name.begin(), wg_dir_name.end(), ::isdigit)) {
                uint64_t id_in_path = std::stoll(wg_dir_name);
                if (used_wg_ids.find(id_in_path) == used_wg_ids.end()) {
                    unused_wg_ids.insert(wg_dir_name);
                }
            }
        }
    }

    // 2 delete unused cgroup path
    int failed_count = 0;
    std::string query_path = _cgroup_v1_cpu_query_path.back() != '/'
                                     ? _cgroup_v1_cpu_query_path + "/"
                                     : _cgroup_v1_cpu_query_path;
    for (const std::string& unused_wg_id : unused_wg_ids) {
        std::string wg_path = query_path + unused_wg_id;
        int ret = rmdir(wg_path.c_str());
        if (ret < 0) {
            LOG(WARNING) << "rmdir failed, path=" << wg_path;
            failed_count++;
        }
    }
    if (failed_count != 0) {
        return Status::InternalError<false>("error happens when delete unused path, count={}",
                                            failed_count);
    }
    return Status::OK();
}

} // namespace doris
