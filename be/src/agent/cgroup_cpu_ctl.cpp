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

namespace doris {

Status CgroupCpuCtl::init() {
    _doris_cgroup_cpu_path = config::doris_cgroup_cpu_path;
    if (_doris_cgroup_cpu_path.empty()) {
        LOG(INFO) << "doris cgroup cpu path is not specify, path=" << _doris_cgroup_cpu_path;
        return Status::InternalError("doris cgroup cpu path {} is not specify.",
                                     _doris_cgroup_cpu_path);
    }

    if (access(_doris_cgroup_cpu_path.c_str(), F_OK) != 0) {
        LOG(ERROR) << "doris cgroup cpu path not exists, path=" << _doris_cgroup_cpu_path;
        return Status::InternalError("doris cgroup cpu path {} not exists.",
                                     _doris_cgroup_cpu_path);
    }

    if (_doris_cgroup_cpu_path.back() != '/') {
        _doris_cgroup_cpu_path = _doris_cgroup_cpu_path + "/";
    }
    return Status::OK();
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

Status CgroupCpuCtl::write_cg_sys_file(std::string file_path, int value, std::string msg,
                                       bool is_append) {
    int fd = open(file_path.c_str(), is_append ? O_RDWR | O_APPEND : O_RDWR);
    if (fd == -1) {
        LOG(ERROR) << "open path failed, path=" << file_path;
        return Status::InternalError("open path failed, path={}", file_path);
    }

    std::stringstream ss;
    ss << value << std::endl;
    const std::string& str = ss.str();
    int ret = write(fd, str.c_str(), str.size());
    if (ret == -1) {
        LOG(ERROR) << msg << " write sys file failed";
        return Status::InternalError("{} write sys file failed", msg);
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
            return Status::InternalError("cgroup v1 mkdir query failed, path=",
                                         _cgroup_v1_cpu_query_path);
        }
    }

    // quota path
    _cgroup_v1_cpu_query_quota_path = _cgroup_v1_cpu_query_path + "/cpu.cfs_quota_us";
    // task path
    _cgroup_v1_cpu_query_task_path = _cgroup_v1_cpu_query_path + "/tasks";
    LOG(INFO) << "cgroup v1 cpu path init success"
              << ", query path=" << _cgroup_v1_cpu_query_path
              << ", query quota path=" << _cgroup_v1_cpu_query_quota_path
              << ", query tasks path=" << _cgroup_v1_cpu_query_task_path
              << ", core num=" << _cpu_core_num;
    _init_succ = true;
    return Status::OK();
}

Status CgroupV1CpuCtl::modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) {
    int val = _cpu_cfs_period_us * _cpu_core_num * cpu_hard_limit / 100;
    std::string msg = "modify cpu quota value to " + std::to_string(val);
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_query_quota_path, val, msg, false);
}

Status CgroupV1CpuCtl::add_thread_to_cgroup() {
    if (!_init_succ) {
        return Status::OK();
    }
    int tid = static_cast<int>(syscall(SYS_gettid));
    std::string msg = "add thread " + std::to_string(tid) + " to group";
    std::lock_guard<std::shared_mutex> w_lock(_lock_mutex);
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_query_task_path, tid, msg, true);
}
} // namespace doris