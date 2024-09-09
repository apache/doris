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
#include <unistd.h>

#include <filesystem>

#include "util/cgroup_util.h"
#include "util/defer_op.h"

namespace doris {

bool CgroupCpuCtl::is_a_valid_cgroup_path(std::string cg_path) {
    if (!cg_path.empty()) {
        if (cg_path.back() != '/') {
            cg_path = cg_path + "/";
        }
        if (_is_enable_cgroup_v2_in_env) {
            std::string query_path_cg_type = cg_path + "cgroup.type";
            std::string query_path_ctl = cg_path + "cgroup.subtree_control";
            std::string query_path_procs = cg_path + "cgroup.procs";
            if (access(query_path_cg_type.c_str(), F_OK) != 0 ||
                access(query_path_ctl.c_str(), F_OK) != 0 ||
                access(query_path_procs.c_str(), F_OK) != 0) {
                LOG(WARNING) << "[cgroup_init_path]invalid cgroup v2 path, access neccessary file "
                                "failed";
            } else {
                return true;
            }
        } else if (_is_enable_cgroup_v1_in_env) {
            std::string query_path_tasks = cg_path + "tasks";
            std::string query_path_cpu_shares = cg_path + "cpu.shares";
            std::string query_path_quota = cg_path + "cpu.cfs_quota_us";
            if (access(query_path_tasks.c_str(), F_OK) != 0 ||
                access(query_path_cpu_shares.c_str(), F_OK) != 0 ||
                access(query_path_quota.c_str(), F_OK) != 0) {
                LOG(WARNING) << "[cgroup_init_path]invalid cgroup v1 path, access neccessary file "
                                "failed";
            } else {
                return true;
            }
        }
    }
    return false;
}

void CgroupCpuCtl::init_doris_cgroup_path() {
    std::string conf_path = config::doris_cgroup_cpu_path;
    if (conf_path.empty()) {
        LOG(INFO) << "[cgroup_init_path]doris cgroup home path is not specify, if you not use "
                     "workload group, you can ignore this log.";
        return;
    }

    if (access(conf_path.c_str(), F_OK) != 0) {
        LOG(INFO) << "[cgroup_init_path]doris cgroup home path not exists, path=" << conf_path;
        return;
    }

    if (conf_path.back() != '/') {
        conf_path = conf_path + "/";
    }

    // check whether current user specified path is a valid cgroup path
    std::string cg_msg = "not set cgroup in env";
    if (CGroupUtil::cgroupsv2_enable()) {
        _is_enable_cgroup_v2_in_env = true;
        cg_msg = "cgroup v2 is enabled in env";
    } else if (CGroupUtil::cgroupsv1_enable()) {
        _is_enable_cgroup_v1_in_env = true;
        cg_msg = "cgroup v1 is enabled in env";
    }
    bool is_cgroup_path_valid = CgroupCpuCtl::is_a_valid_cgroup_path(conf_path);

    std::string tmp_query_path = conf_path + "query";
    if (is_cgroup_path_valid) {
        if (access(tmp_query_path.c_str(), F_OK) != 0) {
            int ret = mkdir(tmp_query_path.c_str(), S_IRWXU);
            if (ret != 0) {
                LOG(ERROR) << "[cgroup_init_path]cgroup mkdir query failed, path="
                           << tmp_query_path;
            }
        }
        _is_cgroup_query_path_valid = CgroupCpuCtl::is_a_valid_cgroup_path(tmp_query_path);
    }

    _doris_cgroup_cpu_path = conf_path;
    _doris_cgroup_cpu_query_path = tmp_query_path;
    std::string query_path_msg = _is_cgroup_query_path_valid ? "cgroup query path is valid"
                                                             : "cgroup query path is not valid";
    _cpu_core_num = CpuInfo::num_cores();

    std::string init_cg_v2_msg = "";
    if (_is_enable_cgroup_v2_in_env && _is_cgroup_query_path_valid) {
        Status ret = init_cgroup_v2_query_path_public_file(_doris_cgroup_cpu_path,
                                                           _doris_cgroup_cpu_query_path);
        if (!ret.ok()) {
            init_cg_v2_msg = " write cgroup v2 file failed, err=" + ret.to_string_no_stack() + ". ";
        } else {
            init_cg_v2_msg = "write cgroup v2 public file succ.";
        }
    }

    LOG(INFO) << "[cgroup_init_path]init cgroup home path finish, home path="
              << _doris_cgroup_cpu_path << ", query path=" << _doris_cgroup_cpu_query_path << ", "
              << cg_msg << ", " << query_path_msg << ", core_num=" << _cpu_core_num << ". "
              << init_cg_v2_msg;
}

Status CgroupCpuCtl::init_cgroup_v2_query_path_public_file(std::string home_path,
                                                           std::string query_path) {
    // 1 enable cpu controller for home path's child
    _doris_cgroup_cpu_path_subtree_ctl_file = home_path + "cgroup.subtree_control";
    if (access(_doris_cgroup_cpu_path_subtree_ctl_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 doris home's subtree control file");
    }
    RETURN_IF_ERROR(CgroupCpuCtl::write_cg_sys_file(_doris_cgroup_cpu_path_subtree_ctl_file, "+cpu",
                                                    "set cpu controller", false));

    // 2 enable cpu controller for query path's child
    _cgroup_v2_query_path_subtree_ctl_file = query_path + "/cgroup.subtree_control";
    if (access(_cgroup_v2_query_path_subtree_ctl_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 query path's subtree control file");
    }
    RETURN_IF_ERROR(CgroupCpuCtl::write_cg_sys_file(_cgroup_v2_query_path_subtree_ctl_file, "+cpu",
                                                    "set cpu controller", false));

    // 3 write cgroup.procs
    _doris_cg_v2_procs_file = query_path + "/cgroup.procs";
    if (access(_doris_cg_v2_procs_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 cgroup.procs file");
    }
    RETURN_IF_ERROR(CgroupCpuCtl::write_cg_sys_file(_doris_cg_v2_procs_file,
                                                    std::to_string(getpid()),
                                                    "set pid to cg v2 procs file", false));
    return Status::OK();
}

uint64_t CgroupCpuCtl::cpu_soft_limit_default_value() {
    return _is_enable_cgroup_v2_in_env ? 100 : 1024;
}

std::unique_ptr<CgroupCpuCtl> CgroupCpuCtl::create_cgroup_cpu_ctl(uint64_t wg_id) {
    if (_is_enable_cgroup_v2_in_env) {
        return std::make_unique<CgroupV2CpuCtl>(wg_id);
    } else if (_is_enable_cgroup_v1_in_env) {
        return std::make_unique<CgroupV1CpuCtl>(wg_id);
    }
    return nullptr;
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

Status CgroupCpuCtl::write_cg_sys_file(std::string file_path, std::string value, std::string msg,
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

Status CgroupCpuCtl::add_thread_to_cgroup(std::string task_path) {
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
    return CgroupCpuCtl::write_cg_sys_file(task_path, std::to_string(tid), msg, true);
#endif
}

Status CgroupCpuCtl::delete_unused_cgroup_path(std::set<uint64_t>& used_wg_ids) {
    if (!_is_cgroup_query_path_valid) {
        return Status::InternalError<false>("not find a valid cgroup query path");
    }
    // 1 get unused wg id
    std::set<std::string> unused_wg_ids;
    for (const auto& entry : std::filesystem::directory_iterator(_doris_cgroup_cpu_query_path)) {
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
    std::string query_path = _doris_cgroup_cpu_query_path.back() != '/'
                                     ? _doris_cgroup_cpu_query_path + "/"
                                     : _doris_cgroup_cpu_query_path;
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

Status CgroupV1CpuCtl::init() {
    if (!_is_cgroup_query_path_valid) {
        return Status::InternalError<false>("cgroup query path is not valid");
    }

    if (_wg_id <= 0) {
        return Status::InternalError<false>("find an invalid wg_id {}", _wg_id);
    }

    // workload group path
    _cgroup_v1_cpu_tg_path = _doris_cgroup_cpu_query_path + "/" + std::to_string(_wg_id);
    if (access(_cgroup_v1_cpu_tg_path.c_str(), F_OK) != 0) {
        int ret = mkdir(_cgroup_v1_cpu_tg_path.c_str(), S_IRWXU);
        if (ret != 0) {
            LOG(ERROR) << "cgroup v1 mkdir workload group failed, path=" << _cgroup_v1_cpu_tg_path;
            return Status::InternalError<false>("cgroup v1 mkdir workload group failed, path={}",
                                                _cgroup_v1_cpu_tg_path);
        }
    }

    _cgroup_v1_cpu_tg_quota_file = _cgroup_v1_cpu_tg_path + "/cpu.cfs_quota_us";
    if (access(_cgroup_v1_cpu_tg_quota_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v1 cpu.cfs_quota_us file");
    }
    _cgroup_v1_cpu_tg_shares_file = _cgroup_v1_cpu_tg_path + "/cpu.shares";
    if (access(_cgroup_v1_cpu_tg_shares_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v1 cpu.shares file");
    }
    _cgroup_v1_cpu_tg_task_file = _cgroup_v1_cpu_tg_path + "/tasks";
    if (access(_cgroup_v1_cpu_tg_task_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v1 cpu.shares file");
    }
    LOG(INFO) << "cgroup v1 cpu path init success"
              << ", query tg path=" << _cgroup_v1_cpu_tg_path
              << ", query wg quota file path=" << _cgroup_v1_cpu_tg_quota_file
              << ", query wg share file path=" << _cgroup_v1_cpu_tg_shares_file
              << ", query wg tasks file path=" << _cgroup_v1_cpu_tg_task_file
              << ", core num=" << _cpu_core_num;
    _init_succ = true;
    return Status::OK();
}

Status CgroupV1CpuCtl::modify_cg_cpu_soft_limit_no_lock(int cpu_shares) {
    std::string cpu_share_str = std::to_string(cpu_shares);
    std::string msg = "modify cpu shares to " + cpu_share_str;
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_tg_shares_file, cpu_share_str, msg,
                                           false);
}

Status CgroupV1CpuCtl::modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) {
    int val = cpu_hard_limit > 0 ? (_cpu_cfs_period_us * _cpu_core_num * cpu_hard_limit / 100)
                                 : CGROUP_CPU_HARD_LIMIT_DEFAULT_VALUE;
    std::string str_val = std::to_string(val);
    std::string msg = "modify cpu quota value to " + str_val;
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v1_cpu_tg_quota_file, str_val, msg, false);
}

Status CgroupV1CpuCtl::add_thread_to_cgroup() {
    return CgroupCpuCtl::add_thread_to_cgroup(_cgroup_v1_cpu_tg_task_file);
}

Status CgroupV2CpuCtl::init() {
    if (!_is_cgroup_query_path_valid) {
        return Status::InternalError<false>(" cgroup query path is empty");
    }

    if (_wg_id <= 0) {
        return Status::InternalError<false>("find an invalid wg_id {}", _wg_id);
    }

    // wg path
    _cgroup_v2_query_wg_path = _doris_cgroup_cpu_query_path + "/" + std::to_string(_wg_id);
    if (access(_cgroup_v2_query_wg_path.c_str(), F_OK) != 0) {
        int ret = mkdir(_cgroup_v2_query_wg_path.c_str(), S_IRWXU);
        if (ret != 0) {
            return Status::InternalError<false>("cgroup v2 mkdir wg failed, path={}",
                                                _cgroup_v2_query_wg_path);
        }
    }

    _cgroup_v2_query_wg_cpu_max_file = _cgroup_v2_query_wg_path + "/cpu.max";
    if (access(_cgroup_v2_query_wg_cpu_max_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 wg cpu.max file");
    }

    _cgroup_v2_query_wg_cpu_weight_file = _cgroup_v2_query_wg_path + "/cpu.weight";
    if (access(_cgroup_v2_query_wg_cpu_weight_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 wg cpu.weight file");
    }

    _cgroup_v2_query_wg_thread_file = _cgroup_v2_query_wg_path + "/cgroup.threads";
    if (access(_cgroup_v2_query_wg_thread_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 wg cgroup.threads file");
    }

    _cgroup_v2_query_wg_type_file = _cgroup_v2_query_wg_path + "/cgroup.type";
    if (access(_cgroup_v2_query_wg_type_file.c_str(), F_OK) != 0) {
        return Status::InternalError<false>("not find cgroup v2 wg cgroup.type file");
    }
    RETURN_IF_ERROR(CgroupCpuCtl::write_cg_sys_file(_cgroup_v2_query_wg_type_file, "threaded",
                                                    "set cgroup type", false));

    LOG(INFO) << "cgroup v2 cpu path init success"
              << ", query wg path=" << _cgroup_v2_query_wg_path
              << ", cpu.max file = " << _cgroup_v2_query_wg_cpu_max_file
              << ", cgroup.threads file = " << _cgroup_v2_query_wg_thread_file
              << ", core num=" << _cpu_core_num;
    _init_succ = true;
    return Status::OK();
}

Status CgroupV2CpuCtl::modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) {
    std::string value = "";
    if (cpu_hard_limit > 0) {
        uint64_t int_val = _cpu_cfs_period_us * _cpu_core_num * cpu_hard_limit / 100;
        value = std::to_string(int_val) + " 100000";
    } else {
        value = CGROUP_V2_CPU_HARD_LIMIT_DEFAULT_VALUE;
    }
    std::string msg = "modify cpu.max to [" + value + "]";
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v2_query_wg_cpu_max_file, value, msg, false);
}

Status CgroupV2CpuCtl::modify_cg_cpu_soft_limit_no_lock(int cpu_weight) {
    std::string cpu_weight_str = std::to_string(cpu_weight);
    std::string msg = "modify cpu.weight to " + cpu_weight_str;
    return CgroupCpuCtl::write_cg_sys_file(_cgroup_v2_query_wg_cpu_weight_file, cpu_weight_str, msg,
                                           false);
}

Status CgroupV2CpuCtl::add_thread_to_cgroup() {
    return CgroupCpuCtl::add_thread_to_cgroup(_cgroup_v2_query_wg_thread_file);
}

} // namespace doris
