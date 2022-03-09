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

#include "agent/cgroups_mgr.h"

#include <asm/unistd.h>
#include <linux/magic.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <fstream>
#include <future>
#include <map>
#include <sstream>

#include "common/logging.h"
#include "olap/data_dir.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"

using std::string;
using std::map;
using std::vector;
using std::stringstream;
using apache::thrift::TException;
using apache::thrift::transport::TTransportException;

namespace doris {

static CgroupsMgr* s_global_cg_mgr;

const std::string CgroupsMgr::_s_system_user = "system";
const std::string CgroupsMgr::_s_system_group = "normal";

std::map<TResourceType::type, std::string> CgroupsMgr::_s_resource_cgroups = {
        {TResourceType::type::TRESOURCE_CPU_SHARE, "cpu.shares"},
        {TResourceType::type::TRESOURCE_IO_SHARE, "blkio.weight"}};

CgroupsMgr::CgroupsMgr(ExecEnv* exec_env, const string& root_cgroups_path)
        : _root_cgroups_path(root_cgroups_path), _is_cgroups_init_success(false), _cur_version(-1) {
    if (s_global_cg_mgr == nullptr) {
        s_global_cg_mgr = this;
    }
}

CgroupsMgr::~CgroupsMgr() {}

Status CgroupsMgr::update_local_cgroups(const TFetchResourceResult& new_fetched_resource) {
    std::lock_guard<std::mutex> lck(_update_cgroups_mtx);
    if (!_is_cgroups_init_success) {
        return Status::InternalError("Cgroup not inited");
    }

    if (_cur_version >= new_fetched_resource.resourceVersion) {
        return Status::OK();
    }

    const std::map<std::string, TUserResource>& new_user_resource =
            new_fetched_resource.resourceByUser;

    if (!_local_users.empty()) {
        std::set<std::string>::const_iterator old_it = _local_users.begin();
        for (; old_it != _local_users.end(); ++old_it) {
            if (new_user_resource.count(*old_it) == 0) {
                this->delete_user_cgroups(*old_it);
            }
        }
    }

    // Clear local users set, because it will be inserted again
    _local_users.clear();

    std::map<std::string, TUserResource>::const_iterator new_it = new_user_resource.begin();
    for (; new_it != new_user_resource.end(); ++new_it) {
        const string& user_name = new_it->first;
        const std::map<std::string, int32_t>& level_share = new_it->second.shareByGroup;
        std::map<std::string, int32_t> user_share;
        const std::map<TResourceType::type, int32_t>& resource_share =
                new_it->second.resource.resourceByType;
        std::map<TResourceType::type, int32_t>::const_iterator resource_it = resource_share.begin();
        for (; resource_it != resource_share.end(); ++resource_it) {
            if (_s_resource_cgroups.count(resource_it->first) > 0) {
                user_share[_s_resource_cgroups[resource_it->first]] = resource_it->second;
            }
        }

        modify_user_cgroups(user_name, user_share, level_share);
        _config_user_disk_throttle(user_name, resource_share);

        // Insert user to local user's set
        _local_users.insert(user_name);
    }

    // Using resource version, not subscribe version
    _cur_version = new_fetched_resource.resourceVersion;
    return Status::OK();
}

void CgroupsMgr::_config_user_disk_throttle(
        std::string user_name, const std::map<TResourceType::type, int32_t>& resource_share) {
    int64_t hdd_read_iops =
            _get_resource_value(TResourceType::type::TRESOURCE_HDD_READ_IOPS, resource_share);
    int64_t hdd_write_iops =
            _get_resource_value(TResourceType::type::TRESOURCE_HDD_WRITE_IOPS, resource_share);
    int64_t hdd_read_mbps =
            _get_resource_value(TResourceType::type::TRESOURCE_HDD_READ_MBPS, resource_share);
    int64_t hdd_write_mbps =
            _get_resource_value(TResourceType::type::TRESOURCE_HDD_WRITE_MBPS, resource_share);
    int64_t ssd_read_iops =
            _get_resource_value(TResourceType::type::TRESOURCE_SSD_READ_IOPS, resource_share);
    int64_t ssd_write_iops =
            _get_resource_value(TResourceType::type::TRESOURCE_SSD_WRITE_IOPS, resource_share);
    int64_t ssd_read_mbps =
            _get_resource_value(TResourceType::type::TRESOURCE_SSD_READ_MBPS, resource_share);
    int64_t ssd_write_mbps =
            _get_resource_value(TResourceType::type::TRESOURCE_SSD_WRITE_MBPS, resource_share);

    _config_disk_throttle(user_name, "", hdd_read_iops, hdd_write_iops, hdd_read_mbps,
                          hdd_write_mbps, ssd_read_iops, ssd_write_iops, ssd_read_mbps,
                          ssd_write_mbps);
    _config_disk_throttle(user_name, "low", hdd_read_iops, hdd_write_iops, hdd_read_mbps,
                          hdd_write_mbps, ssd_read_iops, ssd_write_iops, ssd_read_mbps,
                          ssd_write_mbps);
    _config_disk_throttle(user_name, "normal", hdd_read_iops, hdd_write_iops, hdd_read_mbps,
                          hdd_write_mbps, ssd_read_iops, ssd_write_iops, ssd_read_mbps,
                          ssd_write_mbps);
    _config_disk_throttle(user_name, "high", hdd_read_iops, hdd_write_iops, hdd_read_mbps,
                          hdd_write_mbps, ssd_read_iops, ssd_write_iops, ssd_read_mbps,
                          ssd_write_mbps);
}

int64_t CgroupsMgr::_get_resource_value(
        const TResourceType::type resource_type,
        const std::map<TResourceType::type, int32_t>& resource_share) {
    int64_t resource_value = -1;
    std::map<TResourceType::type, int32_t>::const_iterator it = resource_share.find(resource_type);
    if (it != resource_share.end()) {
        resource_value = it->second;
    }
    return resource_value;
}

Status CgroupsMgr::_config_disk_throttle(std::string user_name, std::string level,
                                         int64_t hdd_read_iops, int64_t hdd_write_iops,
                                         int64_t hdd_read_mbps, int64_t hdd_write_mbps,
                                         int64_t ssd_read_iops, int64_t ssd_write_iops,
                                         int64_t ssd_read_mbps, int64_t ssd_write_mbps) {
    string cgroups_path = this->_root_cgroups_path + "/" + user_name + "/" + level;
    string read_bps_path = cgroups_path + "/blkio.throttle.read_bps_device";
    string write_bps_path = cgroups_path + "/blkio.throttle.write_bps_device";
    string read_iops_path = cgroups_path + "/blkio.throttle.read_iops_device";
    string write_iops_path = cgroups_path + "/blkio.throttle.write_iops_device";

    if (!is_file_exist(cgroups_path.c_str())) {
        if (!std::filesystem::create_directory(cgroups_path)) {
            LOG(ERROR) << "Create cgroups: " << cgroups_path << " failed";
            return Status::InternalError("Create cgroups " + cgroups_path + " failed");
        }
    }

    // add olap engine data path here
    auto stores = StorageEngine::instance()->get_stores();
    // buld load data path, it is alreay in data path
    // _exec_env->load_path_mgr()->get_load_data_path(&data_paths);

    std::stringstream ctrl_cmd;
    for (auto store : stores) {
        // check disk type
        int64_t read_iops = hdd_read_iops;
        int64_t write_iops = hdd_write_iops;
        int64_t read_mbps = hdd_read_mbps;
        int64_t write_mbps = hdd_write_mbps;
        // if user set hdd not ssd, then use hdd for ssd
        if (store->is_ssd_disk()) {
            read_iops = ssd_read_iops == -1 ? hdd_read_iops : ssd_read_iops;
            write_iops = ssd_write_iops == -1 ? hdd_write_iops : ssd_write_iops;
            read_mbps = ssd_read_mbps == -1 ? hdd_read_mbps : ssd_read_mbps;
            write_mbps = ssd_write_mbps == -1 ? hdd_write_mbps : ssd_write_mbps;
        }
        struct stat file_stat;
        if (stat(store->path().c_str(), &file_stat) != 0) {
            continue;
        }
        int major_number = major(file_stat.st_dev);
        int minor_number = minor(file_stat.st_dev);
        minor_number = (minor_number / 16) * 16;
        if (read_iops != -1) {
            ctrl_cmd << major_number << ":" << minor_number << "  " << read_iops;
            _echo_cmd_to_cgroup(ctrl_cmd, read_iops_path);
            ctrl_cmd.clear();
            ctrl_cmd.str(std::string());
        }
        if (write_iops != -1) {
            ctrl_cmd << major_number << ":" << minor_number << "  " << write_iops;
            _echo_cmd_to_cgroup(ctrl_cmd, write_iops_path);
            ctrl_cmd.clear();
            ctrl_cmd.str(std::string());
        }
        if (read_mbps != -1) {
            ctrl_cmd << major_number << ":" << minor_number << "  " << (read_mbps << 20);
            _echo_cmd_to_cgroup(ctrl_cmd, read_bps_path);
            ctrl_cmd.clear();
            ctrl_cmd.str(std::string());
        }
        if (write_mbps != -1) {
            ctrl_cmd << major_number << ":" << minor_number << "  " << (write_mbps << 20);
            _echo_cmd_to_cgroup(ctrl_cmd, write_bps_path);
            ctrl_cmd.clear();
            ctrl_cmd.str(std::string());
        }
    }
    return Status::OK();
}

Status CgroupsMgr::modify_user_cgroups(const string& user_name,
                                       const map<string, int32_t>& user_share,
                                       const map<string, int32_t>& level_share) {
    // Check if the user's cgroups exists, if not create it
    string user_cgroups_path = this->_root_cgroups_path + "/" + user_name;
    if (!is_file_exist(user_cgroups_path.c_str())) {
        if (!std::filesystem::create_directory(user_cgroups_path)) {
            LOG(ERROR) << "Create cgroups for user " << user_name << " failed";
            return Status::InternalError("Create cgroups for user " + user_name + " failed");
        }
    }

    // Traverse the user resource share map to append share value to cgroup's file
    for (map<string, int32_t>::const_iterator user_resource = user_share.begin();
         user_resource != user_share.end(); ++user_resource) {
        string resource_file_name = user_resource->first;
        int32_t user_share_weight = user_resource->second;
        // Append the share_weight value to the file
        string user_resource_path = user_cgroups_path + "/" + resource_file_name;
        std::ofstream user_cgroups(user_resource_path.c_str(), std::ios::out | std::ios::app);
        if (!user_cgroups.is_open()) {
            return Status::InternalError("User cgroup is not open");
        }
        user_cgroups << user_share_weight << std::endl;
        user_cgroups.close();
        LOG(INFO) << "Append " << user_share_weight << " to " << user_resource_path;
        for (map<string, int32_t>::const_iterator level_resource = level_share.begin();
             level_resource != level_share.end(); ++level_resource) {
            // Append resource share to level shares
            string level_name = level_resource->first;
            int32_t level_share_weight = level_resource->second;
            // Check if the level cgroups exist
            string level_cgroups_path = user_cgroups_path + "/" + level_name;
            if (!is_file_exist(level_cgroups_path.c_str())) {
                if (!std::filesystem::create_directory(level_cgroups_path)) {
                    return Status::InternalError("User level cgroups not exist");
                }
            }

            // Append the share_weight value to the file
            string level_resource_path = level_cgroups_path + "/" + resource_file_name;
            std::ofstream level_cgroups(level_resource_path.c_str(), std::ios::out | std::ios::app);
            if (!level_cgroups.is_open()) {
                return Status::InternalError("User level cgroup is not open");
            }
            level_cgroups << level_share_weight << std::endl;
            level_cgroups.close();

            LOG(INFO) << "Append " << level_share_weight << " to " << level_resource_path;
        }
    }
    return Status::OK();
}

Status CgroupsMgr::init_cgroups() {
    std::string root_cgroups_tasks_path = this->_root_cgroups_path + "/tasks";
    // Check if the root cgroups exists
    if (is_directory(this->_root_cgroups_path.c_str()) &&
        is_file_exist(root_cgroups_tasks_path.c_str())) {
        // Check the folder's virtual filesystem to find whether it is a cgroup file system
#ifndef BE_TEST
        struct statfs fs_type;
        statfs(root_cgroups_tasks_path.c_str(), &fs_type);
        if (fs_type.f_type != CGROUP_SUPER_MAGIC) {
            LOG(ERROR) << _root_cgroups_path << " is not a cgroups file system.";
            _is_cgroups_init_success = false;
            return Status::InternalError(_root_cgroups_path + " is not a cgroups file system");
            ;
        }
#endif
        // Check if current user have write permission to cgroup folder
        if (access(_root_cgroups_path.c_str(), W_OK) != 0) {
            LOG(ERROR) << "Doris does not have write permission to " << _root_cgroups_path;
            _is_cgroups_init_success = false;
            return Status::InternalError("Doris does not have write permission to " +
                                         _root_cgroups_path);
        }
        // If root folder exists, then delete all subfolders under it
        std::filesystem::directory_iterator item_begin(this->_root_cgroups_path);
        std::filesystem::directory_iterator item_end;
        for (; item_begin != item_end; item_begin++) {
            if (is_directory(item_begin->path().string().c_str())) {
                // Delete the sub folder
                if (!delete_user_cgroups(item_begin->path().filename().string()).ok()) {
                    LOG(ERROR) << "Could not clean subfolder " << item_begin->path().string();
                    _is_cgroups_init_success = false;
                    return Status::InternalError("Could not clean subfolder " +
                                                 item_begin->path().string());
                }
            }
        }
        LOG(INFO) << "Initialize doris cgroups successfully under folder " << _root_cgroups_path;
        _is_cgroups_init_success = true;
        return Status::OK();
    } else {
        VLOG_NOTICE << "Could not find a valid cgroups path for resource isolation,"
                    << "current value is " << _root_cgroups_path << ". ignore it.";
        _is_cgroups_init_success = false;
        return Status::InternalError("Could not find a valid cgroups path for resource isolation");
        ;
    }
}

#define gettid() syscall(__NR_gettid)
void CgroupsMgr::apply_cgroup(const string& user_name, const string& level) {
    if (s_global_cg_mgr == nullptr) {
        return;
    }
    s_global_cg_mgr->assign_to_cgroups(user_name, level);
}

Status CgroupsMgr::assign_to_cgroups(const string& user_name, const string& level) {
    if (!_is_cgroups_init_success) {
        return Status::InternalError("Cgroups not inited");
    }
    int64_t tid = gettid();
    return assign_thread_to_cgroups(tid, user_name, level);
}

Status CgroupsMgr::assign_thread_to_cgroups(int64_t thread_id, const string& user_name,
                                            const string& level) {
    if (!_is_cgroups_init_success) {
        return Status::InternalError("Cgroups not inited");
    }
    string tasks_path = _root_cgroups_path + "/" + user_name + "/" + level + "/tasks";
    if (!is_file_exist(_root_cgroups_path + "/" + user_name)) {
        tasks_path = this->_root_cgroups_path + "/" + _default_user_name + "/" + _default_level +
                     "/tasks";
    } else if (!is_file_exist(_root_cgroups_path + "/" + user_name + "/" + level)) {
        tasks_path = this->_root_cgroups_path + "/" + user_name + "/tasks";
    }
    if (!is_file_exist(tasks_path.c_str())) {
        LOG(ERROR) << "Cgroups path " << tasks_path << " not exist!";
        return Status::InternalError("Cgroups path not exist");
    }
    std::ofstream tasks(tasks_path.c_str(), std::ios::out | std::ios::app);
    if (!tasks.is_open()) {
        // This means doris could not open this file. May be it does not have access to it
        LOG(ERROR) << "Echo thread: " << thread_id << " to " << tasks_path << " failed!";
        return Status::InternalError("Echo thread to cgroup failed");
    }
    // Append thread id to the tasks file directly
    tasks << thread_id << std::endl;
    tasks.close();

    return Status::OK();
}

Status CgroupsMgr::delete_user_cgroups(const string& user_name) {
    string user_cgroups_path = this->_root_cgroups_path + "/" + user_name;
    if (is_file_exist(user_cgroups_path.c_str())) {
        // Delete sub cgroups --> level cgroups
        std::filesystem::directory_iterator item_begin(user_cgroups_path);
        std::filesystem::directory_iterator item_end;
        for (; item_begin != item_end; item_begin++) {
            if (is_directory(item_begin->path().string().c_str())) {
                string cur_cgroups_path = item_begin->path().string();
                RETURN_IF_ERROR(drop_cgroups(cur_cgroups_path));
            }
        }
        // Delete user cgroups
        RETURN_IF_ERROR(drop_cgroups(user_cgroups_path));
    }
    return Status::OK();
}

Status CgroupsMgr::drop_cgroups(const string& deleted_cgroups_path) {
    // Try to delete the cgroups folder
    // If failed then there maybe exist active tasks under it and try to relocate them
    // Currently, try 10 times to relocate and delete the cgroups.
    int32_t i = 0;
    while (is_file_exist(deleted_cgroups_path) && rmdir(deleted_cgroups_path.c_str()) < 0 &&
           i < this->_drop_retry_times) {
        this->relocate_tasks(deleted_cgroups_path, this->_root_cgroups_path);
        ++i;
#ifdef BE_TEST
        std::filesystem::remove_all(deleted_cgroups_path);
#endif
        if (i == this->_drop_retry_times) {
            LOG(ERROR) << "drop cgroups under path: " << deleted_cgroups_path << " failed.";
            return Status::InternalError("Drop cgroup failed");
        }
    }
    return Status::OK();
}

Status CgroupsMgr::relocate_tasks(const string& src_cgroups, const string& dest_cgroups) {
    string src_tasks_path = src_cgroups + "/tasks";
    string dest_tasks_path = dest_cgroups + "/tasks";
    std::ifstream src_tasks(src_tasks_path.c_str());
    if (!src_tasks) {
        return Status::InternalError("Src tasks is null");
    }
    std::ofstream dest_tasks(dest_tasks_path.c_str(), std::ios::out | std::ios::app);
    if (!dest_tasks) {
        return Status::InternalError("Desk task is null");
    }
    int64_t taskid;
    while (src_tasks >> taskid) {
        dest_tasks << taskid << std::endl;
        // If the thread id or process id not exists, then error occurs in the stream.
        // Clear the error state for every append.
        dest_tasks.clear();
    }
    src_tasks.close();
    dest_tasks.close();
    return Status::OK();
}

void CgroupsMgr::_echo_cmd_to_cgroup(stringstream& ctrl_cmd, string& cgroups_path) {
    std::ofstream cgroups_stream(cgroups_path.c_str(), std::ios::out | std::ios::app);
    if (cgroups_stream.is_open()) {
        cgroups_stream << ctrl_cmd.str() << std::endl;
        cgroups_stream.close();
    }
}

bool CgroupsMgr::is_directory(const char* file_path) {
    struct stat file_stat;
    if (stat(file_path, &file_stat) != 0) {
        return false;
    }
    if (S_ISDIR(file_stat.st_mode)) {
        return true;
    } else {
        return false;
    }
}

bool CgroupsMgr::is_file_exist(const char* file_path) {
    struct stat file_stat;
    if (stat(file_path, &file_stat) != 0) {
        return false;
    }
    return true;
}

bool CgroupsMgr::is_file_exist(const std::string& file_path) {
    return is_file_exist(file_path.c_str());
}

} // namespace doris
