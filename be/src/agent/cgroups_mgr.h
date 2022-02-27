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

#ifndef DORIS_BE_SRC_AGENT_CGROUPS_MGR_H
#define DORIS_BE_SRC_AGENT_CGROUPS_MGR_H

#include <cstdint>
#include <map>
#include <mutex>
#include <string>

#include "common/status.h"
#include "gen_cpp/MasterService_types.h"

namespace doris {

class ExecEnv;

class CgroupsMgr {
public:
    // Input parameters:
    //   exec_env: global variable to get global objects
    //   cgroups_root_path: root cgroup allocated to doris by admin
    explicit CgroupsMgr(ExecEnv* exec_env, const std::string& root_cgroups_path);
    ~CgroupsMgr();

    // Compare the old user resource and new user resource to find deleted user
    // then delete nonexisting cgroups, create new user cgroups, update all user cgroups
    Status update_local_cgroups(const TFetchResourceResult& new_fetched_resource);

    // Delete all existing cgroups under root path
    Status init_cgroups();

    // Modify cgroup resource shares under cgroups_root_path.
    // Create related cgroups if it not exist.
    //
    // Input parameters:
    //   user_name: unique name for the user. it is a dir under cgroups_root_path
    //
    //   user_share: a mapping for shares for different resource like (cpu.share, 100)
    //               mapping key is resource file name in cgroup; value is share weight
    //
    //   level_share: a mapping for shares for different levels under the user.
    //               mapping key is level name; value is level's share. Currently, different resource using the same share.
    Status modify_user_cgroups(const std::string& user_name,
                                    const std::map<std::string, int32_t>& user_share,
                                    const std::map<std::string, int32_t>& level_share);

    static void apply_cgroup(const std::string& user_name, const std::string& level);

    static void apply_system_cgroup() { apply_cgroup(_s_system_user, _s_system_group); }

    // Assign the thread calling this funciton to the cgroup identified by user name and level
    //
    // Input parameters:
    //   user_name&level: the user name and level used to find the cgroup
    Status assign_to_cgroups(const std::string& user_name, const std::string& level);

    // Assign the thread identified by thread id to the cgroup identified by user name and level
    //
    // Input parameters:
    //   thread_id: the unique id for the thread
    //   user_name&level: the user name and level used to find the cgroup
    Status assign_thread_to_cgroups(int64_t thread_id, const std::string& user_name,
                                         const std::string& level);

    // Delete the user's cgroups and its sub level cgroups using DropCgroups
    // Input parameters:
    //   user name: user name to be deleted
    Status delete_user_cgroups(const std::string& user_name);
    // Delete a cgroup
    // If there are active tasks in this cgroups, they will be relocated
    // to root cgroups.
    // If there are sub cgroups, it will return error.
    // Input parameters:
    //   deleted_cgroups_path: the absolute cgroups path to be deleted
    Status drop_cgroups(const std::string& deleted_cgroups_path);

    // Relocate all threads or processes in src cgroups to dest cgroups
    // Ignore errors when echo to dest cgroups
    // Input parameters:
    //   src_cgroups: absolute path for src cgroups folder
    //   dest_cgroups: absolute path for dest cgroups folder
    Status relocate_tasks(const std::string& src_cgroups, const std::string& dest_cgroups);

    int64_t get_cgroups_version() { return _cur_version; }

    // set the disk throttle for the user by getting resource value from the map and echo it to the cgroups.
    // currently, both the user and groups under the user are set to the same value
    // because throttle does not support hierachy.
    // Input parameters:
    //  user_name: name for the user
    //  resource_share: resource value get from fe
    void _config_user_disk_throttle(std::string user_name,
                                    const std::map<TResourceType::type, int32_t>& resource_share);

    // get user resource share value from the map
    int64_t _get_resource_value(const TResourceType::type resource_type,
                                const std::map<TResourceType::type, int32_t>& resource_share);

    // set disk throttle according to the parameters. currently, we set different
    // values for hdd and ssd.
    // Input parameters:
    //  hdd_read_iops: read iops number for hdd disk.
    //  hdd_write_iops: write iops number for hdd disk.
    //  hdd_read_mbps: read bps number for hdd disk, using mb not byte or kb.
    //  hdd_write_mbps: write bps number for hdd disk, using mb not byte or kb.
    //  ssd_read_iops: read iops number for ssd disk.
    //  ssd_write_iops: write iops number for ssd disk.
    //  ssd_read_mbps: read bps number for ssd disk, using mb not byte or kb.
    //  ssd_write_mbps: write bps number for ssd disk, using mb not byte or kb.
    Status _config_disk_throttle(std::string user_name, std::string level,
                                      int64_t hdd_read_iops, int64_t hdd_write_iops,
                                      int64_t hdd_read_mbps, int64_t hdd_write_mbps,
                                      int64_t ssd_read_iops, int64_t ssd_write_iops,
                                      int64_t ssd_read_mbps, int64_t ssd_write_mbps);

    // echo command in string stream to the cgroup file
    // Input parameters:
    //  ctrl_cmd: stringstream that contains the string to echo
    //  cgroups_path: target cgroup file path
    void _echo_cmd_to_cgroup(std::stringstream& ctrl_cmd, std::string& cgroups_path);

    // check if the path exists and it is a directory
    // Input parameters:
    //   file_path: path to the file
    bool is_directory(const char* file_path);

    // check if the path exists
    // Input parameters:
    //   file_path: path to the file
    bool is_file_exist(const char* file_path);

    // check if the path exists
    // Input parameters:
    //   file_path: string value of the path
    bool is_file_exist(const std::string& file_path);

public:
    const static std::string _s_system_user;
    const static std::string _s_system_group;

private:
    std::string _root_cgroups_path;
    int32_t _drop_retry_times = 10;
    bool _is_cgroups_init_success;
    std::string _default_user_name = "default";
    std::string _default_level = "normal";
    int64_t _cur_version;
    std::set<std::string> _local_users;
    std::mutex _update_cgroups_mtx;

    // A static mapping from fe's resource type to cgroups file
    static std::map<TResourceType::type, std::string> _s_resource_cgroups;
};
} // namespace doris
#endif
