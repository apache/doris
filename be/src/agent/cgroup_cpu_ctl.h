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

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <shared_mutex>

#include "common/config.h"
#include "common/status.h"
#include "util/cpu_info.h"

namespace doris {

// cgroup cpu.cfs_quota_us default value, it means disable cpu hard limit
const static int CGROUP_CPU_HARD_LIMIT_DEFAULT_VALUE = -1;
const static std::string CGROUP_V2_CPU_HARD_LIMIT_DEFAULT_VALUE = "max 100000";

class CgroupCpuCtl {
public:
    virtual ~CgroupCpuCtl() = default;
    CgroupCpuCtl(uint64_t wg_id) { _wg_id = wg_id; }

    virtual Status init() = 0;

    virtual Status add_thread_to_cgroup() = 0;

    void update_cpu_hard_limit(int cpu_hard_limit);

    void update_cpu_soft_limit(int cpu_shares);

    // for log
    void get_cgroup_cpu_info(uint64_t* cpu_shares, int* cpu_hard_limit);

    static void init_doris_cgroup_path();

    static Status delete_unused_cgroup_path(std::set<uint64_t>& used_wg_ids);

    static std::unique_ptr<CgroupCpuCtl> create_cgroup_cpu_ctl(uint64_t wg_id);

    static bool is_a_valid_cgroup_path(std::string cg_path);

    static uint64_t cpu_soft_limit_default_value();

protected:
    virtual Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) = 0;

    virtual Status modify_cg_cpu_soft_limit_no_lock(int cpu_shares) = 0;

    Status add_thread_to_cgroup(std::string task_file);

    static Status write_cg_sys_file(std::string file_path, std::string value, std::string msg,
                                    bool is_append);

    static Status init_cgroup_v2_query_path_public_file(std::string home_path,
                                                        std::string query_path);

protected:
    inline static uint64_t _cpu_core_num;
    const static uint64_t _cpu_cfs_period_us = 100000;
    inline static std::string _doris_cgroup_cpu_path = "";
    inline static std::string _doris_cgroup_cpu_query_path = "";
    inline static bool _is_enable_cgroup_v1_in_env = false;
    inline static bool _is_enable_cgroup_v2_in_env = false;
    inline static bool _is_cgroup_query_path_valid = false;

    // cgroup v2 public file
    inline static std::string _doris_cgroup_cpu_path_subtree_ctl_file = "";
    inline static std::string _cgroup_v2_query_path_subtree_ctl_file = "";
    inline static std::string _doris_cg_v2_procs_file = "";

protected:
    int _cpu_hard_limit = 0;
    std::shared_mutex _lock_mutex;
    bool _init_succ = false;
    uint64_t _wg_id = -1; // workload group id
    uint64_t _cpu_shares = 0;
};

/*
    NOTE: directory structure
    1 sys cgroup root path:
        /sys/fs/cgroup
    
    2 sys cgroup cpu controller path:
        /sys/fs/cgroup/cpu
    
    3 doris home path:
        /sys/fs/cgroup/cpu/{doris_home}/
    
    4 doris query path
        /sys/fs/cgroup/cpu/{doris_home}/query
    
    5 workload group path
        /sys/fs/cgroup/cpu/{doris_home}/query/{workload group id}
    
    6 workload group quota file:
        /sys/fs/cgroup/cpu/{doris_home}/query/{workload group id}/cpu.cfs_quota_us
    
    7 workload group tasks file:
        /sys/fs/cgroup/cpu/{doris_home}/query/{workload group id}/tasks
    
    8 workload group cpu.shares file:
    /sys/fs/cgroup/cpu/{doris_home}/query/{workload group id}/cpu.shares
*/
class CgroupV1CpuCtl : public CgroupCpuCtl {
public:
    CgroupV1CpuCtl(uint64_t tg_id) : CgroupCpuCtl(tg_id) {}
    Status init() override;
    Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) override;
    Status modify_cg_cpu_soft_limit_no_lock(int cpu_shares) override;
    Status add_thread_to_cgroup() override;

private:
    std::string _cgroup_v1_cpu_tg_path; // workload group path
    std::string _cgroup_v1_cpu_tg_quota_file;
    std::string _cgroup_v1_cpu_tg_shares_file;
    std::string _cgroup_v1_cpu_tg_task_file;
};

/*
    NOTE: cgroup v2 directory structure
    1 root path:
        /sys/fs/cgroup
    
    2 doris home path:
        /sys/fs/cgroup/{doris_home}/

    3 doris home subtree_control file:
        /sys/fs/cgroup/{doris_home}/cgroup.subtree_control
    
    4 query path:
        /sys/fs/cgroup/{doris_home}/query/

    5 query path subtree_control file:
        /sys/fs/cgroup/{doris_home}/query/cgroup.subtree_control

    6 query path procs file:
        /sys/fs/cgroup/{doris_home}/query/cgroup.procs

    7 workload group path:
        /sys/fs/cgroup/{doris_home}/query/{workload_group_id}

    8 workload grou cpu.max file:
        /sys/fs/cgroup/{doris_home}/query/{workload_group_id}/cpu.max

    9 workload grou cpu.weight file:
        /sys/fs/cgroup/{doris_home}/query/{workload_group_id}/cpu.weight

    10 workload group cgroup type file:
        /sys/fs/cgroup/{doris_home}/query/{workload_group_id}/cgroup.type

*/
class CgroupV2CpuCtl : public CgroupCpuCtl {
public:
    CgroupV2CpuCtl(uint64_t tg_id) : CgroupCpuCtl(tg_id) {}
    Status init() override;
    Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) override;
    Status modify_cg_cpu_soft_limit_no_lock(int cpu_shares) override;
    Status add_thread_to_cgroup() override;

private:
    std::string _cgroup_v2_query_wg_path;
    std::string _cgroup_v2_query_wg_cpu_max_file;
    std::string _cgroup_v2_query_wg_cpu_weight_file;
    std::string _cgroup_v2_query_wg_thread_file;
    std::string _cgroup_v2_query_wg_type_file;
};

} // namespace doris