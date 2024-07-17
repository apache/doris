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

class CgroupCpuCtl {
public:
    virtual ~CgroupCpuCtl() = default;
    CgroupCpuCtl() = default;
    CgroupCpuCtl(uint64_t wg_id) { _wg_id = wg_id; }

    virtual Status init();

    virtual Status add_thread_to_cgroup() = 0;

    void update_cpu_hard_limit(int cpu_hard_limit);

    void update_cpu_soft_limit(int cpu_shares);

    // for log
    void get_cgroup_cpu_info(uint64_t* cpu_shares, int* cpu_hard_limit);

    virtual Status delete_unused_cgroup_path(std::set<uint64_t>& used_wg_ids) = 0;

protected:
    Status write_cg_sys_file(std::string file_path, int value, std::string msg, bool is_append);

    virtual Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) = 0;

    virtual Status modify_cg_cpu_soft_limit_no_lock(int cpu_shares) = 0;

    std::string _doris_cgroup_cpu_path;
    uint64_t _cpu_core_num = CpuInfo::num_cores();
    uint64_t _cpu_cfs_period_us = 100000;
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
    CgroupV1CpuCtl() = default;
    Status init() override;
    Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) override;
    Status modify_cg_cpu_soft_limit_no_lock(int cpu_shares) override;
    Status add_thread_to_cgroup() override;

    Status delete_unused_cgroup_path(std::set<uint64_t>& used_wg_ids) override;

private:
    std::string _cgroup_v1_cpu_query_path;
    std::string _cgroup_v1_cpu_tg_path; // workload group path
    std::string _cgroup_v1_cpu_tg_quota_file;
    std::string _cgroup_v1_cpu_tg_shares_file;
    std::string _cgroup_v1_cpu_tg_task_file;
};

} // namespace doris