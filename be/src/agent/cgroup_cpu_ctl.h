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

class CgroupCpuCtl {
public:
    CgroupCpuCtl() {}
    virtual ~CgroupCpuCtl() {}

    virtual Status init();

    virtual Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) = 0;

    virtual Status add_thread_to_cgroup() = 0;

    void update_cpu_hard_limit(int cpu_hard_limit);

protected:
    Status write_cg_sys_file(std::string file_path, int value, std::string msg, bool is_append);

    std::string _doris_cgroup_cpu_path;
    uint64_t _cpu_core_num = CpuInfo::num_cores();
    uint64_t _cpu_cfs_period_us = 100000;
    uint64_t _cpu_hard_limit = 0;
    std::shared_mutex _lock_mutex;
    bool _init_succ = false;
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
    
    5 doris query quota file:
        /sys/fs/cgroup/cpu/{doris_home}/query/cpu.cfs_quota_us
    
    6 doris query tasks file:
        /sys/fs/cgroup/cpu/{doris_home}/query/tasks
*/
class CgroupV1CpuCtl : public CgroupCpuCtl {
public:
    Status init() override;
    Status modify_cg_cpu_hard_limit_no_lock(int cpu_hard_limit) override;
    Status add_thread_to_cgroup() override;

private:
    // todo(wb) support load/compaction path
    std::string _cgroup_v1_cpu_query_path;
    std::string _cgroup_v1_cpu_query_quota_path;
    std::string _cgroup_v1_cpu_query_task_path;
};

} // namespace doris