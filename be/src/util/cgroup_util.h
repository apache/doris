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

#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include "common/status.h"
namespace doris {
class CGroupUtil {
public:
    // Determines the CGroup memory limit from the current processes' cgroup.
    // If the limit is more than INT64_MAX, INT64_MAX is returned (since that is
    // effectively unlimited anyway). Does not take into account memory limits
    // set on any ancestor CGroups.
    static Status find_cgroup_mem_limit(int64_t* bytes);

    // memory.usage_in_bytes ~= free.used + free.(buff/cache) - (buff)
    // https://serverfault.com/questions/902009/the-memory-usage-reported-in-cgroup-differs-from-the-free-command
    static Status find_cgroup_mem_usage(int64_t* bytes);
    static Status find_cgroup_mem_info(std::string* file_path);

    // Determines the CGroup cpu cores limit from the current processes' cgroup.
    static Status find_cgroup_cpu_limit(float* cpu_count);

    // Returns a human-readable string with information about CGroups.
    static std::string debug_string();

    // detect if cgroup is enabled
    static bool enable();

private:
    // return the global cgroup path of subsystem like 12:memory:/user.slice -> user.slice
    static Status find_global_cgroup(const std::string& subsystem, std::string* path);

    // Returns the absolute path to the CGroup from inside the container.
    // E.g. if this process belongs to
    // /sys/fs/cgroup/memory/kubepods/burstable/pod-<long unique id>, which is mounted at
    // /sys/fs/cgroup/memory inside the container, this function returns
    // "/sys/fs/cgroup/memory".
    static Status find_abs_cgroup_path(const std::string& subsystem, std::string* path);

    // Figures out the mapping of the cgroup root from the container's point of view to
    // the full path relative to the system-wide cgroups outside of the container.
    // E.g. /sys/fs/cgroup/memory/kubepods/burstable/pod-<long unique id> may be mounted at
    // /sys/fs/cgroup/memory inside the container. In that case this function would return
    // ("/sys/fs/cgroup/memory", "kubepods/burstable/pod-<long unique id>").
    static Status find_cgroup_mounts(const std::string& subsystem,
                                     std::pair<std::string, std::string>* result);
};
} // namespace doris
