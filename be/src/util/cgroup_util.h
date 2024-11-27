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
#include <filesystem>
#include <optional>
#include <string>
#include <utility>

#include "common/status.h"
namespace doris {

// I think it is possible to mount the cgroups hierarchy somewhere else (e.g. when in containers).
// /sys/fs/cgroup was still symlinked to the actual mount in the cases that I have seen.
static inline const std::filesystem::path default_cgroups_mount = "/sys/fs/cgroup";

/* Cgroup debugging steps
 * CgroupV1:
 *  sudo cgcreate -t username:username -g memory:test
 *  sudo sh -c "echo 6000M > /sys/fs/cgroup/memory/test/memory.limit_in_bytes"
 *  // process started by the current terminal will join Cgroup test
 *  sudo sh -c "echo $$ >> /sys/fs/cgroup/memory/test/cgroup.procs"
 *
 * CgroupV2:
 *  sudo mkdir /sys/fs/cgroup/test
 *  sudo echo 3000M > /sys/fs/cgroup/test/memory.max
 *  // process started by the current terminal will join Cgroup test
 *  sudo sh -c "echo $$ >> /sys/fs/cgroup/test/cgroup.procs"
 *  or
 *  // only memory allocated after joining the Cgroup is counted in `memory.current`.
 *  sudo echo pid > /sys/fs/cgroup/test/cgroup.procs
*/
class CGroupUtil {
public:
    enum class CgroupsVersion : uint8_t { V1, V2 };

    // Detect if cgroup is enabled.
    // If true, it only means that the OS allows the use of Cgroup v1 or v2,
    // not that the current BE process is using Cgroup.
    // To confirm whether the process is using Cgroup need to use `find_global_cgroupv1` or `cgroupv2_of_process`.
    // To confirm whether the process is using a subsystem of Cgroup,
    // need to use `find_abs_cgroupv1_path` or `get_cgroupsv2_path`.
    static bool cgroupsv1_enable();
    static bool cgroupsv2_enable();

    // return the global cgroup path of subsystem like 12:memory:/user.slice -> user.slice
    static Status find_global_cgroupv1(const std::string& subsystem, std::string* path);

    // Returns the absolute path to the CGroup from inside the container.
    // E.g. if this process belongs to
    // /sys/fs/cgroup/memory/kubepods/burstable/pod-<long unique id>, which is mounted at
    // /sys/fs/cgroup/memory inside the container, this function returns
    // "/sys/fs/cgroup/memory".
    static Status find_abs_cgroupv1_path(const std::string& subsystem, std::string* path);

    // Figures out the mapping of the cgroup root from the container's point of view to
    // the full path relative to the system-wide cgroups outside of the container.
    // E.g. /sys/fs/cgroup/memory/kubepods/burstable/pod-<long unique id> may be mounted at
    // /sys/fs/cgroup/memory inside the container. In that case this function would return
    // ("/sys/fs/cgroup/memory", "kubepods/burstable/pod-<long unique id>").
    static Status find_cgroupv1_mounts(const std::string& subsystem,
                                       std::pair<std::string, std::string>* result);

    // Which cgroup does the process belong to?
    // Returns an empty string if the cgroup cannot be determined.
    // Assumes that cgroupsV2Enabled() is enabled.
    static std::string cgroupv2_of_process();

    // Caveats:
    // - All of the logic in this file assumes that the current process is the only process in the
    //   containing cgroup (or more precisely: the only process with significant memory consumption).
    //   If this is not the case, then other processe's memory consumption may affect the internal
    //   memory tracker ...
    // - Cgroups v1 and v2 allow nested cgroup hierarchies. As v1 is deprecated for over half a
    //   decade and will go away at some point, hierarchical detection is only implemented for v2.
    // - I did not test what happens if a host has v1 and v2 simultaneously enabled. I believe such
    //   systems existed only for a short transition period.
    static std::optional<std::string> get_cgroupsv2_path(const std::string& subsystem);

    // Cgroup file with only one line of numbers.
    static Status read_int_line_from_cgroup_file(const std::filesystem::path& file_path,
                                                 int64_t* val);

    // Multi-line Cgroup files, format is
    //   kernel 5
    //   rss 15
    //   [...]
    static void read_int_metric_from_cgroup_file(
            const std::filesystem::path& file_path,
            std::unordered_map<std::string, int64_t>& metrics_map);
};
} // namespace doris
