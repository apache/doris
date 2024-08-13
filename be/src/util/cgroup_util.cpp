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

#include "util/cgroup_util.h"

#include <algorithm>
#include <cfloat>
#include <fstream>
#include <iomanip>
#include <memory>
#include <utility>
#include <vector>

#include "gutil/stringprintf.h"
#include "gutil/strings/escaping.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "io/fs/local_file_system.h"
#include "util/error_util.h"
#include "util/string_parser.hpp"

using strings::CUnescape;
using strings::Split;
using strings::SkipWhitespace;
using std::pair;

namespace doris {

Status CGroupUtil::find_global_cgroup(const string& subsystem, string* path) {
    std::ifstream proc_cgroups("/proc/self/cgroup", std::ios::in);
    string line;
    while (true) {
        if (proc_cgroups.fail()) {
            return Status::IOError("Error reading /proc/self/cgroup: {}", get_str_err_msg());
        } else if (proc_cgroups.peek() == std::ifstream::traits_type::eof()) {
            return Status::NotFound("Could not find subsystem {} in /proc/self/cgroup", subsystem);
        }
        // The line format looks like this:
        // 4:memory:/user.slice
        // 9:cpu,cpuacct:/user.slice
        // so field size will be 3
        getline(proc_cgroups, line);
        if (!proc_cgroups.good()) {
            continue;
        }
        std::vector<string> fields = Split(line, ":");
        // ":" in the path does not appear to be escaped - bail in the unusual case that
        // we get too many tokens.
        if (fields.size() != 3) {
            return Status::InvalidArgument(
                    "Could not parse line from /proc/self/cgroup - had {} > 3 tokens: '{}'",
                    fields.size(), line);
        }
        std::vector<string> subsystems = Split(fields[1], ",");
        auto it = std::find(subsystems.begin(), subsystems.end(), subsystem);
        if (it != subsystems.end()) {
            *path = std::move(fields[2]);
            return Status::OK();
        }
    }
}

static Status unescape_path(const string& escaped, string* unescaped) {
    string err;
    if (!CUnescape(escaped, unescaped, &err)) {
        return Status::InvalidArgument("Could not unescape path '{}': {}", escaped, err);
    }
    return Status::OK();
}

static Status read_cgroup_value(const string& limit_file_path, int64_t* val) {
    std::ifstream limit_file(limit_file_path, std::ios::in);
    string line;
    getline(limit_file, line);
    if (limit_file.fail() || limit_file.bad()) {
        return Status::IOError("Error reading {}: {}", limit_file_path, get_str_err_msg());
    }
    StringParser::ParseResult pr;
    // Parse into an int64_t If it overflows, returning the max value of int64_t is ok because that
    // is effectively unlimited.
    *val = StringParser::string_to_int<int64_t>(line.c_str(), line.size(), &pr);
    if ((pr != StringParser::PARSE_SUCCESS && pr != StringParser::PARSE_OVERFLOW)) {
        return Status::InvalidArgument("Failed to parse {} as int64: '{}'", limit_file_path, line);
    }
    return Status::OK();
}

Status CGroupUtil::find_cgroup_mounts(const string& subsystem, pair<string, string>* result) {
    std::ifstream mountinfo("/proc/self/mountinfo", std::ios::in);
    string line;
    while (true) {
        if (mountinfo.fail() || mountinfo.bad()) {
            return Status::IOError("Error reading /proc/self/mountinfo: {}", get_str_err_msg());
        } else if (mountinfo.eof()) {
            return Status::NotFound("Could not find subsystem {} in /proc/self/mountinfo",
                                    subsystem);
        }
        // The relevant lines look like below (see proc manpage for full documentation). The
        // first example is running outside of a container, the second example is running
        // inside a docker container. Field 3 is the path relative to the root CGroup on
        // the host and Field 4 is the mount point from this process's point of view.
        // 34 29 0:28 / /sys/fs/cgroup/memory rw,nosuid,nodev,noexec,relatime shared:15 -
        //    cgroup cgroup rw,memory
        // 275 271 0:28 /docker/f23eee6f88c2ba99fcce /sys/fs/cgroup/memory
        //    ro,nosuid,nodev,noexec,relatime master:15 - cgroup cgroup rw,memory
        getline(mountinfo, line);
        if (!mountinfo.good()) continue;
        std::vector<string> fields = Split(line, " ", SkipWhitespace());
        if (fields.size() < 7) {
            return Status::InvalidArgument(
                    "Could not parse line from /proc/self/mountinfo - had {} > 7 tokens: '{}'",
                    fields.size(), line);
        }
        if (fields[fields.size() - 3] != "cgroup") continue;
        // This is a cgroup mount. Check if it's the mount we're looking for.
        std::vector<string> cgroup_opts = Split(fields[fields.size() - 1], ",", SkipWhitespace());
        auto it = std::find(cgroup_opts.begin(), cgroup_opts.end(), subsystem);
        if (it == cgroup_opts.end()) {
            continue;
        }
        // This is the right mount.
        string mount_path, system_path;
        RETURN_IF_ERROR(unescape_path(fields[4], &mount_path));
        RETURN_IF_ERROR(unescape_path(fields[3], &system_path));
        // Strip trailing "/" so that both returned paths match in whether they have a
        // trailing "/".
        if (system_path[system_path.size() - 1] == '/') system_path.pop_back();
        *result = {mount_path, system_path};
        return Status::OK();
    }
}

Status CGroupUtil::find_abs_cgroup_path(const string& subsystem, string* path) {
    RETURN_IF_ERROR(find_global_cgroup(subsystem, path));
    pair<string, string> paths;
    RETURN_IF_ERROR(find_cgroup_mounts(subsystem, &paths));
    const string& mount_path = paths.first;
    const string& system_path = paths.second;
    if (path->compare(0, system_path.size(), system_path) != 0) {
        return Status::InvalidArgument("Expected CGroup path '{}' to start with '{}'", *path,
                                       system_path);
    }
    path->replace(0, system_path.size(), mount_path);
    return Status::OK();
}

Status CGroupUtil::find_cgroup_mem_limit(int64_t* bytes) {
    if (!enable()) {
        return Status::InvalidArgument("cgroup is not enabled!");
    }
    string cgroup_path;
    RETURN_IF_ERROR(find_abs_cgroup_path("memory", &cgroup_path));
    string limit_file_path = cgroup_path + "/memory.limit_in_bytes";
    return read_cgroup_value(limit_file_path, bytes);
}

Status CGroupUtil::find_cgroup_mem_usage(int64_t* bytes) {
    if (!enable()) {
        return Status::InvalidArgument("cgroup is not enabled!");
    }
    string cgroup_path;
    RETURN_IF_ERROR(find_abs_cgroup_path("memory", &cgroup_path));
    string usage_file_path = cgroup_path + "/memory.usage_in_bytes";
    return read_cgroup_value(usage_file_path, bytes);
}

Status CGroupUtil::find_cgroup_mem_info(std::string* file_path) {
    if (!enable()) {
        return Status::InvalidArgument("cgroup is not enabled!");
    }
    string cgroup_path;
    RETURN_IF_ERROR(find_abs_cgroup_path("memory", &cgroup_path));
    *file_path = cgroup_path + "/memory.stat";
    return Status::OK();
}

Status CGroupUtil::find_cgroup_cpu_limit(float* cpu_count) {
    if (!enable()) {
        return Status::InvalidArgument("cgroup is not enabled!");
    }
    int64_t quota;
    int64_t period;
    string cgroup_path;
    if (!find_abs_cgroup_path("cpu", &cgroup_path).ok()) {
        RETURN_IF_ERROR(find_abs_cgroup_path("cpuacct", &cgroup_path));
    }
    string cfs_quota_filename = cgroup_path + "/cpu.cfs_quota_us";
    RETURN_IF_ERROR(read_cgroup_value(cfs_quota_filename, &quota));
    if (quota <= 0) {
        *cpu_count = -1;
        return Status::OK();
    }
    string cfs_period_filename = cgroup_path + "/cpu.cfs_period_us";
    RETURN_IF_ERROR(read_cgroup_value(cfs_period_filename, &period));
    if (quota <= period) {
        return Status::InvalidArgument("quota <= period");
    }
    *cpu_count = float(quota) / float(period);
    if (*cpu_count >= FLT_MAX) {
        return Status::InvalidArgument("unknown");
    }
    return Status::OK();
}

std::string CGroupUtil::debug_string() {
    if (!enable()) {
        return std::string("cgroup is not enabled!");
    }
    string mem_limit_str;
    int64_t mem_limit;
    Status status = find_cgroup_mem_limit(&mem_limit);
    if (status.ok()) {
        mem_limit_str = strings::Substitute("$0", mem_limit);
    } else {
        mem_limit_str = status.to_string();
    }
    string cpu_limit_str;
    float cpu_limit;
    status = find_cgroup_cpu_limit(&cpu_limit);
    if (status.ok()) {
        if (cpu_limit > 0) {
            std::stringstream stream;
            stream << std::fixed << std::setprecision(1) << cpu_limit;
            cpu_limit_str = stream.str();
        } else {
            cpu_limit_str = "unlimited";
        }
    } else {
        cpu_limit_str = status.to_string();
    }
    return strings::Substitute("Process CGroup Info: memory.limit_in_bytes=$0, cpu cfs limits: $1",
                               mem_limit_str, cpu_limit_str);
}

bool CGroupUtil::enable() {
    bool exists = true;
    Status st = io::global_local_filesystem()->exists("/proc/cgroups", &exists);
    return st.ok() && exists;
}

} // namespace doris
