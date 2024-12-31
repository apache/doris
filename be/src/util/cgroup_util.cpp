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
#include <boost/algorithm/string.hpp>
#include <fstream>
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

bool CGroupUtil::cgroupsv1_enable() {
    bool exists = true;
    Status st = io::global_local_filesystem()->exists("/proc/cgroups", &exists);
    return st.ok() && exists;
}

bool CGroupUtil::cgroupsv2_enable() {
#if defined(OS_LINUX)
    // This file exists iff the host has cgroups v2 enabled.
    auto controllers_file = default_cgroups_mount / "cgroup.controllers";
    bool exists = true;
    Status st = io::global_local_filesystem()->exists(controllers_file, &exists);
    return st.ok() && exists;
#else
    return false;
#endif
}

Status CGroupUtil::find_global_cgroupv1(const string& subsystem, string* path) {
    std::ifstream proc_cgroups("/proc/self/cgroup", std::ios::in);
    string line;
    while (true) {
        if (proc_cgroups.fail()) {
            return Status::CgroupError("Error reading /proc/self/cgroup: {}", get_str_err_msg());
        } else if (proc_cgroups.peek() == std::ifstream::traits_type::eof()) {
            return Status::CgroupError("Could not find subsystem {} in /proc/self/cgroup",
                                       subsystem);
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

Status CGroupUtil::find_cgroupv1_mounts(const string& subsystem, pair<string, string>* result) {
    std::ifstream mountinfo("/proc/self/mountinfo", std::ios::in);
    string line;
    while (true) {
        if (mountinfo.fail() || mountinfo.bad()) {
            return Status::CgroupError("Error reading /proc/self/mountinfo: {}", get_str_err_msg());
        } else if (mountinfo.eof()) {
            return Status::CgroupError("Could not find subsystem {} in /proc/self/mountinfo",
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
        if (!mountinfo.good()) {
            continue;
        }
        std::vector<string> fields = Split(line, " ", SkipWhitespace());
        if (fields.size() < 7) {
            return Status::InvalidArgument(
                    "Could not parse line from /proc/self/mountinfo - had {} > 7 tokens: '{}'",
                    fields.size(), line);
        }
        if (fields[fields.size() - 3] != "cgroup") {
            continue;
        }
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
        if (system_path[system_path.size() - 1] == '/') {
            system_path.pop_back();
        }
        *result = {mount_path, system_path};
        return Status::OK();
    }
}

Status CGroupUtil::find_abs_cgroupv1_path(const string& subsystem, string* path) {
    if (!cgroupsv1_enable()) {
        return Status::InvalidArgument("cgroup is not enabled!");
    }
    RETURN_IF_ERROR(find_global_cgroupv1(subsystem, path));
    pair<string, string> paths;
    RETURN_IF_ERROR(find_cgroupv1_mounts(subsystem, &paths));
    const string& mount_path = paths.first;
    const string& system_path = paths.second;
    if (path->compare(0, system_path.size(), system_path) != 0) {
        return Status::InvalidArgument("Expected CGroup path '{}' to start with '{}'", *path,
                                       system_path);
    }
    path->replace(0, system_path.size(), mount_path);
    return Status::OK();
}

std::string CGroupUtil::cgroupv2_of_process() {
#if defined(OS_LINUX)
    if (!cgroupsv2_enable()) {
        return "";
    }
    // All PIDs assigned to a cgroup are in /sys/fs/cgroups/{cgroup_name}/cgroup.procs
    // A simpler way to get the membership is:
    std::ifstream cgroup_name_file("/proc/self/cgroup");
    if (!cgroup_name_file.is_open()) {
        return "";
    }
    // With cgroups v2, there will be a *single* line with prefix "0::/"
    // (see https://docs.kernel.org/admin-guide/cgroup-v2.html)
    std::string cgroup;
    std::getline(cgroup_name_file, cgroup);
    static const std::string v2_prefix = "0::/";
    if (!cgroup.starts_with(v2_prefix)) {
        return "";
    }
    cgroup = cgroup.substr(v2_prefix.length());
    return cgroup;
#else
    return "";
#endif
}

std::optional<std::string> CGroupUtil::get_cgroupsv2_path(const std::string& subsystem) {
#if defined(OS_LINUX)
    if (!CGroupUtil::cgroupsv2_enable()) {
        return {};
    }

    std::string cgroup = CGroupUtil::cgroupv2_of_process();
    auto current_cgroup = cgroup.empty() ? default_cgroups_mount : (default_cgroups_mount / cgroup);

    // Return the bottom-most nested current memory file. If there is no such file at the current
    // level, try again at the parent level as memory settings are inherited.
    while (current_cgroup != default_cgroups_mount.parent_path()) {
        if (std::filesystem::exists(current_cgroup / subsystem)) {
            return {current_cgroup};
        }
        current_cgroup = current_cgroup.parent_path();
    }
    return {};
#else
    return {};
#endif
}

Status CGroupUtil::read_int_line_from_cgroup_file(const std::filesystem::path& file_path,
                                                  int64_t* val) {
    std::ifstream file_stream(file_path, std::ios::in);
    if (!file_stream.is_open()) {
        return Status::CgroupError("Error open {}", file_path.string());
    }

    string line;
    getline(file_stream, line);
    if (file_stream.fail() || file_stream.bad()) {
        return Status::CgroupError("Error reading {}: {}", file_path.string(), get_str_err_msg());
    }
    StringParser::ParseResult pr;
    // Parse into an int64_t If it overflows, returning the max value of int64_t is ok because that
    // is effectively unlimited.
    *val = StringParser::string_to_int<int64_t>(line.c_str(), line.size(), &pr);
    if ((pr != StringParser::PARSE_SUCCESS && pr != StringParser::PARSE_OVERFLOW)) {
        return Status::InvalidArgument("Failed to parse {} as int64: '{}'", file_path.string(),
                                       line);
    }
    return Status::OK();
}

void CGroupUtil::read_int_metric_from_cgroup_file(
        const std::filesystem::path& file_path,
        std::unordered_map<std::string, int64_t>& metrics_map) {
    std::ifstream cgroup_file(file_path, std::ios::in);
    std::string line;
    while (cgroup_file.good() && !cgroup_file.eof()) {
        getline(cgroup_file, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());
        if (fields.size() < 2) {
            continue;
        }
        std::string key = fields[0].substr(0, fields[0].size());

        StringParser::ParseResult result;
        auto value =
                StringParser::string_to_int<int64_t>(fields[1].data(), fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            if (fields.size() == 2) {
                metrics_map[key] = value;
            } else if (fields[2] == "kB") {
                metrics_map[key] = value * 1024L;
            }
        }
    }
    if (cgroup_file.is_open()) {
        cgroup_file.close();
    }
}

Status CGroupUtil::read_string_line_from_cgroup_file(const std::filesystem::path& file_path,
                                                     std::string* line_ptr) {
    std::ifstream file_stream(file_path, std::ios::in);
    if (!file_stream.is_open()) {
        return Status::CgroupError("Error open {}", file_path.string());
    }
    string line;
    getline(file_stream, line);
    if (file_stream.fail() || file_stream.bad()) {
        return Status::CgroupError("Error reading {}: {}", file_path.string(), get_str_err_msg());
    }
    *line_ptr = line;
    return Status::OK();
}

Status CGroupUtil::parse_cpuset_line(std::string cpuset_line, int* cpu_count_ptr) {
    if (cpuset_line.empty()) {
        return Status::CgroupError("cpuset line is empty");
    }
    std::vector<string> ranges;
    boost::split(ranges, cpuset_line, boost::is_any_of(","));
    int cpu_count = 0;

    for (const std::string& range : ranges) {
        std::vector<std::string> cpu_values;
        boost::split(cpu_values, range, boost::is_any_of("-"));

        if (cpu_values.size() == 2) {
            int start = std::stoi(cpu_values[0]);
            int end = std::stoi(cpu_values[1]);
            cpu_count += (end - start) + 1;
        } else {
            cpu_count++;
        }
    }
    *cpu_count_ptr = cpu_count;
    return Status::OK();
}

int CGroupUtil::get_cgroup_limited_cpu_number(int physical_cores) {
    if (physical_cores <= 0) {
        return physical_cores;
    }
    int ret = physical_cores;
#if defined(OS_LINUX)
    // For cgroup v2
    // Child cgroup's cpu.max may bigger than parent group's cpu.max,
    //      so it should look up from current cgroup to top group.
    // For cpuset, child cgroup's cpuset.cpus could not bigger thant parent's cpuset.cpus.
    if (CGroupUtil::cgroupsv2_enable()) {
        std::string cgroupv2_process_path = CGroupUtil::cgroupv2_of_process();
        if (cgroupv2_process_path.empty()) {
            return ret;
        }
        std::filesystem::path current_cgroup_path = (default_cgroups_mount / cgroupv2_process_path);
        ret = get_cgroup_v2_cpu_quota_number(current_cgroup_path, default_cgroups_mount, ret);

        current_cgroup_path = (default_cgroups_mount / cgroupv2_process_path);
        ret = get_cgroup_v2_cpuset_number(current_cgroup_path, default_cgroups_mount, ret);
    } else if (CGroupUtil::cgroupsv1_enable()) {
        // cpu quota, should find first not empty config from current path to top.
        // because if a process attach to current cgroup, its cpu quota may not be set.
        std::string cpu_quota_path = "";
        Status cpu_quota_ret = CGroupUtil::find_abs_cgroupv1_path("cpu", &cpu_quota_path);
        if (cpu_quota_ret.ok() && !cpu_quota_path.empty()) {
            std::filesystem::path current_cgroup_path = cpu_quota_path;
            ret = get_cgroup_v1_cpu_quota_number(current_cgroup_path, default_cgroups_mount, ret);
        }

        //cpuset
        // just lookup current process cgroup path is enough
        // because if a process attach to current cgroup, its cpuset.cpus must be set.
        std::string cpuset_path = "";
        Status cpuset_ret = CGroupUtil::find_abs_cgroupv1_path("cpuset", &cpuset_path);
        if (cpuset_ret.ok() && !cpuset_path.empty()) {
            std::filesystem::path current_path = cpuset_path;
            ret = get_cgroup_v1_cpuset_number(current_path, ret);
        }
    }
#endif
    return ret;
}

int CGroupUtil::get_cgroup_v2_cpu_quota_number(std::filesystem::path& current_path,
                                               const std::filesystem::path& default_cg_mout_path,
                                               int cpu_num) {
    int ret = cpu_num;
    while (current_path != default_cg_mout_path.parent_path()) {
        std::ifstream cpu_max_file(current_path / "cpu.max");
        if (cpu_max_file.is_open()) {
            std::string cpu_limit_str;
            double cpu_period;
            cpu_max_file >> cpu_limit_str >> cpu_period;
            if (cpu_limit_str != "max" && cpu_period != 0) {
                double cpu_limit = std::stod(cpu_limit_str);
                ret = std::min(static_cast<int>(std::ceil(cpu_limit / cpu_period)), ret);
            }
        }
        current_path = current_path.parent_path();
    }
    return ret;
}

int CGroupUtil::get_cgroup_v2_cpuset_number(std::filesystem::path& current_path,
                                            const std::filesystem::path& default_cg_mout_path,
                                            int cpu_num) {
    int ret = cpu_num;
    while (current_path != default_cg_mout_path.parent_path()) {
        std::ifstream cpuset_cpus_file(current_path / "cpuset.cpus.effective");
        current_path = current_path.parent_path();
        if (cpuset_cpus_file.is_open()) {
            std::string cpuset_line;
            cpuset_cpus_file >> cpuset_line;
            if (cpuset_line.empty()) {
                continue;
            }
            int cpus_count = 0;
            static_cast<void>(CGroupUtil::parse_cpuset_line(cpuset_line, &cpus_count));
            ret = std::min(cpus_count, ret);
            break;
        }
    }
    return ret;
}

int CGroupUtil::get_cgroup_v1_cpu_quota_number(std::filesystem::path& current_path,
                                               const std::filesystem::path& default_cg_mout_path,
                                               int cpu_num) {
    int ret = cpu_num;
    while (current_path != default_cg_mout_path.parent_path()) {
        std::ifstream cpu_quota_file(current_path / "cpu.cfs_quota_us");
        std::ifstream cpu_period_file(current_path / "cpu.cfs_period_us");
        if (cpu_quota_file.is_open() && cpu_period_file.is_open()) {
            double cpu_quota_value;
            double cpu_period_value;
            cpu_quota_file >> cpu_quota_value;
            cpu_period_file >> cpu_period_value;
            if (cpu_quota_value > 0 && cpu_period_value > 0) {
                ret = std::min(ret,
                               static_cast<int>(std::ceil(cpu_quota_value / cpu_period_value)));
                break;
            }
        }
        current_path = current_path.parent_path();
    }
    return ret;
}

int CGroupUtil::get_cgroup_v1_cpuset_number(std::filesystem::path& current_path, int cpu_num) {
    int ret = cpu_num;
    std::string cpuset_line = "";
    Status cpuset_ret = CGroupUtil::read_string_line_from_cgroup_file(
            (current_path / "cpuset.cpus"), &cpuset_line);
    if (cpuset_ret.ok() && !cpuset_line.empty()) {
        int cpuset_count = 0;
        static_cast<void>(CGroupUtil::parse_cpuset_line(cpuset_line, &cpuset_count));
        if (cpuset_count > 0) {
            ret = std::min(ret, cpuset_count);
        }
    }
    return ret;
}

} // namespace doris
