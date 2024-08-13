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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CgroupsMemoryUsageObserver.cpp
// and modified by Doris

#include "common/cgroup_memory_ctl.h"

#include <filesystem>
#include <fstream>
#include <memory>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "util/cgroup_util.h"

namespace doris {

// Is the memory controller of cgroups v2 enabled on the system?
// Assumes that cgroupsv2_enable() is enabled.
bool cgroupsv2_memory_controller_enabled() {
#if defined(OS_LINUX)
    assert(CGroupUtil::cgroupsv2_enable());
    // According to https://docs.kernel.org/admin-guide/cgroup-v2.html, file "cgroup.controllers" defines which controllers are available
    // for the current + child cgroups. The set of available controllers can be restricted from level to level using file
    // "cgroups.subtree_control". It is therefore sufficient to check the bottom-most nested "cgroup.controllers" file.
    std::string cgroup = CGroupUtil::cgroupv2_of_process();
    auto cgroup_dir = cgroup.empty() ? default_cgroups_mount : (default_cgroups_mount / cgroup);
    std::ifstream controllers_file(cgroup_dir / "cgroup.controllers");
    if (!controllers_file.is_open()) {
        return false;
    }
    std::string controllers;
    std::getline(controllers_file, controllers);
    return controllers.find("memory") != std::string::npos;
#else
    return false;
#endif
}

struct CgroupsV1Reader : CGroupMemoryCtl::ICgroupsReader {
    explicit CgroupsV1Reader(std::filesystem::path mount_file_dir)
            : _mount_file_dir(std::move(mount_file_dir)) {}

    uint64_t read_memory_limit() override {
        int64_t value;
        auto st = CGroupUtil::read_int_line_from_cgroup_file(
                (_mount_file_dir / "memory.limit_in_bytes"), &value);
        if (!st.ok()) {
            throw doris::Exception(doris::ErrorCode::END_OF_FILE,
                                   "Cannot read cgroupv1 memory.limit_in_bytes, " + st.to_string());
        }
        return value;
    }

    uint64_t read_memory_usage() override {
        std::unordered_map<std::string, int64_t> metrics_map;
        CGroupUtil::read_int_metric_from_cgroup_file((_mount_file_dir / "memory.stat"),
                                                     metrics_map);
        return metrics_map["rss"];
    }

private:
    std::filesystem::path _mount_file_dir;
};

struct CgroupsV2Reader : CGroupMemoryCtl::ICgroupsReader {
    explicit CgroupsV2Reader(std::filesystem::path mount_file_dir)
            : _mount_file_dir(std::move(mount_file_dir)) {}

    uint64_t read_memory_limit() override {
        int64_t value;
        auto st = CGroupUtil::read_int_line_from_cgroup_file((_mount_file_dir / "memory.max"),
                                                             &value);
        if (!st.ok()) {
            throw doris::Exception(doris::ErrorCode::END_OF_FILE,
                                   "Cannot read cgroupv2 memory.max, " + st.to_string());
        }
        return value;
    }

    uint64_t read_memory_usage() override {
        int64_t mem_usage = 0;
        // memory.current contains a single number
        // the reason why we subtract it described here: https://github.com/ClickHouse/ClickHouse/issues/64652#issuecomment-2149630667
        auto st = CGroupUtil::read_int_line_from_cgroup_file((_mount_file_dir / "memory.current"),
                                                             &mem_usage);
        if (!st.ok()) {
            throw doris::Exception(doris::ErrorCode::END_OF_FILE,
                                   "Cannot read cgroupv2 memory.current, " + st.to_string());
        }
        std::unordered_map<std::string, int64_t> metrics_map;
        CGroupUtil::read_int_metric_from_cgroup_file((_mount_file_dir / "memory.stat"),
                                                     metrics_map);
        mem_usage -= metrics_map["inactive_file"];
        if (mem_usage < 0) {
            throw doris::Exception(doris::ErrorCode::END_OF_FILE, "Negative memory usage");
        }
        return mem_usage;
    }

private:
    std::filesystem::path _mount_file_dir;
};

std::pair<std::string, CGroupUtil::CgroupsVersion> get_cgroups_path() {
    if (CGroupUtil::cgroupsv2_enable() && cgroupsv2_memory_controller_enabled()) {
        auto v2_path = CGroupUtil::get_cgroupsv2_path("memory.stat");
        if (v2_path.has_value()) {
            return {*v2_path, CGroupUtil::CgroupsVersion::V2};
        }
    }

    std::string cgroup_path;
    auto st = CGroupUtil::find_abs_cgroupv1_path("memory", &cgroup_path);
    if (st.ok()) {
        return {cgroup_path, CGroupUtil::CgroupsVersion::V1};
    }

    throw doris::Exception(doris::ErrorCode::END_OF_FILE,
                           "Cannot find cgroups v1 or v2 current memory file");
}

std::shared_ptr<CGroupMemoryCtl::ICgroupsReader> get_cgroups_reader() {
    const auto [cgroup_path, version] = get_cgroups_path();

    if (version == CGroupUtil::CgroupsVersion::V2) {
        return std::make_shared<CgroupsV2Reader>(cgroup_path);
    } else {
        return std::make_shared<CgroupsV1Reader>(cgroup_path);
    }
}

Status CGroupMemoryCtl::find_cgroup_mem_limit(int64_t* bytes) {
    try {
        *bytes = get_cgroups_reader()->read_memory_limit();
        return Status::OK();
    } catch (const doris::Exception& e) {
        LOG(WARNING) << "Cgroup find_cgroup_mem_limit failed, " << e.to_string();
        return Status::EndOfFile(e.to_string());
    }
}

Status CGroupMemoryCtl::find_cgroup_mem_usage(int64_t* bytes) {
    try {
        *bytes = get_cgroups_reader()->read_memory_usage();
        return Status::OK();
    } catch (const doris::Exception& e) {
        LOG(WARNING) << "Cgroup find_cgroup_mem_usage failed, " << e.to_string();
        return Status::EndOfFile(e.to_string());
    }
}

std::string CGroupMemoryCtl::debug_string() {
    const auto [cgroup_path, version] = get_cgroups_path();

    int64_t mem_limit;
    auto mem_limit_st = find_cgroup_mem_limit(&mem_limit);

    int64_t mem_usage;
    auto mem_usage_st = find_cgroup_mem_usage(&mem_usage);

    return fmt::format(
            "Process CGroup Memory Info (cgroups path: {}, cgroup version: {}): memory limit: {}, "
            "memory usage: {}",
            cgroup_path, (version == CGroupUtil::CgroupsVersion::V1) ? "v1" : "v2",
            mem_limit_st.ok() ? std::to_string(mem_limit) : mem_limit_st.to_string(),
            mem_usage_st.ok() ? std::to_string(mem_usage) : mem_usage_st.to_string());
}

} // namespace doris
