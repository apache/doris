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

#include "common/status.h"
#include "util/cgroup_util.h"

namespace doris {

// Is the memory controller of cgroups v2 enabled on the system?
// Assumes that cgroupsv2_enable() is enabled.
Status cgroupsv2_memory_controller_enabled(bool* ret) {
#if defined(OS_LINUX)
    if (!CGroupUtil::cgroupsv2_enable()) {
        return Status::CgroupError("cgroupsv2_enable is false");
    }
    // According to https://docs.kernel.org/admin-guide/cgroup-v2.html, file "cgroup.controllers" defines which controllers are available
    // for the current + child cgroups. The set of available controllers can be restricted from level to level using file
    // "cgroups.subtree_control". It is therefore sufficient to check the bottom-most nested "cgroup.controllers" file.
    std::string cgroup = CGroupUtil::cgroupv2_of_process();
    auto cgroup_dir = cgroup.empty() ? default_cgroups_mount : (default_cgroups_mount / cgroup);
    std::ifstream controllers_file(cgroup_dir / "cgroup.controllers");
    if (!controllers_file.is_open()) {
        *ret = false;
        return Status::CgroupError("open cgroup.controllers failed");
    }
    std::string controllers;
    std::getline(controllers_file, controllers);
    *ret = controllers.find("memory") != std::string::npos;
    return Status::OK();
#else
    *ret = false;
    return Status::CgroupError("cgroupsv2 only support Linux");
#endif
}

struct CgroupsV1Reader : CGroupMemoryCtl::ICgroupsReader {
    explicit CgroupsV1Reader(std::filesystem::path mount_file_dir)
            : _mount_file_dir(std::move(mount_file_dir)) {}

    Status read_memory_limit(int64_t* value) override {
        RETURN_IF_ERROR(CGroupUtil::read_int_line_from_cgroup_file(
                (_mount_file_dir / "memory.limit_in_bytes"), value));
        return Status::OK();
    }

    Status read_memory_usage(int64_t* value) override {
        std::unordered_map<std::string, int64_t> metrics_map;
        CGroupUtil::read_int_metric_from_cgroup_file((_mount_file_dir / "memory.stat"),
                                                     metrics_map);
        *value = metrics_map["rss"];
        return Status::OK();
    }

private:
    std::filesystem::path _mount_file_dir;
};

struct CgroupsV2Reader : CGroupMemoryCtl::ICgroupsReader {
    explicit CgroupsV2Reader(std::filesystem::path mount_file_dir)
            : _mount_file_dir(std::move(mount_file_dir)) {}

    Status read_memory_limit(int64_t* value) override {
        RETURN_IF_ERROR(CGroupUtil::read_int_line_from_cgroup_file((_mount_file_dir / "memory.max"),
                                                                   value));
        return Status::OK();
    }

    Status read_memory_usage(int64_t* value) override {
        // memory.current contains a single number
        // the reason why we subtract it described here: https://github.com/ClickHouse/ClickHouse/issues/64652#issuecomment-2149630667
        RETURN_IF_ERROR(CGroupUtil::read_int_line_from_cgroup_file(
                (_mount_file_dir / "memory.current"), value));
        std::unordered_map<std::string, int64_t> metrics_map;
        CGroupUtil::read_int_metric_from_cgroup_file((_mount_file_dir / "memory.stat"),
                                                     metrics_map);
        if (*value < metrics_map["inactive_file"]) {
            return Status::CgroupError("CgroupsV2Reader read_memory_usage negative memory usage");
        }
        *value -= metrics_map["inactive_file"];
        return Status::OK();
    }

private:
    std::filesystem::path _mount_file_dir;
};

std::pair<std::string, CGroupUtil::CgroupsVersion> get_cgroups_path() {
    bool enable_controller;
    auto cgroupsv2_memory_controller_st = cgroupsv2_memory_controller_enabled(&enable_controller);
    if (CGroupUtil::cgroupsv2_enable() && cgroupsv2_memory_controller_st.ok() &&
        enable_controller) {
        auto v2_memory_stat_path = CGroupUtil::get_cgroupsv2_path("memory.stat");
        auto v2_memory_current_path = CGroupUtil::get_cgroupsv2_path("memory.current");
        auto v2_memory_max_path = CGroupUtil::get_cgroupsv2_path("memory.max");
        if (v2_memory_stat_path.has_value() && v2_memory_current_path.has_value() &&
            v2_memory_max_path.has_value() && v2_memory_stat_path == v2_memory_current_path &&
            v2_memory_current_path == v2_memory_max_path) {
            return {*v2_memory_stat_path, CGroupUtil::CgroupsVersion::V2};
        }
    }

    std::string cgroup_path;
    auto st = CGroupUtil::find_abs_cgroupv1_path("memory", &cgroup_path);
    if (st.ok()) {
        return {cgroup_path, CGroupUtil::CgroupsVersion::V1};
    }

    return {"", CGroupUtil::CgroupsVersion::V1};
}

Status get_cgroups_reader(std::shared_ptr<CGroupMemoryCtl::ICgroupsReader>& reader) {
    const auto [cgroup_path, version] = get_cgroups_path();
    if (cgroup_path.empty()) {
        bool enable_controller;
        auto st = cgroupsv2_memory_controller_enabled(&enable_controller);
        return Status::CgroupError(
                "Cannot find cgroups v1 or v2 current memory file, cgroupsv2_enable: {},{}, "
                "cgroupsv2_memory_controller_enabled: {}, cgroupsv1_enable: {}",
                CGroupUtil::cgroupsv2_enable(), enable_controller, st.to_string(),
                CGroupUtil::cgroupsv1_enable());
    }

    if (version == CGroupUtil::CgroupsVersion::V2) {
        reader = std::make_shared<CgroupsV2Reader>(cgroup_path);
    } else {
        reader = std::make_shared<CgroupsV1Reader>(cgroup_path);
    }
    return Status::OK();
}

Status CGroupMemoryCtl::find_cgroup_mem_limit(int64_t* bytes) {
    std::shared_ptr<CGroupMemoryCtl::ICgroupsReader> reader;
    RETURN_IF_ERROR(get_cgroups_reader(reader));
    RETURN_IF_ERROR(reader->read_memory_limit(bytes));
    return Status::OK();
}

Status CGroupMemoryCtl::find_cgroup_mem_usage(int64_t* bytes) {
    std::shared_ptr<CGroupMemoryCtl::ICgroupsReader> reader;
    RETURN_IF_ERROR(get_cgroups_reader(reader));
    RETURN_IF_ERROR(reader->read_memory_usage(bytes));
    return Status::OK();
}

std::string CGroupMemoryCtl::debug_string() {
    const auto [cgroup_path, version] = get_cgroups_path();
    if (cgroup_path.empty()) {
        bool enable_controller;
        auto st = cgroupsv2_memory_controller_enabled(&enable_controller);
        return fmt::format(
                "Cannot find cgroups v1 or v2 current memory file, cgroupsv2_enable: {},{}, "
                "cgroupsv2_memory_controller_enabled: {}, cgroupsv1_enable: {}",
                CGroupUtil::cgroupsv2_enable(), enable_controller, st.to_string(),
                CGroupUtil::cgroupsv1_enable());
    }

    int64_t mem_limit;
    auto mem_limit_st = find_cgroup_mem_limit(&mem_limit);

    int64_t mem_usage;
    auto mem_usage_st = find_cgroup_mem_usage(&mem_usage);

    return fmt::format(
            "Process CGroup Memory Info (cgroups path: {}, cgroup version: {}): memory limit: "
            "{}, "
            "memory usage: {}",
            cgroup_path, (version == CGroupUtil::CgroupsVersion::V1) ? "v1" : "v2",
            mem_limit_st.ok() ? std::to_string(mem_limit) : mem_limit_st.to_string(),
            mem_usage_st.ok() ? std::to_string(mem_usage) : mem_usage_st.to_string());
}

} // namespace doris
