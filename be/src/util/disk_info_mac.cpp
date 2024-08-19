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

#include <libgen.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <cstring>
#include <iterator>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "disk_info.h"
#include "io/fs/local_file_system.h"

namespace doris {

bool DiskInfo::_s_initialized;
std::vector<DiskInfo::Disk> DiskInfo::_s_disks;
std::map<dev_t, int> DiskInfo::_s_device_id_to_disk_id;
std::map<std::string, int> DiskInfo::_s_disk_name_to_disk_id;
int DiskInfo::_s_num_datanode_dirs;

std::vector<std::pair<std::string, std::string>> get_mount_infos() {
    struct statfs* stfs;
    auto num = getmntinfo(&stfs, MNT_WAIT);

    std::vector<std::pair<std::string, std::string>> mount_infos;
    mount_infos.reserve(num);
    for (int i = 0; i < num; ++i) {
        auto device = stfs[i].f_mntfromname;
        auto mount_point = stfs[i].f_mntonname;
        if (strncmp(device, "/dev/", std::min(strlen("/dev/"), strlen(device))) != 0) {
            continue;
        }
        mount_infos.push_back({device, mount_point});
    }
    return mount_infos;
}

void DiskInfo::get_device_names() {
    auto mount_infos = get_mount_infos();
    std::sort(std::begin(mount_infos), std::end(mount_infos),
              [](const auto& info, const auto& other) {
                  const auto& mount_point = info.second;
                  const auto& other_mount_point = other.second;
                  return mount_point < other_mount_point;
              });

    std::map<int, int> major_to_disk_id;
    for (const auto& info : mount_infos) {
        const auto* device = info.first.c_str();
        struct stat st;
        if (stat(device, &st) == 0) {
            dev_t dev = st.st_rdev;
            auto it = major_to_disk_id.find(major(dev));
            int disk_id;
            if (it == major_to_disk_id.end()) {
                disk_id = _s_disks.size();
                major_to_disk_id[major(dev)] = disk_id;

                std::string name = "disk" + std::to_string(disk_id);
                _s_disks.push_back(Disk(name, disk_id, true));
                _s_disk_name_to_disk_id[name] = disk_id;
            } else {
                disk_id = it->second;
            }
            _s_device_id_to_disk_id[dev] = disk_id;
        }
    }

    if (_s_disks.empty()) {
        // If all else fails, return 1
        LOG(WARNING) << "Could not determine number of disks on this machine.";
        _s_disks.push_back(Disk("sda", 0));
    }
}

void DiskInfo::init() {
    get_device_names();
    _s_initialized = true;
}

int DiskInfo::disk_id(const char* path) {
    struct stat s;
    stat(path, &s);
    std::map<dev_t, int>::iterator it = _s_device_id_to_disk_id.find(s.st_dev);

    if (it == _s_device_id_to_disk_id.end()) {
        return -1;
    }

    return it->second;
}

std::string DiskInfo::debug_string() {
    DCHECK(_s_initialized);
    std::stringstream stream;
    stream << "Disk Info: " << std::endl;
    stream << "  Num disks " << num_disks() << ": ";

    for (int i = 0; i < _s_disks.size(); ++i) {
        stream << _s_disks[i].name;

        if (i < num_disks() - 1) {
            stream << ", ";
        }
    }

    stream << std::endl;
    return stream.str();
}

Status DiskInfo::get_disk_devices(const std::vector<std::string>& paths,
                                  std::set<std::string>* devices) {
    std::vector<std::string> real_paths;
    for (const auto& path : paths) {
        std::string p;
        Status st = io::global_local_filesystem()->canonicalize(path, &p);
        if (!st.ok()) {
            LOG(WARNING) << "skip disk monitoring of path. " << st;
            continue;
        }
        real_paths.emplace_back(std::move(p));
    }

    auto mount_infos = get_mount_infos();
    for (const auto& path : real_paths) {
        size_t max_mount_size = 0;
        std::string match_dev;
        for (const auto& info : mount_infos) {
            auto mount_point = info.second;
            size_t mount_size = mount_point.length();
            if (mount_size < max_mount_size || path.size() < mount_point.size() ||
                mount_point.compare(0, mount_point.length(), path, 0, mount_point.length()) != 0) {
                continue;
            }
            max_mount_size = mount_size;
            match_dev = info.first;
        }
        if (max_mount_size > 0) {
            devices->emplace(basename(const_cast<char*>(match_dev.c_str())));
        }
    }
    return Status::OK();
}

} // namespace doris
