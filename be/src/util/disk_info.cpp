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

#include "util/disk_info.h"

#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <sys/types.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <fstream>
#include <iostream>
#include <sstream>

#include "gutil/strings/split.h"
#include "util/file_utils.h"

namespace doris {

bool DiskInfo::_s_initialized;
std::vector<DiskInfo::Disk> DiskInfo::_s_disks;
std::map<dev_t, int> DiskInfo::_s_device_id_to_disk_id;
std::map<std::string, int> DiskInfo::_s_disk_name_to_disk_id;
int DiskInfo::_s_num_datanode_dirs;

// Parses /proc/partitions to get the number of disks.  A bit of looking around
// seems to indicate this as the best way to do this.
// TODO: is there not something better than this?
void DiskInfo::get_device_names() {
    // Format of this file is:
    //    major, minor, #blocks, name
    // We are only interesting in name which is formatted as device_name<partition #>
    // The same device will show up multiple times for each partition (e.g. sda1, sda2).
    std::ifstream partitions("/proc/partitions", std::ios::in);

    while (partitions.good() && !partitions.eof()) {
        std::string line;
        getline(partitions, line);
        boost::trim(line);

        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());

        if (fields.size() != 4) {
            continue;
        }

        std::string name = fields[3];

        if (name == "name") {
            continue;
        }

        // Remove the partition# from the name.  e.g. sda2 --> sda
        boost::trim_right_if(name, boost::is_any_of("0123456789"));

        // Create a mapping of all device ids (one per partition) to the disk id.
        int major_dev_id = atoi(fields[0].c_str());
        int minor_dev_id = atoi(fields[1].c_str());
        dev_t dev = makedev(major_dev_id, minor_dev_id);
        DCHECK(_s_device_id_to_disk_id.find(dev) == _s_device_id_to_disk_id.end());

        int disk_id = -1;
        std::map<std::string, int>::iterator it = _s_disk_name_to_disk_id.find(name);

        if (it == _s_disk_name_to_disk_id.end()) {
            // First time seeing this disk
            disk_id = _s_disks.size();
            _s_disks.push_back(Disk(name, disk_id));
            _s_disk_name_to_disk_id[name] = disk_id;
        } else {
            disk_id = it->second;
        }

        _s_device_id_to_disk_id[dev] = disk_id;
    }

    if (partitions.is_open()) {
        partitions.close();
    }

    if (_s_disks.empty()) {
        // If all else fails, return 1
        LOG(WARNING) << "Could not determine number of disks on this machine.";
        _s_disks.push_back(Disk("sda", 0));
    }

    // Determine if the disk is rotational or not.
    for (int i = 0; i < _s_disks.size(); ++i) {
        // We can check if it is rotational by reading:
        // /sys/block/<device>/queue/rotational
        // If the file is missing or has unexpected data, default to rotational.
        std::stringstream ss;
        ss << "/sys/block/" << _s_disks[i].name << "/queue/rotational";
        std::ifstream rotational(ss.str().c_str(), std::ios::in);
        if (rotational.good()) {
            std::string line;
            getline(rotational, line);
            if (line == "0") {
                _s_disks[i].is_rotational = false;
            }
        }
        if (rotational.is_open()) {
            rotational.close();
        }
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
    for (auto& path : paths) {
        std::string p;
        WARN_IF_ERROR(FileUtils::canonicalize(path, &p),
                      "canonicalize path " + path + " failed, skip disk monitoring of this path");
        real_paths.emplace_back(std::move(p));
    }

    FILE* fp = fopen("/proc/mounts", "r");
    if (fp == nullptr) {
        std::stringstream ss;
        char buf[64];
        ss << "open /proc/mounts failed, errno:" << errno
           << ", message:" << strerror_r(errno, buf, 64);
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    Status status;
    char* line_ptr = 0;
    size_t line_buf_size = 0;
    for (auto& path : real_paths) {
        size_t max_mount_size = 0;
        std::string match_dev;
        rewind(fp);
        while (getline(&line_ptr, &line_buf_size, fp) > 0) {
            char dev_path[4096];
            char mount_path[4096];
            int num = sscanf(line_ptr, "%4095s %4095s", dev_path, mount_path);
            if (num < 2) {
                continue;
            }
            size_t mount_size = strlen(mount_path);
            if (mount_size < max_mount_size || path.size() < mount_size ||
                strncmp(path.c_str(), mount_path, mount_size) != 0) {
                continue;
            }
            std::string dev(basename(dev_path));
            boost::trim_right_if(dev, boost::is_any_of("0123456789"));
            if (_s_disk_name_to_disk_id.find(dev) != std::end(_s_disk_name_to_disk_id)) {
                max_mount_size = mount_size;
                match_dev = dev;
            }
        }
        if (ferror(fp) != 0) {
            std::stringstream ss;
            char buf[64];
            ss << "open /proc/mounts failed, errno:" << errno
               << ", message:" << strerror_r(errno, buf, 64);
            LOG(WARNING) << ss.str();
            status = Status::InternalError(ss.str());
            break;
        }
        if (max_mount_size > 0) {
            devices->emplace(match_dev);
        }
    }
    if (line_ptr != nullptr) {
        free(line_ptr);
    }
    fclose(fp);
    return status;
}

} // namespace doris
