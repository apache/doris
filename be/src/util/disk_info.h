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

#ifndef DORIS_BE_SRC_COMMON_UTIL_DISK_INFO_H
#define DORIS_BE_SRC_COMMON_UTIL_DISK_INFO_H

#include <map>
#include <set>
#include <string>

#include "common/logging.h"
#include "common/status.h"

namespace doris {

// DiskInfo is an interface to query for the disk information at runtime.  This
// contains information about the system as well as the specific data node
// configuration.
// This information is pulled from /proc/partitions.
// TODO: datanode information not implemented
class DiskInfo {
public:
    // Initialize DiskInfo.  Just be called before any other functions.
    static void init();

    // Returns the number of (logical) disks on the system
    static int num_disks() {
        DCHECK(_s_initialized);
        return _s_disks.size();
    }

    // Returns the 0-based disk index for 'path' (path must be a FS path, not
    // hdfs path).
    static int disk_id(const char* path);

    // Returns the device name (e.g. sda) for disk_id
    static const std::string& device_name(int disk_id) {
        DCHECK_GE(disk_id, 0);
        DCHECK_LT(disk_id, _s_disks.size());
        return _s_disks[disk_id].name;
    }

    static bool is_rotational(int disk_id) {
        DCHECK_GE(disk_id, 0);
        DCHECK_LT(disk_id, _s_disks.size());
        return _s_disks[disk_id].is_rotational;
    }

    static std::string debug_string();

    // get disk devices of given path
    static Status get_disk_devices(const std::vector<std::string>& paths,
                                   std::set<std::string>* devices);

private:
    static bool _s_initialized;

    struct Disk {
        // Name of the disk (e.g. sda)
        std::string name;

        // 0 based index.  Does not map to anything in the system, useful to index into
        // our structures
        int id;

        bool is_rotational;

        Disk() : name(""), id(0) {}
        Disk(const std::string& name) : name(name), id(0), is_rotational(true) {}
        Disk(const std::string& name, int id) : name(name), id(id), is_rotational(true) {}
        Disk(const std::string& name, int id, bool is_rotational)
                : name(name), id(id), is_rotational(is_rotational) {}
    };

    // All disks
    static std::vector<Disk> _s_disks;

    // mapping of dev_ts to disk ids
    static std::map<dev_t, int> _s_device_id_to_disk_id;

    // mapping of devices names to disk ids
    static std::map<std::string, int> _s_disk_name_to_disk_id;

    static int _s_num_datanode_dirs;

    static void get_device_names();
};

} // namespace doris
#endif
