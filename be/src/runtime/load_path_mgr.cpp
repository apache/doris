// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "runtime/load_path_mgr.h"

#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/algorithm/string/join.hpp>

#include "olap/olap_define.h"
#include "olap/olap_rootpath.h"
#include "util/file_utils.h"

namespace palo {

LoadPathMgr::LoadPathMgr() : _idx(0) { }

Status LoadPathMgr::init() {
    OLAPRootPath::RootPathVec all_available_root_path;
    OLAPRootPath::get_instance()->get_all_available_root_path(&all_available_root_path);
    _path_vec.clear();
    for (auto& one_path : all_available_root_path) {
        _path_vec.push_back(one_path + MINI_PREFIX);
    }

    LOG(INFO) << "Load path configured to [" << boost::join(_path_vec, ",") << "]";
    _idx = 0;
    _reserved_hours = std::max(config::load_data_reserve_hours, 1L);
    pthread_create(&_cleaner_id, nullptr, LoadPathMgr::cleaner, this);
    return Status::OK;
}

void* LoadPathMgr::cleaner(void* param) {
    // TODO(zc): add this thread to cgroup for control resource it use
    LoadPathMgr* mgr = (LoadPathMgr*)param;
    int64_t sleep_time = std::max(3600L / 4, config::load_data_reserve_hours * 3600 / 4);
    while (true) {
        sleep(sleep_time);
        mgr->clean();
    }
    return nullptr;
}

Status LoadPathMgr::allocate_dir(
        const std::string& db,
        const std::string& label,
        std::string* prefix) {
    if (_path_vec.empty()) {
        return Status("No load path configed.");
    }
    std::string path;
    auto size = _path_vec.size();
    auto retry = size;
    Status status = Status::OK;
    while (retry--) {
        {
            std::lock_guard<std::mutex> l(_lock);
            path = _path_vec[_idx] + "/" + db + "/" + label;
            _idx = (_idx + 1) % size;
        }
        status = FileUtils::create_dir(path);
        if (LIKELY(status.ok())) {
            *prefix = path;
            return Status::OK;
        }
    }

    return status;
}

bool LoadPathMgr::can_delete_label(time_t cur_time, const std::string& label_dir) {
    struct stat dir_stat;
    if (stat(label_dir.c_str(), &dir_stat)) {
        char buf[64];
        // State failed, just information
        LOG(WARNING) << "stat directory failed.path=" << label_dir 
            << ",code=" << strerror_r(errno, buf, 64);
        return false;
    }

    if (!S_ISDIR(dir_stat.st_mode)) {
        // Not a directory
        return false;
    }

    if ((cur_time - dir_stat.st_mtime) < _reserved_hours * 3600) {
        return false;
    }

    return true;
}

void LoadPathMgr::get_load_data_path(std::vector<std::string>* data_paths) {
    data_paths->insert(data_paths->end(), _path_vec.begin(), _path_vec.end());
    return;
}

void LoadPathMgr::clean_one_path(const std::string& path) {
    std::vector<std::string> dbs;
    Status status = FileUtils::scan_dir(path, &dbs);
    if (!status.ok()) {
        LOG(WARNING) << "scan one path to delete directory failed. path=" << path;
        return;
    }

    time_t now = time(nullptr);
    for (auto& db : dbs) {
        std::string db_dir = path + "/" + db;
        std::vector<std::string> labels;
        status = FileUtils::scan_dir(db_dir, &labels);
        if (!status.ok()) {
            LOG(WARNING) << "scan db of trash dir failed, continue. dir=" << db_dir;
            continue;
        }
        // delete this file
        for (auto& label : labels) {
            std::string label_dir = db_dir + "/" + label;
            if (!can_delete_label(now, label_dir)) {
                continue;
            }
            LOG(INFO) << "Going to remove load directory. path=" << label_dir;
            status = FileUtils::remove_all(label_dir);
            if (status.ok()) {
                LOG(INFO) << "Remove load directory success. path=" << label_dir;
            } else {
                LOG(WARNING) << "Remove load directory failed. path=" << label_dir;
            }
        }
    }
}

void LoadPathMgr::clean() {
    for (auto& path : _path_vec) {
        clean_one_path(path);
    }
}

}
