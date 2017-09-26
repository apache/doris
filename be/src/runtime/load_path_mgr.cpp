// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include "gen_cpp/Types_types.h"

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

    // error log is saved in first root path
    _error_log_dir = all_available_root_path[0] + ERROR_LOG_PREFIX;
    // check and make dir
    RETURN_IF_ERROR(FileUtils::create_dir(_error_log_dir));

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

bool LoadPathMgr::is_too_old(time_t cur_time, const std::string& label_dir) {
    struct stat dir_stat;
    if (stat(label_dir.c_str(), &dir_stat)) {
        char buf[64];
        // State failed, just information
        LOG(WARNING) << "stat directory failed.path=" << label_dir
            << ",code=" << strerror_r(errno, buf, 64);
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

const std::string ERROR_FILE_NAME = "error_log";

Status LoadPathMgr::get_load_error_file_name(
        const std::string& db,
        const std::string&label,
        const TUniqueId& fragment_instance_id,
        std::string* error_path) {
    std::stringstream ss;
    ss << ERROR_FILE_NAME << "_" << db << "_" << label
        << "_" << std::hex << fragment_instance_id.hi
        << "_" << fragment_instance_id.lo;
    *error_path = ss.str();
    return Status::OK;
}

std::string LoadPathMgr::get_load_error_absolute_path(const std::string& file_name) {
    std::string path;
    path.append(_error_log_dir);
    path.append("/");
    path.append(file_name);
    return path;
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
            if (!is_too_old(now, label_dir)) {
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
    clean_error_log();
}

void LoadPathMgr::clean_error_log() {
    time_t now = time(nullptr);
    std::vector<std::string> error_logs;
    Status status = FileUtils::scan_dir(_error_log_dir, &error_logs);
    if (!status.ok()) {
        LOG(WARNING) << "scan error_log dir failed. dir=" << _error_log_dir;
        return;
    }

    for (auto& error_log : error_logs) {
        std::string log_path = _error_log_dir + "/" + error_log;
        if (!is_too_old(now, log_path)) {
            continue;
        }
        LOG(INFO) << "Going to remove error log file. path=" << log_path;
        status = FileUtils::remove_all(log_path);
        if (status.ok()) {
            LOG(INFO) << "Remove load directory success. path=" << log_path;
        } else {
            LOG(WARNING) << "Remove load directory failed. path=" << log_path;
        }
    }
}

}
