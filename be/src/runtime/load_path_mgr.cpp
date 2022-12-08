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

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <boost/algorithm/string/join.hpp>
#include <string>

#include "env/env.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/file_utils.h"

namespace doris {

static const uint32_t MAX_SHARD_NUM = 1024;
static const std::string SHARD_PREFIX = "__shard_";

LoadPathMgr::LoadPathMgr(ExecEnv* exec_env)
        : _exec_env(exec_env),
          _idx(0),
          _next_shard(0),
          _error_path_next_shard(0),
          _stop_background_threads_latch(1) {}

LoadPathMgr::~LoadPathMgr() {
    _stop_background_threads_latch.count_down();
    if (_clean_thread) {
        _clean_thread->join();
    }
}

Status LoadPathMgr::init() {
    _path_vec.clear();
    for (auto& path : _exec_env->store_paths()) {
        _path_vec.push_back(path.path + "/" + MINI_PREFIX);
    }
    LOG(INFO) << "Load path configured to [" << boost::join(_path_vec, ",") << "]";

    // error log is saved in first root path
    _error_log_dir = _exec_env->store_paths()[0].path + "/" + ERROR_LOG_PREFIX;
    // check and make dir
    RETURN_IF_ERROR(FileUtils::create_dir(_error_log_dir));

    _idx = 0;
    _reserved_hours = std::max<int64_t>(config::load_data_reserve_hours, 1L);
    RETURN_IF_ERROR(Thread::create(
            "LoadPathMgr", "clean_expired_temp_path",
            [this]() {
                // TODO(zc): add this thread to cgroup for control resource it use
                while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(3600))) {
                    this->clean();
                }
            },
            &_clean_thread));
    return Status::OK();
}

Status LoadPathMgr::allocate_dir(const std::string& db, const std::string& label,
                                 std::string* prefix) {
    if (_path_vec.empty()) {
        return Status::InternalError("No load path configured.");
    }
    std::string path;
    auto size = _path_vec.size();
    auto retry = size;
    Status status = Status::OK();
    while (retry--) {
        {
            // add SHARD_PREFIX for compatible purpose
            std::lock_guard<std::mutex> l(_lock);
            std::string shard = SHARD_PREFIX + std::to_string(_next_shard++ % MAX_SHARD_NUM);
            path = _path_vec[_idx] + "/" + db + "/" + shard + "/" + label;
            _idx = (_idx + 1) % size;
        }
        status = FileUtils::create_dir(path);
        if (LIKELY(status.ok())) {
            *prefix = path;
            return Status::OK();
        } else {
            LOG(WARNING) << "create dir failed:" << path
                         << ", error msg:" << status.get_error_msg();
        }
    }

    return status;
}

bool LoadPathMgr::is_too_old(time_t cur_time, const std::string& label_dir, int64_t reserve_hours) {
    struct stat dir_stat;
    if (stat(label_dir.c_str(), &dir_stat)) {
        char buf[64];
        // State failed, just information
        LOG(WARNING) << "stat directory failed.path=" << label_dir
                     << ",code=" << strerror_r(errno, buf, 64);
        return false;
    }

    if ((cur_time - dir_stat.st_mtime) < reserve_hours * 3600) {
        return false;
    }

    return true;
}

void LoadPathMgr::get_load_data_path(std::vector<std::string>* data_paths) {
    data_paths->insert(data_paths->end(), _path_vec.begin(), _path_vec.end());
    return;
}

const std::string ERROR_FILE_NAME = "error_log";

Status LoadPathMgr::get_load_error_file_name(const std::string& db, const std::string& label,
                                             const TUniqueId& fragment_instance_id,
                                             std::string* error_path) {
    std::stringstream ss;
    std::string shard = "";
    {
        std::lock_guard<std::mutex> l(_lock);
        shard = SHARD_PREFIX + std::to_string(_error_path_next_shard++ % MAX_SHARD_NUM);
    }
    std::string shard_path = _error_log_dir + "/" + shard;
    // check and create shard path
    Status status = FileUtils::create_dir(shard_path);
    if (!status.ok()) {
        LOG(WARNING) << "create error sub path failed. path=" << shard_path;
    }
    // add shard sub dir to file path
    ss << shard << "/" << ERROR_FILE_NAME << "_" << db << "_" << label << "_" << std::hex
       << fragment_instance_id.hi << "_" << fragment_instance_id.lo;
    *error_path = ss.str();
    return Status::OK();
}

std::string LoadPathMgr::get_load_error_absolute_path(const std::string& file_path) {
    std::string path;
    path.append(_error_log_dir);
    path.append("/");
    path.append(file_path);
    return path;
}

void LoadPathMgr::process_path(time_t now, const std::string& path, int64_t reserve_hours) {
    if (!is_too_old(now, path, reserve_hours)) {
        return;
    }
    LOG(INFO) << "Going to remove path. path=" << path;
    Status status = FileUtils::remove_all(path);
    if (status.ok()) {
        LOG(INFO) << "Remove path success. path=" << path;
    } else {
        LOG(WARNING) << "Remove path failed. path=" << path;
    }
}

void LoadPathMgr::clean_one_path(const std::string& path) {
    Env* env = Env::Default();

    std::vector<std::string> dbs;
    Status status = FileUtils::list_files(env, path, &dbs);
    // path may not exist
    if (!status.ok() && !status.is_not_found()) {
        LOG(WARNING) << "scan one path to delete directory failed. path=" << path;
        return;
    }

    time_t now = time(nullptr);
    for (auto& db : dbs) {
        std::string db_dir = path + "/" + db;
        std::vector<std::string> sub_dirs;
        status = FileUtils::list_files(env, db_dir, &sub_dirs);
        if (!status.ok()) {
            LOG(WARNING) << "scan db of trash dir failed, continue. dir=" << db_dir;
            continue;
        }
        // delete this file
        for (auto& sub_dir : sub_dirs) {
            std::string sub_path = db_dir + "/" + sub_dir;
            // for compatible
            if (sub_dir.find(SHARD_PREFIX) == 0) {
                // sub_dir starts with SHARD_PREFIX
                // process shard sub dir
                std::vector<std::string> labels;
                Status status = FileUtils::list_files(env, sub_path, &labels);
                if (!status.ok()) {
                    LOG(WARNING) << "scan one path to delete directory failed. path=" << sub_path;
                    continue;
                }
                for (auto& label : labels) {
                    std::string label_dir = sub_path + "/" + label;
                    process_path(now, label_dir, config::load_data_reserve_hours);
                }
            } else {
                // process label dir
                process_path(now, sub_path, config::load_data_reserve_hours);
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
    Env* env = Env::Default();

    time_t now = time(nullptr);
    std::vector<std::string> sub_dirs;
    Status status = FileUtils::list_files(env, _error_log_dir, &sub_dirs);
    if (!status.ok()) {
        LOG(WARNING) << "scan error_log dir failed. dir=" << _error_log_dir;
        return;
    }

    for (auto& sub_dir : sub_dirs) {
        std::string sub_path = _error_log_dir + "/" + sub_dir;
        // for compatible
        if (sub_dir.find(SHARD_PREFIX) == 0) {
            // sub_dir starts with SHARD_PREFIX
            // process shard sub dir
            std::vector<std::string> error_log_files;
            Status status = FileUtils::list_files(env, sub_path, &error_log_files);
            if (!status.ok()) {
                LOG(WARNING) << "scan one path to delete directory failed. path=" << sub_path;
                continue;
            }
            for (auto& error_log : error_log_files) {
                std::string error_log_path = sub_path + "/" + error_log;
                process_path(now, error_log_path, config::load_error_log_reserve_hours);
            }
        } else {
            // process error log file
            process_path(now, sub_path, config::load_error_log_reserve_hours);
        }
    }
}

} // namespace doris
