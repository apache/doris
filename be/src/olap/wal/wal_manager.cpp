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

#include "olap/wal/wal_manager.h"

#include <bvar/bvar.h>
#include <glog/logging.h>

#include <chrono>
#include <filesystem>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "io/fs/local_file_system.h"
#include "olap/wal/wal_dirs_info.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "util/parse_util.h"
#include "vec/exec/format/wal/wal_reader.h"

namespace doris {

bvar::Status<size_t> g_wal_total_count("wal_total_count", 0);

WalManager::WalManager(ExecEnv* exec_env, const std::string& wal_dir_list)
        : _exec_env(exec_env),
          _stop(false),
          _stop_background_threads_latch(1),
          _first_replay(true) {
    _wal_dirs = strings::Split(wal_dir_list, ";", strings::SkipWhitespace());
    static_cast<void>(ThreadPoolBuilder("GroupCommitReplayWalThreadPool")
                              .set_min_threads(1)
                              .set_max_threads(config::group_commit_relay_wal_threads)
                              .build(&_thread_pool));
    _wal_dirs_info = WalDirsInfo::create_unique();
}

WalManager::~WalManager() {
    LOG(INFO) << "WalManager is destoried";
}

bool WalManager::is_running() {
    return !_stop.load();
}

void WalManager::stop() {
    if (!this->_stop.load()) {
        this->_stop.store(true);
        _stop_relay_wal();
        _stop_background_threads_latch.count_down();
        if (_replay_thread) {
            _replay_thread->join();
        }
        if (_update_wal_dirs_info_thread) {
            _update_wal_dirs_info_thread->join();
        }
        _thread_pool->shutdown();
        LOG(INFO) << "WalManager is stopped";
    }
}

Status WalManager::init() {
    RETURN_IF_ERROR(_init_wal_dirs_conf());
    RETURN_IF_ERROR(_init_wal_dirs());
    RETURN_IF_ERROR(_init_wal_dirs_info());
    return Thread::create(
            "WalMgr", "replay_wal", [this]() { static_cast<void>(this->_replay_background()); },
            &_replay_thread);
}

Status WalManager::_init_wal_dirs_conf() {
    std::vector<std::string> tmp_dirs;
    if (_wal_dirs.empty()) {
        // default case.
        for (const StorePath& path : ExecEnv::GetInstance()->store_paths()) {
            tmp_dirs.emplace_back(path.path + "/wal");
        }
    } else {
        // user config must be absolute path.
        for (const std::string& wal_dir : _wal_dirs) {
            if (std::filesystem::path(wal_dir).is_absolute()) {
                tmp_dirs.emplace_back(wal_dir);
            } else {
                return Status::InternalError(
                        "BE config group_commit_replay_wal_dir has to be absolute path!");
            }
        }
    }
    _wal_dirs = tmp_dirs;
    return Status::OK();
}

Status WalManager::_init_wal_dirs() {
    bool exists = false;
    for (auto wal_dir : _wal_dirs) {
        std::string tmp_dir = wal_dir + "/" + _tmp;
        LOG(INFO) << "wal_dir:" << wal_dir << ", tmp_dir:" << tmp_dir;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(wal_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(wal_dir));
        }
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(tmp_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(tmp_dir));
        }
    }
    return Status::OK();
}

Status WalManager::_init_wal_dirs_info() {
    for (const std::string& wal_dir : _wal_dirs) {
        size_t available_bytes;
#ifndef BE_TEST
        size_t disk_capacity_bytes;
        RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(wal_dir, &disk_capacity_bytes,
                                                                      &available_bytes));
#else
        available_bytes = wal_limit_test_bytes;
#endif
        bool is_percent = true;
        int64_t wal_disk_limit = ParseUtil::parse_mem_spec(config::group_commit_wal_max_disk_limit,
                                                           -1, available_bytes, &is_percent);
        if (wal_disk_limit < 0) {
            return Status::InternalError(
                    "group_commit_wal_max_disk_limit config is wrong, please check your config!");
        }
        // if there are some wal files in wal dir, we need to add it to wal disk limit.
        size_t wal_dir_size = 0;
#ifndef BE_TEST
        RETURN_IF_ERROR(io::global_local_filesystem()->directory_size(wal_dir, &wal_dir_size));
#endif
        if (is_percent) {
            wal_disk_limit += wal_dir_size;
        }
        RETURN_IF_ERROR(_wal_dirs_info->add(wal_dir, wal_disk_limit, wal_dir_size, 0));

#ifdef BE_TEST
        wal_limit_test_bytes = wal_disk_limit;
#endif
    }
    return Thread::create(
            "WalMgr", "update_wal_dir_info",
            [this]() { static_cast<void>(this->_update_wal_dir_info_thread()); },
            &_update_wal_dirs_info_thread);
}

void WalManager::add_wal_queue(int64_t table_id, int64_t wal_id) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_queue_lock);
    LOG(INFO) << "add wal to queue, table_id: " << table_id << ", wal_id: " << wal_id;
    auto it = _wal_queues.find(table_id);
    if (it == _wal_queues.end()) {
        std::set<int64_t> tmp_set;
        tmp_set.insert(wal_id);
        _wal_queues.emplace(table_id, tmp_set);
    } else {
        it->second.insert(wal_id);
    }
}

void WalManager::erase_wal_queue(int64_t table_id, int64_t wal_id) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_queue_lock);
    auto it = _wal_queues.find(table_id);
    if (it != _wal_queues.end()) {
        LOG(INFO) << "remove wal from queue, table_id: " << table_id << ", wal_id: " << wal_id;
        it->second.erase(wal_id);
        if (it->second.empty()) {
            _wal_queues.erase(table_id);
        }
    }
}

size_t WalManager::get_wal_queue_size(int64_t table_id) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_queue_lock);
    size_t count = 0;
    if (table_id > 0) {
        auto it = _wal_queues.find(table_id);
        if (it != _wal_queues.end()) {
            return it->second.size();
        } else {
            return 0;
        }
    } else {
        // table_id is -1 meaning get all table wal size
        for (auto& [_, table_wals] : _wal_queues) {
            count += table_wals.size();
        }
    }
    return count;
}

Status WalManager::create_wal_path(int64_t db_id, int64_t table_id, int64_t wal_id,
                                   const std::string& label, std::string& base_path,
                                   uint32_t wal_version) {
    base_path = _wal_dirs_info->get_available_random_wal_dir();
    std::stringstream ss;
    ss << base_path << "/" << std::to_string(db_id) << "/" << std::to_string(table_id) << "/"
       << std::to_string(wal_version) << "_" << _exec_env->master_info()->backend_id << "_"
       << std::to_string(wal_id) << "_" << label;
    {
        std::lock_guard<std::shared_mutex> wrlock(_wal_path_lock);
        auto it = _wal_path_map.find(wal_id);
        if (it != _wal_path_map.end()) {
            return Status::InternalError("wal_id {} already in wal_path_map", wal_id);
        }
        _wal_path_map.emplace(wal_id, ss.str());
    }
    return Status::OK();
}

Status WalManager::get_wal_path(int64_t wal_id, std::string& wal_path) {
    std::shared_lock rdlock(_wal_path_lock);
    auto it = _wal_path_map.find(wal_id);
    if (it != _wal_path_map.end()) {
        wal_path = _wal_path_map[wal_id];
    } else {
        return Status::InternalError("can not find wal_id {} in wal_path_map", wal_id);
    }
    return Status::OK();
}

Status WalManager::parse_wal_path(const std::string& file_name, int64_t& version,
                                  int64_t& backend_id, int64_t& wal_id, std::string& label) {
    try {
        // find version
        auto pos = file_name.find("_");
        version = std::strtoll(file_name.substr(0, pos).c_str(), NULL, 10);
        // find be id
        auto substring1 = file_name.substr(pos + 1);
        pos = substring1.find("_");
        backend_id = std::strtoll(substring1.substr(0, pos).c_str(), NULL, 10);
        // find wal id
        auto substring2 = substring1.substr(pos + 1);
        pos = substring2.find("_");
        wal_id = std::strtoll(substring2.substr(0, pos).c_str(), NULL, 10);
        // find label
        label = substring2.substr(pos + 1);
        VLOG_DEBUG << "version:" << version << "backend_id:" << backend_id << ",wal_id:" << wal_id
                   << ",label:" << label;
    } catch (const std::invalid_argument& e) {
        return Status::InvalidArgument("Invalid format, {}", e.what());
    }
    return Status::OK();
}

Status WalManager::_load_wals() {
    std::vector<ScanWalInfo> wals;
    for (auto wal_dir : _wal_dirs) {
        WARN_IF_ERROR(_scan_wals(wal_dir, wals), fmt::format("fail to scan wal dir={}", wal_dir));
    }
    for (const auto& wal : wals) {
        bool exists = false;
        WARN_IF_ERROR(io::global_local_filesystem()->exists(wal.wal_path, &exists),
                      fmt::format("fail to check exist on wal file={}", wal.wal_path));
        if (!exists) {
            continue;
        }
        LOG(INFO) << "find wal: " << wal.wal_path;
        {
            std::lock_guard<std::shared_mutex> wrlock(_wal_path_lock);
            auto it = _wal_path_map.find(wal.wal_id);
            if (it != _wal_path_map.end()) {
                LOG(INFO) << "wal_id " << wal.wal_id << " already in wal_path_map, skip it";
                continue;
            }
            _wal_path_map.emplace(wal.wal_id, wal.wal_path);
        }
        // this config is use for test p0 case in pipeline
        if (config::group_commit_wait_replay_wal_finish) {
            auto lock = std::make_shared<std::mutex>();
            auto cv = std::make_shared<std::condition_variable>();
            auto add_st = add_wal_cv_map(wal.wal_id, lock, cv);
            if (!add_st.ok()) {
                LOG(WARNING) << "fail to add wal_id " << wal.wal_id << " to wal_cv_map";
                continue;
            }
        }
        _exec_env->wal_mgr()->add_wal_queue(wal.tb_id, wal.wal_id);
        WARN_IF_ERROR(add_recover_wal(wal.db_id, wal.tb_id, wal.wal_id, wal.wal_path),
                      fmt::format("Failed to add recover wal={}", wal.wal_path));
    }
    return Status::OK();
}

Status WalManager::_scan_wals(const std::string& wal_path, std::vector<ScanWalInfo>& res) {
    bool exists = false;
    auto last_total_size = res.size();
    std::vector<io::FileInfo> dbs;
    Status st = io::global_local_filesystem()->list(wal_path, false, &dbs, &exists);
    if (!st.ok()) {
        LOG(WARNING) << "failed list files for wal_dir=" << wal_path << ", st=" << st.to_string();
        return st;
    }
    for (const auto& database_id : dbs) {
        if (database_id.is_file || database_id.file_name == _tmp) {
            continue;
        }
        std::vector<io::FileInfo> tables;
        auto db_path = wal_path + "/" + database_id.file_name;
        st = io::global_local_filesystem()->list(db_path, false, &tables, &exists);
        if (!st.ok()) {
            LOG(WARNING) << "failed list files for wal_dir=" << db_path
                         << ", st=" << st.to_string();
            return st;
        }
        for (const auto& table_id : tables) {
            if (table_id.is_file) {
                continue;
            }
            std::vector<io::FileInfo> wals;
            auto table_path = db_path + "/" + table_id.file_name;
            st = io::global_local_filesystem()->list(table_path, false, &wals, &exists);
            if (!st.ok()) {
                LOG(WARNING) << "failed list files for wal_dir=" << table_path
                             << ", st=" << st.to_string();
                return st;
            }
            if (wals.empty()) {
                continue;
            }
            int64_t db_id = -1;
            int64_t tb_id = -1;
            try {
                db_id = std::strtoll(database_id.file_name.c_str(), NULL, 10);
                tb_id = std::strtoll(table_id.file_name.c_str(), NULL, 10);
            } catch (const std::invalid_argument& e) {
                return Status::InvalidArgument("Invalid format, {}", e.what());
            }
            for (const auto& wal : wals) {
                int64_t version = -1;
                int64_t backend_id = -1;
                int64_t wal_id = -1;
                std::string label = "";
                auto parse_st = parse_wal_path(wal.file_name, version, backend_id, wal_id, label);
                if (!parse_st.ok()) {
                    LOG(WARNING) << "fail to parse file=" << wal.file_name
                                 << ",st=" << parse_st.to_string();
                    continue;
                }
                auto wal_file = table_path + "/" + wal.file_name;
                struct ScanWalInfo scan_wal_info;
                scan_wal_info.wal_path = wal_file;
                scan_wal_info.db_id = db_id;
                scan_wal_info.tb_id = tb_id;
                scan_wal_info.wal_id = wal_id;
                scan_wal_info.be_id = backend_id;
                res.emplace_back(scan_wal_info);
            }
        }
    }
    LOG(INFO) << "Finish list wal_dir=" << wal_path
              << ", wal count=" << std::to_string(res.size() - last_total_size);
    return Status::OK();
}

Status WalManager::_replay_background() {
    do {
        if (_stop.load()) {
            break;
        }
        // port == 0 means not received heartbeat yet
        if (_exec_env->master_info() != nullptr &&
            _exec_env->master_info()->network_address.port == 0) {
            continue;
        }
        // replay residual wal,only replay once
        bool expected = true;
        if (_first_replay.compare_exchange_strong(expected, false)) {
            RETURN_IF_ERROR(_load_wals());
        }
        g_wal_total_count.set_value(get_wal_queue_size(-1));
        // replay wal of current process
        std::vector<int64_t> replay_tables;
        {
            std::lock_guard<std::shared_mutex> wrlock(_table_lock);
            auto it = _table_map.begin();
            while (it != _table_map.end()) {
                if (it->second->size() > 0) {
                    replay_tables.push_back(it->first);
                }
                it++;
            }
        }
        for (const auto& table_id : replay_tables) {
            RETURN_IF_ERROR(_thread_pool->submit_func([table_id, this] {
                auto st = this->_table_map[table_id]->replay_wals();
                if (!st.ok()) {
                    LOG(WARNING) << "failed to submit replay wal for table=" << table_id;
                }
            }));
        }
    } while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::group_commit_replay_wal_retry_interval_seconds)));
    return Status::OK();
}

Status WalManager::add_recover_wal(int64_t db_id, int64_t table_id, int64_t wal_id,
                                   std::string wal) {
    std::lock_guard<std::shared_mutex> wrlock(_table_lock);
    std::shared_ptr<WalTable> table_ptr;
    auto it = _table_map.find(table_id);
    if (it == _table_map.end()) {
        table_ptr = std::make_shared<WalTable>(_exec_env, db_id, table_id);
        _table_map.emplace(table_id, table_ptr);
    } else {
        table_ptr = it->second;
    }
    table_ptr->add_wal(wal_id, wal);
#ifndef BE_TEST
    WARN_IF_ERROR(update_wal_dir_limit(get_base_wal_path(wal)),
                  "Failed to update wal dir limit while add recover wal!");
    WARN_IF_ERROR(update_wal_dir_used(get_base_wal_path(wal)),
                  "Failed to update wal dir used while add recove wal!");
#endif
    return Status::OK();
}

size_t WalManager::get_wal_table_size(int64_t table_id) {
    std::shared_lock rdlock(_table_lock);
    auto it = _table_map.find(table_id);
    if (it != _table_map.end()) {
        return it->second->size();
    } else {
        return 0;
    }
}

void WalManager::_stop_relay_wal() {
    std::lock_guard<std::shared_mutex> wrlock(_table_lock);
    for (auto& [_, wal_table] : _table_map) {
        wal_table->stop();
    }
}

size_t WalManager::get_max_available_size() {
    return _wal_dirs_info->get_max_available_size();
}

std::string WalManager::get_wal_dirs_info_string() {
    return _wal_dirs_info->get_wal_dirs_info_string();
}

Status WalManager::update_wal_dir_limit(const std::string& wal_dir, size_t limit) {
    return _wal_dirs_info->update_wal_dir_limit(wal_dir, limit);
}

Status WalManager::update_wal_dir_used(const std::string& wal_dir, size_t used) {
    return _wal_dirs_info->update_wal_dir_used(wal_dir, used);
}

Status WalManager::update_wal_dir_estimated_wal_bytes(const std::string& wal_dir,
                                                      size_t increase_estimated_wal_bytes,
                                                      size_t decrease_estimated_wal_bytes) {
    return _wal_dirs_info->update_wal_dir_estimated_wal_bytes(wal_dir, increase_estimated_wal_bytes,
                                                              decrease_estimated_wal_bytes);
}

Status WalManager::_update_wal_dir_info_thread() {
    while (!_stop.load()) {
        if (!ExecEnv::ready()) {
            LOG(INFO) << "Sleep 1s to wait for storage engine init.";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            continue;
        }
        static_cast<void>(_wal_dirs_info->update_all_wal_dir_limit());
        static_cast<void>(_wal_dirs_info->update_all_wal_dir_used());
        LOG_EVERY_N(INFO, 100) << "Scheduled(every 10s) WAL info: " << get_wal_dirs_info_string();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return Status::OK();
}

Status WalManager::get_wal_dir_available_size(const std::string& wal_dir, size_t* available_bytes) {
    return _wal_dirs_info->get_wal_dir_available_size(wal_dir, available_bytes);
}

std::string WalManager::get_base_wal_path(const std::string& wal_path_str) {
    io::Path wal_path = wal_path_str;
    for (int i = 0; i < 3; ++i) {
        if (!wal_path.has_parent_path()) {
            return "";
        }
        wal_path = wal_path.parent_path();
    }
    return wal_path.string();
}

Status WalManager::add_wal_cv_map(int64_t wal_id, std::shared_ptr<std::mutex> lock,
                                  std::shared_ptr<std::condition_variable> cv) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_cv_lock);
    auto it = _wal_cv_map.find(wal_id);
    if (it != _wal_cv_map.end()) {
        return Status::InternalError("wal {} is already in _wal_cv_map ", wal_id);
    }
    auto pair = std::make_pair(lock, cv);
    _wal_cv_map.emplace(wal_id, pair);
    LOG(INFO) << "add  " << wal_id << " to _wal_cv_map";
    return Status::OK();
}

Status WalManager::erase_wal_cv_map(int64_t wal_id) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_cv_lock);
    if (_wal_cv_map.erase(wal_id)) {
        LOG(INFO) << "erase " << wal_id << " from _wal_cv_map";
    } else {
        return Status::InternalError("fail to erase wal {} from wal_cv_map", wal_id);
    }
    return Status::OK();
}

Status WalManager::wait_replay_wal_finish(int64_t wal_id) {
    std::shared_ptr<std::mutex> lock = nullptr;
    std::shared_ptr<std::condition_variable> cv = nullptr;
    auto st = get_lock_and_cv(wal_id, lock, cv);
    if (st.ok()) {
        std::unique_lock l(*(lock));
        LOG(INFO) << "start wait " << wal_id;
        if (cv->wait_for(l, std::chrono::seconds(180)) == std::cv_status::timeout) {
            LOG(WARNING) << "wait for " << wal_id << " is time out";
        }
        LOG(INFO) << "get wal " << wal_id << ",finish wait";
        RETURN_IF_ERROR(erase_wal_cv_map(wal_id));
        LOG(INFO) << "erase wal " << wal_id;
    }
    return Status::OK();
}

Status WalManager::notify_relay_wal(int64_t wal_id) {
    std::shared_ptr<std::mutex> lock = nullptr;
    std::shared_ptr<std::condition_variable> cv = nullptr;
    auto st = get_lock_and_cv(wal_id, lock, cv);
    if (st.ok()) {
        std::unique_lock l(*(lock));
        cv->notify_all();
        LOG(INFO) << "get wal " << wal_id << ",notify all";
    }
    return Status::OK();
}

Status WalManager::get_lock_and_cv(int64_t wal_id, std::shared_ptr<std::mutex>& lock,
                                   std::shared_ptr<std::condition_variable>& cv) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_cv_lock);
    auto it = _wal_cv_map.find(wal_id);
    if (it == _wal_cv_map.end()) {
        return Status::InternalError("cannot find txn {} in _wal_cv_map", wal_id);
    }
    lock = it->second.first;
    cv = it->second.second;
    return Status::OK();
}

Status WalManager::delete_wal(int64_t table_id, int64_t wal_id) {
    std::string wal_path;
    {
        std::lock_guard<std::shared_mutex> wrlock(_wal_path_lock);
        auto it = _wal_path_map.find(wal_id);
        if (it != _wal_path_map.end()) {
            wal_path = it->second;
            auto st = io::global_local_filesystem()->delete_file(wal_path);
            if (st.ok()) {
                LOG(INFO) << "delete wal=" << wal_path;
            } else {
                LOG(WARNING) << "failed to delete wal=" << wal_path << ", st=" << st.to_string();
            }
            _wal_path_map.erase(wal_id);
        }
    }
    erase_wal_queue(table_id, wal_id);
    return Status::OK();
}

Status WalManager::rename_to_tmp_path(const std::string wal, int64_t table_id, int64_t wal_id) {
    io::Path wal_path = wal;
    std::list<std::string> path_element;
    for (int i = 0; i < 3; ++i) {
        if (!wal_path.has_parent_path()) {
            return Status::InternalError("parent path is not enough when rename " + wal);
        }
        path_element.push_front(wal_path.filename().string());
        wal_path = wal_path.parent_path();
    }
    wal_path.append(_tmp);
    for (auto path : path_element) {
        wal_path.append(path);
    }
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(wal_path.parent_path(), &exists));
    if (!exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(wal_path.parent_path()));
    }
    auto res = std::rename(wal.c_str(), wal_path.string().c_str());
    if (res < 0) {
        LOG(INFO) << "failed to rename wal from " << wal << " to " << wal_path.string();
        return Status::InternalError("rename fail on path " + wal);
    }
    LOG(INFO) << "rename wal from " << wal << " to " << wal_path.string();
    {
        std::lock_guard<std::shared_mutex> wrlock(_wal_path_lock);
        auto it = _wal_path_map.find(wal_id);
        if (it != _wal_path_map.end()) {
            _wal_path_map.erase(wal_id);
        } else {
            LOG(WARNING) << "can't find " << wal_id << " in _wal_path_map when trying to rename";
        }
    }
    erase_wal_queue(table_id, wal_id);
    return Status::OK();
}

} // namespace doris
