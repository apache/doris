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

#include "olap/wal_manager.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <chrono>
#include <filesystem>

#include "io/fs/local_file_system.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/path_util.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/format/wal/wal_reader.h"

namespace doris {
WalManager::WalManager(ExecEnv* exec_env, const std::string& wal_dir_list)
        : _exec_env(exec_env), _stop_background_threads_latch(1) {
    doris::vectorized::WalReader::string_split(wal_dir_list, ",", _wal_dirs);
}

WalManager::~WalManager() {
    LOG(INFO) << "WalManager is destoried";
}
void WalManager::stop() {
    _stop = true;
    _stop_background_threads_latch.count_down();
    if (_replay_thread) {
        _replay_thread->join();
    }
    LOG(INFO) << "WalManager is stopped";
}

Status WalManager::init() {
    bool exists = false;
    for (auto wal_dir : _wal_dirs) {
        std::string tmp_dir = wal_dir + "/tmp";
        LOG(INFO) << "wal_dir:" << wal_dir << ",tmp_dir:" << tmp_dir;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(wal_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(wal_dir));
        }
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(tmp_dir, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(tmp_dir));
        }
        RETURN_IF_ERROR(scan_wals(wal_dir));
    }
    return Thread::create(
            "WalMgr", "replay_wal", [this]() { static_cast<void>(this->replay()); },
            &_replay_thread);
}

Status WalManager::add_wal_path(int64_t db_id, int64_t table_id, int64_t wal_id,
                                const std::string& label) {
    std::string base_path =
            _wal_dirs.size() == 1 ? _wal_dirs[0] : _wal_dirs[rand() % _wal_dirs.size()];
    std::stringstream ss;
    ss << base_path << "/" << std::to_string(db_id) << "/" << std::to_string(table_id) << "/"
       << std::to_string(wal_id) << "_" << label;
    {
        std::lock_guard<std::shared_mutex> wrlock(_wal_lock);
        _wal_path_map.emplace(wal_id, ss.str());
    }
    return Status::OK();
}

Status WalManager::get_wal_path(int64_t wal_id, std::string& wal_path) {
    std::shared_lock rdlock(_wal_lock);
    auto it = _wal_path_map.find(wal_id);
    if (it != _wal_path_map.end()) {
        wal_path = _wal_path_map[wal_id];
    } else {
        return Status::InternalError("can not find wal_id {} in wal_path_map", wal_id);
    }
    return Status::OK();
}

Status WalManager::create_wal_reader(const std::string& wal_path,
                                     std::shared_ptr<WalReader>& wal_reader) {
    wal_reader = std::make_shared<WalReader>(wal_path);
    RETURN_IF_ERROR(wal_reader->init());
    return Status::OK();
}

Status WalManager::create_wal_writer(int64_t wal_id, std::shared_ptr<WalWriter>& wal_writer) {
    std::string wal_path;
    RETURN_IF_ERROR(get_wal_path(wal_id, wal_path));
    std::vector<std::string> path_element;
    doris::vectorized::WalReader::string_split(wal_path, "/", path_element);
    std::stringstream ss;
    for (int i = 0; i < path_element.size() - 1; i++) {
        ss << path_element[i] << "/";
    }
    std::string base_path = ss.str();
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(base_path, &exists));
    if (!exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(base_path));
    }
    LOG(INFO) << "create wal " << wal_path;
    wal_writer = std::make_shared<WalWriter>(wal_path);
    RETURN_IF_ERROR(wal_writer->init());
    return Status::OK();
}

Status WalManager::scan_wals(const std::string& wal_path) {
    size_t count = 0;
    bool exists = true;
    std::vector<io::FileInfo> dbs;
    Status st = io::global_local_filesystem()->list(wal_path, false, &dbs, &exists);
    if (!st.ok()) {
        LOG(WARNING) << "Failed list files for dir=" << wal_path << ", st=" << st.to_string();
        return st;
    }
    for (const auto& db_id : dbs) {
        if (db_id.is_file) {
            continue;
        }
        std::vector<io::FileInfo> tables;
        auto db_path = wal_path + "/" + db_id.file_name;
        st = io::global_local_filesystem()->list(db_path, false, &tables, &exists);
        if (!st.ok()) {
            LOG(WARNING) << "Failed list files for dir=" << db_path << ", st=" << st.to_string();
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
                LOG(WARNING) << "Failed list files for dir=" << table_path
                             << ", st=" << st.to_string();
                return st;
            }
            if (wals.size() == 0) {
                continue;
            }
            std::vector<std::string> res;
            for (const auto& wal : wals) {
                auto wal_file = table_path + "/" + wal.file_name;
                res.emplace_back(wal_file);
                {
                    std::lock_guard<std::shared_mutex> wrlock(_wal_lock);
                    int64_t wal_id = std::strtoll(wal.file_name.c_str(), NULL, 10);
                    _wal_path_map.emplace(wal_id, wal_file);
                }
            }
            st = add_recover_wal(db_id.file_name, table_id.file_name, res);
            count += res.size();
            if (!st.ok()) {
                LOG(WARNING) << "Failed add replay wal, db=" << db_id.file_name
                             << ", table=" << table_id.file_name << ", st=" << st.to_string();
                return st;
            }
        }
    }
    LOG(INFO) << "Finish list all wals, size:" << count;
    return Status::OK();
}

Status WalManager::replay() {
    do {
        if (_stop || _exec_env->master_info() == nullptr) {
            break;
        }
        // port == 0 means not received heartbeat yet
        if (_exec_env->master_info()->network_address.port == 0) {
            continue;
        }
        std::vector<std::string> replay_tables;
        {
            std::lock_guard<std::shared_mutex> wrlock(_lock);
            auto it = _table_map.begin();
            while (it != _table_map.end()) {
                if (it->second->size() == 0) {
                    it = _table_map.erase(it);
                } else {
                    replay_tables.push_back(it->first);
                    it++;
                }
            }
        }
        for (const auto& table_id : replay_tables) {
            auto st = _table_map[table_id]->replay_wals();
            if (!st.ok()) {
                LOG(WARNING) << "Failed add replay wal on table " << table_id;
            }
        }
    } while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::group_commit_replay_wal_retry_interval_seconds)));
    return Status::OK();
}

Status WalManager::add_recover_wal(const std::string& db_id, const std::string& table_id,
                                   std::vector<std::string> wals) {
    std::lock_guard<std::shared_mutex> wrlock(_lock);
    std::shared_ptr<WalTable> table_ptr;
    auto it = _table_map.find(table_id);
    if (it == _table_map.end()) {
        table_ptr = std::make_shared<WalTable>(_exec_env, std::stoll(db_id), std::stoll(table_id));
        _table_map.emplace(table_id, table_ptr);
    } else {
        table_ptr = it->second;
    }
    table_ptr->add_wals(wals);
    return Status::OK();
}

size_t WalManager::get_wal_table_size(const std::string& table_id) {
    std::shared_lock rdlock(_lock);
    auto it = _table_map.find(table_id);
    if (it != _table_map.end()) {
        return it->second->size();
    } else {
        return 0;
    }
}

Status WalManager::delete_wal(int64_t wal_id) {
    {
        std::lock_guard<std::shared_mutex> wrlock(_wal_lock);
        std::string wal_path = _wal_path_map[wal_id];
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(wal_path));
        LOG(INFO) << "delete file=" << wal_path;
        _wal_path_map.erase(wal_id);
    }
    return Status::OK();
}

} // namespace doris