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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <utility>

#include "io/fs/local_file_system.h"
#include "olap/wal_writer.h"
#include "runtime/client_cache.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/path_util.h"
#include "util/thrift_rpc_helper.h"
#include "vec/exec/format/wal/wal_reader.h"

namespace doris {
WalManager::WalManager(ExecEnv* exec_env, const std::string& wal_dir_list)
        : _exec_env(exec_env), _stop_background_threads_latch(1), _stop(false) {
    doris::vectorized::WalReader::string_split(wal_dir_list, ",", _wal_dirs);
    _all_wal_disk_bytes = std::make_shared<std::atomic_size_t>(0);
    _cv = std::make_shared<std::condition_variable>();
}

WalManager::~WalManager() {
    LOG(INFO) << "WalManager is destoried";
}

void WalManager::stop() {
    if (!this->_stop.load()) {
        this->_stop.store(true);
        stop_relay_wal();
        _stop_background_threads_latch.count_down();
        if (_replay_thread) {
            _replay_thread->join();
        }
        LOG(INFO) << "WalManager is stopped";
    }
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

void WalManager::add_wal_status_queue(int64_t table_id, int64_t wal_id, WAL_STATUS wal_status) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_status_lock);
    LOG(INFO) << "add wal queue "
              << ",table_id:" << table_id << ",wal_id:" << wal_id << ",status:" << wal_status;
    auto it = _wal_status_queues.find(table_id);
    if (it == _wal_status_queues.end()) {
        std::unordered_map<int64_t, WAL_STATUS> tmp;
        tmp.emplace(wal_id, wal_status);
        _wal_status_queues.emplace(table_id, tmp);
    } else {
        it->second.emplace(wal_id, wal_status);
    }
}

Status WalManager::erase_wal_status_queue(int64_t table_id, int64_t wal_id) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_status_lock);
    auto it = _wal_status_queues.find(table_id);
    LOG(INFO) << "remove wal queue "
              << ",table_id:" << table_id << ",wal_id:" << wal_id;
    if (it == _wal_status_queues.end()) {
        return Status::InternalError("table_id " + std::to_string(table_id) +
                                     " not found in wal status queue");
    } else {
        it->second.erase(wal_id);
        if (it->second.empty()) {
            _wal_status_queues.erase(table_id);
        }
    }
    return Status::OK();
}

Status WalManager::get_wal_status_queue_size(const PGetWalQueueSizeRequest* request,
                                             PGetWalQueueSizeResponse* response) {
    std::lock_guard<std::shared_mutex> wrlock(_wal_status_lock);
    size_t count = 0;
    auto table_id = request->table_id();
    auto txn_id = request->txn_id();
    if (table_id > 0 && txn_id > 0) {
        auto it = _wal_status_queues.find(table_id);
        if (it == _wal_status_queues.end()) {
            LOG(INFO) << ("table_id " + std::to_string(table_id) +
                          " not found in wal status queue");
        } else {
            for (auto wal_it = it->second.begin(); wal_it != it->second.end(); ++wal_it) {
                if (wal_it->first <= txn_id) {
                    count += 1;
                }
            }
        }
    } else {
        for (auto it = _wal_status_queues.begin(); it != _wal_status_queues.end(); it++) {
            count += it->second.size();
        }
    }
    response->set_size(count);
    if (count > 0) {
        print_wal_status_queue();
    }
    return Status::OK();
}

void WalManager::print_wal_status_queue() {
    std::stringstream ss;
    for (auto it = _wal_status_queues.begin(); it != _wal_status_queues.end(); ++it) {
        ss << "table_id:" << it->first << std::endl;
        for (auto wal_it = it->second.begin(); wal_it != it->second.end(); ++wal_it) {
            ss << "wal_id:" << wal_it->first << ",status:" << wal_it->second << std::endl;
        }
    }
    LOG(INFO) << ss.str();
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
    wal_writer = std::make_shared<WalWriter>(wal_path, _all_wal_disk_bytes, _cv);
    RETURN_IF_ERROR(wal_writer->init());
    {
        std::lock_guard<std::shared_mutex> wrlock(_wal_lock);
        _wal_id_to_writer_map.emplace(wal_id, wal_writer);
    }
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
                    auto pos = wal.file_name.find("_");
                    try {
                        int64_t wal_id =
                                std::strtoll(wal.file_name.substr(0, pos).c_str(), NULL, 10);
                        _wal_path_map.emplace(wal_id, wal_file);
                        int64_t tb_id = std::strtoll(table_id.file_name.c_str(), NULL, 10);
                        add_wal_status_queue(tb_id, wal_id, WalManager::WAL_STATUS::REPLAY);
                    } catch (const std::invalid_argument& e) {
                        return Status::InvalidArgument("Invalid format, {}", e.what());
                    }
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
        if (_stop.load()) {
            break;
        }
        // port == 0 means not received heartbeat yet
        if (_exec_env->master_info() != nullptr &&
            _exec_env->master_info()->network_address.port == 0) {
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
        if (_wal_id_to_writer_map.find(wal_id) != _wal_id_to_writer_map.end()) {
            _all_wal_disk_bytes->fetch_sub(_wal_id_to_writer_map[wal_id]->disk_bytes(),
                                           std::memory_order_relaxed);
            _cv->notify_one();
            std::string wal_path = _wal_path_map[wal_id];
            LOG(INFO) << "wal delete file=" << wal_path << ", this file disk usage is "
                      << _wal_id_to_writer_map[wal_id]->disk_bytes()
                      << " ,after deleting it, all wals disk usage is "
                      << _all_wal_disk_bytes->load(std::memory_order_relaxed);
            _wal_id_to_writer_map.erase(wal_id);
        }
        if (_wal_id_to_writer_map.empty()) {
            CHECK_EQ(_all_wal_disk_bytes->load(std::memory_order_relaxed), 0);
        }
        std::string wal_path = _wal_path_map[wal_id];
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(wal_path));
        LOG(INFO) << "delete file=" << wal_path;
        _wal_path_map.erase(wal_id);
    }
    return Status::OK();
}

bool WalManager::is_running() {
    return !_stop.load();
}

void WalManager::stop_relay_wal() {
    std::lock_guard<std::shared_mutex> wrlock(_lock);
    for (auto it = _table_map.begin(); it != _table_map.end(); it++) {
        it->second->stop();
    }
}

void WalManager::add_wal_column_index(int64_t wal_id, std::vector<size_t>& column_index) {
    _wal_column_id_map.emplace(wal_id, column_index);
}

void WalManager::erase_wal_column_index(int64_t wal_id) {
    if (_wal_column_id_map.erase(wal_id)) {
        LOG(INFO) << "erase " << wal_id << " from wal_column_id_map";
    } else {
        LOG(WARNING) << "fail to erase wal " << wal_id << " from wal_column_id_map";
    }
}

Status WalManager::get_wal_column_index(int64_t wal_id, std::vector<size_t>& column_index) {
    auto it = _wal_column_id_map.find(wal_id);
    if (it != _wal_column_id_map.end()) {
        column_index = it->second;
    } else {
        return Status::InternalError("cannot find wal {} in wal_column_id_map", wal_id);
    }
    return Status::OK();
}

} // namespace doris