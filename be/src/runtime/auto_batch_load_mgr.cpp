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

#include "runtime/auto_batch_load_mgr.h"

#include "gutil/strings/split.h"
#include "gutil/strtoint.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/file_utils.h"

namespace doris {

class RuntimeProfile;
class FragmentMgr;

AutoBatchLoadMgr::AutoBatchLoadMgr(ExecEnv* exec_env)
        : _exec_env(exec_env), _wal_recovering(true), _stop_background_threads_latch(1) {
    // start a thread to scan WAL directory and do recovery
    std::thread(&AutoBatchLoadMgr::recovery_wals, this).detach();
    // start a thread to check if auto commit conditions are satisfied and do commit
    auto st = Thread::create(
            "AutoBatchLoadMgr", "commit_auto_batch_load",
            [this]() { this->_commit_auto_batch_load(); }, &_commit_thread);
    CHECK(st.ok()) << st.to_string();
}

AutoBatchLoadMgr::~AutoBatchLoadMgr() {
    _stop_background_threads_latch.count_down();
    if (_commit_thread) {
        _commit_thread->join();
    }
}

Status AutoBatchLoadMgr::auto_batch_load(const PAutoBatchLoadRequest* request, std::string& label,
                                         int64_t& txn_id) {
    if (_wal_recovering) {
        return Status::InternalError("recovering WALs");
    }
    auto db_id = request->db_id();
    auto table_id = request->table_id();
    if (!_table_map.count(table_id)) {
        std::lock_guard<std::mutex> lock(_lock);
        if (!_table_map.count(table_id)) {
            _table_map.emplace(table_id,
                               std::make_shared<AutoBatchLoadTable>(_exec_env, db_id, table_id));
        }
    }
    auto it = _table_map.find(table_id);
    return it->second->auto_batch_load(request, label, txn_id);
}

// When starting be, scan auto_batch_load directory and recovery WALs
void AutoBatchLoadMgr::recovery_wals() {
    // port == 0 means not received heartbeat yet
    while (_exec_env->master_info()->network_address.port == 0) {
        sleep(5);
        continue;
    }
    std::string path = _exec_env->storage_engine()->auto_batch_load_dir();
    std::vector<string> db_ids;
    // TODO should we abort if list directory failed or recovery wal failed
    Status st = FileUtils::list_files(Env::Default(), path, &db_ids);
    // map of {table_id, {wal_id, wal_path}}
    std::map<int64_t, std::map<int64_t, std::string>> table_wal_map;
    for (const auto& db_id_str : db_ids) {
        int64_t db_id = atoi64(db_id_str);
        std::vector<string> table_ids;
        auto db_path = path + "/" + db_id_str;
        st = FileUtils::list_files(Env::Default(), db_path, &table_ids);
        for (const auto& table_id_str : table_ids) {
            int64_t table_id = atoi64(table_id_str);
            std::vector<string> wals;
            auto table_path = db_path + "/" + table_id_str;
            st = FileUtils::list_files(Env::Default(), table_path, &wals);
            for (const auto& wal : wals) {
                int64_t wal_id = atoi64(wal);
                auto wal_path = table_path + "/" + wal;
                if (!_table_map.count(table_id)) {
                    std::lock_guard<std::mutex> lock(_lock);
                    if (!_table_map.count(table_id)) {
                        _table_map.emplace(table_id, std::make_shared<AutoBatchLoadTable>(
                                                             _exec_env, db_id, table_id));
                    }
                }
                table_wal_map[table_id][wal_id] = wal_path;
            }
        }
    }
    for (const auto& [table_id, wals] : table_wal_map) {
        for (const auto& [wal_id, wal_path] : wals) {
            auto it = _table_map.find(table_id);
            LOG(INFO) << "Start recovery wal: " << wal_path;
            st = it->second->recovery_wal(wal_id, wal_path);
            if (st.ok()) {
                LOG(INFO) << "Finish recovery wal: " << wal_path;
            } else {
                LOG(WARNING) << "Failed recovery wal: " << wal_path
                             << ", error: " << st.to_string();
            }
            it->second->set_wal_id(wal_id);
        }
    }
    LOG(INFO) << "Finish recovery all WALs";
    _wal_recovering = false;
}

// Check if tables meet commit conditions and do commit
void AutoBatchLoadMgr::_commit_auto_batch_load() {
    do {
        std::vector<int64_t> need_commit_table;
        {
            std::lock_guard<std::mutex> lock(_lock);
            for (auto& it : _table_map) {
                if (it.second->need_commit()) {
                    need_commit_table.push_back(it.first);
                }
            }
        }
        // NOTE: To improve performance, maybe submit to thread pool but
        // need to make sure one table is committed by order
        for (auto& table_id : need_commit_table) {
            _commit_auto_batch_load_table(table_id);
        }
    } while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::auto_batch_load_commit_interval_seconds)));
    LOG(INFO) << "AutoBatchLoadMgr commit worker is going to exit.";
}

void AutoBatchLoadMgr::_commit_auto_batch_load_table(int64_t table_id) {
    auto it = _table_map.find(table_id);
    if (it != _table_map.end()) {
        int64_t wal_id;
        std::string wal_path;
        Status st = it->second->commit(wal_id, wal_path);
        if (!st.ok() && !st.is_cancelled()) {
            // TODO handle commit failed
        }
    }
}

} // namespace doris
