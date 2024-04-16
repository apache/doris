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

#pragma once

#include <gen_cpp/PaloInternalService_types.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include "common/config.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gutil/ref_counted.h"
#include "olap/wal/wal_dirs_info.h"
#include "olap/wal/wal_reader.h"
#include "olap/wal/wal_table.h"
#include "olap/wal/wal_writer.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/thread.h"
#include "util/threadpool.h"

namespace doris {
class WalManager {
    ENABLE_FACTORY_CREATOR(WalManager);
    struct ScanWalInfo {
        std::string wal_path;
        int64_t db_id;
        int64_t tb_id;
        int64_t wal_id;
        int64_t be_id;
    };

public:
    WalManager(ExecEnv* exec_env, const std::string& wal_dir);
    ~WalManager();
    Status init();
    bool is_running();
    void stop();

    // wal back pressure
    Status update_wal_dir_limit(const std::string& wal_dir, size_t limit = -1);
    Status update_wal_dir_used(const std::string& wal_dir, size_t used = -1);
    Status update_wal_dir_estimated_wal_bytes(const std::string& wal_dir,
                                              size_t increase_estimated_wal_bytes,
                                              size_t decrease_estimated_wal_bytes);
    Status get_wal_dir_available_size(const std::string& wal_dir, size_t* available_bytes);
    size_t get_max_available_size();
    std::string get_wal_dirs_info_string();

    // replay wal
    Status create_wal_path(int64_t db_id, int64_t table_id, int64_t wal_id,
                           const std::string& label, std::string& base_path, uint32_t wal_version);
    Status get_wal_path(int64_t wal_id, std::string& wal_path);
    Status delete_wal(int64_t table_id, int64_t wal_id);
    Status rename_to_tmp_path(const std::string wal, int64_t table_id, int64_t wal_id);
    Status add_recover_wal(int64_t db_id, int64_t table_id, int64_t wal_id, std::string wal);
    void add_wal_queue(int64_t table_id, int64_t wal_id);
    void erase_wal_queue(int64_t table_id, int64_t wal_id);
    size_t get_wal_queue_size(int64_t table_id);
    // filename format:a_b_c_group_commit_xxx
    // a:version
    // b:be id
    // c:wal id
    // group_commit_xxx:label
    static Status parse_wal_path(const std::string& file_name, int64_t& version,
                                 int64_t& backend_id, int64_t& wal_id, std::string& label);
    // fot ut
    size_t get_wal_table_size(int64_t table_id);

    //for test relay
    Status add_wal_cv_map(int64_t wal_id, std::shared_ptr<std::mutex> lock,
                          std::shared_ptr<std::condition_variable> cv);
    Status erase_wal_cv_map(int64_t wal_id);
    Status get_lock_and_cv(int64_t wal_id, std::shared_ptr<std::mutex>& lock,
                           std::shared_ptr<std::condition_variable>& cv);
    Status wait_replay_wal_finish(int64_t wal_id);
    Status notify_relay_wal(int64_t wal_id);
    static std::string get_base_wal_path(const std::string& wal_path_str);

private:
    // wal back pressure
    Status _init_wal_dirs_conf();
    Status _init_wal_dirs();
    Status _init_wal_dirs_info();
    Status _update_wal_dir_info_thread();

    // scan all wal files under storage path
    Status _scan_wals(const std::string& wal_path, std::vector<ScanWalInfo>& res);
    // use a background thread to do replay task
    Status _replay_background();
    // load residual wals
    Status _load_wals();
    void _stop_relay_wal();

public:
    // used for be ut
    size_t wal_limit_test_bytes;

private:
    ExecEnv* _exec_env = nullptr;
    std::atomic<bool> _stop;
    CountDownLatch _stop_background_threads_latch;
    const std::string _tmp = "tmp";

    // wal back pressure
    std::vector<std::string> _wal_dirs;
    scoped_refptr<Thread> _update_wal_dirs_info_thread;
    std::unique_ptr<WalDirsInfo> _wal_dirs_info;

    // replay wal
    scoped_refptr<Thread> _replay_thread;
    std::unique_ptr<doris::ThreadPool> _thread_pool;

    std::shared_mutex _table_lock;
    std::map<int64_t, std::shared_ptr<WalTable>> _table_map;

    std::shared_mutex _wal_path_lock;
    std::unordered_map<int64_t, std::string> _wal_path_map;

    std::shared_mutex _wal_queue_lock;
    std::unordered_map<int64_t, std::set<int64_t>> _wal_queues;

    std::atomic<bool> _first_replay;

    // for test relay
    // <lock, condition_variable>
    using WalCvInfo =
            std::pair<std::shared_ptr<std::mutex>, std::shared_ptr<std::condition_variable>>;
    std::shared_mutex _wal_cv_lock;
    std::unordered_map<int64_t, WalCvInfo> _wal_cv_map;
};

// In doris 2.1.0, wal version is 0, now need to upgrade it to 1 to solve compatibility issues.
// see https://github.com/apache/doris/pull/32299
constexpr inline uint32_t WAL_VERSION = 1;
} // namespace doris
