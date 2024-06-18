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
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "http/action/http_stream.h"
#include "olap/wal/wal_info.h"
#include "runtime/exec_env.h"
#include "runtime/stream_load/stream_load_context.h"

namespace doris {
class WalTable {
public:
    WalTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id);
    ~WalTable();
    // used when be start and there are wals need to do recovery
    void add_wal(int64_t wal_id, std::string wal);
    Status replay_wals();
    size_t size();
    void stop();

private:
    void _pick_relay_wals();
    bool _need_replay(std::shared_ptr<WalInfo>);
    Status _relay_wal_one_by_one();

    Status _replay_wal_internal(const std::string& wal);
    Status _try_abort_txn(int64_t db_id, std::string& label);
    Status _get_column_info(int64_t db_id, int64_t tb_id,
                            std::map<int64_t, std::string>& column_info_map);

    Status _replay_one_wal_with_streamload(int64_t wal_id, const std::string& wal,
                                           const std::string& label);
    Status _handle_stream_load(int64_t wal_id, const std::string& wal, const std::string& label);
    Status _construct_sql_str(const std::string& wal, const std::string& label,
                              std::string& sql_str);
    Status _read_wal_header(const std::string& wal, std::string& columns);

private:
    ExecEnv* _exec_env;
    int64_t _db_id;
    int64_t _table_id;
    std::shared_ptr<HttpStreamAction> _http_stream_action;
    mutable std::mutex _replay_wal_lock;
    // key is wal_path
    std::map<std::string, std::shared_ptr<WalInfo>> _replay_wal_map;
    std::list<std::shared_ptr<WalInfo>> _replaying_queue;
};
} // namespace doris