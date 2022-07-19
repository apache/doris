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

#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gen_cpp/DorisExternalService_types.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "olap/wal_writer.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "util/metrics.h"
#include "util/thread.h"

namespace doris {

class ExecEnv;
class TUniqueId;

// This class used to manage all infos for "auto_batch_load" enabled table
class AutoBatchLoadTable {
public:
    AutoBatchLoadTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id);
    virtual ~AutoBatchLoadTable();

    Status auto_batch_load(const PAutoBatchLoadRequest* request, std::string& label,
                           int64_t& txn_id);
    bool need_commit();
    Status commit(int64_t& wal_id, std::string& wal_path);

    void set_wal_id(int64_t wal_id) {
        if (wal_id > _wal_id) _wal_id = wal_id;
    }
    Status recovery_wal(const int64_t& wal_id, const std::string& wal_path);

private:
    int64_t _next_wal_id();
    std::string _wal_path(int64_t wal_id);

    // Request fe start a txn with 'label'(in the format 'auto_batch_load_{table_id}_{wal_id}'),
    // fe return the 'fragment_instance_id' and 'txn_id' for this txn.
    Status _begin_auto_batch_load(const int64_t& wal_id, std::string& label,
                                  TUniqueId& fragment_instance_id, int64_t& txn_id);
    // Request fe abort txn with 'label'(in the format 'auto_batch_load_{table_id}_{wal_id}')
    // with 'reason'
    Status _abort_txn(std::string& label, std::string& reason);

    bool _need_commit();
    Status _commit_auto_batch_load(std::shared_ptr<StreamLoadPipe> pipe, std::string& label,
                                   int64_t& txn_id, std::shared_ptr<WalWriter> wal_writer);
    Status _wait_txn_success(std::string& label, int64_t txn_id);

    ExecEnv* _exec_env;
    int64_t _db_id;
    int64_t _table_id;
    std::string _table_load_dir;

    int64_t _wal_id;

    std::mutex _lock;
    // true means the txn is already began
    bool _begin = false;
    std::string _label;
    TUniqueId _fragment_instance_id;
    int64_t _txn_id;

    std::shared_ptr<WalWriter> _wal_writer;

    const int64_t AUTO_LOAD_BATCH_SIZE_BYTES = config::auto_batch_load_size_mbytes * 1024 * 1024;
};

} // namespace doris
