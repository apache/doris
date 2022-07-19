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

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/auto_batch_load_table.h"
#include "util/thread.h"

namespace doris {

class ExecEnv;

// This class used to manage all infos for "auto_batch_load" enabled tables
class AutoBatchLoadMgr {
public:
    AutoBatchLoadMgr(ExecEnv* exec_env);
    virtual ~AutoBatchLoadMgr();

    Status auto_batch_load(const PAutoBatchLoadRequest* request, std::string& label,
                           int64_t& txn_id);
    void recovery_wals();

private:
    void _commit_auto_batch_load();
    void _commit_auto_batch_load_table(int64_t table_id);

    ExecEnv* _exec_env;

    mutable std::mutex _lock;
    std::map<int64_t, std::shared_ptr<AutoBatchLoadTable>> _table_map;

    // True if recovering WALs, can not receive auto_batch_load request(TODO)
    bool _wal_recovering;

    // Thread to check if tables reach commit conditions and do commit
    scoped_refptr<Thread> _commit_thread;
    CountDownLatch _stop_background_threads_latch;
};

} // namespace doris
