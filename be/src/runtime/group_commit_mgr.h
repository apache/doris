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

#include "common/status.h"
#include "io/fs/stream_load_pipe.h"
#include "util/lock.h"
#include "util/threadpool.h"
#include "util/thrift_util.h"
#include "vec/core/block.h"
#include "vec/core/future_block.h"

namespace doris {
class ExecEnv;
class TPlan;
class TDescriptorTable;
class TUniqueId;
class TExecPlanFragmentParams;
class ObjectPool;
class RuntimeState;
class StreamLoadContext;
class StreamLoadPipe;

class LoadBlockQueue {
public:
    LoadBlockQueue(const UniqueId& load_instance_id, std::string& label, int64_t txn_id,
                   int64_t schema_version)
            : load_instance_id(load_instance_id),
              label(label),
              txn_id(txn_id),
              schema_version(schema_version),
              _start_time(std::chrono::steady_clock::now()) {
        _mutex = std::make_shared<doris::Mutex>();
        _cv = std::make_shared<doris::ConditionVariable>();
    };

    Status add_block(std::shared_ptr<vectorized::FutureBlock> block);
    Status get_block(vectorized::Block* block, bool* find_block, bool* eos);
    void remove_load_id(const UniqueId& load_id);
    void cancel(const Status& st);

    UniqueId load_instance_id;
    std::string label;
    int64_t txn_id;
    int64_t schema_version;
    bool need_commit = false;

private:
    std::chrono::steady_clock::time_point _start_time;

    std::shared_ptr<doris::Mutex> _mutex;
    std::shared_ptr<doris::ConditionVariable> _cv;
    // the set of load ids of all blocks in this queue
    std::set<UniqueId> _load_ids;
    std::list<std::shared_ptr<vectorized::FutureBlock>> _block_queue;

    Status _status = Status::OK();
};

class GroupCommitTable {
public:
    GroupCommitTable(ExecEnv* exec_env, int64_t db_id, int64_t table_id)
            : _exec_env(exec_env), _db_id(db_id), _table_id(table_id) {};
    Status get_first_block_load_queue(int64_t table_id,
                                      std::shared_ptr<vectorized::FutureBlock> block,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue);
    Status get_load_block_queue(const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue);

private:
    Status _create_group_commit_load(int64_t table_id,
                                     std::shared_ptr<LoadBlockQueue>& load_block_queue);
    Status _exec_plan_fragment(int64_t db_id, int64_t table_id, const std::string& label,
                               int64_t txn_id, bool is_pipeline,
                               const TExecPlanFragmentParams& params,
                               const TPipelineFragmentParams& pipeline_params);
    Status _finish_group_commit_load(int64_t db_id, int64_t table_id, const std::string& label,
                                     int64_t txn_id, const TUniqueId& instance_id, Status& status,
                                     bool prepare_failed, RuntimeState* state);

    ExecEnv* _exec_env;
    int64_t _db_id;
    int64_t _table_id;
    doris::Mutex _lock;
    // fragment_instance_id to load_block_queue
    std::unordered_map<UniqueId, std::shared_ptr<LoadBlockQueue>> _load_block_queues;

    doris::Mutex _request_fragment_mutex;
};

class GroupCommitMgr {
public:
    GroupCommitMgr(ExecEnv* exec_env);
    virtual ~GroupCommitMgr();

    void stop();

    // insert into
    Status group_commit_insert(int64_t table_id, const TPlan& plan,
                               const TDescriptorTable& desc_tbl,
                               const TScanRangeParams& scan_range_params,
                               const PGroupCommitInsertRequest* request,
                               PGroupCommitInsertResponse* response);

    // stream load
    Status group_commit_stream_load(std::shared_ptr<StreamLoadContext> ctx);

    // used when init group_commit_scan_node
    Status get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue);

private:
    // used by insert into
    Status _append_row(std::shared_ptr<io::StreamLoadPipe> pipe,
                       const PGroupCommitInsertRequest* request);
    // used by stream load
    Status _group_commit_stream_load(std::shared_ptr<StreamLoadContext> ctx);
    Status _get_first_block_load_queue(int64_t db_id, int64_t table_id,
                                       std::shared_ptr<vectorized::FutureBlock> block,
                                       std::shared_ptr<LoadBlockQueue>& load_block_queue);

    ExecEnv* _exec_env;

    doris::Mutex _lock;
    // TODO remove table when unused
    std::unordered_map<int64_t, std::shared_ptr<GroupCommitTable>> _table_map;

    // thread pool to handle insert into: append data to pipe
    std::unique_ptr<doris::ThreadPool> _insert_into_thread_pool;
};

} // namespace doris