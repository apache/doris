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
    };

    Status add_block(std::shared_ptr<vectorized::FutureBlock> block);
    Status get_block(vectorized::Block* block, bool* find_block, bool* eos);
    Status add_load_id(const UniqueId& load_id);
    void remove_load_id(const UniqueId& load_id);
    void cancel(const Status& st);
    // memory consumption of one load block queue, used for back pressure.
    size_t bytes();

    UniqueId load_instance_id;
    std::string label;
    int64_t txn_id;
    int64_t schema_version;
    bool need_commit = false;

private:
    std::chrono::steady_clock::time_point _start_time;

    std::shared_ptr<doris::Mutex> _mutex;
    doris::ConditionVariable _put_cond;
    doris::ConditionVariable _get_cond;
    // the set of load ids of all blocks in this queue
    std::set<UniqueId> _load_ids;
    std::list<std::shared_ptr<vectorized::FutureBlock>> _block_queue;

    Status _status = Status::OK();
};

class GroupCommitTable {
public:
    GroupCommitTable(ExecEnv* exec_env, doris::ThreadPool* thread_pool, int64_t db_id,
                     int64_t table_id)
            : _exec_env(exec_env), _thread_pool(thread_pool), _db_id(db_id), _table_id(table_id) {};
    Status get_first_block_load_queue(int64_t table_id,
                                      std::shared_ptr<vectorized::FutureBlock> block,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue);
    Status get_load_block_queue(const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue);
    // memory consumption of load block queues, used for back pressure.
    size_t load_block_queues_bytes();

private:
    Status _create_group_commit_load(std::shared_ptr<LoadBlockQueue>& load_block_queue);
    Status _exec_plan_fragment(int64_t db_id, int64_t table_id, const std::string& label,
                               int64_t txn_id, bool is_pipeline,
                               const TExecPlanFragmentParams& params,
                               const TPipelineFragmentParams& pipeline_params);
    Status _finish_group_commit_load(int64_t db_id, int64_t table_id, const std::string& label,
                                     int64_t txn_id, const TUniqueId& instance_id, Status& status,
                                     bool prepare_failed, RuntimeState* state);

    ExecEnv* _exec_env;
    ThreadPool* _thread_pool;
    int64_t _db_id;
    int64_t _table_id;
    doris::Mutex _lock;
    doris::ConditionVariable _cv;
    // fragment_instance_id to load_block_queue
    std::unordered_map<UniqueId, std::shared_ptr<LoadBlockQueue>> _load_block_queues;
    bool _need_plan_fragment = false;
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

    // used when init group_commit_scan_node
    Status get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue);
    Status get_first_block_load_queue(int64_t db_id, int64_t table_id,
                                      std::shared_ptr<vectorized::FutureBlock> block,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue);
    // memory consumption of all tables' load block queues, used for back pressure.
    size_t all_block_queues_bytes();

private:
    // used by insert into
    Status _append_row(std::shared_ptr<io::StreamLoadPipe> pipe,
                       const PGroupCommitInsertRequest* request);

    ExecEnv* _exec_env;

    doris::Mutex _lock;
    // TODO remove table when unused
    std::unordered_map<int64_t, std::shared_ptr<GroupCommitTable>> _table_map;

    // thread pool to handle insert into: append data to pipe
    std::unique_ptr<doris::ThreadPool> _insert_into_thread_pool;
    std::unique_ptr<doris::ThreadPool> _thread_pool;
};

} // namespace doris