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
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "olap/wal_manager.h"
#include "runtime/exec_env.h"
#include "util/threadpool.h"
#include "vec/core/block.h"
#include "vec/sink/writer/vwal_writer.h"

namespace doris {
class ExecEnv;
class TUniqueId;
class RuntimeState;

class LoadBlockQueue {
public:
    LoadBlockQueue(const UniqueId& load_instance_id, std::string& label, int64_t txn_id,
                   int64_t schema_version,
                   std::shared_ptr<std::atomic_size_t> all_block_queues_bytes,
                   bool wait_internal_group_commit_finish, int64_t group_commit_interval_ms)
            : load_instance_id(load_instance_id),
              label(label),
              txn_id(txn_id),
              schema_version(schema_version),
              wait_internal_group_commit_finish(wait_internal_group_commit_finish),
              _start_time(std::chrono::steady_clock::now()),
              _all_block_queues_bytes(all_block_queues_bytes),
              _group_commit_interval_ms(group_commit_interval_ms) {};

    Status add_block(std::shared_ptr<vectorized::Block> block, bool write_wal);
    Status get_block(RuntimeState* runtime_state, vectorized::Block* block, bool* find_block,
                     bool* eos);
    Status add_load_id(const UniqueId& load_id);
    void remove_load_id(const UniqueId& load_id);
    void cancel(const Status& st);
    Status create_wal(int64_t db_id, int64_t tb_id, int64_t wal_id, const std::string& import_label,
                      WalManager* wal_manager, std::vector<TSlotDescriptor>& slot_desc,
                      int be_exe_version);
    Status close_wal();
    bool has_enough_wal_disk_space(const std::vector<std::shared_ptr<vectorized::Block>>& blocks,
                                   const TUniqueId& load_id, bool is_blocks_contain_all_load_data);

    static constexpr size_t MAX_BLOCK_QUEUE_ADD_WAIT_TIME = 1000;
    UniqueId load_instance_id;
    std::string label;
    int64_t txn_id;
    int64_t schema_version;
    bool need_commit = false;
    bool wait_internal_group_commit_finish = false;
    std::mutex mutex;
    bool process_finish = false;
    std::condition_variable internal_group_commit_finish_cv;
    Status status = Status::OK();
    std::string wal_base_path;
    std::atomic_size_t block_queue_pre_allocated = 0;

private:
    void _cancel_without_lock(const Status& st);
    std::chrono::steady_clock::time_point _start_time;

    std::condition_variable _put_cond;
    std::condition_variable _get_cond;
    // the set of load ids of all blocks in this queue
    std::set<UniqueId> _load_ids;
    std::list<std::shared_ptr<vectorized::Block>> _block_queue;

    // memory consumption of all tables' load block queues, used for back pressure.
    std::shared_ptr<std::atomic_size_t> _all_block_queues_bytes;
    // group commit interval in ms, can be changed by 'ALTER TABLE my_table SET ("group_commit_interval_ms"="1000");'
    int64_t _group_commit_interval_ms;
    std::shared_ptr<vectorized::VWalWriter> _v_wal_writer;
};

class GroupCommitTable {
public:
    GroupCommitTable(ExecEnv* exec_env, doris::ThreadPool* thread_pool, int64_t db_id,
                     int64_t table_id, std::shared_ptr<std::atomic_size_t> all_block_queue_bytes)
            : _exec_env(exec_env),
              _thread_pool(thread_pool),
              _db_id(db_id),
              _table_id(table_id),
              _all_block_queues_bytes(all_block_queue_bytes) {};
    Status get_first_block_load_queue(int64_t table_id, int64_t base_schema_version,
                                      const UniqueId& load_id,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                      int be_exe_version);
    Status get_load_block_queue(const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue);

private:
    Status _create_group_commit_load(std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                     int be_exe_version);
    Status _exec_plan_fragment(int64_t db_id, int64_t table_id, const std::string& label,
                               int64_t txn_id, bool is_pipeline,
                               const TExecPlanFragmentParams& params,
                               const TPipelineFragmentParams& pipeline_params);
    Status _finish_group_commit_load(int64_t db_id, int64_t table_id, const std::string& label,
                                     int64_t txn_id, const TUniqueId& instance_id, Status& status,
                                     RuntimeState* state);

    ExecEnv* _exec_env = nullptr;
    ThreadPool* _thread_pool = nullptr;
    int64_t _db_id;
    int64_t _table_id;
    std::mutex _lock;
    std::condition_variable _cv;
    // fragment_instance_id to load_block_queue
    std::unordered_map<UniqueId, std::shared_ptr<LoadBlockQueue>> _load_block_queues;
    bool _need_plan_fragment = false;
    // memory consumption of all tables' load block queues, used for back pressure.
    std::shared_ptr<std::atomic_size_t> _all_block_queues_bytes;
};

class GroupCommitMgr {
public:
    GroupCommitMgr(ExecEnv* exec_env);
    virtual ~GroupCommitMgr();

    void stop();

    // used when init group_commit_scan_node
    Status get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue);
    Status get_first_block_load_queue(int64_t db_id, int64_t table_id, int64_t base_schema_version,
                                      const UniqueId& load_id,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                      int be_exe_version);
    Status update_load_info(TUniqueId load_id, size_t content_length);
    Status get_load_info(TUniqueId load_id, size_t* content_length);
    Status remove_load_info(TUniqueId load_id);

private:
    ExecEnv* _exec_env = nullptr;

    std::mutex _lock;
    // TODO remove table when unused
    std::unordered_map<int64_t, std::shared_ptr<GroupCommitTable>> _table_map;
    std::unique_ptr<doris::ThreadPool> _thread_pool;
    // memory consumption of all tables' load block queues, used for back pressure.
    std::shared_ptr<std::atomic_size_t> _all_block_queues_bytes;
    std::shared_mutex _load_info_lock;
    std::unordered_map<TUniqueId, size_t> _load_id_to_content_length_map;
};

} // namespace doris