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
#include <future>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

#include "common/status.h"
#include "olap/wal/wal_manager.h"
#include "runtime/exec_env.h"
#include "util/threadpool.h"
#include "vec/core/block.h"
#include "vec/sink/writer/vwal_writer.h"

namespace doris {
class ExecEnv;
class TUniqueId;
class RuntimeState;

namespace pipeline {
class Dependency;
}

struct BlockData {
    BlockData(const std::shared_ptr<vectorized::Block>& block)
            : block(block), block_bytes(block->bytes()) {};
    std::shared_ptr<vectorized::Block> block;
    size_t block_bytes;
};

class LoadBlockQueue {
public:
    LoadBlockQueue(const UniqueId& load_instance_id, std::string& label, int64_t txn_id,
                   int64_t schema_version,
                   std::shared_ptr<std::atomic_size_t> all_block_queues_bytes,
                   bool wait_internal_group_commit_finish, int64_t group_commit_interval_ms,
                   int64_t group_commit_data_bytes)
            : load_instance_id(load_instance_id),
              label(label),
              txn_id(txn_id),
              schema_version(schema_version),
              wait_internal_group_commit_finish(wait_internal_group_commit_finish),
              _group_commit_interval_ms(group_commit_interval_ms),
              _start_time(std::chrono::steady_clock::now()),
              _last_print_time(_start_time),
              _group_commit_data_bytes(group_commit_data_bytes),
              _all_block_queues_bytes(all_block_queues_bytes) {};

    Status add_block(RuntimeState* runtime_state, std::shared_ptr<vectorized::Block> block,
                     bool write_wal, UniqueId& load_id);
    Status get_block(RuntimeState* runtime_state, vectorized::Block* block, bool* find_block,
                     bool* eos, std::shared_ptr<pipeline::Dependency> get_block_dep);
    bool contain_load_id(const UniqueId& load_id);
    Status add_load_id(const UniqueId& load_id,
                       const std::shared_ptr<pipeline::Dependency> put_block_dep);
    Status remove_load_id(const UniqueId& load_id);
    void cancel(const Status& st);
    bool need_commit() { return _need_commit; }

    Status create_wal(int64_t db_id, int64_t tb_id, int64_t wal_id, const std::string& import_label,
                      WalManager* wal_manager, std::vector<TSlotDescriptor>& slot_desc,
                      int be_exe_version);
    Status close_wal();
    bool has_enough_wal_disk_space(size_t estimated_wal_bytes);
    void append_dependency(std::shared_ptr<pipeline::Dependency> finish_dep);
    void append_read_dependency(std::shared_ptr<pipeline::Dependency> read_dep);

    std::string debug_string() const {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(
                debug_string_buffer,
                "load_instance_id={}, label={}, txn_id={}, "
                "wait_internal_group_commit_finish={}, data_size_condition={}, "
                "group_commit_load_count={}, process_finish={}, _need_commit={}, schema_version={}",
                load_instance_id.to_string(), label, txn_id, wait_internal_group_commit_finish,
                data_size_condition, group_commit_load_count, process_finish.load(), _need_commit,
                schema_version);
        return fmt::to_string(debug_string_buffer);
    }

    UniqueId load_instance_id;
    std::string label;
    int64_t txn_id;
    int64_t schema_version;
    bool wait_internal_group_commit_finish = false;
    bool data_size_condition = false;

    // counts of load in one group commit
    std::atomic_size_t group_commit_load_count = 0;

    // the execute status of this internal group commit
    std::mutex mutex;
    std::atomic<bool> process_finish = false;
    Status status = Status::OK();
    std::vector<std::shared_ptr<pipeline::Dependency>> dependencies;

private:
    void _cancel_without_lock(const Status& st);

    // the set of load ids of all blocks in this queue
    std::map<UniqueId, std::shared_ptr<pipeline::Dependency>> _load_ids_to_write_dep;
    std::vector<std::shared_ptr<pipeline::Dependency>> _read_deps;
    std::list<BlockData> _block_queue;

    // wal
    std::string _wal_base_path;
    std::shared_ptr<vectorized::VWalWriter> _v_wal_writer;

    // commit
    bool _need_commit = false;
    // commit by time interval, can be changed by 'ALTER TABLE my_table SET ("group_commit_interval_ms"="1000");'
    int64_t _group_commit_interval_ms;
    std::chrono::steady_clock::time_point _start_time;
    std::chrono::steady_clock::time_point _last_print_time;
    // commit by data size
    int64_t _group_commit_data_bytes;
    int64_t _data_bytes = 0;

    // memory back pressure, memory consumption of all tables' load block queues
    std::shared_ptr<std::atomic_size_t> _all_block_queues_bytes;
    std::condition_variable _get_cond;
};

class GroupCommitTable {
public:
    GroupCommitTable(ExecEnv* exec_env, doris::ThreadPool* thread_pool, int64_t db_id,
                     int64_t table_id, std::shared_ptr<std::atomic_size_t> all_block_queue_bytes)
            : _exec_env(exec_env),
              _thread_pool(thread_pool),
              _all_block_queues_bytes(all_block_queue_bytes),
              _db_id(db_id),
              _table_id(table_id) {};
    Status get_first_block_load_queue(int64_t table_id, int64_t base_schema_version,
                                      const UniqueId& load_id,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                      int be_exe_version,
                                      std::shared_ptr<MemTrackerLimiter> mem_tracker,
                                      std::shared_ptr<pipeline::Dependency> create_plan_dep,
                                      std::shared_ptr<pipeline::Dependency> put_block_dep);
    Status get_load_block_queue(const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                std::shared_ptr<pipeline::Dependency> get_block_dep);
    void remove_load_id(const UniqueId& load_id);

private:
    Status _create_group_commit_load(int be_exe_version,
                                     std::shared_ptr<MemTrackerLimiter> mem_tracker);
    Status _exec_plan_fragment(int64_t db_id, int64_t table_id, const std::string& label,
                               int64_t txn_id, const TPipelineFragmentParams& pipeline_params);
    Status _finish_group_commit_load(int64_t db_id, int64_t table_id, const std::string& label,
                                     int64_t txn_id, const TUniqueId& instance_id, Status& status,
                                     RuntimeState* state);

    ExecEnv* _exec_env = nullptr;
    ThreadPool* _thread_pool = nullptr;
    // memory consumption of all tables' load block queues, used for memory back pressure.
    std::shared_ptr<std::atomic_size_t> _all_block_queues_bytes;

    int64_t _db_id;
    int64_t _table_id;

    std::mutex _lock;
    // fragment_instance_id to load_block_queue
    std::unordered_map<UniqueId, std::shared_ptr<LoadBlockQueue>> _load_block_queues;
    bool _is_creating_plan_fragment = false;
    // user_load_id -> <create_plan_dep, put_block_dep, base_schema_version>
    std::unordered_map<UniqueId, std::tuple<std::shared_ptr<pipeline::Dependency>,
                                            std::shared_ptr<pipeline::Dependency>, int64_t>>
            _create_plan_deps;
};

class GroupCommitMgr {
public:
    GroupCommitMgr(ExecEnv* exec_env);
    virtual ~GroupCommitMgr();

    void stop();

    // used when init group_commit_scan_node
    Status get_load_block_queue(int64_t table_id, const TUniqueId& instance_id,
                                std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                std::shared_ptr<pipeline::Dependency> get_block_dep);
    Status get_first_block_load_queue(int64_t db_id, int64_t table_id, int64_t base_schema_version,
                                      const UniqueId& load_id,
                                      std::shared_ptr<LoadBlockQueue>& load_block_queue,
                                      int be_exe_version,
                                      std::shared_ptr<MemTrackerLimiter> mem_tracker,
                                      std::shared_ptr<pipeline::Dependency> create_plan_dep,
                                      std::shared_ptr<pipeline::Dependency> put_block_dep);
    void remove_load_id(int64_t table_id, const UniqueId& load_id);
    std::promise<Status> debug_promise;
    std::future<Status> debug_future = debug_promise.get_future();

private:
    ExecEnv* _exec_env = nullptr;
    std::unique_ptr<doris::ThreadPool> _thread_pool;
    // memory consumption of all tables' load block queues, used for memory back pressure.
    std::shared_ptr<std::atomic_size_t> _all_block_queues_bytes;

    std::mutex _lock;
    // TODO remove table when unused
    std::unordered_map<int64_t, std::shared_ptr<GroupCommitTable>> _table_map;
};

} // namespace doris