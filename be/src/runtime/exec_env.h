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

#include <gen_cpp/HeartbeatService_types.h>
#include <stddef.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/options.h"
#include "util/threadpool.h"
#include "vec/common/hash_table/phmap_fwd_decl.h"

namespace doris {
struct FrontendInfo;
namespace vectorized {
class VDataStreamMgr;
class ScannerScheduler;
using ZoneList = std::unordered_map<std::string, cctz::time_zone>;
} // namespace vectorized
namespace pipeline {
class TaskScheduler;
}
namespace taskgroup {
class TaskGroupManager;
}
class BfdParser;
class BrokerMgr;
template <class T>
class BrpcClientCache;
class ExternalScanContextMgr;
class FragmentMgr;
class ResultCache;
class LoadPathMgr;
class NewLoadStreamMgr;
class MemTrackerLimiter;
class MemTracker;
class StorageEngine;
class ResultBufferMgr;
class ResultQueueMgr;
class TMasterInfo;
class LoadChannelMgr;
class LoadStreamMgr;
class StreamLoadExecutor;
class RoutineLoadTaskExecutor;
class SmallFileMgr;
class BlockSpillManager;
class BackendServiceClient;
class TPaloBrokerServiceClient;
class PBackendService_Stub;
class PFunctionService_Stub;
template <class T>
class ClientCache;
class HeartbeatFlags;
class FrontendServiceClient;
class FileMetaCache;
class GroupCommitMgr;

inline bool k_doris_exit = false;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Initial exec environment. must call this to init all
    static Status init(ExecEnv* env, const std::vector<StorePath>& store_paths);
    static void destroy(ExecEnv* exec_env);

    /// Returns the first created exec env instance. In a normal doris, this is
    /// the only instance. In test setups with multiple ExecEnv's per process,
    /// we return the most recently created instance.
    static ExecEnv* GetInstance() {
        static ExecEnv s_exec_env;
        return &s_exec_env;
    }

    // Empty destructor because the compiler-generated one requires full
    // declarations for classes in scoped_ptrs.
    ~ExecEnv();

    static bool ready() { return _s_ready.load(std::memory_order_acquire); }
    const std::string& token() const;
    ExternalScanContextMgr* external_scan_context_mgr() { return _external_scan_context_mgr; }
    doris::vectorized::VDataStreamMgr* vstream_mgr() { return _vstream_mgr; }
    ResultBufferMgr* result_mgr() { return _result_mgr; }
    ResultQueueMgr* result_queue_mgr() { return _result_queue_mgr; }
    ClientCache<BackendServiceClient>* client_cache() { return _backend_client_cache; }
    ClientCache<FrontendServiceClient>* frontend_client_cache() { return _frontend_client_cache; }
    ClientCache<TPaloBrokerServiceClient>* broker_client_cache() { return _broker_client_cache; }

    pipeline::TaskScheduler* pipeline_task_scheduler() { return _pipeline_task_scheduler; }
    pipeline::TaskScheduler* pipeline_task_group_scheduler() {
        return _pipeline_task_group_scheduler;
    }
    taskgroup::TaskGroupManager* task_group_manager() { return _task_group_manager; }

    // using template to simplify client cache management
    template <typename T>
    inline ClientCache<T>* get_client_cache() {
        return nullptr;
    }

    void init_mem_tracker();
    std::shared_ptr<MemTrackerLimiter> orphan_mem_tracker() { return _orphan_mem_tracker; }
    MemTrackerLimiter* orphan_mem_tracker_raw() { return _orphan_mem_tracker_raw; }
    MemTrackerLimiter* experimental_mem_tracker() { return _experimental_mem_tracker.get(); }
    std::shared_ptr<MemTracker> page_no_cache_mem_tracker() { return _page_no_cache_mem_tracker; }
    MemTracker* brpc_iobuf_block_memory_tracker() { return _brpc_iobuf_block_memory_tracker.get(); }

    ThreadPool* send_batch_thread_pool() { return _send_batch_thread_pool.get(); }
    ThreadPool* download_cache_thread_pool() { return _download_cache_thread_pool.get(); }
    ThreadPool* buffered_reader_prefetch_thread_pool() {
        return _buffered_reader_prefetch_thread_pool.get();
    }
    ThreadPool* send_report_thread_pool() { return _send_report_thread_pool.get(); }
    ThreadPool* join_node_thread_pool() { return _join_node_thread_pool.get(); }

    void set_serial_download_cache_thread_token() {
        _serial_download_cache_thread_token =
                download_cache_thread_pool()->new_token(ThreadPool::ExecutionMode::SERIAL, 1);
    }
    ThreadPoolToken* get_serial_download_cache_thread_token() {
        return _serial_download_cache_thread_token.get();
    }
    void init_download_cache_buf();
    void init_download_cache_required_components();
    Status init_pipeline_task_scheduler();
    char* get_download_cache_buf(ThreadPoolToken* token) {
        if (_download_cache_buf_map.find(token) == _download_cache_buf_map.end()) {
            return nullptr;
        }
        return _download_cache_buf_map[token].get();
    }
    FragmentMgr* fragment_mgr() { return _fragment_mgr; }
    ResultCache* result_cache() { return _result_cache; }
    TMasterInfo* master_info() { return _master_info; }
    LoadPathMgr* load_path_mgr() { return _load_path_mgr; }
    BfdParser* bfd_parser() const { return _bfd_parser; }
    BrokerMgr* broker_mgr() const { return _broker_mgr; }
    BrpcClientCache<PBackendService_Stub>* brpc_internal_client_cache() const {
        return _internal_client_cache;
    }
    BrpcClientCache<PFunctionService_Stub>* brpc_function_client_cache() const {
        return _function_client_cache;
    }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    std::shared_ptr<NewLoadStreamMgr> new_load_stream_mgr() { return _new_load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }
    BlockSpillManager* block_spill_mgr() { return _block_spill_mgr; }
    GroupCommitMgr* group_commit_mgr() { return _group_commit_mgr; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }

    std::shared_ptr<StreamLoadExecutor> stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }
    doris::vectorized::ScannerScheduler* scanner_scheduler() { return _scanner_scheduler; }
    FileMetaCache* file_meta_cache() { return _file_meta_cache; }
    MemTableMemoryLimiter* memtable_memory_limiter() { return _memtable_memory_limiter.get(); }
#ifdef BE_TEST
    void set_memtable_memory_limiter(MemTableMemoryLimiter* limiter) {
        _memtable_memory_limiter.reset(limiter);
    }
#endif
    vectorized::ZoneList& global_zone_cache() { return *_global_zone_cache; }
    std::shared_mutex& zone_cache_rw_lock() { return _zone_cache_rw_lock; }

    // only for unit test
    void set_master_info(TMasterInfo* master_info) { this->_master_info = master_info; }
    void set_new_load_stream_mgr(std::shared_ptr<NewLoadStreamMgr> new_load_stream_mgr) {
        this->_new_load_stream_mgr = new_load_stream_mgr;
    }
    void set_stream_load_executor(std::shared_ptr<StreamLoadExecutor> stream_load_executor) {
        this->_stream_load_executor = stream_load_executor;
    }

    void wait_for_all_tasks_done();

    void update_frontends(const std::vector<TFrontendInfo>& new_infos);
    std::map<TNetworkAddress, FrontendInfo> get_frontends();
    std::map<TNetworkAddress, FrontendInfo> get_running_frontends();

private:
    ExecEnv();

    Status _init(const std::vector<StorePath>& store_paths);
    void _destroy();

    Status _init_mem_env();

    void _register_metrics();
    void _deregister_metrics();

    inline static std::atomic_bool _s_ready {false};
    std::vector<StorePath> _store_paths;

    // Leave protected so that subclasses can override
    ExternalScanContextMgr* _external_scan_context_mgr = nullptr;
    doris::vectorized::VDataStreamMgr* _vstream_mgr = nullptr;
    ResultBufferMgr* _result_mgr = nullptr;
    ResultQueueMgr* _result_queue_mgr = nullptr;
    ClientCache<BackendServiceClient>* _backend_client_cache = nullptr;
    ClientCache<FrontendServiceClient>* _frontend_client_cache = nullptr;
    ClientCache<TPaloBrokerServiceClient>* _broker_client_cache = nullptr;

    // The default tracker consumed by mem hook. If the thread does not attach other trackers,
    // by default all consumption will be passed to the process tracker through the orphan tracker.
    // In real time, `consumption of all limiter trackers` + `orphan tracker consumption` = `process tracker consumption`.
    // Ideally, all threads are expected to attach to the specified tracker, so that "all memory has its own ownership",
    // and the consumption of the orphan mem tracker is close to 0, but greater than 0.
    std::shared_ptr<MemTrackerLimiter> _orphan_mem_tracker;
    MemTrackerLimiter* _orphan_mem_tracker_raw;
    std::shared_ptr<MemTrackerLimiter> _experimental_mem_tracker;
    // page size not in cache, data page/index page/etc.
    std::shared_ptr<MemTracker> _page_no_cache_mem_tracker;
    std::shared_ptr<MemTracker> _brpc_iobuf_block_memory_tracker;

    std::unique_ptr<ThreadPool> _send_batch_thread_pool;

    // Threadpool used to download cache from remote storage
    std::unique_ptr<ThreadPool> _download_cache_thread_pool;
    // Threadpool used to prefetch remote file for buffered reader
    std::unique_ptr<ThreadPool> _buffered_reader_prefetch_thread_pool;
    // A token used to submit download cache task serially
    std::unique_ptr<ThreadPoolToken> _serial_download_cache_thread_token;
    // Pool used by fragment manager to send profile or status to FE coordinator
    std::unique_ptr<ThreadPool> _send_report_thread_pool;
    // Pool used by join node to build hash table
    std::unique_ptr<ThreadPool> _join_node_thread_pool;
    // ThreadPoolToken -> buffer
    std::unordered_map<ThreadPoolToken*, std::unique_ptr<char[]>> _download_cache_buf_map;
    FragmentMgr* _fragment_mgr = nullptr;
    pipeline::TaskScheduler* _pipeline_task_scheduler = nullptr;
    pipeline::TaskScheduler* _pipeline_task_group_scheduler = nullptr;
    taskgroup::TaskGroupManager* _task_group_manager = nullptr;

    ResultCache* _result_cache = nullptr;
    TMasterInfo* _master_info = nullptr;
    LoadPathMgr* _load_path_mgr = nullptr;

    BfdParser* _bfd_parser = nullptr;
    BrokerMgr* _broker_mgr = nullptr;
    LoadChannelMgr* _load_channel_mgr = nullptr;
    std::shared_ptr<NewLoadStreamMgr> _new_load_stream_mgr;
    BrpcClientCache<PBackendService_Stub>* _internal_client_cache = nullptr;
    BrpcClientCache<PFunctionService_Stub>* _function_client_cache = nullptr;

    std::shared_ptr<StreamLoadExecutor> _stream_load_executor;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;
    doris::vectorized::ScannerScheduler* _scanner_scheduler = nullptr;

    BlockSpillManager* _block_spill_mgr = nullptr;
    // To save meta info of external file, such as parquet footer.
    FileMetaCache* _file_meta_cache = nullptr;
    std::unique_ptr<MemTableMemoryLimiter> _memtable_memory_limiter;

    std::unique_ptr<vectorized::ZoneList> _global_zone_cache;
    std::shared_mutex _zone_cache_rw_lock;

    std::mutex _frontends_lock;
    std::map<TNetworkAddress, FrontendInfo> _frontends;
    GroupCommitMgr* _group_commit_mgr = nullptr;
};

template <>
inline ClientCache<BackendServiceClient>* ExecEnv::get_client_cache<BackendServiceClient>() {
    return _backend_client_cache;
}
template <>
inline ClientCache<FrontendServiceClient>* ExecEnv::get_client_cache<FrontendServiceClient>() {
    return _frontend_client_cache;
}
template <>
inline ClientCache<TPaloBrokerServiceClient>*
ExecEnv::get_client_cache<TPaloBrokerServiceClient>() {
    return _broker_client_cache;
}

} // namespace doris
