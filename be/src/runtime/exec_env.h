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

#ifndef DORIS_BE_RUNTIME_EXEC_ENV_H
#define DORIS_BE_RUNTIME_EXEC_ENV_H

#include "common/status.h"
#include "olap/options.h"
#include "util/threadpool.h"

namespace doris {
namespace vectorized {
class VDataStreamMgr;
}
class BfdParser;
class BrokerMgr;

template <class T>
class BrpcClientCache;

class BufferPool;
class CgroupsMgr;
class DataStreamMgr;
class DiskIoMgr;
class EtlJobMgr;
class EvHttpServer;
class ExternalScanContextMgr;
class FragmentMgr;
class ResultCache;
class LoadPathMgr;
class LoadStreamMgr;
class MemTracker;
class StorageEngine;
class PoolMemTrackerRegistry;
class PriorityThreadPool;
class PriorityWorkStealingThreadPool;
class ReservationTracker;
class ResultBufferMgr;
class ResultQueueMgr;
class TMasterInfo;
class LoadChannelMgr;
class ThreadResourceMgr;
class TmpFileMgr;
class WebPageHandler;
class StreamLoadExecutor;
class RoutineLoadTaskExecutor;
class SmallFileMgr;

class BackendServiceClient;
class FrontendServiceClient;
class TPaloBrokerServiceClient;
class TExtDataSourceServiceClient;
class PBackendService_Stub;
class PFunctionService_Stub;

template <class T>
class ClientCache;

class HeartbeatFlags;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Initial exec enviorment. must call this to init all
    static Status init(ExecEnv* env, const std::vector<StorePath>& store_paths);
    static void destroy(ExecEnv* exec_env);

    /// Returns the first created exec env instance. In a normal doris, this is
    /// the only instance. In test setups with multiple ExecEnv's per process,
    /// we return the most recently created instance.
    static ExecEnv* GetInstance() {
        static ExecEnv s_exec_env;
        return &s_exec_env;
    }

    // only used for test
    ExecEnv();

    // Empty destructor because the compiler-generated one requires full
    // declarations for classes in scoped_ptrs.
    ~ExecEnv();

    const std::string& token() const;
    ExternalScanContextMgr* external_scan_context_mgr() { return _external_scan_context_mgr; }
    DataStreamMgr* stream_mgr() { return _stream_mgr; }
    doris::vectorized::VDataStreamMgr* vstream_mgr() { return _vstream_mgr; }
    ResultBufferMgr* result_mgr() { return _result_mgr; }
    ResultQueueMgr* result_queue_mgr() { return _result_queue_mgr; }
    ClientCache<BackendServiceClient>* client_cache() { return _backend_client_cache; }
    ClientCache<FrontendServiceClient>* frontend_client_cache() { return _frontend_client_cache; }
    ClientCache<TPaloBrokerServiceClient>* broker_client_cache() { return _broker_client_cache; }
    ClientCache<TExtDataSourceServiceClient>* extdatasource_client_cache() {
        return _extdatasource_client_cache;
    }

    // using template to simplify client cache management
    template <typename T>
    ClientCache<T>* get_client_cache() {
        return nullptr;
    }

    std::shared_ptr<MemTracker> process_mem_tracker() { return _mem_tracker; }
    PoolMemTrackerRegistry* pool_mem_trackers() { return _pool_mem_trackers; }
    ThreadResourceMgr* thread_mgr() { return _thread_mgr; }
    PriorityThreadPool* scan_thread_pool() { return _scan_thread_pool; }
    ThreadPool* limited_scan_thread_pool() { return _limited_scan_thread_pool.get(); }
    PriorityThreadPool* etl_thread_pool() { return _etl_thread_pool; }
    ThreadPool* send_batch_thread_pool() { return _send_batch_thread_pool.get(); }
    CgroupsMgr* cgroups_mgr() { return _cgroups_mgr; }
    FragmentMgr* fragment_mgr() { return _fragment_mgr; }
    ResultCache* result_cache() { return _result_cache; }
    TMasterInfo* master_info() { return _master_info; }
    EtlJobMgr* etl_job_mgr() { return _etl_job_mgr; }
    LoadPathMgr* load_path_mgr() { return _load_path_mgr; }
    DiskIoMgr* disk_io_mgr() { return _disk_io_mgr; }
    TmpFileMgr* tmp_file_mgr() { return _tmp_file_mgr; }
    BfdParser* bfd_parser() const { return _bfd_parser; }
    BrokerMgr* broker_mgr() const { return _broker_mgr; }
    BrpcClientCache<PBackendService_Stub>* brpc_internal_client_cache() const {
        return _internal_client_cache;
    }
    BrpcClientCache<PFunctionService_Stub>* brpc_function_client_cache() const {
        return _function_client_cache;
    }
    ReservationTracker* buffer_reservation() { return _buffer_reservation; }
    BufferPool* buffer_pool() { return _buffer_pool; }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    LoadStreamMgr* load_stream_mgr() { return _load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }
    size_t store_path_to_index(const std::string& path) { return _store_path_map[path]; }
    void set_store_paths(const std::vector<StorePath>& paths) { _store_paths = paths; }
    StorageEngine* storage_engine() { return _storage_engine; }
    void set_storage_engine(StorageEngine* storage_engine) { _storage_engine = storage_engine; }

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }

    // The root tracker should be set before calling ExecEnv::init();
    void set_root_mem_tracker(std::shared_ptr<MemTracker> root_tracker);

private:
    Status _init(const std::vector<StorePath>& store_paths);
    void _destroy();

    Status _init_mem_tracker();
    /// Initialise 'buffer_pool_' and 'buffer_reservation_' with given capacity.
    void _init_buffer_pool(int64_t min_page_len, int64_t capacity, int64_t clean_pages_limit);

    void _register_metrics();
    void _deregister_metrics();

private:
    bool _is_init;
    std::vector<StorePath> _store_paths;
    // path => store index
    std::map<std::string, size_t> _store_path_map;
    // Leave protected so that subclasses can override
    ExternalScanContextMgr* _external_scan_context_mgr = nullptr;
    DataStreamMgr* _stream_mgr = nullptr;
    doris::vectorized::VDataStreamMgr* _vstream_mgr = nullptr;
    ResultBufferMgr* _result_mgr = nullptr;
    ResultQueueMgr* _result_queue_mgr = nullptr;
    ClientCache<BackendServiceClient>* _backend_client_cache = nullptr;
    ClientCache<FrontendServiceClient>* _frontend_client_cache = nullptr;
    ClientCache<TPaloBrokerServiceClient>* _broker_client_cache = nullptr;
    ClientCache<TExtDataSourceServiceClient>* _extdatasource_client_cache = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker;
    PoolMemTrackerRegistry* _pool_mem_trackers = nullptr;
    ThreadResourceMgr* _thread_mgr = nullptr;

    // The following two thread pools are used in different scenarios.
    // _scan_thread_pool is a priority thread pool.
    // Scanner threads for common queries will use this thread pool,
    // and the priority of each scan task is set according to the size of the query.

    // _limited_scan_thread_pool is also the thread pool used for scanner.
    // The difference is that it is no longer a priority queue, but according to the concurrency
    // set by the user to control the number of threads that can be used by a query.

    // TODO(cmy): find a better way to unify these 2 pools.
    PriorityThreadPool* _scan_thread_pool = nullptr;
    std::unique_ptr<ThreadPool> _limited_scan_thread_pool;

    std::unique_ptr<ThreadPool> _send_batch_thread_pool;
    PriorityThreadPool* _etl_thread_pool = nullptr;
    CgroupsMgr* _cgroups_mgr = nullptr;
    FragmentMgr* _fragment_mgr = nullptr;
    ResultCache* _result_cache = nullptr;
    TMasterInfo* _master_info = nullptr;
    EtlJobMgr* _etl_job_mgr = nullptr;
    LoadPathMgr* _load_path_mgr = nullptr;
    DiskIoMgr* _disk_io_mgr = nullptr;
    TmpFileMgr* _tmp_file_mgr = nullptr;

    BfdParser* _bfd_parser = nullptr;
    BrokerMgr* _broker_mgr = nullptr;
    LoadChannelMgr* _load_channel_mgr = nullptr;
    LoadStreamMgr* _load_stream_mgr = nullptr;
    BrpcClientCache<PBackendService_Stub>* _internal_client_cache = nullptr;
    BrpcClientCache<PFunctionService_Stub>* _function_client_cache = nullptr;

    ReservationTracker* _buffer_reservation = nullptr;
    BufferPool* _buffer_pool = nullptr;

    StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;

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
template <>
inline ClientCache<TExtDataSourceServiceClient>*
ExecEnv::get_client_cache<TExtDataSourceServiceClient>() {
    return _extdatasource_client_cache;
}

} // namespace doris

#endif
