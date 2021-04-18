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

namespace doris {

class BfdParser;
class BrokerMgr;
class BrpcStubCache;
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
class ReservationTracker;
class ResultBufferMgr;
class ResultQueueMgr;
class TMasterInfo;
class LoadChannelMgr;
class TestExecEnv;
class ThreadResourceMgr;
class TmpFileMgr;
class WebPageHandler;
class StreamLoadExecutor;
class RoutineLoadTaskExecutor;
class SmallFileMgr;
class FileBlockManager;
class PluginMgr;

class BackendServiceClient;
class FrontendServiceClient;
class TPaloBrokerServiceClient;
class TExtDataSourceServiceClient;
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
    PriorityThreadPool* thread_pool() { return _thread_pool; }
    PriorityThreadPool* etl_thread_pool() { return _etl_thread_pool; }
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
    BrpcStubCache* brpc_stub_cache() const { return _brpc_stub_cache; }
    ReservationTracker* buffer_reservation() { return _buffer_reservation; }
    BufferPool* buffer_pool() { return _buffer_pool; }
    LoadChannelMgr* load_channel_mgr() { return _load_channel_mgr; }
    LoadStreamMgr* load_stream_mgr() { return _load_stream_mgr; }
    SmallFileMgr* small_file_mgr() { return _small_file_mgr; }

    const std::vector<StorePath>& store_paths() const { return _store_paths; }
    void set_store_paths(const std::vector<StorePath>& paths) { _store_paths = paths; }
    StorageEngine* storage_engine() { return _storage_engine; }
    void set_storage_engine(StorageEngine* storage_engine) { _storage_engine = storage_engine; }

    StreamLoadExecutor* stream_load_executor() { return _stream_load_executor; }
    RoutineLoadTaskExecutor* routine_load_task_executor() { return _routine_load_task_executor; }
    HeartbeatFlags* heartbeat_flags() { return _heartbeat_flags; }

    PluginMgr* plugin_mgr() { return _plugin_mgr; }

private:
    Status _init(const std::vector<StorePath>& store_paths);
    void _destroy();

    Status _init_mem_tracker();
    /// Initialise 'buffer_pool_' and 'buffer_reservation_' with given capacity.
    void _init_buffer_pool(int64_t min_page_len, int64_t capacity, int64_t clean_pages_limit);

private:
    bool _is_init;
    std::vector<StorePath> _store_paths;
    // Leave protected so that subclasses can override
    ExternalScanContextMgr* _external_scan_context_mgr = nullptr;
    DataStreamMgr* _stream_mgr = nullptr;
    ResultBufferMgr* _result_mgr = nullptr;
    ResultQueueMgr* _result_queue_mgr = nullptr;
    ClientCache<BackendServiceClient>* _backend_client_cache = nullptr;
    ClientCache<FrontendServiceClient>* _frontend_client_cache = nullptr;
    ClientCache<TPaloBrokerServiceClient>* _broker_client_cache = nullptr;
    ClientCache<TExtDataSourceServiceClient>* _extdatasource_client_cache = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker;
    PoolMemTrackerRegistry* _pool_mem_trackers = nullptr;
    ThreadResourceMgr* _thread_mgr = nullptr;
    PriorityThreadPool* _thread_pool = nullptr;
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
    BrpcStubCache* _brpc_stub_cache = nullptr;

    ReservationTracker* _buffer_reservation = nullptr;
    BufferPool* _buffer_pool = nullptr;

    StorageEngine* _storage_engine = nullptr;

    StreamLoadExecutor* _stream_load_executor = nullptr;
    RoutineLoadTaskExecutor* _routine_load_task_executor = nullptr;
    SmallFileMgr* _small_file_mgr = nullptr;
    HeartbeatFlags* _heartbeat_flags = nullptr;

    PluginMgr* _plugin_mgr = nullptr;
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
