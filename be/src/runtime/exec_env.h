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

#include <common/multi_version.h>
#include <stddef.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/tablet_fwd.h"
#include "runtime/frontend_info.h" // TODO(zhiqiang): find a way to remove this include header
#include "util/threadpool.h"

namespace doris {
namespace vectorized {
class VDataStreamMgr;
class ScannerScheduler;
class DeltaWriterV2Pool;
} // namespace vectorized
namespace pipeline {
class TaskScheduler;
}
namespace taskgroup {
class TaskGroupManager;
}
namespace stream_load {
class LoadStreamStubPool;
} // namespace stream_load
namespace io {
class S3FileBufferPool;
class FileCacheFactory;
} // namespace io
namespace segment_v2 {
class InvertedIndexSearcherCache;
class InvertedIndexQueryCache;
} // namespace segment_v2

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
class TabletSchemaCache;
class UserFunctionCache;
class SchemaCache;
class StoragePageCache;
class SegmentLoader;
class LookupConnectionCache;
class RowCache;
class CacheManager;
class WalManager;

inline bool k_doris_exit = false;

// Execution environment for queries/plan fragments.
// Contains all required global structures, and handles to
// singleton services. Clients must call StartServices exactly
// once to properly initialise service state.
class ExecEnv {
public:
    // Empty destructor because the compiler-generated one requires full
    // declarations for classes in scoped_ptrs.
    ~ExecEnv();

    // Initial exec environment. must call this to init all
    [[nodiscard]] static Status init(ExecEnv* env, const std::vector<StorePath>& store_paths,
                                     const std::set<std::string>& broken_paths);

    // Stop all threads and delete resources.
    void destroy();

    /// Returns the first created exec env instance. In a normal doris, this is
    /// the only instance. In test setups with multiple ExecEnv's per process,
    /// we return the most recently created instance.
    static ExecEnv* GetInstance() {
        static ExecEnv s_exec_env;
        return &s_exec_env;
    }

    // Requires ExenEnv ready
    static Result<BaseTabletSPtr> get_tablet(int64_t tablet_id);

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
    ThreadPool* s3_file_upload_thread_pool() { return _s3_file_upload_thread_pool.get(); }
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
    void init_file_cache_factory();
    char* get_download_cache_buf(ThreadPoolToken* token) {
        if (_download_cache_buf_map.find(token) == _download_cache_buf_map.end()) {
            return nullptr;
        }
        return _download_cache_buf_map[token].get();
    }
    io::FileCacheFactory* file_cache_factory() { return _file_cache_factory; }
    UserFunctionCache* user_function_cache() { return _user_function_cache; }
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
    WalManager* wal_mgr() { return _wal_manager.get(); }
#ifdef BE_TEST
    void set_ready() { this->_s_ready = true; }
    void set_not_ready() { this->_s_ready = false; }
    void set_memtable_memory_limiter(MemTableMemoryLimiter* limiter) {
        _memtable_memory_limiter.reset(limiter);
    }
    void set_master_info(TMasterInfo* master_info) { this->_master_info = master_info; }
    void set_new_load_stream_mgr(std::shared_ptr<NewLoadStreamMgr> new_load_stream_mgr) {
        this->_new_load_stream_mgr = new_load_stream_mgr;
    }
    void set_stream_load_executor(std::shared_ptr<StreamLoadExecutor> stream_load_executor) {
        this->_stream_load_executor = stream_load_executor;
    }

    void set_storage_engine(StorageEngine* se) { this->_storage_engine = se; }
    void set_cache_manager(CacheManager* cm) { this->_cache_manager = cm; }
    void set_tablet_schema_cache(TabletSchemaCache* c) { this->_tablet_schema_cache = c; }
    void set_storage_page_cache(StoragePageCache* c) { this->_storage_page_cache = c; }
    void set_segment_loader(SegmentLoader* sl) { this->_segment_loader = sl; }
    void set_routine_load_task_executor(RoutineLoadTaskExecutor* r) {
        this->_routine_load_task_executor = r;
    }

#endif
    stream_load::LoadStreamStubPool* load_stream_stub_pool() {
        return _load_stream_stub_pool.get();
    }

    vectorized::DeltaWriterV2Pool* delta_writer_v2_pool() { return _delta_writer_v2_pool.get(); }

    void wait_for_all_tasks_done();

    void update_frontends(const std::vector<TFrontendInfo>& new_infos);
    std::map<TNetworkAddress, FrontendInfo> get_frontends();
    std::map<TNetworkAddress, FrontendInfo> get_running_frontends();

    TabletSchemaCache* get_tablet_schema_cache() { return _tablet_schema_cache; }
    StorageEngine* get_storage_engine() { return _storage_engine; }
    io::S3FileBufferPool* get_s3_file_buffer_pool() { return _s3_buffer_pool; }
    SchemaCache* schema_cache() { return _schema_cache; }
    StoragePageCache* get_storage_page_cache() { return _storage_page_cache; }
    SegmentLoader* segment_loader() { return _segment_loader; }
    LookupConnectionCache* get_lookup_connection_cache() { return _lookup_connection_cache; }
    RowCache* get_row_cache() { return _row_cache; }
    CacheManager* get_cache_manager() { return _cache_manager; }
    segment_v2::InvertedIndexSearcherCache* get_inverted_index_searcher_cache() {
        return _inverted_index_searcher_cache;
    }
    segment_v2::InvertedIndexQueryCache* get_inverted_index_query_cache() {
        return _inverted_index_query_cache;
    }

    CgroupCpuCtl* get_cgroup_cpu_ctl() { return _cgroup_cpu_ctl.get(); }

private:
    ExecEnv();

    [[nodiscard]] Status _init(const std::vector<StorePath>& store_paths,
                               const std::set<std::string>& broken_paths);
    void _destroy();

    Status _init_mem_env();

    void _register_metrics();
    void _deregister_metrics();

    inline static std::atomic_bool _s_ready {false};
    std::vector<StorePath> _store_paths;

    io::FileCacheFactory* _file_cache_factory = nullptr;
    UserFunctionCache* _user_function_cache = nullptr;
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
    // Threadpool used to upload local file to s3
    std::unique_ptr<ThreadPool> _s3_file_upload_thread_pool;
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
    // TODO(zhiqiang): Do not use shared_ptr in exec_env, we can not control its life cycle.
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
    std::unique_ptr<stream_load::LoadStreamStubPool> _load_stream_stub_pool;
    std::unique_ptr<vectorized::DeltaWriterV2Pool> _delta_writer_v2_pool;
    std::shared_ptr<WalManager> _wal_manager;

    std::mutex _frontends_lock;
    std::map<TNetworkAddress, FrontendInfo> _frontends;
    GroupCommitMgr* _group_commit_mgr = nullptr;

    // Maybe we should use unique_ptr, but it need complete type, which means we need
    // to include many headers, and for some cpp file that do not need class like TabletSchemaCache,
    // these redundancy header could introduce potential bug, at least, more header means slow compile.
    // So we choose to use raw pointer, please remember to delete these pointer in deconstructor.
    TabletSchemaCache* _tablet_schema_cache = nullptr;
    io::S3FileBufferPool* _s3_buffer_pool = nullptr;
    StorageEngine* _storage_engine = nullptr;
    SchemaCache* _schema_cache = nullptr;
    StoragePageCache* _storage_page_cache = nullptr;
    SegmentLoader* _segment_loader = nullptr;
    LookupConnectionCache* _lookup_connection_cache = nullptr;
    RowCache* _row_cache = nullptr;
    CacheManager* _cache_manager = nullptr;
    segment_v2::InvertedIndexSearcherCache* _inverted_index_searcher_cache = nullptr;
    segment_v2::InvertedIndexQueryCache* _inverted_index_query_cache = nullptr;

    std::unique_ptr<CgroupCpuCtl> _cgroup_cpu_ctl = nullptr;
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

inline segment_v2::InvertedIndexQueryCache* GetInvertedIndexQueryCache() {
    return ExecEnv::GetInstance()->get_inverted_index_query_cache();
}

} // namespace doris
