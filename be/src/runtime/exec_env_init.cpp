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

#include "agent/cgroups_mgr.h"
#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "olap/page_cache.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy_mgr.h"
#include "runtime/broker_mgr.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/cache/result_cache.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/disk_io_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fold_constant_executor.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_task_pool.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/tmp_file_mgr.h"
#include "util/bfd_parser.h"
#include "util/brpc_client_cache.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/priority_thread_pool.hpp"
#include "util/priority_work_stealing_thread_pool.hpp"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/vdata_stream_mgr.h"

#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include "runtime/memory/tcmalloc_hook.h"
#endif

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(scanner_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(send_batch_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(send_batch_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(download_cache_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(download_cache_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(query_mem_consumption, MetricUnit::BYTES, "", mem_consumption,
                                   Labels({{"type", "query"}}));
DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(load_mem_consumption, MetricUnit::BYTES, "", mem_consumption,
                                   Labels({{"type", "load"}}));

Status ExecEnv::init(ExecEnv* env, const std::vector<StorePath>& store_paths) {
    return env->_init(store_paths);
}

Status ExecEnv::_init(const std::vector<StorePath>& store_paths) {
    //Only init once before be destroyed
    if (_is_init) {
        return Status::OK();
    }
    _store_paths = store_paths;
    // path_name => path_index
    for (int i = 0; i < store_paths.size(); i++) {
        _store_path_map[store_paths[i].path] = i;
    }

    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _stream_mgr = new DataStreamMgr();
    _vstream_mgr = new doris::vectorized::VDataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);
    _task_pool_mem_tracker_registry = new MemTrackerTaskPool();
    _thread_mgr = new ThreadResourceMgr();
    if (config::doris_enable_scanner_thread_pool_per_disk &&
        config::doris_scanner_thread_pool_thread_num >= store_paths.size() &&
        store_paths.size() > 0) {
        _scan_thread_pool = new PriorityWorkStealingThreadPool(
                config::doris_scanner_thread_pool_thread_num, store_paths.size(),
                config::doris_scanner_thread_pool_queue_size);
        LOG(INFO) << "scan thread pool use PriorityWorkStealingThreadPool";
    } else {
        _scan_thread_pool = new PriorityThreadPool(config::doris_scanner_thread_pool_thread_num,
                                                   config::doris_scanner_thread_pool_queue_size);
        LOG(INFO) << "scan thread pool use PriorityThreadPool";
    }

    _remote_scan_thread_pool =
            new PriorityThreadPool(config::doris_remote_scanner_thread_pool_thread_num,
                                   config::doris_remote_scanner_thread_pool_queue_size);

    ThreadPoolBuilder("LimitedScanThreadPool")
            .set_min_threads(config::doris_scanner_thread_pool_thread_num)
            .set_max_threads(config::doris_scanner_thread_pool_thread_num)
            .set_max_queue_size(config::doris_scanner_thread_pool_queue_size)
            .build(&_limited_scan_thread_pool);

    ThreadPoolBuilder("SendBatchThreadPool")
            .set_min_threads(config::send_batch_thread_pool_thread_num)
            .set_max_threads(config::send_batch_thread_pool_thread_num)
            .set_max_queue_size(config::send_batch_thread_pool_queue_size)
            .build(&_send_batch_thread_pool);

    init_download_cache_required_components();

    _scanner_scheduler = new doris::vectorized::ScannerScheduler();

    _cgroups_mgr = new CgroupsMgr(this, config::doris_cgroups);
    _fragment_mgr = new FragmentMgr(this);
    _result_cache = new ResultCache(config::query_cache_max_size_mb,
                                    config::query_cache_elasticity_size_mb);
    _master_info = new TMasterInfo();
    _load_path_mgr = new LoadPathMgr(this);
    _disk_io_mgr = new DiskIoMgr();
    _tmp_file_mgr = new TmpFileMgr(this);
    _bfd_parser = BfdParser::create();
    _broker_mgr = new BrokerMgr(this);
    _load_channel_mgr = new LoadChannelMgr();
    _load_stream_mgr = new LoadStreamMgr();
    _internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
    _function_client_cache = new BrpcClientCache<PFunctionService_Stub>();
    _stream_load_executor = new StreamLoadExecutor(this);
    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _storage_policy_mgr = new StoragePolicyMgr();

    _backend_client_cache->init_metrics("backend");
    _frontend_client_cache->init_metrics("frontend");
    _broker_client_cache->init_metrics("broker");
    _result_mgr->init();
    _cgroups_mgr->init_cgroups();
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status.get_error_msg();
        exit(-1);
    }
    _broker_mgr->init();
    _small_file_mgr->init();
    _scanner_scheduler->init(this);

    _init_mem_tracker();

    RETURN_IF_ERROR(
            _load_channel_mgr->init(ExecEnv::GetInstance()->process_mem_tracker()->limit()));
    _heartbeat_flags = new HeartbeatFlags();
    _register_metrics();
    _is_init = true;
    return Status::OK();
}

Status ExecEnv::_init_mem_tracker() {
    // 1. init global memory limit.
    int64_t global_memory_limit_bytes = 0;
    bool is_percent = false;
    std::stringstream ss;
    global_memory_limit_bytes =
            ParseUtil::parse_mem_spec(config::mem_limit, -1, MemInfo::physical_mem(), &is_percent);
    if (global_memory_limit_bytes <= 0) {
        ss << "Failed to parse mem limit from '" + config::mem_limit + "'.";
        return Status::InternalError(ss.str());
    }

    if (global_memory_limit_bytes > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit "
                     << PrettyPrinter::print(global_memory_limit_bytes, TUnit::BYTES)
                     << " exceeds physical memory, using physical memory instead";
        global_memory_limit_bytes = MemInfo::physical_mem();
    }
    _process_mem_tracker =
            std::make_shared<MemTrackerLimiter>(global_memory_limit_bytes, "Process");
    _orphan_mem_tracker = std::make_shared<MemTrackerLimiter>(-1, "Orphan", _process_mem_tracker);
    _orphan_mem_tracker_raw = _orphan_mem_tracker.get();
    _bthread_mem_tracker = std::make_shared<MemTrackerLimiter>(-1, "Bthread", _orphan_mem_tracker);
    thread_context()->_thread_mem_tracker_mgr->init();
    thread_context()->_thread_mem_tracker_mgr->set_check_attach(false);
#if defined(USE_MEM_TRACKER) && !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && \
        !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
    if (doris::config::enable_tcmalloc_hook) {
        init_hook();
    }
#endif
    _allocator_cache_mem_tracker = std::make_shared<MemTracker>("Tc/JemallocAllocatorCache");
    _query_pool_mem_tracker =
            std::make_shared<MemTrackerLimiter>(-1, "QueryPool", _process_mem_tracker);
    REGISTER_HOOK_METRIC(query_mem_consumption,
                         [this]() { return _query_pool_mem_tracker->consumption(); });
    _load_pool_mem_tracker =
            std::make_shared<MemTrackerLimiter>(-1, "LoadPool", _process_mem_tracker);
    REGISTER_HOOK_METRIC(load_mem_consumption,
                         [this]() { return _load_pool_mem_tracker->consumption(); });
    LOG(INFO) << "Using global memory limit: "
              << PrettyPrinter::print(global_memory_limit_bytes, TUnit::BYTES)
              << ", origin config value: " << config::mem_limit;

    // 2. init buffer pool
    if (!BitUtil::IsPowerOf2(config::min_buffer_size)) {
        ss << "Config min_buffer_size must be a power-of-two: " << config::min_buffer_size;
        return Status::InternalError(ss.str());
    }

    int64_t buffer_pool_limit =
            ParseUtil::parse_mem_spec(config::buffer_pool_limit, global_memory_limit_bytes,
                                      MemInfo::physical_mem(), &is_percent);
    if (buffer_pool_limit <= 0) {
        ss << "Invalid config buffer_pool_limit value, must be a percentage or "
              "positive bytes value or percentage: "
           << config::buffer_pool_limit;
        return Status::InternalError(ss.str());
    }
    buffer_pool_limit = BitUtil::RoundDown(buffer_pool_limit, config::min_buffer_size);
    while (!is_percent && buffer_pool_limit > global_memory_limit_bytes / 2) {
        // If buffer_pool_limit is not a percentage, and the value exceeds 50% of the total memory limit,
        // it is forced to be reduced to less than 50% of the total memory limit.
        // This is to ensure compatibility. In principle, buffer_pool_limit should be set as a percentage.
        buffer_pool_limit = buffer_pool_limit / 2;
    }

    int64_t clean_pages_limit =
            ParseUtil::parse_mem_spec(config::buffer_pool_clean_pages_limit, buffer_pool_limit,
                                      MemInfo::physical_mem(), &is_percent);
    if (clean_pages_limit <= 0) {
        ss << "Invalid buffer_pool_clean_pages_limit value, must be a percentage or "
              "positive bytes value or percentage: "
           << config::buffer_pool_clean_pages_limit;
        return Status::InternalError(ss.str());
    }
    while (!is_percent && clean_pages_limit > buffer_pool_limit / 2) {
        // Reason same as buffer_pool_limit
        clean_pages_limit = clean_pages_limit / 2;
    }
    _init_buffer_pool(config::min_buffer_size, buffer_pool_limit, clean_pages_limit);
    LOG(INFO) << "Buffer pool memory limit: "
              << PrettyPrinter::print(buffer_pool_limit, TUnit::BYTES)
              << ", origin config value: " << config::buffer_pool_limit
              << ". clean pages limit: " << PrettyPrinter::print(clean_pages_limit, TUnit::BYTES)
              << ", origin config value: " << config::buffer_pool_clean_pages_limit;

    // 3. init storage page cache
    int64_t storage_cache_limit =
            ParseUtil::parse_mem_spec(config::storage_page_cache_limit, global_memory_limit_bytes,
                                      MemInfo::physical_mem(), &is_percent);
    while (!is_percent && storage_cache_limit > global_memory_limit_bytes / 2) {
        // Reason same as buffer_pool_limit
        storage_cache_limit = storage_cache_limit / 2;
    }
    int32_t index_percentage = config::index_page_cache_percentage;
    uint32_t num_shards = config::storage_page_cache_shard_size;
    StoragePageCache::create_global_cache(storage_cache_limit, index_percentage, num_shards);
    LOG(INFO) << "Storage page cache memory limit: "
              << PrettyPrinter::print(storage_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::storage_page_cache_limit;

    uint64_t fd_number = config::min_file_descriptor_number;
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE, &l);
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
    } else {
        fd_number = static_cast<uint64_t>(l.rlim_cur);
    }
    // SegmentLoader caches segments in rowset granularity. So the size of
    // opened files will greater than segment_cache_capacity.
    uint64_t segment_cache_capacity = fd_number / 3 * 2;
    LOG(INFO) << "segment_cache_capacity = fd_number / 3 * 2, fd_number: " << fd_number
              << " segment_cache_capacity: " << segment_cache_capacity;
    SegmentLoader::create_global_instance(segment_cache_capacity);

    // 4. init other managers
    RETURN_IF_ERROR(_disk_io_mgr->init(global_memory_limit_bytes));
    RETURN_IF_ERROR(_tmp_file_mgr->init());

    int64_t chunk_reserved_bytes_limit =
            ParseUtil::parse_mem_spec(config::chunk_reserved_bytes_limit, global_memory_limit_bytes,
                                      MemInfo::physical_mem(), &is_percent);
    if (chunk_reserved_bytes_limit <= 0) {
        ss << "Invalid config chunk_reserved_bytes_limit value, must be a percentage or "
              "positive bytes value or percentage: "
           << config::chunk_reserved_bytes_limit;
        return Status::InternalError(ss.str());
    }
    // Has to round to multiple of page size(4096 bytes), chunk allocator will also check this
    chunk_reserved_bytes_limit = BitUtil::RoundDown(chunk_reserved_bytes_limit, 4096);
    ChunkAllocator::init_instance(chunk_reserved_bytes_limit);
    LOG(INFO) << "Chunk allocator memory limit: "
              << PrettyPrinter::print(chunk_reserved_bytes_limit, TUnit::BYTES)
              << ", origin config value: " << config::chunk_reserved_bytes_limit;
    return Status::OK();
}

void ExecEnv::_init_buffer_pool(int64_t min_page_size, int64_t capacity,
                                int64_t clean_pages_limit) {
    DCHECK(_buffer_pool == nullptr);
    _buffer_pool = new BufferPool(min_page_size, capacity, clean_pages_limit);
}

void ExecEnv::init_download_cache_buf() {
    std::unique_ptr<char[]> download_cache_buf(new char[config::download_cache_buffer_size]);
    memset(download_cache_buf.get(), 0, config::download_cache_buffer_size);
    _download_cache_buf_map[_serial_download_cache_thread_token.get()] =
            std::move(download_cache_buf);
}

void ExecEnv::init_download_cache_required_components() {
    ThreadPoolBuilder("DownloadCacheThreadPool")
            .set_min_threads(1)
            .set_max_threads(config::download_cache_thread_pool_thread_num)
            .set_max_queue_size(config::download_cache_thread_pool_queue_size)
            .build(&_download_cache_thread_pool);
    set_serial_download_cache_thread_token();
    init_download_cache_buf();
}

void ExecEnv::_register_metrics() {
    REGISTER_HOOK_METRIC(scanner_thread_pool_queue_size,
                         [this]() { return _scan_thread_pool->get_queue_size(); });

    REGISTER_HOOK_METRIC(send_batch_thread_pool_thread_num,
                         [this]() { return _send_batch_thread_pool->num_threads(); });

    REGISTER_HOOK_METRIC(send_batch_thread_pool_queue_size,
                         [this]() { return _send_batch_thread_pool->get_queue_size(); });

    REGISTER_HOOK_METRIC(download_cache_thread_pool_thread_num,
                         [this]() { return _download_cache_thread_pool->num_threads(); });

    REGISTER_HOOK_METRIC(download_cache_thread_pool_queue_size,
                         [this]() { return _download_cache_thread_pool->get_queue_size(); });
}

void ExecEnv::_deregister_metrics() {
    DEREGISTER_HOOK_METRIC(scanner_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(send_batch_thread_pool_thread_num);
    DEREGISTER_HOOK_METRIC(send_batch_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(download_cache_thread_pool_thread_num);
    DEREGISTER_HOOK_METRIC(download_cache_thread_pool_queue_size);
}

void ExecEnv::_destroy() {
    //Only destroy once after init
    if (!_is_init) {
        return;
    }
    _deregister_metrics();
    SAFE_DELETE(_internal_client_cache);
    SAFE_DELETE(_function_client_cache);
    SAFE_DELETE(_load_stream_mgr);
    SAFE_DELETE(_load_channel_mgr);
    SAFE_DELETE(_broker_mgr);
    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_tmp_file_mgr);
    SAFE_DELETE(_disk_io_mgr);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_master_info);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_cgroups_mgr);
    SAFE_DELETE(_scan_thread_pool);
    SAFE_DELETE(_remote_scan_thread_pool);
    SAFE_DELETE(_thread_mgr);
    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_result_queue_mgr);
    SAFE_DELETE(_stream_mgr);
    SAFE_DELETE(_stream_load_executor);
    SAFE_DELETE(_routine_load_task_executor);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_heartbeat_flags);
    SAFE_DELETE(_task_pool_mem_tracker_registry);
    SAFE_DELETE(_scanner_scheduler);

    DEREGISTER_HOOK_METRIC(query_mem_consumption);
    DEREGISTER_HOOK_METRIC(load_mem_consumption);

    _is_init = false;
}

void ExecEnv::destroy(ExecEnv* env) {
    env->_destroy();
}

} // namespace doris
