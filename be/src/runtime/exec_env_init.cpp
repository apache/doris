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

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>

#include <limits>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/schema_cache.h"
#include "olap/segment_loader.h"
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "runtime/block_spill_manager.h"
#include "runtime/broker_mgr.h"
#include "runtime/cache/result_cache.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_context.h"
#include "service/point_query_executor.h"
#include "util/bfd_parser.h"
#include "util/bit_util.h"
#include "util/brpc_client_cache.h"
#include "util/cpu_info.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/threadpool.h"
#include "vec/exec/format/file_meta_cache.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/vdata_stream_mgr.h"

#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include "runtime/memory/tcmalloc_hook.h"
#endif

namespace doris {
class PBackendService_Stub;
class PFunctionService_Stub;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(scanner_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(send_batch_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(send_batch_thread_pool_queue_size, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(download_cache_thread_pool_thread_num, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(download_cache_thread_pool_queue_size, MetricUnit::NOUNIT);

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
    _vstream_mgr = new doris::vectorized::VDataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);

    ThreadPoolBuilder("SendBatchThreadPool")
            .set_min_threads(config::send_batch_thread_pool_thread_num)
            .set_max_threads(config::send_batch_thread_pool_thread_num)
            .set_max_queue_size(config::send_batch_thread_pool_queue_size)
            .build(&_send_batch_thread_pool);

    init_download_cache_required_components();

    ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
            .set_min_threads(16)
            .set_max_threads(64)
            .build(&_buffered_reader_prefetch_thread_pool);

    // min num equal to fragment pool's min num
    // max num is useless because it will start as many as requested in the past
    // queue size is useless because the max thread num is very large
    ThreadPoolBuilder("SendReportThreadPool")
            .set_min_threads(config::fragment_pool_thread_num_min)
            .set_max_threads(std::numeric_limits<int>::max())
            .set_max_queue_size(config::fragment_pool_queue_size)
            .build(&_send_report_thread_pool);

    ThreadPoolBuilder("JoinNodeThreadPool")
            .set_min_threads(config::fragment_pool_thread_num_min)
            .set_max_threads(std::numeric_limits<int>::max())
            .set_max_queue_size(config::fragment_pool_queue_size)
            .build(&_join_node_thread_pool);

    RETURN_IF_ERROR(init_pipeline_task_scheduler());
    _scanner_scheduler = new doris::vectorized::ScannerScheduler();
    _fragment_mgr = new FragmentMgr(this);
    _result_cache = new ResultCache(config::query_cache_max_size_mb,
                                    config::query_cache_elasticity_size_mb);
    _master_info = new TMasterInfo();
    _load_path_mgr = new LoadPathMgr(this);
    _bfd_parser = BfdParser::create();
    _broker_mgr = new BrokerMgr(this);
    _load_channel_mgr = new LoadChannelMgr();
    _new_load_stream_mgr = NewLoadStreamMgr::create_shared();
    _internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
    _function_client_cache = new BrpcClientCache<PFunctionService_Stub>();
    _stream_load_executor = StreamLoadExecutor::create_shared(this);
    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _block_spill_mgr = new BlockSpillManager(_store_paths);
    _file_meta_cache = new FileMetaCache(128 * 1024 * 1024);

    _backend_client_cache->init_metrics("backend");
    _frontend_client_cache->init_metrics("frontend");
    _broker_client_cache->init_metrics("broker");
    _result_mgr->init();
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status;
        exit(-1);
    }
    _broker_mgr->init();
    _small_file_mgr->init();
    _scanner_scheduler->init(this);

    _init_mem_env();

    RETURN_IF_ERROR(_load_channel_mgr->init(MemInfo::mem_limit()));
    _heartbeat_flags = new HeartbeatFlags();
    _register_metrics();
    _is_init = true;
    return Status::OK();
}

Status ExecEnv::init_pipeline_task_scheduler() {
    auto executors_size = config::pipeline_executor_size;
    if (executors_size <= 0) {
        executors_size = CpuInfo::num_cores();
    }

    // TODO pipeline task group combie two blocked schedulers.
    auto t_queue = std::make_shared<pipeline::MultiCoreTaskQueue>(executors_size);
    auto b_scheduler = std::make_shared<pipeline::BlockedTaskScheduler>(t_queue);
    _pipeline_task_scheduler = new pipeline::TaskScheduler(this, b_scheduler, t_queue);
    RETURN_IF_ERROR(_pipeline_task_scheduler->start());

    auto tg_queue = std::make_shared<pipeline::TaskGroupTaskQueue>(executors_size);
    auto tg_b_scheduler = std::make_shared<pipeline::BlockedTaskScheduler>(tg_queue);
    _pipeline_task_group_scheduler = new pipeline::TaskScheduler(this, tg_b_scheduler, tg_queue);
    RETURN_IF_ERROR(_pipeline_task_group_scheduler->start());

    return Status::OK();
}

Status ExecEnv::_init_mem_env() {
    bool is_percent = false;
    std::stringstream ss;
    // 1. init mem tracker
    init_mem_tracker();
    thread_context()->thread_mem_tracker_mgr->init();
#if defined(USE_MEM_TRACKER) && !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && \
        !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
    init_hook();
#endif

    // 2. init buffer pool
    if (!BitUtil::IsPowerOf2(config::min_buffer_size)) {
        ss << "Config min_buffer_size must be a power-of-two: " << config::min_buffer_size;
        return Status::InternalError(ss.str());
    }

    // 3. init storage page cache
    int64_t storage_cache_limit =
            ParseUtil::parse_mem_spec(config::storage_page_cache_limit, MemInfo::mem_limit(),
                                      MemInfo::physical_mem(), &is_percent);
    while (!is_percent && storage_cache_limit > MemInfo::mem_limit() / 2) {
        storage_cache_limit = storage_cache_limit / 2;
    }
    int32_t index_percentage = config::index_page_cache_percentage;
    uint32_t num_shards = config::storage_page_cache_shard_size;
    if ((num_shards & (num_shards - 1)) != 0) {
        int old_num_shards = num_shards;
        num_shards = BitUtil::RoundUpToPowerOfTwo(num_shards);
        LOG(WARNING) << "num_shards should be power of two, but got " << old_num_shards
                     << ". Rounded up to " << num_shards
                     << ". Please modify the 'storage_page_cache_shard_size' parameter in your "
                        "conf file to be a power of two for better performance.";
    }
    int64_t pk_storage_page_cache_limit =
            ParseUtil::parse_mem_spec(config::pk_storage_page_cache_limit, MemInfo::mem_limit(),
                                      MemInfo::physical_mem(), &is_percent);
    while (!is_percent && pk_storage_page_cache_limit > MemInfo::mem_limit() / 2) {
        pk_storage_page_cache_limit = storage_cache_limit / 2;
    }
    StoragePageCache::create_global_cache(storage_cache_limit, index_percentage,
                                          pk_storage_page_cache_limit, num_shards);
    LOG(INFO) << "Storage page cache memory limit: "
              << PrettyPrinter::print(storage_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::storage_page_cache_limit;

    // Init row cache
    int64_t row_cache_mem_limit =
            ParseUtil::parse_mem_spec(config::row_cache_mem_limit, MemInfo::mem_limit(),
                                      MemInfo::physical_mem(), &is_percent);
    while (!is_percent && row_cache_mem_limit > MemInfo::mem_limit() / 2) {
        // Reason same as buffer_pool_limit
        row_cache_mem_limit = row_cache_mem_limit / 2;
    }
    RowCache::create_global_cache(row_cache_mem_limit);
    LOG(INFO) << "Row cache memory limit: "
              << PrettyPrinter::print(row_cache_mem_limit, TUnit::BYTES)
              << ", origin config value: " << config::row_cache_mem_limit;

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

    SchemaCache::create_global_instance(config::schema_cache_capacity);

    // use memory limit
    int64_t inverted_index_cache_limit =
            ParseUtil::parse_mem_spec(config::inverted_index_searcher_cache_limit,
                                      MemInfo::mem_limit(), MemInfo::physical_mem(), &is_percent);
    while (!is_percent && inverted_index_cache_limit > MemInfo::mem_limit() / 2) {
        // Reason same as buffer_pool_limit
        inverted_index_cache_limit = inverted_index_cache_limit / 2;
    }
    InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit);
    LOG(INFO) << "Inverted index searcher cache memory limit: "
              << PrettyPrinter::print(inverted_index_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::inverted_index_searcher_cache_limit;

    // use memory limit
    int64_t inverted_index_query_cache_limit =
            ParseUtil::parse_mem_spec(config::inverted_index_query_cache_limit,
                                      MemInfo::mem_limit(), MemInfo::physical_mem(), &is_percent);
    while (!is_percent && inverted_index_query_cache_limit > MemInfo::mem_limit() / 2) {
        // Reason same as buffer_pool_limit
        inverted_index_query_cache_limit = inverted_index_query_cache_limit / 2;
    }
    InvertedIndexQueryCache::create_global_cache(inverted_index_query_cache_limit, 10);
    LOG(INFO) << "Inverted index query match cache memory limit: "
              << PrettyPrinter::print(inverted_index_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::inverted_index_query_cache_limit;

    // 4. init other managers
    RETURN_IF_ERROR(_block_spill_mgr->init());

    // 5. init chunk allocator
    if (!BitUtil::IsPowerOf2(config::min_chunk_reserved_bytes)) {
        ss << "Config min_chunk_reserved_bytes must be a power-of-two: "
           << config::min_chunk_reserved_bytes;
        return Status::InternalError(ss.str());
    }

    int64_t chunk_reserved_bytes_limit =
            ParseUtil::parse_mem_spec(config::chunk_reserved_bytes_limit, MemInfo::mem_limit(),
                                      MemInfo::physical_mem(), &is_percent);
    chunk_reserved_bytes_limit =
            BitUtil::RoundDown(chunk_reserved_bytes_limit, config::min_chunk_reserved_bytes);
    ChunkAllocator::init_instance(chunk_reserved_bytes_limit);
    LOG(INFO) << "Chunk allocator memory limit: "
              << PrettyPrinter::print(chunk_reserved_bytes_limit, TUnit::BYTES)
              << ", origin config value: " << config::chunk_reserved_bytes_limit;
    return Status::OK();
}

void ExecEnv::init_mem_tracker() {
    _orphan_mem_tracker =
            std::make_shared<MemTrackerLimiter>(MemTrackerLimiter::Type::GLOBAL, "Orphan");
    _orphan_mem_tracker_raw = _orphan_mem_tracker.get();
    _experimental_mem_tracker = std::make_shared<MemTrackerLimiter>(
            MemTrackerLimiter::Type::EXPERIMENTAL, "ExperimentalSet");
    _page_no_cache_mem_tracker =
            std::make_shared<MemTracker>("PageNoCache", _orphan_mem_tracker_raw);
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
    SAFE_DELETE(_load_channel_mgr);
    SAFE_DELETE(_broker_mgr);
    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_pipeline_task_scheduler);
    SAFE_DELETE(_pipeline_task_group_scheduler);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_result_queue_mgr);
    SAFE_DELETE(_routine_load_task_executor);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_heartbeat_flags);
    SAFE_DELETE(_scanner_scheduler);
    SAFE_DELETE(_file_meta_cache);
    // Master Info is a thrift object, it could be the last one to deconstruct.
    // Master info should be deconstruct later than fragment manager, because fragment will
    // access master_info.backend id to access some info. If there is a running query and master
    // info is deconstructed then BE process will core at coordinator back method in fragment mgr.
    SAFE_DELETE(_master_info);

    _send_batch_thread_pool.reset(nullptr);
    _buffered_reader_prefetch_thread_pool.reset(nullptr);
    _send_report_thread_pool.reset(nullptr);
    _join_node_thread_pool.reset(nullptr);
    _serial_download_cache_thread_token.reset(nullptr);
    _download_cache_thread_pool.reset(nullptr);

    _is_init = false;
}

void ExecEnv::destroy(ExecEnv* env) {
    env->_destroy();
}

} // namespace doris
