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
#include <common/multi_version.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <sys/resource.h>

#include <cerrno> // IWYU pragma: keep
#include <cstdint>
#include <cstdlib>
#include <cstring>
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
#include "io/cache/block/block_file_cache_factory.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/s3_file_bufferpool.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/schema_cache.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema_cache.h"
#include "olap/wal_manager.h"
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
#include "runtime/block_spill_manager.h"
#include "runtime/broker_mgr.h"
#include "runtime/cache/result_cache.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/group_commit_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/task_group/task_group_manager.h"
#include "runtime/thread_context.h"
#include "runtime/user_function_cache.h"
#include "runtime/workload_management/workload_sched_policy_mgr.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/point_query_executor.h"
#include "util/bfd_parser.h"
#include "util/bit_util.h"
#include "util/brpc_client_cache.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/timezone_utils.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/sink/delta_writer_v2_pool.h"
#include "vec/sink/load_stream_stub_pool.h"

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

static void init_doris_metrics(const std::vector<StorePath>& store_paths) {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    std::vector<std::string> paths;
    for (const auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    if (init_system_metrics) {
        auto st = DiskInfo::get_disk_devices(paths, &disk_devices);
        if (!st.ok()) {
            LOG(WARNING) << "get disk devices failed, status=" << st;
            return;
        }
        st = get_inet_interfaces(&network_interfaces, BackendOptions::is_bind_ipv6());
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, status=" << st;
            return;
        }
    }
    DorisMetrics::instance()->initialize(init_system_metrics, disk_devices, network_interfaces);
}

Status ExecEnv::init(ExecEnv* env, const std::vector<StorePath>& store_paths,
                     const std::set<std::string>& broken_paths) {
    return env->_init(store_paths, broken_paths);
}

Status ExecEnv::_init(const std::vector<StorePath>& store_paths,
                      const std::set<std::string>& broken_paths) {
    //Only init once before be destroyed
    if (ready()) {
        return Status::OK();
    }
    init_doris_metrics(store_paths);
    _store_paths = store_paths;
    _user_function_cache = new UserFunctionCache();
    static_cast<void>(_user_function_cache->init(doris::config::user_function_dir));
    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _vstream_mgr = new doris::vectorized::VDataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);

    TimezoneUtils::load_timezone_names();
    TimezoneUtils::load_timezones_to_cache();

    static_cast<void>(ThreadPoolBuilder("SendBatchThreadPool")
                              .set_min_threads(config::send_batch_thread_pool_thread_num)
                              .set_max_threads(config::send_batch_thread_pool_thread_num)
                              .set_max_queue_size(config::send_batch_thread_pool_queue_size)
                              .build(&_send_batch_thread_pool));

    static_cast<void>(ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                              .set_min_threads(16)
                              .set_max_threads(64)
                              .build(&_buffered_reader_prefetch_thread_pool));

    static_cast<void>(ThreadPoolBuilder("S3FileUploadThreadPool")
                              .set_min_threads(16)
                              .set_max_threads(64)
                              .build(&_s3_file_upload_thread_pool));

    // min num equal to fragment pool's min num
    // max num is useless because it will start as many as requested in the past
    // queue size is useless because the max thread num is very large
    static_cast<void>(ThreadPoolBuilder("SendReportThreadPool")
                              .set_min_threads(config::fragment_pool_thread_num_min)
                              .set_max_threads(std::numeric_limits<int>::max())
                              .set_max_queue_size(config::fragment_pool_queue_size)
                              .build(&_send_report_thread_pool));

    static_cast<void>(ThreadPoolBuilder("JoinNodeThreadPool")
                              .set_min_threads(config::fragment_pool_thread_num_min)
                              .set_max_threads(std::numeric_limits<int>::max())
                              .set_max_queue_size(config::fragment_pool_queue_size)
                              .build(&_join_node_thread_pool));
    static_cast<void>(ThreadPoolBuilder("LazyReleaseMemoryThreadPool")
                              .set_min_threads(1)
                              .set_max_threads(1)
                              .set_max_queue_size(1000000)
                              .build(&_lazy_release_obj_pool));

    init_file_cache_factory();
    RETURN_IF_ERROR(init_pipeline_task_scheduler());
    _task_group_manager = new taskgroup::TaskGroupManager();
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
    _block_spill_mgr = new BlockSpillManager(store_paths);
    _group_commit_mgr = new GroupCommitMgr(this);
    _memtable_memory_limiter = std::make_unique<MemTableMemoryLimiter>();
    _load_stream_stub_pool = std::make_unique<LoadStreamStubPool>();
    _delta_writer_v2_pool = std::make_unique<vectorized::DeltaWriterV2Pool>();
    _wal_manager = WalManager::create_shared(this, config::group_commit_wal_path);

    _backend_client_cache->init_metrics("backend");
    _frontend_client_cache->init_metrics("frontend");
    _broker_client_cache->init_metrics("broker");
    static_cast<void>(_result_mgr->init());
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "Load path mgr init failed. " << status;
        return status;
    }
    _broker_mgr->init();
    static_cast<void>(_small_file_mgr->init());
    status = _scanner_scheduler->init(this);
    if (!status.ok()) {
        LOG(ERROR) << "Scanner scheduler init failed. " << status;
        return status;
    }

    static_cast<void>(_init_mem_env());

    RETURN_IF_ERROR(_memtable_memory_limiter->init(MemInfo::mem_limit()));
    RETURN_IF_ERROR(_load_channel_mgr->init(MemInfo::mem_limit()));
    RETURN_IF_ERROR(_wal_manager->init());
    _heartbeat_flags = new HeartbeatFlags();
    _register_metrics();

    _tablet_schema_cache = TabletSchemaCache::create_global_schema_cache();
    _tablet_schema_cache->start();

    // S3 buffer pool
    _s3_buffer_pool = new io::S3FileBufferPool();
    _s3_buffer_pool->init(config::s3_write_buffer_whole_size, config::s3_write_buffer_size,
                          this->s3_file_upload_thread_pool());

    // Storage engine
    doris::EngineOptions options;
    options.store_paths = store_paths;
    options.broken_paths = broken_paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    _storage_engine = new StorageEngine(options);
    auto st = _storage_engine->open();
    if (!st.ok()) {
        LOG(ERROR) << "Lail to open StorageEngine, res=" << st;
        return st;
    }
    _storage_engine->set_heartbeat_flags(this->heartbeat_flags());
    if (st = _storage_engine->start_bg_threads(); !st.ok()) {
        LOG(ERROR) << "Failed to starge bg threads of storage engine, res=" << st;
        return st;
    }

    _workload_sched_mgr = new WorkloadSchedPolicyMgr();
    _workload_sched_mgr->start(this);

    _s_ready = true;

    return Status::OK();
}

Status ExecEnv::init_pipeline_task_scheduler() {
    auto executors_size = config::pipeline_executor_size;
    if (executors_size <= 0) {
        executors_size = CpuInfo::num_cores();
    }

    // TODO pipeline task group combie two blocked schedulers.
    auto t_queue = std::make_shared<pipeline::MultiCoreTaskQueue>(executors_size);
    _without_group_block_scheduler =
            std::make_shared<pipeline::BlockedTaskScheduler>("PipeNoGSchePool");
    _without_group_task_scheduler = new pipeline::TaskScheduler(
            this, _without_group_block_scheduler, t_queue, "PipeNoGSchePool", nullptr);
    RETURN_IF_ERROR(_without_group_task_scheduler->start());
    RETURN_IF_ERROR(_without_group_block_scheduler->start());

    auto tg_queue = std::make_shared<pipeline::TaskGroupTaskQueue>(executors_size);
    _with_group_block_scheduler = std::make_shared<pipeline::BlockedTaskScheduler>("PipeGSchePool");
    _with_group_task_scheduler = new pipeline::TaskScheduler(this, _with_group_block_scheduler,
                                                             tg_queue, "PipeGSchePool", nullptr);
    RETURN_IF_ERROR(_with_group_task_scheduler->start());
    RETURN_IF_ERROR(_with_group_block_scheduler->start());

    _global_block_scheduler = std::make_shared<pipeline::BlockedTaskScheduler>("PipeGBlockSche");
    RETURN_IF_ERROR(_global_block_scheduler->start());
    _runtime_filter_timer_queue = new doris::pipeline::RuntimeFilterTimerQueue();
    _runtime_filter_timer_queue->run();
    return Status::OK();
}

void ExecEnv::init_file_cache_factory() {
    // Load file cache before starting up daemon threads to make sure StorageEngine is read.
    if (doris::config::enable_file_cache) {
        _file_cache_factory = new io::FileCacheFactory();
        io::IFileCache::init();
        std::unordered_set<std::string> cache_path_set;
        std::vector<doris::CachePath> cache_paths;
        Status olap_res =
                doris::parse_conf_cache_paths(doris::config::file_cache_path, cache_paths);
        if (!olap_res) {
            LOG(FATAL) << "parse config file cache path failed, path="
                       << doris::config::file_cache_path;
            exit(-1);
        }

        std::unique_ptr<doris::ThreadPool> file_cache_init_pool;
        static_cast<void>(doris::ThreadPoolBuilder("FileCacheInitThreadPool")
                                  .set_min_threads(cache_paths.size())
                                  .set_max_threads(cache_paths.size())
                                  .build(&file_cache_init_pool));

        std::list<doris::Status> cache_status;
        for (auto& cache_path : cache_paths) {
            if (cache_path_set.find(cache_path.path) != cache_path_set.end()) {
                LOG(WARNING) << fmt::format("cache path {} is duplicate", cache_path.path);
                continue;
            }

            olap_res = file_cache_init_pool->submit_func(
                    [this, capture0 = cache_path.path, capture1 = cache_path.init_settings(),
                     capture2 = &(cache_status.emplace_back())] {
                        _file_cache_factory->create_file_cache(capture0, capture1, capture2);
                    });

            if (!olap_res.ok()) {
                LOG(FATAL) << "failed to init file cache, err: " << olap_res;
                exit(-1);
            }
            cache_path_set.emplace(cache_path.path);
        }

        file_cache_init_pool->wait();
        for (const auto& status : cache_status) {
            if (!status.ok()) {
                LOG(FATAL) << "failed to init file cache, err: " << status;
                exit(-1);
            }
        }
    }
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

    if (!BitUtil::IsPowerOf2(config::min_buffer_size)) {
        ss << "Config min_buffer_size must be a power-of-two: " << config::min_buffer_size;
        return Status::InternalError(ss.str());
    }

    _dummy_lru_cache = std::make_shared<DummyLRUCache>();

    _cache_manager = CacheManager::create_global_instance();

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
    if (storage_cache_limit < num_shards * 2) {
        LOG(WARNING) << "storage_cache_limit(" << storage_cache_limit << ") less than num_shards("
                     << num_shards
                     << ") * 2, cache capacity will be 0, continuing to use "
                        "cache will only have negative effects, will be disabled.";
    }
    int64_t pk_storage_page_cache_limit =
            ParseUtil::parse_mem_spec(config::pk_storage_page_cache_limit, MemInfo::mem_limit(),
                                      MemInfo::physical_mem(), &is_percent);
    while (!is_percent && pk_storage_page_cache_limit > MemInfo::mem_limit() / 2) {
        pk_storage_page_cache_limit = storage_cache_limit / 2;
    }
    _storage_page_cache = StoragePageCache::create_global_cache(
            storage_cache_limit, index_percentage, pk_storage_page_cache_limit, num_shards);
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
    _row_cache = RowCache::create_global_cache(row_cache_mem_limit);
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
    int64_t segment_cache_capacity = config::segment_cache_capacity;
    if (segment_cache_capacity < 0 || segment_cache_capacity > fd_number * 2 / 5) {
        segment_cache_capacity = fd_number * 2 / 5;
    }
    LOG(INFO) << "segment_cache_capacity <= fd_number * 2 / 5, fd_number: " << fd_number
              << " segment_cache_capacity: " << segment_cache_capacity;
    _segment_loader = new SegmentLoader(segment_cache_capacity);

    _schema_cache = new SchemaCache(config::schema_cache_capacity);

    _file_meta_cache = new FileMetaCache(config::max_external_file_meta_cache_num);

    _lookup_connection_cache = LookupConnectionCache::create_global_instance(
            config::lookup_connection_cache_bytes_limit);

    // use memory limit
    int64_t inverted_index_cache_limit =
            ParseUtil::parse_mem_spec(config::inverted_index_searcher_cache_limit,
                                      MemInfo::mem_limit(), MemInfo::physical_mem(), &is_percent);
    while (!is_percent && inverted_index_cache_limit > MemInfo::mem_limit() / 2) {
        // Reason same as buffer_pool_limit
        inverted_index_cache_limit = inverted_index_cache_limit / 2;
    }
    _inverted_index_searcher_cache =
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
    _inverted_index_query_cache =
            InvertedIndexQueryCache::create_global_cache(inverted_index_query_cache_limit);
    LOG(INFO) << "Inverted index query match cache memory limit: "
              << PrettyPrinter::print(inverted_index_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::inverted_index_query_cache_limit;

    RETURN_IF_ERROR(_block_spill_mgr->init());
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
    _brpc_iobuf_block_memory_tracker =
            std::make_shared<MemTracker>("IOBufBlockMemory", _orphan_mem_tracker_raw);
}

void ExecEnv::_register_metrics() {
    REGISTER_HOOK_METRIC(send_batch_thread_pool_thread_num,
                         [this]() { return _send_batch_thread_pool->num_threads(); });

    REGISTER_HOOK_METRIC(send_batch_thread_pool_queue_size,
                         [this]() { return _send_batch_thread_pool->get_queue_size(); });
}

void ExecEnv::_deregister_metrics() {
    DEREGISTER_HOOK_METRIC(scanner_thread_pool_queue_size);
    DEREGISTER_HOOK_METRIC(send_batch_thread_pool_thread_num);
    DEREGISTER_HOOK_METRIC(send_batch_thread_pool_queue_size);
}

// TODO(zhiqiang): Need refactor all thread pool. Each thread pool must have a Stop method.
// We need to stop all threads before releasing resource.
void ExecEnv::destroy() {
    //Only destroy once after init
    if (!ready()) {
        return;
    }
    // Memory barrier to prevent other threads from accessing destructed resources
    _s_ready = false;

    SAFE_STOP(_wal_manager);
    SAFE_STOP(_tablet_schema_cache);
    SAFE_STOP(_load_channel_mgr);
    SAFE_STOP(_scanner_scheduler);
    SAFE_STOP(_broker_mgr);
    SAFE_STOP(_load_path_mgr);
    SAFE_STOP(_result_mgr);
    SAFE_STOP(_group_commit_mgr);
    // _routine_load_task_executor should be stopped before _new_load_stream_mgr.
    SAFE_STOP(_routine_load_task_executor);
    // stop workload scheduler
    SAFE_STOP(_workload_sched_mgr);
    // stop pipline step 1, non-cgroup execution
    SAFE_SHUTDOWN(_without_group_block_scheduler.get());
    SAFE_STOP(_without_group_task_scheduler);
    SAFE_SHUTDOWN(_with_group_block_scheduler.get());
    SAFE_STOP(_with_group_task_scheduler);
    // stop pipline step 2, cgroup execution
    SAFE_SHUTDOWN(_global_block_scheduler.get());
    SAFE_STOP(_task_group_manager);

    SAFE_STOP(_external_scan_context_mgr);
    SAFE_STOP(_fragment_mgr);
    SAFE_STOP(_runtime_filter_timer_queue);
    // NewLoadStreamMgr should be destoried before storage_engine & after fragment_mgr stopped.
    _new_load_stream_mgr.reset();
    _stream_load_executor.reset();
    _memtable_memory_limiter.reset();
    _delta_writer_v2_pool.reset();
    _load_stream_stub_pool.reset();
    SAFE_STOP(_storage_engine);
    SAFE_SHUTDOWN(_buffered_reader_prefetch_thread_pool);
    SAFE_SHUTDOWN(_s3_file_upload_thread_pool);
    SAFE_SHUTDOWN(_join_node_thread_pool);
    SAFE_SHUTDOWN(_lazy_release_obj_pool);
    SAFE_SHUTDOWN(_send_report_thread_pool);
    SAFE_SHUTDOWN(_send_batch_thread_pool);

    // Free resource after threads are stopped.
    // Some threads are still running, like threads created by _new_load_stream_mgr ...
    _wal_manager.reset();
    SAFE_DELETE(_s3_buffer_pool);
    SAFE_DELETE(_tablet_schema_cache);
    _deregister_metrics();
    SAFE_DELETE(_load_channel_mgr);

    // shared_ptr maybe no need to be reset
    // _brpc_iobuf_block_memory_tracker.reset();
    // _page_no_cache_mem_tracker.reset();
    // _experimental_mem_tracker.reset();
    // _orphan_mem_tracker.reset();

    SAFE_DELETE(_block_spill_mgr);
    SAFE_DELETE(_inverted_index_query_cache);
    SAFE_DELETE(_inverted_index_searcher_cache);
    SAFE_DELETE(_lookup_connection_cache);
    SAFE_DELETE(_schema_cache);
    SAFE_DELETE(_segment_loader);
    SAFE_DELETE(_row_cache);

    // StorageEngine must be destoried before _page_no_cache_mem_tracker.reset
    // StorageEngine must be destoried before _cache_manager destory
    SAFE_DELETE(_storage_engine);

    // _scanner_scheduler must be desotried before _storage_page_cache
    SAFE_DELETE(_scanner_scheduler);
    // _storage_page_cache must be destoried before _cache_manager
    SAFE_DELETE(_storage_page_cache);

    SAFE_DELETE(_small_file_mgr);
    SAFE_DELETE(_broker_mgr);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_file_meta_cache);
    SAFE_DELETE(_group_commit_mgr);
    SAFE_DELETE(_routine_load_task_executor);
    // _stream_load_executor
    SAFE_DELETE(_function_client_cache);
    SAFE_DELETE(_internal_client_cache);

    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_result_cache);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_workload_sched_mgr);
    SAFE_DELETE(_task_group_manager);
    SAFE_DELETE(_with_group_task_scheduler);
    SAFE_DELETE(_without_group_task_scheduler);
    SAFE_DELETE(_file_cache_factory);
    SAFE_DELETE(_runtime_filter_timer_queue);
    // TODO(zhiqiang): Maybe we should call shutdown before release thread pool?
    _join_node_thread_pool.reset(nullptr);
    _lazy_release_obj_pool.reset(nullptr);
    _send_report_thread_pool.reset(nullptr);
    _buffered_reader_prefetch_thread_pool.reset(nullptr);
    _s3_file_upload_thread_pool.reset(nullptr);
    _send_batch_thread_pool.reset(nullptr);

    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_queue_mgr);

    SAFE_DELETE(_vstream_mgr);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_user_function_cache);

    // cache_manager must be destoried after _inverted_index_query_cache
    // https://github.com/apache/doris/issues/24082#issuecomment-1712544039
    SAFE_DELETE(_cache_manager);

    // _heartbeat_flags must be destoried after staroge engine
    SAFE_DELETE(_heartbeat_flags);

    // Master Info is a thrift object, it could be the last one to deconstruct.
    // Master info should be deconstruct later than fragment manager, because fragment will
    // access master_info.backend id to access some info. If there is a running query and master
    // info is deconstructed then BE process will core at coordinator back method in fragment mgr.
    SAFE_DELETE(_master_info);

    LOG(INFO) << "Doris exec envorinment is destoried.";
}

} // namespace doris
