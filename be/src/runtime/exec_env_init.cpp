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
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_stream_load_executor.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/cloud_warm_up_manager.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_downloader.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/fs_file_cache_storage.h"
#include "io/fs/file_meta_cache.h"
#include "io/fs/local_file_reader.h"
#include "olap/memtable_memory_limiter.h"
#include "olap/olap_define.h"
#include "olap/options.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/schema_cache.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema_cache.h"
#include "olap/wal/wal_manager.h"
#include "pipeline/pipeline_tracing.h"
#include "pipeline/query_cache/query_cache.h"
#include "pipeline/task_queue.h"
#include "pipeline/task_scheduler.h"
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
#include "runtime/load_stream_mgr.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/runtime_query_statistics_mgr.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/new_load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_context.h"
#include "runtime/user_function_cache.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "runtime/workload_management/workload_sched_policy_mgr.h"
#include "service/backend_options.h"
#include "service/backend_service.h"
#include "service/point_query_executor.h"
#include "util/bfd_parser.h"
#include "util/bit_util.h"
#include "util/brpc_client_cache.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/dns_cache.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/threadpool.h"
#include "util/thrift_rpc_helper.h"
#include "util/timezone_utils.h"
#include "vec/exec/format/orc/orc_memory_pool.h"
#include "vec/exec/format/parquet/arrow_memory_pool.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/sink/delta_writer_v2_pool.h"
#include "vec/sink/load_stream_map_pool.h"
#include "vec/spill/spill_stream_manager.h"

#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include "runtime/memory/tcmalloc_hook.h"
#endif

// Used for unit test
namespace {
std::once_flag flag;
std::unique_ptr<doris::ThreadPool> non_block_close_thread_pool;
void init_threadpool_for_test() {
    static_cast<void>(doris::ThreadPoolBuilder("NonBlockCloseThreadPool")
                              .set_min_threads(12)
                              .set_max_threads(48)
                              .build(&non_block_close_thread_pool));
}

[[maybe_unused]] doris::ThreadPool* get_non_block_close_thread_pool() {
    std::call_once(flag, init_threadpool_for_test);
    return non_block_close_thread_pool.get();
}
} // namespace

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

// Used to calculate the num of min thread and max thread based on the passed config
static pair<size_t, size_t> get_num_threads(size_t min_num, size_t max_num) {
    auto num_cores = doris::CpuInfo::num_cores();
    min_num = (min_num == 0) ? num_cores : min_num;
    max_num = (max_num == 0) ? num_cores : max_num;
    auto factor = max_num / min_num;
    min_num = std::min(num_cores * factor, min_num);
    max_num = std::min(min_num * factor, max_num);
    return {min_num, max_num};
}

ThreadPool* ExecEnv::non_block_close_thread_pool() {
#ifdef BE_TEST
    return get_non_block_close_thread_pool();
#else
    return _non_block_close_thread_pool.get();
#endif
}

Status ExecEnv::init(ExecEnv* env, const std::vector<StorePath>& store_paths,
                     const std::vector<StorePath>& spill_store_paths,
                     const std::set<std::string>& broken_paths) {
    return env->_init(store_paths, spill_store_paths, broken_paths);
}

Status ExecEnv::_init(const std::vector<StorePath>& store_paths,
                      const std::vector<StorePath>& spill_store_paths,
                      const std::set<std::string>& broken_paths) {
    //Only init once before be destroyed
    if (ready()) {
        return Status::OK();
    }
    std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>> spill_store_map;
    for (const auto& spill_path : spill_store_paths) {
        spill_store_map.emplace(spill_path.path, std::make_unique<vectorized::SpillDataDir>(
                                                         spill_path.path, spill_path.capacity_bytes,
                                                         spill_path.storage_medium));
    }
    init_doris_metrics(store_paths);
    _store_paths = store_paths;
    _tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(_store_paths);
    RETURN_IF_ERROR(_tmp_file_dirs->init());
    _user_function_cache = new UserFunctionCache();
    static_cast<void>(_user_function_cache->init(doris::config::user_function_dir));
    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _vstream_mgr = new doris::vectorized::VDataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);

    TimezoneUtils::load_timezones_to_cache();

    static_cast<void>(ThreadPoolBuilder("SendBatchThreadPool")
                              .set_min_threads(config::send_batch_thread_pool_thread_num)
                              .set_max_threads(config::send_batch_thread_pool_thread_num)
                              .set_max_queue_size(config::send_batch_thread_pool_queue_size)
                              .build(&_send_batch_thread_pool));

    auto [buffered_reader_min_threads, buffered_reader_max_threads] =
            get_num_threads(config::num_buffered_reader_prefetch_thread_pool_min_thread,
                            config::num_buffered_reader_prefetch_thread_pool_max_thread);
    static_cast<void>(ThreadPoolBuilder("BufferedReaderPrefetchThreadPool")
                              .set_min_threads(buffered_reader_min_threads)
                              .set_max_threads(buffered_reader_max_threads)
                              .build(&_buffered_reader_prefetch_thread_pool));

    static_cast<void>(ThreadPoolBuilder("SendTableStatsThreadPool")
                              .set_min_threads(8)
                              .set_max_threads(32)
                              .build(&_send_table_stats_thread_pool));

    auto [s3_file_upload_min_threads, s3_file_upload_max_threads] =
            get_num_threads(config::num_s3_file_upload_thread_pool_min_thread,
                            config::num_s3_file_upload_thread_pool_max_thread);
    static_cast<void>(ThreadPoolBuilder("S3FileUploadThreadPool")
                              .set_min_threads(s3_file_upload_min_threads)
                              .set_max_threads(s3_file_upload_max_threads)
                              .build(&_s3_file_upload_thread_pool));

    // min num equal to fragment pool's min num
    // max num is useless because it will start as many as requested in the past
    // queue size is useless because the max thread num is very large
    static_cast<void>(ThreadPoolBuilder("LazyReleaseMemoryThreadPool")
                              .set_min_threads(1)
                              .set_max_threads(1)
                              .set_max_queue_size(1000000)
                              .build(&_lazy_release_obj_pool));
    static_cast<void>(ThreadPoolBuilder("NonBlockCloseThreadPool")
                              .set_min_threads(config::min_nonblock_close_thread_num)
                              .set_max_threads(config::max_nonblock_close_thread_num)
                              .build(&_non_block_close_thread_pool));
    static_cast<void>(ThreadPoolBuilder("S3FileSystemThreadPool")
                              .set_min_threads(config::min_s3_file_system_thread_num)
                              .set_max_threads(config::max_s3_file_system_thread_num)
                              .build(&_s3_file_system_thread_pool));

    // NOTE: runtime query statistics mgr could be visited by query and daemon thread
    // so it should be created before all query begin and deleted after all query and daemon thread stoppped
    _runtime_query_statistics_mgr = new RuntimeQueryStatisticsMgr();
    CgroupCpuCtl::init_doris_cgroup_path();
    _file_cache_factory = new io::FileCacheFactory();
    std::vector<doris::CachePath> cache_paths;
    init_file_cache_factory(cache_paths);
    doris::io::BeConfDataDirReader::init_be_conf_data_dir(store_paths, spill_store_paths,
                                                          cache_paths);

    _pipeline_tracer_ctx = std::make_unique<pipeline::PipelineTracerContext>(); // before query
    RETURN_IF_ERROR(init_pipeline_task_scheduler());
    _workload_group_manager = new WorkloadGroupMgr();
    _scanner_scheduler = new doris::vectorized::ScannerScheduler();
    _fragment_mgr = new FragmentMgr(this);
    _result_cache = new ResultCache(config::query_cache_max_size_mb,
                                    config::query_cache_elasticity_size_mb);
    _master_info = new TMasterInfo();
    _load_path_mgr = new LoadPathMgr(this);
    _bfd_parser = BfdParser::create();
    _broker_mgr = new BrokerMgr(this);
    _load_channel_mgr = new LoadChannelMgr();
    auto num_flush_threads = std::min(
            _store_paths.size() * config::flush_thread_num_per_store,
            static_cast<size_t>(CpuInfo::num_cores()) * config::max_flush_thread_num_per_cpu);
    _load_stream_mgr = std::make_unique<LoadStreamMgr>(num_flush_threads);
    _new_load_stream_mgr = NewLoadStreamMgr::create_shared();
    _internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
    _streaming_client_cache =
            new BrpcClientCache<PBackendService_Stub>("baidu_std", "single", "streaming");
    _function_client_cache =
            new BrpcClientCache<PFunctionService_Stub>(config::function_service_protocol);
    if (config::is_cloud_mode()) {
        _stream_load_executor = std::make_shared<CloudStreamLoadExecutor>(this);
    } else {
        _stream_load_executor = StreamLoadExecutor::create_shared(this);
    }
    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    RETURN_IF_ERROR(_routine_load_task_executor->init(MemInfo::mem_limit()));
    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _group_commit_mgr = new GroupCommitMgr(this);
    _memtable_memory_limiter = std::make_unique<MemTableMemoryLimiter>();
    _load_stream_map_pool = std::make_unique<LoadStreamMapPool>();
    _delta_writer_v2_pool = std::make_unique<vectorized::DeltaWriterV2Pool>();
    _file_cache_open_fd_cache = std::make_unique<io::FDCache>();
    _wal_manager = WalManager::create_shared(this, config::group_commit_wal_path);
    _dns_cache = new DNSCache();
    _write_cooldown_meta_executors = std::make_unique<WriteCooldownMetaExecutors>();
    _spill_stream_mgr = new vectorized::SpillStreamManager(std::move(spill_store_map));
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

    RETURN_IF_ERROR(_init_mem_env());

    RETURN_IF_ERROR(_memtable_memory_limiter->init(MemInfo::mem_limit()));
    RETURN_IF_ERROR(_load_channel_mgr->init(MemInfo::mem_limit()));
    RETURN_IF_ERROR(_wal_manager->init());
    _heartbeat_flags = new HeartbeatFlags();
    _register_metrics();

    _tablet_schema_cache =
            TabletSchemaCache::create_global_schema_cache(config::tablet_schema_cache_capacity);

    // Storage engine
    doris::EngineOptions options;
    options.store_paths = store_paths;
    options.broken_paths = broken_paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    if (config::is_cloud_mode()) {
        std::cout << "start BE in cloud mode, cloud_unique_id: " << config::cloud_unique_id
                  << ", meta_service_endpoint: " << config::meta_service_endpoint << std::endl;
        _storage_engine = std::make_unique<CloudStorageEngine>(options.backend_uid);
    } else {
        std::cout << "start BE in local mode" << std::endl;
        _storage_engine = std::make_unique<StorageEngine>(options);
    }
    auto st = _storage_engine->open();
    if (!st.ok()) {
        LOG(ERROR) << "Fail to open StorageEngine, res=" << st;
        return st;
    }
    _storage_engine->set_heartbeat_flags(this->heartbeat_flags());
    if (st = _storage_engine->start_bg_threads(); !st.ok()) {
        LOG(ERROR) << "Failed to starge bg threads of storage engine, res=" << st;
        return st;
    }

    _workload_sched_mgr = new WorkloadSchedPolicyMgr();
    _workload_sched_mgr->start(this);

    RETURN_IF_ERROR(_spill_stream_mgr->init());
    _runtime_query_statistics_mgr->start_report_thread();
    _s_ready = true;

    return Status::OK();
}

Status ExecEnv::init_pipeline_task_scheduler() {
    auto executors_size = config::pipeline_executor_size;
    if (executors_size <= 0) {
        executors_size = CpuInfo::num_cores();
    }

    LOG_INFO("pipeline executors_size set ").tag("size", executors_size);
    // TODO pipeline workload group combie two blocked schedulers.
    auto t_queue = std::make_shared<pipeline::MultiCoreTaskQueue>(executors_size);
    _without_group_task_scheduler =
            new pipeline::TaskScheduler(this, t_queue, "PipeNoGSchePool", nullptr);
    RETURN_IF_ERROR(_without_group_task_scheduler->start());

    _runtime_filter_timer_queue = new doris::pipeline::RuntimeFilterTimerQueue();
    _runtime_filter_timer_queue->run();
    return Status::OK();
}

void ExecEnv::init_file_cache_factory(std::vector<doris::CachePath>& cache_paths) {
    // Load file cache before starting up daemon threads to make sure StorageEngine is read.
    if (doris::config::enable_file_cache) {
        if (config::file_cache_each_block_size > config::s3_write_buffer_size ||
            config::s3_write_buffer_size % config::file_cache_each_block_size != 0) {
            LOG_FATAL(
                    "The config file_cache_each_block_size {} must less than or equal to config "
                    "s3_write_buffer_size {} and config::s3_write_buffer_size % "
                    "config::file_cache_each_block_size must be zero",
                    config::file_cache_each_block_size, config::s3_write_buffer_size);
            exit(-1);
        }
        std::unordered_set<std::string> cache_path_set;
        Status rest = doris::parse_conf_cache_paths(doris::config::file_cache_path, cache_paths);
        if (!rest) {
            LOG(FATAL) << "parse config file cache path failed, path="
                       << doris::config::file_cache_path;
            exit(-1);
        }
        std::vector<std::thread> file_cache_init_threads;

        std::list<doris::Status> cache_status;
        for (auto& cache_path : cache_paths) {
            if (cache_path_set.find(cache_path.path) != cache_path_set.end()) {
                LOG(WARNING) << fmt::format("cache path {} is duplicate", cache_path.path);
                continue;
            }

            file_cache_init_threads.emplace_back([&, status = &cache_status.emplace_back()]() {
                *status = doris::io::FileCacheFactory::instance()->create_file_cache(
                        cache_path.path, cache_path.init_settings());
            });

            cache_path_set.emplace(cache_path.path);
        }

        for (std::thread& thread : file_cache_init_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
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
    int64_t segment_cache_fd_limit = fd_number / 100 * config::segment_cache_fd_percentage;
    if (segment_cache_capacity < 0 || segment_cache_capacity > segment_cache_fd_limit) {
        segment_cache_capacity = segment_cache_fd_limit;
    }

    int64_t segment_cache_mem_limit =
            MemInfo::mem_limit() / 100 * config::segment_cache_memory_percentage;

    _segment_loader = new SegmentLoader(segment_cache_mem_limit, segment_cache_capacity);
    LOG(INFO) << "segment_cache_capacity <= fd_number * 1 / 5, fd_number: " << fd_number
              << " segment_cache_capacity: " << segment_cache_capacity
              << " min_segment_cache_mem_limit " << segment_cache_mem_limit;

    _schema_cache = new SchemaCache(config::schema_cache_capacity);

    size_t block_file_cache_fd_cache_size =
            std::min((uint64_t)config::file_cache_max_file_reader_cache_size, fd_number / 3);
    LOG(INFO) << "max file reader cache size is: " << block_file_cache_fd_cache_size
              << ", resource hard limit is: " << fd_number
              << ", config file_cache_max_file_reader_cache_size is: "
              << config::file_cache_max_file_reader_cache_size;
    config::file_cache_max_file_reader_cache_size = block_file_cache_fd_cache_size;

    _file_meta_cache = new FileMetaCache(config::max_external_file_meta_cache_num);

    _lookup_connection_cache =
            LookupConnectionCache::create_global_instance(config::lookup_connection_cache_capacity);

    // use memory limit
    int64_t inverted_index_cache_limit =
            ParseUtil::parse_mem_spec(config::inverted_index_searcher_cache_limit,
                                      MemInfo::mem_limit(), MemInfo::physical_mem(), &is_percent);
    while (!is_percent && inverted_index_cache_limit > MemInfo::mem_limit() / 2) {
        // Reason same as buffer_pool_limit
        inverted_index_cache_limit = inverted_index_cache_limit / 2;
    }
    _inverted_index_searcher_cache =
            InvertedIndexSearcherCache::create_global_instance(inverted_index_cache_limit, 256);
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
    _inverted_index_query_cache = InvertedIndexQueryCache::create_global_cache(
            inverted_index_query_cache_limit, config::inverted_index_query_cache_shards);
    LOG(INFO) << "Inverted index query match cache memory limit: "
              << PrettyPrinter::print(inverted_index_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::inverted_index_query_cache_limit;

    // init orc memory pool
    _orc_memory_pool = new doris::vectorized::ORCMemoryPool();
    _arrow_memory_pool = new doris::vectorized::ArrowMemoryPool();

    _query_cache = QueryCache::create_global_cache(config::query_cache_size * 1024L * 1024L);
    LOG(INFO) << "query cache memory limit: " << config::query_cache_size << "MB";

    return Status::OK();
}

void ExecEnv::init_mem_tracker() {
    mem_tracker_limiter_pool.resize(MEM_TRACKER_GROUP_NUM,
                                    TrackerLimiterGroup()); // before all mem tracker init.
    _s_tracking_memory = true;
    _orphan_mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "Orphan");
    _details_mem_tracker_set =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "DetailsTrackerSet");
    _page_no_cache_mem_tracker =
            std::make_shared<MemTracker>("PageNoCache", _details_mem_tracker_set.get());
    _brpc_iobuf_block_memory_tracker =
            std::make_shared<MemTracker>("IOBufBlockMemory", _details_mem_tracker_set.get());
    _segcompaction_mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "SegCompaction");
    _point_query_executor_mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "PointQueryExecutor");
    _query_cache_mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "QueryCache");
    _block_compression_mem_tracker = _block_compression_mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "BlockCompression");
    _rowid_storage_reader_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "RowIdStorageReader");
    _subcolumns_tree_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "SubcolumnsTree");
    _s3_file_buffer_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::GLOBAL, "S3FileBuffer");
    _stream_load_pipe_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::LOAD, "StreamLoadPipe");
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
    _wal_manager.reset();
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
    SAFE_STOP(_without_group_task_scheduler);
    // stop pipline step 2, cgroup execution
    SAFE_STOP(_workload_group_manager);

    SAFE_STOP(_external_scan_context_mgr);
    SAFE_STOP(_fragment_mgr);
    SAFE_STOP(_runtime_filter_timer_queue);
    // NewLoadStreamMgr should be destoried before storage_engine & after fragment_mgr stopped.
    _new_load_stream_mgr.reset();
    _stream_load_executor.reset();
    _memtable_memory_limiter.reset();
    _delta_writer_v2_pool.reset();
    _load_stream_map_pool.reset();
    _file_cache_open_fd_cache.reset();
    SAFE_STOP(_write_cooldown_meta_executors);

    // StorageEngine must be destoried before _page_no_cache_mem_tracker.reset and _cache_manager destory
    // shouldn't use SAFE_STOP. otherwise will lead to twice stop.
    _storage_engine.reset();

    SAFE_STOP(_spill_stream_mgr);
    if (_runtime_query_statistics_mgr) {
        _runtime_query_statistics_mgr->stop_report_thread();
    }
    SAFE_SHUTDOWN(_buffered_reader_prefetch_thread_pool);
    SAFE_SHUTDOWN(_s3_file_upload_thread_pool);
    SAFE_SHUTDOWN(_lazy_release_obj_pool);
    SAFE_SHUTDOWN(_non_block_close_thread_pool);
    SAFE_SHUTDOWN(_s3_file_system_thread_pool);
    SAFE_SHUTDOWN(_send_batch_thread_pool);

    _deregister_metrics();
    SAFE_DELETE(_load_channel_mgr);

    SAFE_DELETE(_spill_stream_mgr);
    SAFE_DELETE(_inverted_index_query_cache);
    SAFE_DELETE(_inverted_index_searcher_cache);
    SAFE_DELETE(_lookup_connection_cache);
    SAFE_DELETE(_schema_cache);
    SAFE_DELETE(_segment_loader);
    SAFE_DELETE(_row_cache);
    SAFE_DELETE(_query_cache);

    // Free resource after threads are stopped.
    // Some threads are still running, like threads created by _new_load_stream_mgr ...
    SAFE_DELETE(_tablet_schema_cache);

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
    SAFE_DELETE(_streaming_client_cache);
    SAFE_DELETE(_internal_client_cache);

    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_result_cache);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_workload_sched_mgr);
    SAFE_DELETE(_workload_group_manager);
    SAFE_DELETE(_file_cache_factory);
    SAFE_DELETE(_runtime_filter_timer_queue);
    // TODO(zhiqiang): Maybe we should call shutdown before release thread pool?
    _lazy_release_obj_pool.reset(nullptr);
    _non_block_close_thread_pool.reset(nullptr);
    _s3_file_system_thread_pool.reset(nullptr);
    _send_table_stats_thread_pool.reset(nullptr);
    _buffered_reader_prefetch_thread_pool.reset(nullptr);
    _s3_file_upload_thread_pool.reset(nullptr);
    _send_batch_thread_pool.reset(nullptr);
    _file_cache_open_fd_cache.reset(nullptr);
    _write_cooldown_meta_executors.reset(nullptr);

    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_queue_mgr);

    SAFE_DELETE(_vstream_mgr);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_user_function_cache);

    // cache_manager must be destoried after all cache.
    // https://github.com/apache/doris/issues/24082#issuecomment-1712544039
    SAFE_DELETE(_cache_manager);

    // _heartbeat_flags must be destoried after staroge engine
    SAFE_DELETE(_heartbeat_flags);

    // Master Info is a thrift object, it could be the last one to deconstruct.
    // Master info should be deconstruct later than fragment manager, because fragment will
    // access master_info.backend id to access some info. If there is a running query and master
    // info is deconstructed then BE process will core at coordinator back method in fragment mgr.
    SAFE_DELETE(_master_info);

    // NOTE: runtime query statistics mgr could be visited by query and daemon thread
    // so it should be created before all query begin and deleted after all query and daemon thread stoppped
    SAFE_DELETE(_runtime_query_statistics_mgr);

    // We should free task scheduler finally because task queue / scheduler maybe used by pipelineX.
    SAFE_DELETE(_without_group_task_scheduler);

    SAFE_DELETE(_arrow_memory_pool);
    SAFE_DELETE(_orc_memory_pool);

    // dns cache is a global instance and need to be released at last
    SAFE_DELETE(_dns_cache);

    _s_tracking_memory = false;

    LOG(INFO) << "Doris exec envorinment is destoried.";
}

} // namespace doris
