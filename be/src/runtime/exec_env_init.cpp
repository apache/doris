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

#include <boost/algorithm/string.hpp>

#include "agent/cgroups_mgr.h"
#include "common/config.h"
#include "common/logging.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/TExtDataSourceService.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "olap/page_cache.h"
#include "olap/storage_engine.h"
#include "plugin/plugin_mgr.h"
#include "runtime/broker_mgr.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "runtime/cache/result_cache.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/disk_io_mgr.h"
#include "runtime/etl_job_mgr.h"
#include "runtime/exec_env.h"
#include "runtime/external_scan_context_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/heartbeat_flags.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/routine_load/routine_load_task_executor.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/stream_load/stream_load_executor.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/tmp_file_mgr.h"
#include "util/bfd_parser.h"
#include "util/brpc_stub_cache.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/network_util.h"
#include "util/parse_util.h"
#include "util/pretty_printer.h"
#include "util/priority_thread_pool.hpp"

namespace doris {

Status ExecEnv::init(ExecEnv* env, const std::vector<StorePath>& store_paths) {
    return env->_init(store_paths);
}

Status ExecEnv::_init(const std::vector<StorePath>& store_paths) {
    //Only init once before be destroyed
    if (_is_init) {
        return Status::OK();
    }
    _store_paths = store_paths;
    _external_scan_context_mgr = new ExternalScanContextMgr(this);
    _stream_mgr = new DataStreamMgr();
    _result_mgr = new ResultBufferMgr();
    _result_queue_mgr = new ResultQueueMgr();
    _backend_client_cache = new BackendServiceClientCache(config::max_client_cache_size_per_host);
    _frontend_client_cache = new FrontendServiceClientCache(config::max_client_cache_size_per_host);
    _broker_client_cache = new BrokerServiceClientCache(config::max_client_cache_size_per_host);
    _extdatasource_client_cache =
            new ExtDataSourceServiceClientCache(config::max_client_cache_size_per_host);
    _pool_mem_trackers = new PoolMemTrackerRegistry();
    _thread_mgr = new ThreadResourceMgr();
    _thread_pool = new PriorityThreadPool(config::doris_scanner_thread_pool_thread_num,
                                          config::doris_scanner_thread_pool_queue_size);
    _etl_thread_pool = new PriorityThreadPool(config::etl_thread_pool_size,
                                              config::etl_thread_pool_queue_size);
    _cgroups_mgr = new CgroupsMgr(this, config::doris_cgroups);
    _fragment_mgr = new FragmentMgr(this);
    _result_cache = new ResultCache(config::query_cache_max_size_mb,
                                    config::query_cache_elasticity_size_mb);
    _master_info = new TMasterInfo();
    _etl_job_mgr = new EtlJobMgr(this);
    _load_path_mgr = new LoadPathMgr(this);
    _disk_io_mgr = new DiskIoMgr();
    _tmp_file_mgr = new TmpFileMgr(this), _bfd_parser = BfdParser::create();
    _broker_mgr = new BrokerMgr(this);
    _load_channel_mgr = new LoadChannelMgr();
    _load_stream_mgr = new LoadStreamMgr();
    _brpc_stub_cache = new BrpcStubCache();
    _stream_load_executor = new StreamLoadExecutor(this);
    _routine_load_task_executor = new RoutineLoadTaskExecutor(this);
    _small_file_mgr = new SmallFileMgr(this, config::small_file_dir);
    _plugin_mgr = new PluginMgr();

    _backend_client_cache->init_metrics("backend");
    _frontend_client_cache->init_metrics("frontend");
    _broker_client_cache->init_metrics("broker");
    _extdatasource_client_cache->init_metrics("extdatasource");
    _result_mgr->init();
    _cgroups_mgr->init_cgroups();
    _etl_job_mgr->init();
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status.get_error_msg();
        exit(-1);
    }
    _broker_mgr->init();
    _small_file_mgr->init();
    _init_mem_tracker();

    RETURN_IF_ERROR(_load_channel_mgr->init(_mem_tracker->limit()));
    _heartbeat_flags = new HeartbeatFlags();
    _is_init = true;
    return Status::OK();
}

Status ExecEnv::_init_mem_tracker() {
    // Initialize global memory limit.
    int64_t bytes_limit = 0;
    bool is_percent = false;
    std::stringstream ss;
    // --mem_limit="" means no memory limit
    bytes_limit = ParseUtil::parse_mem_spec(config::mem_limit, &is_percent);
    if (bytes_limit <= 0) {
        ss << "Failed to parse mem limit from '" + config::mem_limit + "'.";
        return Status::InternalError(ss.str());
    }

    if (!BitUtil::IsPowerOf2(config::min_buffer_size)) {
        ss << "--min_buffer_size must be a power-of-two: " << config::min_buffer_size;
        return Status::InternalError(ss.str());
    }

    int64_t buffer_pool_limit = ParseUtil::parse_mem_spec(config::buffer_pool_limit, &is_percent);
    if (buffer_pool_limit <= 0) {
        ss << "Invalid --buffer_pool_limit value, must be a percentage or "
              "positive bytes value or percentage: "
           << config::buffer_pool_limit;
        return Status::InternalError(ss.str());
    }
    buffer_pool_limit = BitUtil::RoundDown(buffer_pool_limit, config::min_buffer_size);

    int64_t clean_pages_limit =
            ParseUtil::parse_mem_spec(config::buffer_pool_clean_pages_limit, &is_percent);
    if (clean_pages_limit <= 0) {
        ss << "Invalid --buffer_pool_clean_pages_limit value, must be a percentage or "
              "positive bytes value or percentage: "
           << config::buffer_pool_clean_pages_limit;
        return Status::InternalError(ss.str());
    }

    _init_buffer_pool(config::min_buffer_size, buffer_pool_limit, clean_pages_limit);

    if (bytes_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit " << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                     << " exceeds physical memory of "
                     << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES)
                     << ". Using physical memory instead";
        bytes_limit = MemInfo::physical_mem();
    }

    if (bytes_limit <= 0) {
        ss << "Invalid mem limit: " << bytes_limit;
        return Status::InternalError(ss.str());
    }

    _mem_tracker =
            MemTracker::CreateTracker(bytes_limit, "ExecEnv root", MemTracker::GetRootTracker());

    LOG(INFO) << "Using global memory limit: " << PrettyPrinter::print(bytes_limit, TUnit::BYTES);
    RETURN_IF_ERROR(_disk_io_mgr->init(_mem_tracker));
    RETURN_IF_ERROR(_tmp_file_mgr->init());

    int64_t storage_cache_limit =
            ParseUtil::parse_mem_spec(config::storage_page_cache_limit, &is_percent);
    if (storage_cache_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Config storage_page_cache_limit is greater than memory size, config="
                     << config::storage_page_cache_limit << ", memory=" << MemInfo::physical_mem();
    }
    int32_t index_page_cache_percentage = config::index_page_cache_percentage;
    StoragePageCache::create_global_cache(storage_cache_limit, index_page_cache_percentage);

    // TODO(zc): The current memory usage configuration is a bit confusing,
    // we need to sort out the use of memory
    return Status::OK();
}

void ExecEnv::_init_buffer_pool(int64_t min_page_size, int64_t capacity,
                                int64_t clean_pages_limit) {
    DCHECK(_buffer_pool == nullptr);
    _buffer_pool = new BufferPool(min_page_size, capacity, clean_pages_limit);
    _buffer_reservation = new ReservationTracker();
    _buffer_reservation->InitRootTracker(nullptr, capacity);
}

void ExecEnv::_destroy() {
    //Only destroy once after init
    if (!_is_init) {
        return;
    }
    SAFE_DELETE(_brpc_stub_cache);
    SAFE_DELETE(_load_stream_mgr);
    SAFE_DELETE(_load_channel_mgr);
    SAFE_DELETE(_broker_mgr);
    SAFE_DELETE(_bfd_parser);
    SAFE_DELETE(_tmp_file_mgr);
    SAFE_DELETE(_disk_io_mgr);
    SAFE_DELETE(_load_path_mgr);
    SAFE_DELETE(_etl_job_mgr);
    SAFE_DELETE(_master_info);
    SAFE_DELETE(_fragment_mgr);
    SAFE_DELETE(_cgroups_mgr);
    SAFE_DELETE(_etl_thread_pool);
    SAFE_DELETE(_thread_pool);
    SAFE_DELETE(_thread_mgr);
    SAFE_DELETE(_pool_mem_trackers);
    SAFE_DELETE(_broker_client_cache);
    SAFE_DELETE(_extdatasource_client_cache);
    SAFE_DELETE(_frontend_client_cache);
    SAFE_DELETE(_backend_client_cache);
    SAFE_DELETE(_result_mgr);
    SAFE_DELETE(_result_queue_mgr);
    SAFE_DELETE(_stream_mgr);
    SAFE_DELETE(_stream_load_executor);
    SAFE_DELETE(_routine_load_task_executor);
    SAFE_DELETE(_external_scan_context_mgr);
    SAFE_DELETE(_heartbeat_flags);
    _is_init = false;
}

void ExecEnv::destroy(ExecEnv* env) {
    env->_destroy();
}

} // namespace doris
