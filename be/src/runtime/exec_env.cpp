// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "runtime/exec_env.h"

#include <vector>

#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "runtime/broker_mgr.h"
#include "runtime/bufferpool/buffer_pool.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/disk_io_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/tmp_file_mgr.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "util/metrics.h"
#include "util/network_util.h"
#include "http/web_page_handler.h"
#include "http/default_path_handlers.h"
#include "util/parse_util.h"
#include "util/mem_info.h"
#include "util/debug_util.h"
#include "http/ev_http_server.h"
#include "http/action/mini_load.h"
#include "http/action/checksum_action.h"
#include "http/action/health_action.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/snapshot_action.h"
#include "http/action/pprof_actions.h"
#include "http/action/metrics_action.h"
#include "http/download_action.h"
#include "http/monitor_action.h"
#include "http/http_method.h"
#include "olap/olap_rootpath.h"
#include "util/network_util.h"
#include "util/bfd_parser.h"
#include "runtime/etl_job_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/pull_load_task_mgr.h"
#include "runtime/snapshot_loader.h"
#include "util/pretty_printer.h"
#include "util/palo_metrics.h"
#include "util/brpc_stub_cache.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "gen_cpp/HeartbeatService_types.h"

namespace palo {

ExecEnv* ExecEnv::_exec_env = nullptr;

ExecEnv::ExecEnv() :
        _stream_mgr(new DataStreamMgr()),
        _result_mgr(new ResultBufferMgr()),
        _client_cache(new BackendServiceClientCache()),
        _frontend_client_cache(new FrontendServiceClientCache()),
        _broker_client_cache(new BrokerServiceClientCache()),
        _ev_http_server(new EvHttpServer(config::webserver_port, config::webserver_num_workers)),
        _web_page_handler(new WebPageHandler(_ev_http_server.get())),
        _mem_tracker(NULL),
        _pool_mem_trackers(new PoolMemTrackerRegistry),
        _thread_mgr(new ThreadResourceMgr),
        _thread_pool(new PriorityThreadPool(
                config::palo_scanner_thread_pool_thread_num,
                config::palo_scanner_thread_pool_queue_size)),
        _etl_thread_pool(new ThreadPool(
                config::etl_thread_pool_size,
                config::etl_thread_pool_queue_size)),
        _cgroups_mgr(new CgroupsMgr(this, config::palo_cgroups)),
        _fragment_mgr(new FragmentMgr(this)),
        _master_info(new TMasterInfo()),
        _etl_job_mgr(new EtlJobMgr(this)),
        _load_path_mgr(new LoadPathMgr()),
        _disk_io_mgr(new DiskIoMgr()),
        _tmp_file_mgr(new TmpFileMgr),
        _bfd_parser(BfdParser::create()),
        _pull_load_task_mgr(new PullLoadTaskMgr(config::pull_load_task_dir)),
        _broker_mgr(new BrokerMgr(this)),
        _snapshot_loader(new SnapshotLoader(this)),
        _brpc_stub_cache(new BrpcStubCache()),
        _enable_webserver(true),
        _tz_database(TimezoneDatabase()) {
    _client_cache->init_metrics(PaloMetrics::metrics(), "backend");
    _frontend_client_cache->init_metrics(PaloMetrics::metrics(), "frontend");
    _broker_client_cache->init_metrics(PaloMetrics::metrics(), "broker");
    _result_mgr->init();
    _cgroups_mgr->init_cgroups();
    _etl_job_mgr->init();
    Status status = _load_path_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "load path mgr init failed." << status.get_error_msg();
        exit(-1);
    }
    status = _pull_load_task_mgr->init();
    if (!status.ok()) {
        LOG(ERROR) << "pull load task manager init failed." << status.get_error_msg();
        exit(-1);
    }
    _broker_mgr->init();
    _exec_env = this;
}

ExecEnv::~ExecEnv() {}

Status ExecEnv::init_for_tests() {
    _mem_tracker.reset(new MemTracker(-1));
    return Status::OK;
}

Status ExecEnv::start_services() {
    LOG(INFO) << "Starting global services";

    // Initialize global memory limit.
    int64_t bytes_limit = 0;
    bool is_percent = false;
    // --mem_limit="" means no memory limit
    bytes_limit = ParseUtil::parse_mem_spec(config::mem_limit, &is_percent);

    if (bytes_limit < 0) {
        return Status("Failed to parse mem limit from '" + config::mem_limit + "'.");
    }

    std::stringstream ss;
    if (!BitUtil::IsPowerOf2(config::FLAGS_min_buffer_size)) {
        ss << "--min_buffer_size must be a power-of-two: " << config::FLAGS_min_buffer_size;
        return Status(ss.str());
    }

    int64_t buffer_pool_limit = ParseUtil::parse_mem_spec(config::FLAGS_buffer_pool_limit,
        &is_percent);
    if (buffer_pool_limit <= 0) {
        ss << "Invalid --buffer_pool_limit value, must be a percentage or "
           "positive bytes value or percentage: " << config::FLAGS_buffer_pool_limit;
        return Status(ss.str());
    }
    buffer_pool_limit = BitUtil::RoundDown(buffer_pool_limit, config::FLAGS_min_buffer_size);

    int64_t clean_pages_limit = ParseUtil::parse_mem_spec(config::FLAGS_buffer_pool_clean_pages_limit,
        &is_percent);
    if (clean_pages_limit <= 0) {
        ss << "Invalid --buffer_pool_clean_pages_limit value, must be a percentage or "
              "positive bytes value or percentage: " << config::FLAGS_buffer_pool_clean_pages_limit;
        return Status(ss.str());
    }

    init_buffer_pool(config::FLAGS_min_buffer_size, buffer_pool_limit, clean_pages_limit);
    // Limit of 0 means no memory limit.
    if (bytes_limit > 0) {
        _mem_tracker.reset(new MemTracker(bytes_limit));
    }

    if (bytes_limit > MemInfo::physical_mem()) {
        LOG(WARNING) << "Memory limit "
                     << PrettyPrinter::print(bytes_limit, TUnit::BYTES)
                     << " exceeds physical memory of "
                     << PrettyPrinter::print(MemInfo::physical_mem(),
                                             TUnit::BYTES);
    }

    LOG(INFO) << "Using global memory limit: "
              << PrettyPrinter::print(bytes_limit, TUnit::BYTES);

    RETURN_IF_ERROR(_disk_io_mgr->init(_mem_tracker.get()));

    // Start services in order to ensure that dependencies between them are met
    if (_enable_webserver) {
        RETURN_IF_ERROR(start_webserver());
    } else {
        LOG(INFO) << "Webserver is disabled";
    }

    RETURN_IF_ERROR(_tmp_file_mgr->init(PaloMetrics::metrics()));

    return Status::OK;
}

Status ExecEnv::start_webserver() {
    add_default_path_handlers(_web_page_handler.get(), _mem_tracker.get());
    _ev_http_server->register_handler(HttpMethod::PUT,
                                 "/api/{db}/{table}/_load",
                                 new MiniLoadAction(this));

    std::vector<std::string> allow_paths;
    OLAPRootPath::get_instance()->get_all_available_root_path(&allow_paths);
    DownloadAction* download_action = new DownloadAction(this, allow_paths);
    // = new DownloadAction(this, config::mini_load_download_path);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_download_load", download_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_download_load", download_action);

    DownloadAction* tablet_download_action = new DownloadAction(this, allow_paths);
    _ev_http_server->register_handler(HttpMethod::HEAD,
                                 "/api/_tablet/_download",
                                 tablet_download_action);
    _ev_http_server->register_handler(HttpMethod::GET,
                                 "/api/_tablet/_download",
                                 tablet_download_action);

    DownloadAction* error_log_download_action = new DownloadAction(
            this, _load_path_mgr->get_load_error_file_dir());
    _ev_http_server->register_handler(
            HttpMethod::GET, "/api/_load_error_log", error_log_download_action);
    _ev_http_server->register_handler(
            HttpMethod::HEAD, "/api/_load_error_log", error_log_download_action);

    // Register monitor
    MonitorAction* monitor_action = new MonitorAction();
    monitor_action->register_module("etl_mgr", etl_job_mgr());
    monitor_action->register_module("fragment_mgr", fragment_mgr());
    _ev_http_server->register_handler(HttpMethod::GET, "/_monitor/{module}", monitor_action);

    // Register BE health action
    HealthAction* health_action = new HealthAction(this);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/health", health_action);

    // register pprof actions
    PprofActions::setup(this, _ev_http_server.get());

    {
        auto action = _object_pool.add(new MetricsAction(PaloMetrics::metrics()));
        _ev_http_server->register_handler(HttpMethod::GET, "/metrics", action);
    }

#ifndef BE_TEST
    // Register BE checksum action
    ChecksumAction* checksum_action = new ChecksumAction(this);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/checksum", checksum_action);

    // Register BE reload tablet action
    ReloadTabletAction* reload_tablet_action = new ReloadTabletAction(this);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/reload_tablet", reload_tablet_action);

    // Register BE snapshot action
    SnapshotAction* snapshot_action = new SnapshotAction(this);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/snapshot", snapshot_action);
#endif

    RETURN_IF_ERROR(_ev_http_server->start());
    return Status::OK;
}

uint32_t ExecEnv::cluster_id() {
    return OLAPRootPath::get_instance()->effective_cluster_id();
}

void ExecEnv::init_buffer_pool(int64_t min_page_size, int64_t capacity, int64_t clean_pages_limit) {
  DCHECK(_buffer_pool == nullptr);
  _buffer_pool.reset(new BufferPool(min_page_size, capacity, clean_pages_limit));
  _buffer_reservation.reset(new ReservationTracker);
  _buffer_reservation->InitRootTracker(nullptr, capacity);
}

const std::string& ExecEnv::token() const {
    return _master_info->token;
}

MetricRegistry* ExecEnv::metrics() const {
    return PaloMetrics::metrics();
}

}
