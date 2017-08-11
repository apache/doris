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
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/disk_io_mgr.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/mem_tracker.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/fragment_mgr.h"
#include "runtime/tmp_file_mgr.h"
#include "util/metrics.h"
#include "util/network_util.h"
#include "http/webserver.h"
#include "http/web_page_handler.h"
#include "http/default_path_handlers.h"
#include "util/parse_util.h"
#include "util/mem_info.h"
#include "util/debug_util.h"
#include "http/action/mini_load.h"
#include "http/action/checksum_action.h"
#include "http/action/health_action.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/snapshot_action.h"
#include "http/action/pprof_actions.h"
#include "http/download_action.h"
#include "http/monitor_action.h"
#include "http/http_method.h"
#include "util/network_util.h"
#include "util/bfd_parser.h"
#include "runtime/etl_job_mgr.h"
#include "runtime/load_path_mgr.h"
#include "runtime/pull_load_task_mgr.h"
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
        _webserver(new Webserver()),
        _web_page_handler(new WebPageHandler(_webserver.get())),
        _metrics(new MetricGroup("exec_env")),
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
        _local_ip(new std::string()),
        _load_path_mgr(new LoadPathMgr()),
        _disk_io_mgr(new DiskIoMgr()),
        _tmp_file_mgr(new TmpFileMgr),
        _bfd_parser(BfdParser::create()),
        _pull_load_task_mgr(new PullLoadTaskMgr(config::pull_load_task_dir)),
        _broker_mgr(new BrokerMgr(this)),
        _enable_webserver(true),
        _tz_database(TimezoneDatabase()) {
    get_local_ip(_local_ip.get());
    _client_cache->init_metrics(_metrics.get(), "palo.backends");
    //_frontend_client_cache->init_metrics(_metrics.get(), "frontend-server.backends");
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

    //create connection manger
    _conn_mgr = std::make_shared<ConnectionManager>();

    // Initialize global memory limit.
    int64_t bytes_limit = 0;
    bool is_percent = false;
    // --mem_limit="" means no memory limit
    bytes_limit = ParseUtil::parse_mem_spec(config::mem_limit, &is_percent);

    if (bytes_limit < 0) {
        return Status("Failed to parse mem limit from '" + config::mem_limit + "'.");
    }

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
        add_default_path_handlers(_web_page_handler.get(), _mem_tracker.get());
        _webserver->register_handler(HttpMethod::PUT,
                                     "/api/{db}/{table}/_load",
                                     new MiniLoadAction(this));
        DownloadAction* download_action = new DownloadAction(this, "");
                // = new DownloadAction(this, config::mini_load_download_path);
        _webserver->register_handler(HttpMethod::GET, "/api/_download_load", download_action);
        _webserver->register_handler(HttpMethod::HEAD, "/api/_download_load", download_action);

        DownloadAction* tablet_download_action = new DownloadAction(this, "");
        _webserver->register_handler(HttpMethod::HEAD,
                                     "/api/_tablet/_download",
                                     tablet_download_action);
        _webserver->register_handler(HttpMethod::GET,
                                     "/api/_tablet/_download",
                                     tablet_download_action);

        // Register monitor
        MonitorAction* monitor_action = new MonitorAction();
        monitor_action->register_module("etl_mgr", etl_job_mgr());
        monitor_action->register_module("fragment_mgr", fragment_mgr());
        _webserver->register_handler(HttpMethod::GET, "/_monitor/{module}", monitor_action);

        // Register BE health action
        HealthAction* health_action = new HealthAction(this);
        _webserver->register_handler(HttpMethod::GET, "/api/health", health_action);

        // register pprof actions
        PprofActions::setup(this, _webserver.get());

#ifndef BE_TEST
        // Register BE checksum action
        ChecksumAction* checksum_action = new ChecksumAction(this);
        _webserver->register_handler(HttpMethod::GET, "/api/checksum", checksum_action);

        // Register BE reload tablet action
        ReloadTabletAction* reload_tablet_action = new ReloadTabletAction(this);
        _webserver->register_handler(HttpMethod::GET, "/api/reload_tablet", reload_tablet_action);

        // Register BE snapshot action
        SnapshotAction* snapshot_action = new SnapshotAction(this);
        _webserver->register_handler(HttpMethod::GET, "/api/snapshot", snapshot_action);
#endif

        RETURN_IF_ERROR(_webserver->start());
    } else {
        LOG(INFO) << "Webserver is disabled";
    }

    _metrics->init(_enable_webserver ? _web_page_handler.get() : NULL);
    RETURN_IF_ERROR(_tmp_file_mgr->init(_metrics.get()));

    return Status::OK;
}

}
