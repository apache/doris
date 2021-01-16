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

#include "service/http_service.h"

#include "http/action/checksum_action.h"
#include "http/action/compaction_action.h"
#include "http/action/health_action.h"
#include "http/action/meta_action.h"
#include "http/action/metrics_action.h"
#include "http/action/mini_load.h"
#include "http/action/pprof_actions.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/restore_tablet_action.h"
#include "http/action/snapshot_action.h"
#include "http/action/stream_load.h"
#include "http/action/tablets_distribution_action.h"
#include "http/action/tablet_migration_action.h"
#include "http/action/tablets_info_action.h"
#include "http/action/update_config_action.h"
#include "http/default_path_handlers.h"
#include "http/download_action.h"
#include "http/ev_http_server.h"
#include "http/http_method.h"
#include "http/monitor_action.h"
#include "http/web_page_handler.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "util/doris_metrics.h"

namespace doris {

HttpService::HttpService(ExecEnv* env, int port, int num_threads)
        : _env(env),
          _ev_http_server(new EvHttpServer(port, num_threads)),
          _web_page_handler(new WebPageHandler(_ev_http_server.get())) {}

HttpService::~HttpService() {}

Status HttpService::start() {
    add_default_path_handlers(_web_page_handler.get(), _env->process_mem_tracker());

    // register load
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/{table}/_load",
                                      new MiniLoadAction(_env));
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/{table}/_stream_load",
                                      new StreamLoadAction(_env));

    // register download action
    std::vector<std::string> allow_paths;
    for (auto& path : _env->store_paths()) {
        allow_paths.emplace_back(path.path);
    }
    DownloadAction* download_action = new DownloadAction(_env, allow_paths);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_download_load", download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_download_load", download_action);

    DownloadAction* tablet_download_action = new DownloadAction(_env, allow_paths);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_tablet/_download",
                                      tablet_download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_tablet/_download",
                                      tablet_download_action);

    DownloadAction* error_log_download_action =
            new DownloadAction(_env, _env->load_path_mgr()->get_load_error_file_dir());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_load_error_log",
                                      error_log_download_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_load_error_log",
                                      error_log_download_action);

    // Register BE health action
    HealthAction* health_action = new HealthAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/health", health_action);

    // Register Tablets Info action
    TabletsInfoAction* tablets_info_action = new TabletsInfoAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/tablets_json", tablets_info_action);

    // Register Tablets Distribution action
    TabletsDistributionAction* tablets_distribution_action = new TabletsDistributionAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/api/tablets_distribution", tablets_distribution_action);

    // Register tablet migration action
    TabletMigrationAction* tablet_migration_action = new TabletMigrationAction();
    _ev_http_server->register_handler(HttpMethod::GET, "/api/tablet_migration", tablet_migration_action);

    // register pprof actions
    PprofActions::setup(_env, _ev_http_server.get());

    // register metrics
    {
        auto action = new MetricsAction(DorisMetrics::instance()->metric_registry());
        _ev_http_server->register_handler(HttpMethod::GET, "/metrics", action);
    }

    MetaAction* meta_action = new MetaAction(HEADER);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/meta/header/{tablet_id}/{schema_hash}",
                                      meta_action);

#ifndef BE_TEST
    // Register BE checksum action
    ChecksumAction* checksum_action = new ChecksumAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/checksum", checksum_action);

    // Register BE reload tablet action
    ReloadTabletAction* reload_tablet_action = new ReloadTabletAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/reload_tablet", reload_tablet_action);

    RestoreTabletAction* restore_tablet_action = new RestoreTabletAction(_env);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/restore_tablet",
                                      restore_tablet_action);

    // Register BE snapshot action
    SnapshotAction* snapshot_action = new SnapshotAction(_env);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/snapshot", snapshot_action);
#endif

    // 2 compaction actions
    CompactionAction* show_compaction_action =
            new CompactionAction(CompactionActionType::SHOW_INFO);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/show",
                                      show_compaction_action);
    CompactionAction* run_compaction_action =
            new CompactionAction(CompactionActionType::RUN_COMPACTION);
    _ev_http_server->register_handler(HttpMethod::POST, "/api/compaction/run",
                                      run_compaction_action);
    CompactionAction* run_status_compaction_action =
            new CompactionAction(CompactionActionType::RUN_COMPACTION_STATUS);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/run_status",
                                      run_status_compaction_action);

    UpdateConfigAction* update_config_action = new UpdateConfigAction();
    _ev_http_server->register_handler(HttpMethod::POST, "/api/update_config", update_config_action);

    _ev_http_server->start();
    return Status::OK();
}

void HttpService::stop() {
    _ev_http_server->stop();
}

} // namespace doris
