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

#include <algorithm>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "http/action/check_rpc_channel_action.h"
#include "http/action/check_tablet_segment_action.h"
#include "http/action/checksum_action.h"
#include "http/action/compaction_action.h"
#include "http/action/config_action.h"
#include "http/action/debug_point_action.h"
#include "http/action/download_action.h"
#include "http/action/download_binlog_action.h"
#include "http/action/file_cache_action.h"
#include "http/action/health_action.h"
#include "http/action/http_stream.h"
#include "http/action/jeprofile_actions.h"
#include "http/action/meta_action.h"
#include "http/action/metrics_action.h"
#include "http/action/pad_rowset_action.h"
#include "http/action/pprof_actions.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/reset_rpc_channel_action.h"
#include "http/action/restore_tablet_action.h"
#include "http/action/snapshot_action.h"
#include "http/action/stream_load.h"
#include "http/action/stream_load_2pc.h"
#include "http/action/tablet_migration_action.h"
#include "http/action/tablets_distribution_action.h"
#include "http/action/tablets_info_action.h"
#include "http/action/version_action.h"
#include "http/default_path_handlers.h"
#include "http/ev_http_server.h"
#include "http/http_method.h"
#include "http/web_page_handler.h"
#include "olap/options.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "util/doris_metrics.h"

namespace doris {

HttpService::HttpService(ExecEnv* env, int port, int num_threads)
        : _env(env),
          _ev_http_server(new EvHttpServer(port, num_threads)),
          _web_page_handler(new WebPageHandler(_ev_http_server.get())) {}

HttpService::~HttpService() {
    stop();
}

Status HttpService::start() {
    add_default_path_handlers(_web_page_handler.get());

    // register load
    StreamLoadAction* streamload_action = _pool.add(new StreamLoadAction(_env));
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/{table}/_load",
                                      streamload_action);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/{table}/_stream_load",
                                      streamload_action);
    StreamLoad2PCAction* streamload_2pc_action = _pool.add(new StreamLoad2PCAction(_env));
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/_stream_load_2pc",
                                      streamload_2pc_action);
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/{db}/{table}/_stream_load_2pc",
                                      streamload_2pc_action);

    // register http_stream
    HttpStreamAction* http_stream_action = _pool.add(new HttpStreamAction(_env));
    _ev_http_server->register_handler(HttpMethod::PUT, "/api/_http_stream", http_stream_action);

    // register download action
    std::vector<std::string> allow_paths;
    for (auto& path : _env->store_paths()) {
        allow_paths.emplace_back(path.path);
    }
    DownloadAction* download_action = _pool.add(new DownloadAction(_env, allow_paths));
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_download_load", download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_download_load", download_action);

    DownloadAction* tablet_download_action = _pool.add(new DownloadAction(_env, allow_paths));
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_tablet/_download",
                                      tablet_download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_tablet/_download",
                                      tablet_download_action);
    if (config::enable_single_replica_load) {
        DownloadAction* single_replica_download_action = _pool.add(new DownloadAction(
                _env, allow_paths, config::single_replica_load_download_num_workers));
        _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_single_replica/_download",
                                          single_replica_download_action);
        _ev_http_server->register_handler(HttpMethod::GET, "/api/_single_replica/_download",
                                          single_replica_download_action);
    }

    DownloadAction* error_log_download_action =
            _pool.add(new DownloadAction(_env, _env->load_path_mgr()->get_load_error_file_dir()));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_load_error_log",
                                      error_log_download_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_load_error_log",
                                      error_log_download_action);

    DownloadBinlogAction* download_binlog_action = _pool.add(new DownloadBinlogAction(_env));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_binlog/_download",
                                      download_binlog_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_binlog/_download",
                                      download_binlog_action);

    // Register BE version action
    VersionAction* version_action =
            _pool.add(new VersionAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::NONE));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/be_version_info", version_action);

    // Register BE health action
    HealthAction* health_action = _pool.add(new HealthAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/health", health_action);

    // Register Tablets Info action
    TabletsInfoAction* tablets_info_action =
            _pool.add(new TabletsInfoAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/tablets_json", tablets_info_action);

    // Register Tablets Distribution action
    TabletsDistributionAction* tablets_distribution_action = _pool.add(
            new TabletsDistributionAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/tablets_distribution",
                                      tablets_distribution_action);

    // Register tablet migration action
    TabletMigrationAction* tablet_migration_action = _pool.add(
            new TabletMigrationAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/tablet_migration",
                                      tablet_migration_action);

    // register pprof actions
    static_cast<void>(PprofActions::setup(_env, _ev_http_server.get(), _pool));

    // register jeprof actions
    static_cast<void>(JeprofileActions::setup(_env, _ev_http_server.get(), _pool));

    // register metrics
    {
        auto action = _pool.add(new MetricsAction(DorisMetrics::instance()->metric_registry(), _env,
                                                  TPrivilegeHier::GLOBAL, TPrivilegeType::NONE));
        _ev_http_server->register_handler(HttpMethod::GET, "/metrics", action);
    }

    MetaAction* meta_action =
            _pool.add(new MetaAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/meta/{op}/{tablet_id}", meta_action);

    FileCacheAction* file_cache_action = _pool.add(new FileCacheAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/file_cache", file_cache_action);

#ifndef BE_TEST
    // Register BE checksum action
    ChecksumAction* checksum_action =
            _pool.add(new ChecksumAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/checksum", checksum_action);

    // Register BE reload tablet action
    ReloadTabletAction* reload_tablet_action =
            _pool.add(new ReloadTabletAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/reload_tablet", reload_tablet_action);

    RestoreTabletAction* restore_tablet_action =
            _pool.add(new RestoreTabletAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/restore_tablet",
                                      restore_tablet_action);

    // Register BE snapshot action
    SnapshotAction* snapshot_action =
            _pool.add(new SnapshotAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/snapshot", snapshot_action);
#endif

    // 2 compaction actions
    CompactionAction* show_compaction_action = _pool.add(new CompactionAction(
            CompactionActionType::SHOW_INFO, _env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/show",
                                      show_compaction_action);
    CompactionAction* run_compaction_action =
            _pool.add(new CompactionAction(CompactionActionType::RUN_COMPACTION, _env,
                                           TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/compaction/run",
                                      run_compaction_action);
    CompactionAction* run_status_compaction_action =
            _pool.add(new CompactionAction(CompactionActionType::RUN_COMPACTION_STATUS, _env,
                                           TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/run_status",
                                      run_status_compaction_action);

    ConfigAction* update_config_action =
            _pool.add(new ConfigAction(ConfigActionType::UPDATE_CONFIG));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/update_config", update_config_action);

    ConfigAction* show_config_action = _pool.add(new ConfigAction(ConfigActionType::SHOW_CONFIG));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/show_config", show_config_action);

    // 3 check action
    CheckRPCChannelAction* check_rpc_channel_action = _pool.add(
            new CheckRPCChannelAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET,
                                      "/api/check_rpc_channel/{ip}/{port}/{payload_size}",
                                      check_rpc_channel_action);

    ResetRPCChannelAction* reset_rpc_channel_action = _pool.add(
            new ResetRPCChannelAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/reset_rpc_channel/{endpoints}",
                                      reset_rpc_channel_action);

    CheckTabletSegmentAction* check_tablet_segment_action = _pool.add(
            new CheckTabletSegmentAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/check_tablet_segment_lost",
                                      check_tablet_segment_action);

    PadRowsetAction* pad_rowset_action =
            _pool.add(new PadRowsetAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/pad_rowset", pad_rowset_action);

    // debug point
    AddDebugPointAction* add_debug_point_action =
            _pool.add(new AddDebugPointAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/debug_point/add/{debug_point}",
                                      add_debug_point_action);

    RemoveDebugPointAction* remove_debug_point_action = _pool.add(
            new RemoveDebugPointAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/debug_point/remove/{debug_point}",
                                      remove_debug_point_action);

    ClearDebugPointsAction* clear_debug_points_action = _pool.add(
            new ClearDebugPointsAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/debug_point/clear",
                                      clear_debug_points_action);

    _ev_http_server->start();
    return Status::OK();
}

void HttpService::stop() {
    if (stopped) {
        return;
    }
    _ev_http_server->stop();
    _pool.clear();
    stopped = true;
}

int HttpService::get_real_port() const {
    return _ev_http_server->get_real_port();
}

} // namespace doris
