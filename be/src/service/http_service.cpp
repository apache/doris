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

#include <event2/bufferevent.h>
#include <event2/http.h>

#include <string>
#include <vector>

#include "cloud/cloud_compaction_action.h"
#include "cloud/config.h"
#include "cloud/injection_point_action.h"
#include "common/config.h"
#include "common/status.h"
#include "http/action/adjust_log_level.h"
#include "http/action/adjust_tracing_dump.h"
#include "http/action/be_proc_thread_action.h"
#include "http/action/calc_file_crc_action.h"
#include "http/action/check_rpc_channel_action.h"
#include "http/action/check_tablet_segment_action.h"
#include "http/action/checksum_action.h"
#include "http/action/clear_cache_action.h"
#include "http/action/compaction_action.h"
#include "http/action/config_action.h"
#include "http/action/debug_point_action.h"
#include "http/action/download_action.h"
#include "http/action/download_binlog_action.h"
#include "http/action/file_cache_action.h"
#include "http/action/health_action.h"
#include "http/action/http_stream.h"
#include "http/action/jeprofile_actions.h"
#include "http/action/load_stream_action.h"
#include "http/action/meta_action.h"
#include "http/action/metrics_action.h"
#include "http/action/pad_rowset_action.h"
#include "http/action/pipeline_task_action.h"
#include "http/action/pprof_actions.h"
#include "http/action/reload_tablet_action.h"
#include "http/action/report_action.h"
#include "http/action/reset_rpc_channel_action.h"
#include "http/action/restore_tablet_action.h"
#include "http/action/show_hotspot_action.h"
#include "http/action/show_nested_index_file_action.h"
#include "http/action/shrink_mem_action.h"
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
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "runtime/load_path_mgr.h"
#include "util/doris_metrics.h"

namespace doris {
namespace {
std::shared_ptr<bufferevent_rate_limit_group> get_rate_limit_group(event_base* event_base) {
    auto rate_limit = config::download_binlog_rate_limit_kbs;
    if (rate_limit <= 0) {
        return nullptr;
    }

    auto max_value = std::numeric_limits<int32_t>::max() / 1024 * 10;
    if (rate_limit > max_value) {
        LOG(WARNING) << "rate limit is too large, set to max value.";
        rate_limit = max_value;
    }
    struct timeval cfg_tick = {0, 100 * 1000}; // 100ms
    rate_limit = rate_limit / 10 * 1024;       // convert to KB/S

    auto token_bucket = std::unique_ptr<ev_token_bucket_cfg, decltype(&ev_token_bucket_cfg_free)>(
            ev_token_bucket_cfg_new(rate_limit, rate_limit * 2, rate_limit, rate_limit * 2,
                                    &cfg_tick),
            ev_token_bucket_cfg_free);
    return {bufferevent_rate_limit_group_new(event_base, token_bucket.get()),
            bufferevent_rate_limit_group_free};
}
} // namespace

HttpService::HttpService(ExecEnv* env, int port, int num_threads)
        : _env(env),
          _ev_http_server(new EvHttpServer(port, num_threads)),
          _web_page_handler(new WebPageHandler(_ev_http_server.get())) {}

HttpService::~HttpService() {
    stop();
}

// NOLINTBEGIN(readability-function-size)
Status HttpService::start() {
    add_default_path_handlers(_web_page_handler.get());

    auto event_base = _ev_http_server->get_event_bases()[0];
    _rate_limit_group = get_rate_limit_group(event_base.get());

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

    DownloadAction* error_log_download_action =
            _pool.add(new DownloadAction(_env, _env->load_path_mgr()->get_load_error_file_dir()));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_load_error_log",
                                      error_log_download_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_load_error_log",
                                      error_log_download_action);

    AdjustLogLevelAction* adjust_log_level_action = _pool.add(new AdjustLogLevelAction());
    _ev_http_server->register_handler(HttpMethod::POST, "api/glog/adjust", adjust_log_level_action);

    //TODO: add query GET interface
    auto* adjust_tracing_dump = _pool.add(new AdjustTracingDump());
    _ev_http_server->register_handler(HttpMethod::POST, "api/pipeline/tracing",
                                      adjust_tracing_dump);

    // Register BE version action
    VersionAction* version_action =
            _pool.add(new VersionAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::NONE));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/be_version_info", version_action);

    // Register BE health action
    HealthAction* health_action = _pool.add(new HealthAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/health", health_action);

    // Dump all running pipeline tasks
    ClearDataCacheAction* clear_data_cache_action = _pool.add(new ClearDataCacheAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/clear_data_cache",
                                      clear_data_cache_action);

    // Dump all running pipeline tasks
    PipelineTaskAction* pipeline_task_action = _pool.add(new PipelineTaskAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/running_pipeline_tasks",
                                      pipeline_task_action);

    // Dump all running pipeline tasks which has been running for more than {duration} seconds
    LongPipelineTaskAction* long_pipeline_task_action = _pool.add(new LongPipelineTaskAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/running_pipeline_tasks/{duration}",
                                      long_pipeline_task_action);

    // Dump all running pipeline tasks which has been running for more than {duration} seconds
    QueryPipelineTaskAction* query_pipeline_task_action = _pool.add(new QueryPipelineTaskAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/query_pipeline_tasks/{query_id}",
                                      query_pipeline_task_action);

    // Dump all be process thread num
    BeProcThreadAction* be_proc_thread_action = _pool.add(new BeProcThreadAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/be_process_thread_num",
                                      be_proc_thread_action);

    // Register BE LoadStream action
    LoadStreamAction* load_stream_action = _pool.add(new LoadStreamAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/load_streams", load_stream_action);

    // Register Tablets Info action
    TabletsInfoAction* tablets_info_action =
            _pool.add(new TabletsInfoAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/tablets_json", tablets_info_action);

    // register pprof actions
    static_cast<void>(PprofActions::setup(_env, _ev_http_server.get(), _pool));

    // register jeprof actions
    static_cast<void>(JeprofileActions::setup(_env, _ev_http_server.get(), _pool));

    // register metrics
    {
        auto* action =
                _pool.add(new MetricsAction(DorisMetrics::instance()->metric_registry(), _env,
                                            TPrivilegeHier::GLOBAL, TPrivilegeType::NONE));
        _ev_http_server->register_handler(HttpMethod::GET, "/metrics", action);
    }

    MetaAction* meta_action =
            _pool.add(new MetaAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/meta/{op}/{tablet_id}", meta_action);

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

    register_debug_point_handler();

    ReportAction* report_task_action = _pool.add(
            new ReportAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN, "REPORT_TASK"));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/report/task", report_task_action);

    // shrink memory for starting co-exist process during upgrade
    ShrinkMemAction* shrink_mem_action = _pool.add(new ShrinkMemAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/shrink_mem", shrink_mem_action);

    auto& engine = _env->storage_engine();
    if (config::is_cloud_mode()) {
        register_cloud_handler(engine.to_cloud());
    } else {
        register_local_handler(engine.to_local());
    }

    _ev_http_server->start();
    return Status::OK();
}
// NOLINTEND(readability-function-size)

void HttpService::register_debug_point_handler() {
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
}

// NOLINTBEGIN(readability-function-size)
void HttpService::register_local_handler(StorageEngine& engine) {
    // register download action
    std::vector<std::string> allow_paths;
    for (const auto& path : _env->store_paths()) {
        allow_paths.emplace_back(path.path);
    }
    DownloadAction* download_action = _pool.add(new DownloadAction(_env, nullptr, allow_paths));
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_download_load", download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_download_load", download_action);

    DownloadAction* tablet_download_action =
            _pool.add(new DownloadAction(_env, _rate_limit_group, allow_paths));
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_tablet/_download",
                                      tablet_download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_tablet/_download",
                                      tablet_download_action);
    if (config::enable_single_replica_load) {
        DownloadAction* single_replica_download_action = _pool.add(new DownloadAction(
                _env, nullptr, allow_paths, config::single_replica_load_download_num_workers));
        _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_single_replica/_download",
                                          single_replica_download_action);
        _ev_http_server->register_handler(HttpMethod::GET, "/api/_single_replica/_download",
                                          single_replica_download_action);
    }

    DownloadBinlogAction* download_binlog_action =
            _pool.add(new DownloadBinlogAction(_env, engine, _rate_limit_group));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_binlog/_download",
                                      download_binlog_action);
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_binlog/_download",
                                      download_binlog_action);

    FileCacheAction* file_cache_action = _pool.add(new FileCacheAction());
    _ev_http_server->register_handler(HttpMethod::POST, "/api/file_cache", file_cache_action);

    TabletsDistributionAction* tablets_distribution_action =
            _pool.add(new TabletsDistributionAction(_env, engine, TPrivilegeHier::GLOBAL,
                                                    TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/tablets_distribution",
                                      tablets_distribution_action);

    // Register tablet migration action
    TabletMigrationAction* tablet_migration_action = _pool.add(
            new TabletMigrationAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/tablet_migration",
                                      tablet_migration_action);

#ifndef BE_TEST
    // Register BE checksum action
    ChecksumAction* checksum_action = _pool.add(
            new ChecksumAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/checksum", checksum_action);

    // Register BE reload tablet action
    ReloadTabletAction* reload_tablet_action = _pool.add(
            new ReloadTabletAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/reload_tablet", reload_tablet_action);

    RestoreTabletAction* restore_tablet_action = _pool.add(
            new RestoreTabletAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/restore_tablet",
                                      restore_tablet_action);

    // Register BE snapshot action
    SnapshotAction* snapshot_action = _pool.add(
            new SnapshotAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/snapshot", snapshot_action);
#endif
    // 2 compaction actions
    CompactionAction* show_compaction_action =
            _pool.add(new CompactionAction(CompactionActionType::SHOW_INFO, _env, engine,
                                           TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/show",
                                      show_compaction_action);
    CompactionAction* run_compaction_action =
            _pool.add(new CompactionAction(CompactionActionType::RUN_COMPACTION, _env, engine,
                                           TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/compaction/run",
                                      run_compaction_action);
    CompactionAction* run_status_compaction_action =
            _pool.add(new CompactionAction(CompactionActionType::RUN_COMPACTION_STATUS, _env,
                                           engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));

    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/run_status",
                                      run_status_compaction_action);
    CheckTabletSegmentAction* check_tablet_segment_action = _pool.add(new CheckTabletSegmentAction(
            _env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/check_tablet_segment_lost",
                                      check_tablet_segment_action);

    PadRowsetAction* pad_rowset_action = _pool.add(
            new PadRowsetAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/pad_rowset", pad_rowset_action);

    ReportAction* report_tablet_action = _pool.add(new ReportAction(
            _env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN, "REPORT_OLAP_TABLE"));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/report/tablet", report_tablet_action);

    ReportAction* report_disk_action = _pool.add(new ReportAction(
            _env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN, "REPORT_DISK_STATE"));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/report/disk", report_disk_action);

    CalcFileCrcAction* calc_crc_action = _pool.add(
            new CalcFileCrcAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/calc_crc", calc_crc_action);

    ShowNestedIndexFileAction* show_nested_index_file_action = _pool.add(
            new ShowNestedIndexFileAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/show_nested_index_file",
                                      show_nested_index_file_action);
}

void HttpService::register_cloud_handler(CloudStorageEngine& engine) {
    CloudCompactionAction* show_compaction_action =
            _pool.add(new CloudCompactionAction(CompactionActionType::SHOW_INFO, _env, engine,
                                                TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/show",
                                      show_compaction_action);
    CloudCompactionAction* run_compaction_action =
            _pool.add(new CloudCompactionAction(CompactionActionType::RUN_COMPACTION, _env, engine,
                                                TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::POST, "/api/compaction/run",
                                      run_compaction_action);
    CloudCompactionAction* run_status_compaction_action = _pool.add(
            new CloudCompactionAction(CompactionActionType::RUN_COMPACTION_STATUS, _env, engine,
                                      TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/compaction/run_status",
                                      run_status_compaction_action);
#ifdef ENABLE_INJECTION_POINT
    InjectionPointAction* injection_point_action = _pool.add(new InjectionPointAction);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/injection_point/{op}",
                                      injection_point_action);
#endif
    FileCacheAction* file_cache_action = _pool.add(new FileCacheAction());
    _ev_http_server->register_handler(HttpMethod::GET, "/api/file_cache", file_cache_action);
    auto* show_hotspot_action = _pool.add(new ShowHotspotAction(engine));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/hotspot/tablet", show_hotspot_action);

    CalcFileCrcAction* calc_crc_action = _pool.add(
            new CalcFileCrcAction(_env, engine, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/calc_crc", calc_crc_action);

    ShowNestedIndexFileAction* show_nested_index_file_action = _pool.add(
            new ShowNestedIndexFileAction(_env, TPrivilegeHier::GLOBAL, TPrivilegeType::ADMIN));
    _ev_http_server->register_handler(HttpMethod::GET, "/api/show_nested_index_file",
                                      show_nested_index_file_action);
}
// NOLINTEND(readability-function-size)

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
