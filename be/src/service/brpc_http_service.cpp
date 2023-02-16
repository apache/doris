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

#include "brpc_http_service.h"

#include <string>

#include "http/brpc/action/check_rpc_channel_action.h"
#include "http/brpc/action/check_sum_action.h"
#include "http/brpc/action/check_tablet_segment_action.h"
#include "http/brpc/action/compaction_action.h"
#include "http/brpc/action/config_action.h"
#include "http/brpc/action/download_action.h"
#include "http/brpc/action/health_action.h"
#include "http/brpc/action/jeprofile_action.h"
#include "http/brpc/action/meta_action.h"
#include "http/brpc/action/metrics_action.h"
#include "http/brpc/action/pad_rowset_action.h"
#include "http/brpc/action/pprof_action.h"
#include "http/brpc/action/reload_tablet_action.h"
#include "http/brpc/action/reset_rpc_channel_action.h"
#include "http/brpc/action/restore_tablet_action.h"
#include "http/brpc/action/snapshot_action.h"
#include "http/brpc/action/stream_load_2pc.h"
#include "http/brpc/action/tablet_migration_action.h"
#include "http/brpc/action/tablets_distribution_action.h"
#include "http/brpc/action/tablets_info_action.h"
#include "http/brpc/action/version_action.h"

namespace doris {

#define DEFINE_ENDPOINT(__SIGNATURE__)                                                           \
    void BrpcHttpService::__SIGNATURE__(                                                         \
            ::google::protobuf::RpcController* controller, const ::doris::PHttpRequest* request, \
            ::doris::PHttpResponse* response, ::google::protobuf::Closure* done) {               \
        _dispatcher->dispatch(#__SIGNATURE__, controller, done);                                 \
    }

DEFINE_ENDPOINT(check_rpc_channel)
DEFINE_ENDPOINT(reset_rpc_channel)
DEFINE_ENDPOINT(config)
DEFINE_ENDPOINT(health)
DEFINE_ENDPOINT(jeprofile)
DEFINE_ENDPOINT(meta)
DEFINE_ENDPOINT(metrics)
DEFINE_ENDPOINT(monitor)
DEFINE_ENDPOINT(pad_rowset)
DEFINE_ENDPOINT(pprofile)
DEFINE_ENDPOINT(snapshot)
DEFINE_ENDPOINT(version)
DEFINE_ENDPOINT(check_tablet_segement)
DEFINE_ENDPOINT(check_sum)
DEFINE_ENDPOINT(compaction)
DEFINE_ENDPOINT(reload_tablet)
DEFINE_ENDPOINT(restore_tablet)
DEFINE_ENDPOINT(tablet_migration)
DEFINE_ENDPOINT(tablets_distribution)
DEFINE_ENDPOINT(tablets_info)
DEFINE_ENDPOINT(download)
DEFINE_ENDPOINT(stream_load)
DEFINE_ENDPOINT(stream_load_2pc)

#undef DEFINE_ENDPOINT

BrpcHttpService::BrpcHttpService(ExecEnv* exec_env) : _dispatcher(new HandlerDispatcher()) {
    _dispatcher->add_handler(new CheckRpcChannelHandler(exec_env))
            ->add_handler(new CheckSumHandler())
            ->add_handler(new CheckTabletSegmentHandler())
            ->add_handler(new CompactionHandler())
            ->add_handler(new ConfigHandler())
            ->add_handler(new HealthHandler())
            ->add_handler(new MetaHandler(HEADER))
            ->add_handler(new MetricsHandler(DorisMetrics::instance()->metric_registry()))
            ->add_handler(new PadRowsetHandler())
            ->add_handler(new PProfHandler(exec_env))
            ->add_handler(new ReloadTabletHandler(exec_env))
            ->add_handler(new ResetRpcChannelHandler(exec_env))
            ->add_handler(new RestoreTabletHandler(exec_env))
            ->add_handler(new SnapshotHandler())
            ->add_handler(new StreamLoad2PCHandler(exec_env))
            ->add_handler(new TabletMigrationHandler())
            ->add_handler(new TabletsDistributionHandler())
            ->add_handler(new TabletsInfoHandler())
            ->add_handler(new VersionHandler());

    JeProfileHandler::setup(exec_env, _dispatcher.get());
    DownloadHandler::setup(exec_env, _dispatcher.get());
}

void add_brpc_http_service(brpc::Server* server, ExecEnv* env) {
    const int stat =
            server->AddService(new BrpcHttpService(env), brpc::SERVER_OWNS_SERVICE,
                               "/api/check_rpc_channel/* => check_rpc_channel,"
                               "/api/checksum => check_sum,"
                               "/api/check_tablet_segment_lost => check_tablet_segement,"
                               "/api/compaction/* => compaction,"
                               "/api/*_config => config,"
                               "/api/_download_load => download,"
                               "/api/health => health,"
                               "/jeheap/dump => jeprofile,"
                               "/api/meta/header/* => meta,"
                               "/metrics => metrics,"
                               "/api/pad_rowset => pad_rowset,"
                               // adjust pprof uri to resolve the confict with brpc internal service
                               "/api/pprof/* => pprofile,"
                               "/api/reload_tablet => reload_tablet,"
                               "/api/reset_rpc_channel/* => reset_rpc_channel,"
                               "/api/restore_tablet => restore_tablet,"
                               "/api/snapshot => snapshot,"
                               "/api/*/_stream_load_2pc => stream_load_2pc,"
                               "/api/tablet_migration => tablet_migration,"
                               "/api/tablets_distribution => tablets_distribution,"
                               "/tablets_json => tablets_info,"
                               "/api/be_version_info => version");
    if (stat != 0) {
        LOG(WARNING) << "fail to add brpc http service";
    }
}
} // namespace doris
