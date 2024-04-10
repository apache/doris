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

#include "cloud/cloud_backend_service.h"

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/cache/block_file_cache_factory.h"
#include "util/thrift_server.h"

namespace doris {

CloudBackendService::CloudBackendService(CloudStorageEngine& engine, ExecEnv* exec_env)
        : BaseBackendService(exec_env), _engine(engine) {}

CloudBackendService::~CloudBackendService() = default;

Status CloudBackendService::create_service(CloudStorageEngine& engine, ExecEnv* exec_env, int port,
                                           std::unique_ptr<ThriftServer>* server,
                                           std::shared_ptr<doris::CloudBackendService> service) {
    service->_agent_server->cloud_start_workers(engine, exec_env);
    // TODO: do we want a BoostThreadFactory?
    // TODO: we want separate thread factories here, so that fe requests can't starve
    // be requests
    // std::shared_ptr<TProcessor> be_processor = std::make_shared<BackendServiceProcessor>(service);
    auto be_processor = std::make_shared<BackendServiceProcessor>(service);

    *server = std::make_unique<ThriftServer>("backend", be_processor, port,
                                             config::be_service_threads);

    LOG(INFO) << "Doris CloudBackendService listening on " << port;

    return Status::OK();
}

void CloudBackendService::sync_load_for_tablets(TSyncLoadForTabletsResponse&,
                                                const TSyncLoadForTabletsRequest& request) {
    auto f = [this, tablet_ids = request.tablet_ids]() {
        std::for_each(tablet_ids.cbegin(), tablet_ids.cend(), [this](int64_t tablet_id) {
            CloudTabletSPtr tablet;
            auto result = _engine.tablet_mgr().get_tablet(tablet_id, true);
            if (!result.has_value()) {
                return;
            }
            Status st = result.value()->sync_rowsets(-1, true);
            if (!st.ok()) {
                LOG_WARNING("failed to sync load for tablet").error(st);
            }
        });
    };
    static_cast<void>(_exec_env->sync_load_for_tablets_thread_pool()->submit_func(std::move(f)));
}

void CloudBackendService::get_top_n_hot_partitions(TGetTopNHotPartitionsResponse& response,
                                                   const TGetTopNHotPartitionsRequest& request) {
    TabletHotspot::instance()->get_top_n_hot_partition(&response.hot_tables);
    response.file_cache_size = io::FileCacheFactory::instance()->get_capacity();
    response.__isset.hot_tables = !response.hot_tables.empty();
}

} // namespace doris
