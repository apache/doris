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

#include "common/config.h"
#include "util/thrift_server.h"

namespace doris {

CloudBackendService::CloudBackendService(CloudStorageEngine& engine, ExecEnv* exec_env)
        : BaseBackendService(exec_env), _engine(engine) {}

CloudBackendService::~CloudBackendService() = default;

Status CloudBackendService::create_service(CloudStorageEngine& engine, ExecEnv* exec_env, int port,
                                           std::unique_ptr<ThriftServer>* server) {
    auto service = std::make_shared<CloudBackendService>(engine, exec_env);
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

} // namespace doris
