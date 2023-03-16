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

#include "service/single_replica_load_download_service.h"

#include "http/action/download_action.h"
#include "http/ev_http_server.h"
#include "runtime/exec_env.h"

namespace doris {

SingleReplicaLoadDownloadService::SingleReplicaLoadDownloadService(ExecEnv* env, int port,
                                                                   int num_threads)
        : _env(env), _ev_http_server(new EvHttpServer(port, num_threads)) {}

SingleReplicaLoadDownloadService::~SingleReplicaLoadDownloadService() {}

Status SingleReplicaLoadDownloadService::start() {
    // register download action
    std::vector<std::string> allow_paths;
    for (auto& path : _env->store_paths()) {
        if (FilePathDesc::is_remote(path.storage_medium)) {
            continue;
        }
        allow_paths.emplace_back(path.path);
    }
    DownloadAction* tablet_download_action = _pool.add(new DownloadAction(_env, allow_paths));
    _ev_http_server->register_handler(HttpMethod::HEAD, "/api/_tablet/_download",
                                      tablet_download_action);
    _ev_http_server->register_handler(HttpMethod::GET, "/api/_tablet/_download",
                                      tablet_download_action);

    _ev_http_server->start();
    return Status::OK();
}

void SingleReplicaLoadDownloadService::stop() {
    _ev_http_server->stop();
    _pool.clear();
}

} // namespace doris
