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

#pragma once

#include <cstdint>
#include <memory>

#include "server_starter.h"

namespace doris {
class BaseBackendService;
class ClusterInfo;
class ExecEnv;
} // namespace doris

namespace doris::server {

// Each factory returns the OSS or TLS-capable starter selected by the build.
// Callers should treat the returned object as the owner of the startup flow
// for that protocol instead of constructing protocol-specific servers directly.
Status create_brpc_starter(ExecEnv* env, int port, int num_threads,
                           std::unique_ptr<IServerStarter>* out);

Status create_http_starter(ExecEnv* env, int port, int num_workers,
                           std::unique_ptr<IServerStarter>* out);

Status create_backend_thrift_starter(ExecEnv* env, int port,
                                     std::shared_ptr<BaseBackendService> service,
                                     std::unique_ptr<IServerStarter>* out);

Status create_heartbeat_thrift_starter(ExecEnv* env, int port, uint32_t worker_thread_num,
                                       ClusterInfo* cluster_info,
                                       std::unique_ptr<IServerStarter>* out);

Status create_flight_starter(int port, std::unique_ptr<IServerStarter>* out);

} // namespace doris::server
