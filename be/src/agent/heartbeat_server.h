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

#include <butil/macros.h>
#include <gen_cpp/HeartbeatService.h>
#include <stdint.h>

#include <mutex>

#include "common/status.h"

namespace doris {
class ExecEnv;
class THeartbeatResult;
class TMasterInfo;

const uint32_t HEARTBEAT_INTERVAL = 10;
class BaseStorageEngine;
class ThriftServer;

class HeartbeatServer : public HeartbeatServiceIf {
public:
    explicit HeartbeatServer(TMasterInfo* master_info);
    ~HeartbeatServer() override = default;

    void init_cluster_id();

    // Master send heartbeat to this server
    //
    // Input parameters:
    // * master_info: The struct of master info, contains host ip and port
    //
    // Output parameters:
    // * heartbeat_result: The result of heartbeat set
    void heartbeat(THeartbeatResult& heartbeat_result, const TMasterInfo& master_info) override;

private:
    Status _heartbeat(const TMasterInfo& master_info);

    BaseStorageEngine& _engine;
    int64_t _be_epoch;

    // mutex to protect master_info and _epoch
    std::mutex _hb_mtx;
    // Not owned. Point to the ExecEnv::_master_info
    TMasterInfo* _master_info = nullptr;
    int64_t _fe_epoch;

    DISALLOW_COPY_AND_ASSIGN(HeartbeatServer);
}; // class HeartBeatServer

Status create_heartbeat_server(ExecEnv* exec_env, uint32_t heartbeat_server_port,
                               std::unique_ptr<ThriftServer>* heart_beat_server,
                               uint32_t worker_thread_num, TMasterInfo* local_master_info);
} // namespace doris
