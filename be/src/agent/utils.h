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

#ifndef DORIS_BE_SRC_AGENT_UTILS_H
#define DORIS_BE_SRC_AGENT_UTILS_H

#include <pthread.h>
#include <memory>
#include "thrift/transport/TSocket.h"
#include "thrift/transport/TTransportUtils.h"
#include "agent/status.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/Status_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_define.h"
#include "runtime/client_cache.h"

namespace doris {

// client cache
// All service client should be defined in client_cache.h
//class MasterServiceClient;
//typedef ClientCache<MasterServiceClient> MasterServiceClientCache;
//typedef ClientConnection<MasterServiceClient> MasterServiceConnection;

class AgentServerClient {
public:
    explicit AgentServerClient(const TBackend backend);
    virtual ~AgentServerClient();
    
    // Make a snapshot of tablet
    //
    // Input parameters:
    // * tablet_id: The id of tablet to make snapshot
    // * schema_hash: The schema hash of tablet to make snapshot
    //
    // Output parameters:
    // * result: The result of make snapshot
    virtual AgentStatus make_snapshot(
            const TSnapshotRequest& snapshot_request,
            TAgentResult* result);

    // Release the snapshot
    //
    // Input parameters:
    // * snapshot_path: The path of snapshot
    //
    // Output parameters:
    // * result: The result of release snapshot
    virtual AgentStatus release_snapshot(const std::string& snapshot_path, TAgentResult* result);

private:
    boost::shared_ptr<apache::thrift::transport::TTransport> _socket;
    boost::shared_ptr<apache::thrift::transport::TTransport> _transport;
    boost::shared_ptr<apache::thrift::protocol::TProtocol> _protocol;
    BackendServiceClient _agent_service_client;
    DISALLOW_COPY_AND_ASSIGN(AgentServerClient);
};  // class AgentServerClient

class MasterServerClient {
public:
    MasterServerClient(const TMasterInfo& master_info, FrontendServiceClientCache* client_cache);
    virtual ~MasterServerClient() {};
    
    // Reprot finished task to the master server
    //
    // Input parameters:
    // * request: The infomation of finished task
    //
    // Output parameters:
    // * result: The result of report task
    virtual AgentStatus finish_task(const TFinishTaskRequest& request, TMasterResult* result);
    
    // Report tasks/olap tablet/disk state to the master server
    //
    // Input parameters:
    // * request: The infomation to report
    //
    // Output parameters:
    // * result: The result of report task
    virtual AgentStatus report(const TReportRequest& request, TMasterResult* result);

private:
    const TMasterInfo& _master_info;

    FrontendServiceClientCache* _client_cache;
    DISALLOW_COPY_AND_ASSIGN(MasterServerClient);
};  // class MasterServerClient

class AgentUtils {
public:
    AgentUtils() {};
    virtual ~AgentUtils() {};

    // Use rsync synchronize folder from remote agent to local folder
    //
    // Input parameters:
    // * remote_host: the host of remote server
    // * remote_file_path: remote file folder path
    // * local_file_path: local file folder path
    // * exclude_file_patterns: the patterns of the exclude file
    // * transport_speed_limit_kbps: speed limit of transport(kb/s)
    // * timeout_second: timeout of synchronize
    virtual AgentStatus rsync_from_remote(
            const std::string& remote_host,
            const std::string& remote_file_path,
            const std::string& local_file_path,
            const std::vector<std::string>& exclude_file_patterns,
            const uint32_t transport_speed_limit_kbps,
            const uint32_t timeout_second);

    // Print AgentStatus as string
    virtual std::string print_agent_status(AgentStatus status);

    // Execute shell cmd
    virtual bool exec_cmd(const std::string& command, std::string* errmsg);

    // Write a map to file by json format
    virtual bool write_json_to_file(
            const std::map<std::string, std::string>& info,
            const std::string& path);

private:
    DISALLOW_COPY_AND_ASSIGN(AgentUtils);
};  // class AgentUtils

}  // namespace doris
#endif  // DORIS_BE_SRC_AGENT_UTILS_H
