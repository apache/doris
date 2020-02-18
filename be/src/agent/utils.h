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

#include "agent/status.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "runtime/client_cache.h"

namespace doris {

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
    DISALLOW_COPY_AND_ASSIGN(MasterServerClient);

    // Not ownder. Reference to the ExecEnv::_master_info
    const TMasterInfo& _master_info;
    FrontendServiceClientCache* _client_cache;
};

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
