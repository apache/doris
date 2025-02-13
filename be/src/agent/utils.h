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

#include <map>
#include <string>

#include "common/status.h"

namespace doris {
class TConfirmUnusedRemoteFilesRequest;
class TConfirmUnusedRemoteFilesResult;
class TFinishTaskRequest;
class TMasterResult;
class TReportRequest;
class ClusterInfo;

class MasterServerClient {
public:
    static MasterServerClient* create(const ClusterInfo* cluster_info);
    static MasterServerClient* instance();

    ~MasterServerClient() = default;

    // Report finished task to the master server
    //
    // Input parameters:
    // * request: The information of finished task
    //
    // Output parameters:
    // * result: The result of report task
    Status finish_task(const TFinishTaskRequest& request, TMasterResult* result);

    // Report tasks/olap tablet/disk state to the master server
    //
    // Input parameters:
    // * request: The information to report
    //
    // Output parameters:
    // * result: The result of report task
    Status report(const TReportRequest& request, TMasterResult* result);

    Status confirm_unused_remote_files(const TConfirmUnusedRemoteFilesRequest& request,
                                       TConfirmUnusedRemoteFilesResult* result);

private:
    MasterServerClient(const ClusterInfo* cluster_info);

    DISALLOW_COPY_AND_ASSIGN(MasterServerClient);

    // Not owner. Reference to the ExecEnv::_cluster_info
    const ClusterInfo* _cluster_info;
};

class AgentUtils {
public:
    AgentUtils() = default;
    virtual ~AgentUtils() = default;

    // Execute shell cmd
    virtual bool exec_cmd(const std::string& command, std::string* errmsg,
                          bool redirect_stderr = true);

    // Write a map to file by json format
    virtual bool write_json_to_file(const std::map<std::string, std::string>& info,
                                    const std::string& path);

private:
    DISALLOW_COPY_AND_ASSIGN(AgentUtils);
}; // class AgentUtils

} // namespace doris
