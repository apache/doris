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

#ifndef DORIS_BE_SRC_AGENT_MOCK_MOCK_UTILS_H
#define DORIS_BE_SRC_AGENT_MOCK_MOCK_UTILS_H

#include "agent/utils.h"
#include "gmock/gmock.h"

namespace doris {

class MockAgentServerClient : public AgentServerClient {
public:
    MockAgentServerClient(const TBackend backend);
    MOCK_METHOD2(make_snapshot,
                 Status(const TSnapshotRequest& snapshot_request, TAgentResult* result));
    MOCK_METHOD2(release_snapshot, Status(const std::string& snapshot_path, TAgentResult* result));
}; // class AgentServerClient

class MockMasterServerClient : public MasterServerClient {
public:
    MockMasterServerClient(const TMasterInfo& master_info,
                           FrontendServiceClientCache* client_cache);
    MOCK_METHOD2(finish_task, Status(const TFinishTaskRequest request, TMasterResult* result));
    MOCK_METHOD2(report, Status(const TReportRequest request, TMasterResult* result));
}; // class AgentServerClient

class MockAgentUtils : public AgentUtils {
public:
    MOCK_METHOD0(get_local_ip, char*());
    MOCK_METHOD2(exec_cmd, bool(const std::string& command, std::string* errmsg));
    MOCK_METHOD2(write_json_to_file,
                 bool(const std::map<std::string, std::string>& info, const std::string& path));
};

} // namespace doris
#endif // DORIS_BE_SRC_AGENT_MOCK_MOCK_UTILS_H
