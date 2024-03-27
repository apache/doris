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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace doris {

class TaskWorkerPoolIf;
class TaskWorkerPool;
class PriorTaskWorkerPool;
class ReportWorker;
class TopicSubscriber;
class ExecEnv;
class TAgentPublishRequest;
class TAgentResult;
class TAgentTaskRequest;
class TMasterInfo;
class TSnapshotRequest;
class StorageEngine;
class CloudStorageEngine;

// Each method corresponds to one RPC from FE Master, see BackendService.
class AgentServer {
public:
    explicit AgentServer(ExecEnv* exec_env, const TMasterInfo& master_info);
    ~AgentServer();

    void start_workers(StorageEngine& engine, ExecEnv* exec_env);

    void cloud_start_workers(CloudStorageEngine& engine, ExecEnv* exec_env);

    // Receive agent task from FE master
    void submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks);

    // Deprecated
    // TODO(lingbin): This method is deprecated, should be removed later.
    // [[deprecated]]
    void publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request);

    TopicSubscriber* get_topic_subscriber() { return _topic_subscriber.get(); }

    void stop_report_workers();

private:
    // Reference to the ExecEnv::_master_info
    const TMasterInfo& _master_info;

    std::unordered_map<int64_t /* TTaskType */, std::unique_ptr<TaskWorkerPoolIf>> _workers;

    // These workers do not accept tasks from FE.
    // It is self triggered periodically and reports to FE master
    std::vector<std::unique_ptr<ReportWorker>> _report_workers;

    std::unique_ptr<TopicSubscriber> _topic_subscriber;
};

} // end namespace doris
