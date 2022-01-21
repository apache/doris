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

#ifndef DORIS_BE_SRC_AGENT_AGENT_SERVER_H
#define DORIS_BE_SRC_AGENT_AGENT_SERVER_H

#include <memory>
#include <string>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "runtime/exec_env.h"

namespace doris {

class TaskWorkerPool;
class TopicSubscriber;

// Each method corresponds to one RPC from FE Master, see BackendService.
class AgentServer {
public:
    explicit AgentServer(ExecEnv* exec_env, const TMasterInfo& master_info);
    ~AgentServer();

    // Receive agent task from FE master
    void submit_tasks(TAgentResult& agent_result, const std::vector<TAgentTaskRequest>& tasks);

    // TODO(lingbin): make the agent_result to be a pointer, because it will be modified.
    void make_snapshot(TAgentResult& agent_result, const TSnapshotRequest& snapshot_request);
    void release_snapshot(TAgentResult& agent_result, const std::string& snapshot_path);

    // Deprecated
    // TODO(lingbin): This method is deprecated, should be removed later.
    void publish_cluster_state(TAgentResult& agent_result, const TAgentPublishRequest& request);

    // Multi-Load will still use the following 3 methods for now.
    void submit_etl_task(TAgentResult& agent_result, const TMiniLoadEtlTaskRequest& request);
    void get_etl_status(TMiniLoadEtlStatusResult& agent_result,
                        const TMiniLoadEtlStatusRequest& request);
    void delete_etl_files(TAgentResult& result, const TDeleteEtlFilesRequest& request);

private:
    DISALLOW_COPY_AND_ASSIGN(AgentServer);

    // Not Owned
    ExecEnv* _exec_env;
    // Reference to the ExecEnv::_master_info
    const TMasterInfo& _master_info;

    std::unique_ptr<TaskWorkerPool> _create_tablet_workers;
    std::unique_ptr<TaskWorkerPool> _drop_tablet_workers;
    std::unique_ptr<TaskWorkerPool> _push_workers;
    std::unique_ptr<TaskWorkerPool> _publish_version_workers;
    std::unique_ptr<TaskWorkerPool> _clear_transaction_task_workers;
    std::unique_ptr<TaskWorkerPool> _delete_workers;
    std::unique_ptr<TaskWorkerPool> _alter_tablet_workers;
    std::unique_ptr<TaskWorkerPool> _clone_workers;
    std::unique_ptr<TaskWorkerPool> _storage_medium_migrate_workers;
    std::unique_ptr<TaskWorkerPool> _check_consistency_workers;

    // These 3 worker-pool do not accept tasks from FE.
    // It is self triggered periodically and reports to Fe master
    std::unique_ptr<TaskWorkerPool> _report_task_workers;
    std::unique_ptr<TaskWorkerPool> _report_disk_state_workers;
    std::unique_ptr<TaskWorkerPool> _report_tablet_workers;

    std::unique_ptr<TaskWorkerPool> _upload_workers;
    std::unique_ptr<TaskWorkerPool> _download_workers;
    std::unique_ptr<TaskWorkerPool> _make_snapshot_workers;
    std::unique_ptr<TaskWorkerPool> _release_snapshot_workers;
    std::unique_ptr<TaskWorkerPool> _move_dir_workers;
    std::unique_ptr<TaskWorkerPool> _recover_tablet_workers;
    std::unique_ptr<TaskWorkerPool> _update_tablet_meta_info_workers;

    std::unique_ptr<TaskWorkerPool> _submit_table_compaction_workers;

    std::unique_ptr<TopicSubscriber> _topic_subscriber;
};

} // end namespace doris

#endif // DORIS_BE_SRC_AGENT_AGENT_SERVER_H
