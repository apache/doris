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

#include "thrift/transport/TTransportUtils.h"
#include "agent/status.h"
#include "agent/task_worker_pool.h"
#include "agent/topic_subscriber.h"
#include "agent/utils.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"

namespace doris {

class AgentServer {
public:
    explicit AgentServer(ExecEnv* exec_env, const TMasterInfo& master_info);
    ~AgentServer();

    // Receive agent task from dm
    // 
    // Input parameters:
    // * tasks: The list of agent tasks
    //
    // Output parameters:
    // * return_value: The result of receive agent task,
    //                 contains return code and error messages.
    void submit_tasks(
            TAgentResult& return_value,
            const std::vector<TAgentTaskRequest>& tasks);
    
    // Make a snapshot for a local tablet
    //
    // Input parameters:
    // * tablet_id: The tablet id of local tablet.
    // * schema_hash: The schema hash of local tablet
    //
    // Output parameters:
    // * return_value: The result of make snapshot,
    //                 contains return code and error messages.
    void make_snapshot(
            TAgentResult& return_value,
            const TSnapshotRequest& snapshot_request);

    // Release useless snapshot
    //
    // Input parameters:
    // * snapshot_path: local useless snapshot path
    //
    // Output parameters:
    // * return_value: The result of release snapshot,
    //                 contains return code and error messages.
    void release_snapshot(TAgentResult& return_value, const std::string& snapshot_path);

    // Publish state to agent
    //
    // Input parameters:
    //   request:  
    void publish_cluster_state(TAgentResult& return_value, 
                               const TAgentPublishRequest& request);

    // Master call this rpc to submit a etl task
    void submit_etl_task(TAgentResult& return_value, 
                         const TMiniLoadEtlTaskRequest& request);

    // Master call this rpc to fetch status of elt task
    void get_etl_status(TMiniLoadEtlStatusResult& return_value,
                        const TMiniLoadEtlStatusRequest& request);

    void delete_etl_files(TAgentResult& result, 
                          const TDeleteEtlFilesRequest& request);

private:
    ExecEnv* _exec_env;
    const TMasterInfo& _master_info;

    TaskWorkerPool* _create_tablet_workers;
    TaskWorkerPool* _drop_tablet_workers;
    TaskWorkerPool* _push_workers;
    TaskWorkerPool* _publish_version_workers;
    TaskWorkerPool* _clear_alter_task_workers;
    TaskWorkerPool* _clear_transaction_task_workers;
    TaskWorkerPool* _delete_workers;
    TaskWorkerPool* _alter_tablet_workers;
    TaskWorkerPool* _clone_workers;
    TaskWorkerPool* _storage_medium_migrate_workers;
    TaskWorkerPool* _check_consistency_workers;
    TaskWorkerPool* _report_task_workers;
    TaskWorkerPool* _report_disk_state_workers;
    TaskWorkerPool* _report_tablet_workers;
    TaskWorkerPool* _upload_workers;
    TaskWorkerPool* _download_workers;
    TaskWorkerPool* _make_snapshot_workers;
    TaskWorkerPool* _release_snapshot_workers;
    TaskWorkerPool* _move_dir_workers;
    TaskWorkerPool* _recover_tablet_workers;

    DISALLOW_COPY_AND_ASSIGN(AgentServer);
    
    TopicSubscriber* _topic_subscriber;   
};  // class AgentServer
}  // namespace doris
#endif  // DORIS_BE_SRC_AGENT_AGENT_SERVER_H
