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

#include <gen_cpp/BackendService.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "agent/agent_server.h"
#include "common/status.h"

namespace doris {

class ExecEnv;
class ThriftServer;
class TAgentResult;
class TAgentTaskRequest;
class TAgentPublishRequest;
class TExecPlanFragmentParams;
class TExecPlanFragmentResult;
class TCancelPlanFragmentResult;
class TTransmitDataResult;
class TExportTaskRequest;
class TExportStatusResult;
class TStreamLoadRecordResult;
class TDiskTrashInfo;
class TCancelPlanFragmentParams;
class TCheckStorageFormatResult;
class TRoutineLoadTask;
class TScanBatchResult;
class TScanCloseParams;
class TScanCloseResult;
class TScanNextBatchParams;
class TScanOpenParams;
class TScanOpenResult;
class TSnapshotRequest;
class TStatus;
class TTabletStatResult;
class TTransmitDataParams;
class TUniqueId;
class TIngestBinlogRequest;
class TIngestBinlogResult;

// This class just forward rpc for actual handler
// make this class because we can bind multiple service on single point
class BackendService : public BackendServiceIf {
public:
    BackendService(ExecEnv* exec_env);

    ~BackendService() override = default;

    // NOTE: now we do not support multiple backend in one process
    static Status create_service(ExecEnv* exec_env, int port, ThriftServer** server);

    // Agent service
    void submit_tasks(TAgentResult& return_value,
                      const std::vector<TAgentTaskRequest>& tasks) override {
        _agent_server->submit_tasks(return_value, tasks);
    }

    void make_snapshot(TAgentResult& return_value,
                       const TSnapshotRequest& snapshot_request) override {
        _agent_server->make_snapshot(return_value, snapshot_request);
    }

    void release_snapshot(TAgentResult& return_value, const std::string& snapshot_path) override {
        _agent_server->release_snapshot(return_value, snapshot_path);
    }

    void publish_cluster_state(TAgentResult& result, const TAgentPublishRequest& request) override {
        _agent_server->publish_cluster_state(result, request);
    }

    // DorisServer service
    void exec_plan_fragment(TExecPlanFragmentResult& return_val,
                            const TExecPlanFragmentParams& params) override;

    void cancel_plan_fragment(TCancelPlanFragmentResult& return_val,
                              const TCancelPlanFragmentParams& params) override;

    void transmit_data(TTransmitDataResult& return_val, const TTransmitDataParams& params) override;

    void submit_export_task(TStatus& t_status, const TExportTaskRequest& request) override;

    void get_export_status(TExportStatusResult& result, const TUniqueId& task_id) override;

    void erase_export_task(TStatus& t_status, const TUniqueId& task_id) override;

    void get_tablet_stat(TTabletStatResult& result) override;

    int64_t get_trash_used_capacity() override;

    void get_disk_trash_used_capacity(std::vector<TDiskTrashInfo>& diskTrashInfos) override;

    void submit_routine_load_task(TStatus& t_status,
                                  const std::vector<TRoutineLoadTask>& tasks) override;

    // used for external service, open means start the scan procedure
    void open_scanner(TScanOpenResult& result_, const TScanOpenParams& params) override;

    // used for external service, external use getNext to fetch data batch after batch until eos = true
    void get_next(TScanBatchResult& result_, const TScanNextBatchParams& params) override;

    // used for external service, close some context and release resource related with this context
    void close_scanner(TScanCloseResult& result_, const TScanCloseParams& params) override;

    void get_stream_load_record(TStreamLoadRecordResult& result,
                                const int64_t last_stream_record_time) override;

    void clean_trash() override;

    void check_storage_format(TCheckStorageFormatResult& result) override;

    void ingest_binlog(TIngestBinlogResult& result, const TIngestBinlogRequest& request) override;

private:
    Status start_plan_fragment_execution(const TExecPlanFragmentParams& exec_params);
    ExecEnv* _exec_env;
    std::unique_ptr<AgentServer> _agent_server;
};

} // namespace doris
