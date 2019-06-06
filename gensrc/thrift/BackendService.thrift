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

namespace cpp doris
namespace java org.apache.doris.thrift

include "Status.thrift"
include "Types.thrift"
include "AgentService.thrift"
include "PaloInternalService.thrift"

struct TPullLoadSubTaskInfo {
    1: required Types.TUniqueId id
    2: required i32 sub_task_id
    3: required map<string, i64> file_map
    4: required map<string, string> counters
    5: optional string tracking_url
}

struct TPullLoadTaskInfo {
    1: required Types.TUniqueId id
    2: required Types.TEtlState etl_state
    3: optional map<string, i64> file_map
    4: optional map<string, string> counters
    5: optional list<string> tracking_urls
}

struct TFetchPullLoadTaskInfoResult {
    1: required Status.TStatus status
    2: required TPullLoadTaskInfo task_info
}

struct TFetchAllPullLoadTaskInfosResult {
    1: required Status.TStatus status
    2: required list<TPullLoadTaskInfo> task_infos
}

struct TExportTaskRequest {
    1: required PaloInternalService.TExecPlanFragmentParams params
}

struct TTabletStat {
    1: required i64 tablet_id
    2: optional i64 data_size
    3: optional i64 row_num
}

struct TTabletStatResult {
    1: required map<i64, TTabletStat> tablets_stats
}

struct TKafkaLoadInfo {
    1: required string brokers;
    2: required string topic;
    3: required map<i32, i64> partition_begin_offset;
    4: optional map<string, string> properties;
}

struct TRoutineLoadTask {
    1: required Types.TLoadSourceType type
    2: required i64 job_id
    3: required Types.TUniqueId id
    4: required i64 txn_id
    5: required i64 auth_code
    6: optional string db
    7: optional string tbl
    8: optional string label
    9: optional i64 max_interval_s
    10: optional i64 max_batch_rows
    11: optional i64 max_batch_size
    12: optional TKafkaLoadInfo kafka_load_info
    13: optional PaloInternalService.TExecPlanFragmentParams params
}

struct TKafkaMetaProxyRequest {
    1: optional TKafkaLoadInfo kafka_info
}

struct TKafkaMetaProxyResult {
    1: optional list<i32> partition_ids
}

struct TProxyRequest {
    1: optional TKafkaMetaProxyRequest kafka_meta_request;
}

struct TProxyResult {
    1: required Status.TStatus status;
    2: optional TKafkaMetaProxyResult kafka_meta_result;
}

service BackendService {
    // Called by coord to start asynchronous execution of plan fragment in backend.
    // Returns as soon as all incoming data streams have been set up.
    PaloInternalService.TExecPlanFragmentResult exec_plan_fragment(1:PaloInternalService.TExecPlanFragmentParams params);

    // Called by coord to cancel execution of a single plan fragment, which this
    // coordinator initiated with a prior call to ExecPlanFragment.
    // Cancellation is asynchronous.
    PaloInternalService.TCancelPlanFragmentResult cancel_plan_fragment(
        1:PaloInternalService.TCancelPlanFragmentParams params);

    // Called by sender to transmit single row batch. Returns error indication
    // if params.fragmentId or params.destNodeId are unknown or if data couldn't be read.
    PaloInternalService.TTransmitDataResult transmit_data(
        1:PaloInternalService.TTransmitDataParams params);

    // Coordinator Fetch Data From Root fragment
    PaloInternalService.TFetchDataResult fetch_data(
        1:PaloInternalService.TFetchDataParams params);

    AgentService.TAgentResult submit_tasks(1:list<AgentService.TAgentTaskRequest> tasks);

    AgentService.TAgentResult make_snapshot(1:AgentService.TSnapshotRequest snapshot_request);

    AgentService.TAgentResult release_snapshot(1:string snapshot_path);

    AgentService.TAgentResult publish_cluster_state(1:AgentService.TAgentPublishRequest request);

    AgentService.TAgentResult submit_etl_task(1:AgentService.TMiniLoadEtlTaskRequest request);

    AgentService.TMiniLoadEtlStatusResult get_etl_status(
            1:AgentService.TMiniLoadEtlStatusRequest request);

    AgentService.TAgentResult delete_etl_files(1:AgentService.TDeleteEtlFilesRequest request);

    // Register one pull load task.
    Status.TStatus register_pull_load_task(1: Types.TUniqueId id, 2: i32 num_senders)

    // Call by task coordinator to unregister this task.
    // This task may be failed because load task have been finished or this task
    // has been canceled by coordinator.
    Status.TStatus deregister_pull_load_task(1: Types.TUniqueId id)

    Status.TStatus report_pull_load_sub_task_info(1:TPullLoadSubTaskInfo task_info)

    TFetchPullLoadTaskInfoResult fetch_pull_load_task_info(1:Types.TUniqueId id)

    TFetchAllPullLoadTaskInfosResult fetch_all_pull_load_task_infos()

    Status.TStatus submit_export_task(1:TExportTaskRequest request);

    PaloInternalService.TExportStatusResult get_export_status(1:Types.TUniqueId task_id);

    Status.TStatus erase_export_task(1:Types.TUniqueId task_id);

    TTabletStatResult get_tablet_stat();

    Status.TStatus submit_routine_load_task(1:list<TRoutineLoadTask> tasks);
}
