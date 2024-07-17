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
include "PlanNodes.thrift"
include "AgentService.thrift"
include "PaloInternalService.thrift"
include "DorisExternalService.thrift"
include "FrontendService.thrift"

struct TExportTaskRequest {
    1: required PaloInternalService.TExecPlanFragmentParams params
}

struct TTabletStat {
    1: required i64 tablet_id
    // local data size
    2: optional i64 data_size
    3: optional i64 row_count
    4: optional i64 total_version_count
    5: optional i64 remote_data_size
    6: optional i64 visible_version_count
}

struct TTabletStatResult {
    1: required map<i64, TTabletStat> tablets_stats
    2: optional list<TTabletStat> tablet_stat_list
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
    14: optional PlanNodes.TFileFormatType format
    15: optional PaloInternalService.TPipelineFragmentParams pipeline_params
    16: optional bool is_multi_table
    17: optional bool memtable_on_sink_node;
    18: optional string qualified_user
    19: optional string cloud_cluster
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

struct TStreamLoadRecord {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required string label
    8: required string status
    9: required string message
    10: optional string url
    11: optional i64 auth_code;
    12: required i64 total_rows
    13: required i64 loaded_rows
    14: required i64 filtered_rows
    15: required i64 unselected_rows
    16: required i64 load_bytes
    17: required i64 start_time
    18: required i64 finish_time
    19: optional string comment
}

struct TStreamLoadRecordResult {
    1: required map<string, TStreamLoadRecord> stream_load_record
}

struct TDiskTrashInfo {
    1: required string root_path
    2: required string state
    3: required i64 trash_used_capacity
}

struct TCheckStorageFormatResult {
    1: optional list<i64> v1_tablets;
    2: optional list<i64> v2_tablets;
}

struct TWarmUpCacheAsyncRequest {
    1: required string host
    2: required i32 brpc_port
    3: required list<i64> tablet_ids
}

struct TWarmUpCacheAsyncResponse {
    1: required Status.TStatus status
}

struct TCheckWarmUpCacheAsyncRequest {
    1: optional list<i64> tablets
}

struct TCheckWarmUpCacheAsyncResponse {
    1: required Status.TStatus status
    2: optional map<i64, bool> task_done;
}

struct TSyncLoadForTabletsRequest {
    1: required list<i64> tablet_ids
}

struct TSyncLoadForTabletsResponse {
}

struct THotPartition {
    1: required i64 partition_id
    2: required i64 last_access_time
    3: optional i64 query_per_day
    4: optional i64 query_per_week
}

struct THotTableMessage {
    1: required i64 table_id
    2: required i64 index_id
    3: optional list<THotPartition> hot_partitions
}

struct TGetTopNHotPartitionsRequest {
}

struct TGetTopNHotPartitionsResponse {
    1: required i64 file_cache_size
    2: optional list<THotTableMessage> hot_tables
}

enum TDownloadType {
    BE = 0,
    S3 = 1,
}

enum TWarmUpTabletsRequestType {
    SET_JOB = 0,
    SET_BATCH = 1,
    GET_CURRENT_JOB_STATE_AND_LEASE = 2,
    CLEAR_JOB = 3,
}

struct TJobMeta {
    1: required TDownloadType download_type
    2: optional string be_ip
    3: optional i32 brpc_port
    4: optional list<i64> tablet_ids
}

struct TWarmUpTabletsRequest {
    1: required i64 job_id
    2: required i64 batch_id
    3: optional list<TJobMeta> job_metas
    4: required TWarmUpTabletsRequestType type
}

struct TWarmUpTabletsResponse {
    1: required Status.TStatus status;
    2: optional i64 job_id
    3: optional i64 batch_id
    4: optional i64 pending_job_size
    5: optional i64 finish_job_size
}

struct TIngestBinlogRequest {
    1: optional i64 txn_id;
    2: optional i64 remote_tablet_id;
    3: optional i64 binlog_version;
    4: optional string remote_host;
    5: optional string remote_port;
    6: optional i64 partition_id;
    7: optional i64 local_tablet_id;
    8: optional Types.TUniqueId load_id;
}

struct TIngestBinlogResult {
    1: optional Status.TStatus status;
    2: optional bool is_async;
}

struct TQueryIngestBinlogRequest {
    1: optional i64 txn_id;
    2: optional i64 partition_id;
    3: optional i64 tablet_id;
    4: optional Types.TUniqueId load_id;
}

enum TIngestBinlogStatus {
    ANALYSIS_ERROR,
    UNKNOWN,
    NOT_FOUND,
    OK,
    FAILED,
    DOING
}

struct TQueryIngestBinlogResult {
    1: optional TIngestBinlogStatus status;
    2: optional string err_msg;
}

enum TTopicInfoType {
    WORKLOAD_GROUP
    MOVE_QUERY_TO_GROUP
    WORKLOAD_SCHED_POLICY
}

struct TWorkloadGroupInfo {
  1: optional i64 id
  2: optional string name
  3: optional i64 version
  4: optional i64 cpu_share
  5: optional i32 cpu_hard_limit
  6: optional string mem_limit
  7: optional bool enable_memory_overcommit
  8: optional bool enable_cpu_hard_limit
  9: optional i32 scan_thread_num
  10: optional i32 max_remote_scan_thread_num
  11: optional i32 min_remote_scan_thread_num
  12: optional i32 spill_threshold_low_watermark
  13: optional i32 spill_threshold_high_watermark
}

enum TWorkloadMetricType {
    QUERY_TIME
    BE_SCAN_ROWS
    BE_SCAN_BYTES
    QUERY_BE_MEMORY_BYTES
}

enum TCompareOperator {
    EQUAL
    GREATER
    GREATER_EQUAL
    LESS
    LESS_EQUAL
}

struct TWorkloadCondition {
    1: optional TWorkloadMetricType metric_name
    2: optional TCompareOperator op
    3: optional string value
}

enum TWorkloadActionType {
    MOVE_QUERY_TO_GROUP
    CANCEL_QUERY
}

struct TWorkloadAction {
    1: optional TWorkloadActionType action
    2: optional string action_args
}

struct TWorkloadSchedPolicy {
    1: optional i64 id
    2: optional string name
    3: optional i32 version
    4: optional i32 priority
    5: optional bool enabled
    6: optional list<TWorkloadCondition> condition_list
    7: optional list<TWorkloadAction> action_list
    8: optional list<i64> wg_id_list
}

struct TopicInfo {
    1: optional TWorkloadGroupInfo workload_group_info
    2: optional TWorkloadSchedPolicy workload_sched_policy
}

struct TPublishTopicRequest {
    1: required map<TTopicInfoType, list<TopicInfo>> topic_map
}

struct TPublishTopicResult {
    1: required Status.TStatus status
}

struct TGetRealtimeExecStatusRequest {
    // maybe query id or other unique id
    1: optional Types.TUniqueId id
}

struct TGetRealtimeExecStatusResponse {
    1: optional Status.TStatus status
    2: optional FrontendService.TReportExecStatusParams report_exec_status_params
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

    AgentService.TAgentResult submit_tasks(1:list<AgentService.TAgentTaskRequest> tasks);

    AgentService.TAgentResult make_snapshot(1:AgentService.TSnapshotRequest snapshot_request);

    AgentService.TAgentResult release_snapshot(1:string snapshot_path);

    AgentService.TAgentResult publish_cluster_state(1:AgentService.TAgentPublishRequest request);

    Status.TStatus submit_export_task(1:TExportTaskRequest request);

    PaloInternalService.TExportStatusResult get_export_status(1:Types.TUniqueId task_id);

    Status.TStatus erase_export_task(1:Types.TUniqueId task_id);

    TTabletStatResult get_tablet_stat();

    i64 get_trash_used_capacity();

    list<TDiskTrashInfo> get_disk_trash_used_capacity();

    Status.TStatus submit_routine_load_task(1:list<TRoutineLoadTask> tasks);

    // doris will build  a scan context for this session, context_id returned if success
    DorisExternalService.TScanOpenResult open_scanner(1: DorisExternalService.TScanOpenParams params);

    // return the batch_size of data
    DorisExternalService.TScanBatchResult get_next(1: DorisExternalService.TScanNextBatchParams params);

    // release the context resource associated with the context_id
    DorisExternalService.TScanCloseResult close_scanner(1: DorisExternalService.TScanCloseParams params);

    TStreamLoadRecordResult get_stream_load_record(1: i64 last_stream_record_time);

    // check tablet rowset type
    TCheckStorageFormatResult check_storage_format();

    TWarmUpCacheAsyncResponse warm_up_cache_async(1: TWarmUpCacheAsyncRequest request);

    TCheckWarmUpCacheAsyncResponse check_warm_up_cache_async(1: TCheckWarmUpCacheAsyncRequest request);

    TSyncLoadForTabletsResponse sync_load_for_tablets(1: TSyncLoadForTabletsRequest request);

    TGetTopNHotPartitionsResponse get_top_n_hot_partitions(1: TGetTopNHotPartitionsRequest request);

    TWarmUpTabletsResponse warm_up_tablets(1: TWarmUpTabletsRequest request);

    TIngestBinlogResult ingest_binlog(1: TIngestBinlogRequest ingest_binlog_request);
    TQueryIngestBinlogResult query_ingest_binlog(1: TQueryIngestBinlogRequest query_ingest_binlog_request);

    TPublishTopicResult publish_topic_info(1:TPublishTopicRequest topic_request);

    TGetRealtimeExecStatusResponse get_realtime_exec_status(1:TGetRealtimeExecStatusRequest request);
}
