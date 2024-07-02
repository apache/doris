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

include "AgentService.thrift"
include "PaloInternalService.thrift"
include "Types.thrift"
include "Status.thrift"

struct TTabletInfo {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required Types.TVersion version
    4: required Types.TVersionHash version_hash
    5: required Types.TCount row_count
    // data size on local disk
    6: required Types.TSize data_size
    7: optional Types.TStorageMedium storage_medium
    8: optional list<Types.TTransactionId> transaction_ids
    9: optional i64 total_version_count
    10: optional i64 path_hash
    11: optional bool version_miss
    12: optional bool used
    13: optional Types.TPartitionId partition_id
    14: optional bool is_in_memory
    15: optional Types.TReplicaId replica_id
    // data size on remote storage
    16: optional Types.TSize remote_data_size
    // 17: optional Types.TReplicaId cooldown_replica_id
    // 18: optional bool is_cooldown
    19: optional i64 cooldown_term
    20: optional Types.TUniqueId cooldown_meta_id
    21: optional i64 visible_version_count

    // For cloud
    1000: optional bool is_persistent
}

struct TFinishTaskRequest {
    1: required Types.TBackend backend
    2: required Types.TTaskType task_type
    3: required i64 signature
    4: required Status.TStatus task_status
    5: optional i64 report_version
    6: optional list<TTabletInfo> finish_tablet_infos
    7: optional i64 tablet_checksum
    8: optional i64 request_version
    9: optional i64 request_version_hash
    10: optional string snapshot_path
    11: optional list<Types.TTabletId> error_tablet_ids
    12: optional list<string> snapshot_files
    13: optional map<Types.TTabletId, list<string>> tablet_files
    14: optional list<Types.TTabletId> downloaded_tablet_ids
    15: optional i64 copy_size
    16: optional i64 copy_time_ms
    17: optional map<Types.TTabletId, Types.TVersion> succ_tablets
    18: optional map<i64, i64> table_id_to_delta_num_rows
    19: optional map<i64, map<i64, i64>> table_id_to_tablet_id_to_delta_num_rows
}

struct TTablet {
    1: required list<TTabletInfo> tablet_infos
}

struct TDisk {
    1: required string root_path
    2: required Types.TSize disk_total_capacity
    // local used capacity
    3: required Types.TSize data_used_capacity
    4: required bool used
    5: optional Types.TSize disk_available_capacity
    6: optional i64 path_hash
    7: optional Types.TStorageMedium storage_medium
    8: optional Types.TSize remote_used_capacity
    9: optional Types.TSize trash_used_capacity
}

struct TPluginInfo {
    1: required string plugin_name
    2: required i32 type
}

struct TReportRequest {
    1: required Types.TBackend backend
    2: optional i64 report_version
    3: optional map<Types.TTaskType, set<i64>> tasks // string signature
    4: optional map<Types.TTabletId, TTablet> tablets
    5: optional map<string, TDisk> disks // string root_path
    6: optional bool force_recovery
    7: optional list<TTablet> tablet_list
    // the max compaction score of all tablets on a backend,
    // this field should be set along with tablet report
    8: optional i64 tablet_max_compaction_score
    9: optional list<AgentService.TStoragePolicy> storage_policy // only id and version
    10: optional list<AgentService.TStorageResource> resource // only id and version
    11: i32 num_cores
    12: i32 pipeline_executor_size
    13: optional map<Types.TPartitionId, Types.TVersion> partitions_version
}

struct TMasterResult {
    // required in V1
    1: required Status.TStatus status
}

// Deprecated
enum TResourceType {
    TRESOURCE_CPU_SHARE
    TRESOURCE_IO_SHARE
    TRESOURCE_SSD_READ_IOPS
    TRESOURCE_SSD_WRITE_IOPS
    TRESOURCE_SSD_READ_MBPS
    TRESOURCE_SSD_WRITE_MBPS
    TRESOURCE_HDD_READ_IOPS
    TRESOURCE_HDD_WRITE_IOPS
    TRESOURCE_HDD_READ_MBPS
    TRESOURCE_HDD_WRITE_MBPS
}

// Deprecated
struct TResourceGroup {
    1: required map<TResourceType, i32> resourceByType
}

// Deprecated
struct TUserResource {
    1: required TResourceGroup resource

    // Share in this user quota; Now only support High, Normal, Low
    2: required map<string, i32> shareByGroup
}

// Deprecated
struct TFetchResourceResult {
    // Master service not find protocol version, so using agent service version
    1: required AgentService.TAgentServiceVersion protocolVersion
    2: required i64 resourceVersion
    3: required map<string, TUserResource> resourceByUser
}
