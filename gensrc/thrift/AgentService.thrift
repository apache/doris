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
include "PaloInternalService.thrift"
include "PlanNodes.thrift"
include "Descriptors.thrift"
include "Exprs.thrift"

struct TTabletSchema {
    1: required i16 short_key_column_count
    2: required Types.TSchemaHash schema_hash
    3: required Types.TKeysType keys_type
    4: required Types.TStorageType storage_type
    5: required list<Descriptors.TColumn> columns
    6: optional double bloom_filter_fpp
    7: optional list<Descriptors.TOlapTableIndex> indexes
    8: optional bool is_in_memory
    9: optional i32 delete_sign_idx = -1
    10: optional i32 sequence_col_idx = -1
    11: optional Types.TSortType sort_type
    12: optional i32 sort_col_num
    13: optional bool disable_auto_compaction
    14: optional i32 version_col_idx = -1
    15: optional bool is_dynamic_schema = false // deprecated
    16: optional bool store_row_column = false
    17: optional bool enable_single_replica_compaction = false
    18: optional bool skip_write_index_on_load = false
    19: optional list<i32> cluster_key_idxes
    // col unique id for row store column
    20: optional list<i32> row_store_col_cids
    21: optional i64 row_store_page_size = 16384
    22: optional bool variant_enable_flatten_nested = false 
}

// this enum stands for different storage format in src_backends
// V1 for Segment-V1
// V2 for Segment-V2
enum TStorageFormat {
    DEFAULT,
    V1,
    V2
}

enum TTabletType {
    TABLET_TYPE_DISK = 0,
    TABLET_TYPE_MEMORY = 1
}

enum TObjStorageType {
    UNKNOWN = 0,
    AWS = 1,
    AZURE = 2,
    BOS = 3,
    COS = 4,
    OBS = 5,
    OSS = 6,
    GCP = 7
}

struct TS3StorageParam {
    1: optional string endpoint
    2: optional string region
    3: optional string ak
    4: optional string sk
    5: optional i32 max_conn = 50
    6: optional i32 request_timeout_ms = 3000
    7: optional i32 conn_timeout_ms = 1000
    8: optional string root_path
    9: optional string bucket
    10: optional bool use_path_style = false
    11: optional string token
    12: optional TObjStorageType provider
}

struct TStoragePolicy {
    1: optional i64 id
    2: optional string name
    3: optional i64 version // alter version
    4: optional i64 cooldown_datetime
    5: optional i64 cooldown_ttl
    6: optional i64 resource_id
}

struct TStorageResource {
    1: optional i64 id
    2: optional string name
    3: optional i64 version // alter version
    4: optional TS3StorageParam s3_storage_param
    5: optional PlanNodes.THdfsParams hdfs_storage_param
    // more storage resource type
}

struct TPushStoragePolicyReq {
    1: optional list<TStoragePolicy> storage_policy
    2: optional list<TStorageResource> resource
    3: optional list<i64> dropped_storage_policy
}

struct TCleanTrashReq {}

struct TCleanUDFCacheReq {
    1: optional string function_signature //function_name(arg_type)
}

enum TCompressionType {
    UNKNOWN_COMPRESSION = 0,
    DEFAULT_COMPRESSION = 1,
    NO_COMPRESSION = 2,
    SNAPPY = 3,
    LZ4 = 4,
    LZ4F = 5,
    ZLIB = 6,
    ZSTD = 7,
    LZ4HC = 8
}

// Enumerates the storage formats for inverted indexes in src_backends.
// This enum is used to distinguish between different organizational methods
// of inverted index data, affecting how the index is stored and accessed.
enum TInvertedIndexStorageFormat {
    DEFAULT, // Default format, unspecified storage method.
    V1,      // Index per idx: Each index is stored separately based on its identifier.
    V2       // Segment id per idx: Indexes are organized based on segment identifiers, grouping indexes by their associated segment.
}

struct TBinlogConfig {
    1: optional bool enable;
    2: optional i64 ttl_seconds;
    3: optional i64 max_bytes;
    4: optional i64 max_history_nums;
}

struct TCreateTabletReq {
    1: required Types.TTabletId tablet_id
    2: required TTabletSchema tablet_schema
    3: optional Types.TVersion version
    // Deprecated
    4: optional Types.TVersionHash version_hash 
    5: optional Types.TStorageMedium storage_medium
    6: optional bool in_restore_mode
    // this new tablet should be colocate with base tablet
    7: optional Types.TTabletId base_tablet_id
    8: optional Types.TSchemaHash base_schema_hash
    9: optional i64 table_id
    10: optional i64 partition_id
    // used to find the primary replica among tablet's replicas
    // replica with the largest term is primary replica
    11: optional i64 allocation_term
    // indicate whether this tablet is a compute storage split mode, we call it "eco mode"
    12: optional bool is_eco_mode
    13: optional TStorageFormat storage_format
    14: optional TTabletType tablet_type
    // 15: optional TStorageParam storage_param
    16: optional TCompressionType compression_type = TCompressionType.LZ4F
    17: optional Types.TReplicaId replica_id = 0
    // 18: optional string storage_policy
    19: optional bool enable_unique_key_merge_on_write = false
    20: optional i64 storage_policy_id
    21: optional TBinlogConfig binlog_config
    22: optional string compaction_policy = "size_based"
    23: optional i64 time_series_compaction_goal_size_mbytes = 1024
    24: optional i64 time_series_compaction_file_count_threshold = 2000
    25: optional i64 time_series_compaction_time_threshold_seconds = 3600
    26: optional i64 time_series_compaction_empty_rowsets_threshold = 5
    27: optional i64 time_series_compaction_level_threshold = 1
    28: optional TInvertedIndexStorageFormat inverted_index_storage_format = TInvertedIndexStorageFormat.DEFAULT // Deprecated
    29: optional Types.TInvertedIndexFileStorageFormat inverted_index_file_storage_format = Types.TInvertedIndexFileStorageFormat.V2

    // For cloud
    1000: optional bool is_in_memory = false
    1001: optional bool is_persistent = false
}

struct TDropTabletReq {
    1: required Types.TTabletId tablet_id
    2: optional Types.TSchemaHash schema_hash
    3: optional Types.TReplicaId replica_id = 0
    4: optional bool is_drop_table_or_partition = false
}

struct TAlterTabletReq {
    1: required Types.TTabletId base_tablet_id
    2: required Types.TSchemaHash base_schema_hash
    3: required TCreateTabletReq new_tablet_req
}

enum TAlterTabletType {
    SCHEMA_CHANGE = 1,
    ROLLUP = 2,
    MIGRATION = 3
}

struct TAlterMaterializedViewParam {
    1: required string column_name
    2: optional string origin_column_name
    3: optional Exprs.TExpr mv_expr
}

// This v2 request will replace the old TAlterTabletReq.
// TAlterTabletReq should be deprecated after new alter job process merged.
struct TAlterTabletReqV2 {
    1: required Types.TTabletId base_tablet_id
    2: required Types.TTabletId new_tablet_id
    3: required Types.TSchemaHash base_schema_hash
    4: required Types.TSchemaHash new_schema_hash
    // version of data which this alter task should transform
    5: optional Types.TVersion alter_version
    6: optional Types.TVersionHash alter_version_hash // Deprecated
    7: optional list<TAlterMaterializedViewParam> materialized_view_params
    8: optional TAlterTabletType alter_tablet_type = TAlterTabletType.SCHEMA_CHANGE
    9: optional Descriptors.TDescriptorTable desc_tbl
    10: optional list<Descriptors.TColumn> columns
    11: optional i32 be_exec_version = 0

    // For cloud
    1000: optional i64 job_id
    1001: optional i64 expiration
    1002: optional string storage_vault_id
}

struct TAlterInvertedIndexReq {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: optional Types.TVersion alter_version // Deprecated
    4: optional TAlterTabletType alter_tablet_type = TAlterTabletType.SCHEMA_CHANGE // Deprecated
    5: optional bool is_drop_op= false
    6: optional list<Descriptors.TOlapTableIndex> alter_inverted_indexes
    7: optional list<Descriptors.TOlapTableIndex> indexes_desc
    8: optional list<Descriptors.TColumn> columns
    9: optional i64 job_id
    10: optional i64 expiration
}

struct TTabletGcBinlogInfo {
    1: optional Types.TTabletId tablet_id
    2: optional i64 version
}

struct TGcBinlogReq {
    1: optional list<TTabletGcBinlogInfo> tablet_gc_binlog_infos
}

struct TStorageMigrationReqV2 {
    1: optional Types.TTabletId base_tablet_id
    2: optional Types.TTabletId new_tablet_id
    3: optional Types.TSchemaHash base_schema_hash
    4: optional Types.TSchemaHash new_schema_hash
    5: optional Types.TVersion migration_version
}

struct TClusterInfo {
    1: required string user
    2: required string password
}

struct TPushReq {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required Types.TVersion version
    4: required Types.TVersionHash version_hash // Deprecated
    5: required i64 timeout
    6: required Types.TPushType push_type
    7: optional string http_file_path
    8: optional i64 http_file_size
    9: optional list<PaloInternalService.TCondition> delete_conditions
    10: optional bool need_decompress
    // for real time load
    11: optional Types.TTransactionId transaction_id
    12: optional Types.TPartitionId partition_id
    // fe should inform be that this request is running during schema change
    // be should write two files
    13: optional bool is_schema_changing
    // 14 and 15 are used by spark load
    14: optional PlanNodes.TBrokerScanRange broker_scan_range
    15: optional Descriptors.TDescriptorTable desc_tbl
    16: optional list<Descriptors.TColumn> columns_desc
    17: optional string storage_vault_id
    18: optional i32 schema_version
}

struct TCloneReq {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required list<Types.TBackend> src_backends
    4: optional Types.TStorageMedium storage_medium
    // these are visible version(hash) actually
    5: optional Types.TVersion version
    6: optional Types.TVersionHash committed_version_hash // Deprecated
    7: optional i32 task_version;
    8: optional i64 src_path_hash;
    9: optional i64 dest_path_hash;
    10: optional i32 timeout_s;
    11: optional Types.TReplicaId replica_id = 0
    12: optional i64 partition_id
    13: optional i64 table_id = -1
}

struct TCompactionReq {
    1: optional Types.TTabletId tablet_id
    2: optional Types.TSchemaHash schema_hash
    3: optional string type
}

struct TStorageMediumMigrateReq {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required Types.TStorageMedium storage_medium
    // if data dir is specified, the storage_medium is meaning less,
    // Doris will try to migrate the tablet to the specified data dir.
    4: optional string data_dir
}

struct TCancelDeleteDataReq {
    // deprecated
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required Types.TVersion version
    4: required Types.TVersionHash version_hash // Deprecated
}

struct TCheckConsistencyReq {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required Types.TVersion version
    4: required Types.TVersionHash version_hash // Deprecated
}

struct TUploadReq {
    1: required i64 job_id;
    2: required map<string, string> src_dest_map
    3: required Types.TNetworkAddress broker_addr
    4: optional map<string, string> broker_prop
    5: optional Types.TStorageBackendType storage_backend = Types.TStorageBackendType.BROKER
    6: optional string location // root path
}

struct TRemoteTabletSnapshot {
    1: optional i64 local_tablet_id
    2: optional string local_snapshot_path
    3: optional i64 remote_tablet_id
    4: optional i64 remote_be_id
    5: optional Types.TNetworkAddress remote_be_addr
    6: optional string remote_snapshot_path
    7: optional string remote_token
}

struct TDownloadReq {
    1: required i64 job_id
    2: required map<string, string> src_dest_map
    3: required Types.TNetworkAddress broker_addr
    4: optional map<string, string> broker_prop
    5: optional Types.TStorageBackendType storage_backend = Types.TStorageBackendType.BROKER
    6: optional string location // root path
    7: optional list<TRemoteTabletSnapshot> remote_tablet_snapshots
}

struct TSnapshotRequest {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: optional Types.TVersion version
    4: optional Types.TVersionHash version_hash // Deprecated
    5: optional i64 timeout
    6: optional list<Types.TVersion> missing_version
    7: optional bool list_files
    // if all nodes has been upgraded, it can be removed.
    // Deprecated since version 0.13
    8: optional bool allow_incremental_clone
    9: optional i32 preferred_snapshot_version = Types.TPREFER_SNAPSHOT_REQ_VERSION
    10: optional bool is_copy_tablet_task
    11: optional Types.TVersion start_version
    12: optional Types.TVersion end_version
    13: optional bool is_copy_binlog
    14: optional Types.TTabletId ref_tablet_id
}

struct TReleaseSnapshotRequest {
    1: required string snapshot_path
}

struct TClearRemoteFileReq {
    1: required string remote_file_path
    2: required map<string, string> remote_source_properties
}

struct TPartitionVersionInfo {
    1: required Types.TPartitionId partition_id
    2: required Types.TVersion version
    3: required Types.TVersionHash version_hash // Deprecated
}

struct TMoveDirReq {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
    3: required string src
    4: required i64 job_id
    5: required bool overwrite
}

enum TAgentServiceVersion {
    V1
}

struct TPublishVersionRequest {
    1: required Types.TTransactionId transaction_id
    2: required list<TPartitionVersionInfo> partition_version_infos
    // strict mode means BE will check tablet missing version
    3: optional bool strict_mode = false
    // for delta rows statistics to exclude rollup tablets
    4: optional set<Types.TTabletId> base_tablet_ids
}

struct TVisibleVersionReq {
    1: required map<Types.TPartitionId, Types.TVersion> partition_version
}

struct TCalcDeleteBitmapPartitionInfo {
    1: required Types.TPartitionId partition_id
    2: required Types.TVersion version
    3: required list<Types.TTabletId> tablet_ids
    4: optional list<i64> base_compaction_cnts
    5: optional list<i64> cumulative_compaction_cnts
    6: optional list<i64> cumulative_points
}

struct TCalcDeleteBitmapRequest {
    1: required Types.TTransactionId transaction_id
    2: required list<TCalcDeleteBitmapPartitionInfo> partitions;
}

struct TClearAlterTaskRequest {
    1: required Types.TTabletId tablet_id
    2: required Types.TSchemaHash schema_hash
}

struct TClearTransactionTaskRequest {
    1: required Types.TTransactionId transaction_id
    2: required list<Types.TPartitionId> partition_id
}

struct TRecoverTabletReq {
    1: optional Types.TTabletId tablet_id
    2: optional Types.TSchemaHash schema_hash
    3: optional Types.TVersion version
    4: optional Types.TVersionHash version_hash // Deprecated
}

enum TTabletMetaType {
    PARTITIONID,
    INMEMORY,
    BINLOG_CONFIG
}

struct TTabletMetaInfo {
    1: optional Types.TTabletId tablet_id
    2: optional Types.TSchemaHash schema_hash
    3: optional Types.TPartitionId partition_id
    // 4: optional TTabletMetaType Deprecated_meta_type
    5: optional bool is_in_memory
    // 6: optional string Deprecated_storage_policy
    7: optional i64 storage_policy_id
    8: optional Types.TReplicaId replica_id
    9: optional TBinlogConfig binlog_config
    10: optional string compaction_policy
    11: optional i64 time_series_compaction_goal_size_mbytes
    12: optional i64 time_series_compaction_file_count_threshold
    13: optional i64 time_series_compaction_time_threshold_seconds
    14: optional bool enable_single_replica_compaction
    15: optional bool skip_write_index_on_load
    16: optional bool disable_auto_compaction
    17: optional i64 time_series_compaction_empty_rowsets_threshold
    18: optional i64 time_series_compaction_level_threshold
}

struct TUpdateTabletMetaInfoReq {
    1: optional list<TTabletMetaInfo> tabletMetaInfos
}

struct TPluginMetaInfo {
    1: required string name
    2: required i32 type
    3: optional string so_name
    4: optional string source
}

struct TCooldownConf {
    1: required Types.TTabletId tablet_id
    2: optional Types.TReplicaId cooldown_replica_id
    3: optional i64 cooldown_term
}

struct TPushCooldownConfReq {
    1: required list<TCooldownConf> cooldown_confs
}

struct TAgentTaskRequest {
    1: required TAgentServiceVersion protocol_version
    2: required Types.TTaskType task_type
    3: required i64 signature // every request has unique signature
    4: optional Types.TPriority priority
    5: optional TCreateTabletReq create_tablet_req
    6: optional TDropTabletReq drop_tablet_req
    7: optional TAlterTabletReq alter_tablet_req
    8: optional TCloneReq clone_req
    9: optional TPushReq push_req
    10: optional TCancelDeleteDataReq cancel_delete_data_req //deprecated
    // Deprecated
    11: optional Types.TResourceInfo resource_info
    12: optional TStorageMediumMigrateReq storage_medium_migrate_req
    13: optional TCheckConsistencyReq check_consistency_req
    14: optional TUploadReq upload_req
    15: optional TDownloadReq download_req
    16: optional TSnapshotRequest snapshot_req
    17: optional TReleaseSnapshotRequest release_snapshot_req
    18: optional TClearRemoteFileReq clear_remote_file_req
    19: optional TPublishVersionRequest publish_version_req
    20: optional TClearAlterTaskRequest clear_alter_task_req
    21: optional TClearTransactionTaskRequest clear_transaction_task_req
    22: optional TMoveDirReq move_dir_req
    23: optional TRecoverTabletReq recover_tablet_req
    24: optional TAlterTabletReqV2 alter_tablet_req_v2
    25: optional i64 recv_time // time the task is inserted to queue
    26: optional TUpdateTabletMetaInfoReq update_tablet_meta_info_req
    27: optional TCompactionReq compaction_req
    28: optional TStorageMigrationReqV2 storage_migration_req_v2
    // DEPRECATED 29: optional TGetStoragePolicy update_policy
    30: optional TPushCooldownConfReq push_cooldown_conf
    31: optional TPushStoragePolicyReq push_storage_policy_req
    32: optional TAlterInvertedIndexReq alter_inverted_index_req
    33: optional TGcBinlogReq gc_binlog_req
    34: optional TCleanTrashReq clean_trash_req
    35: optional TVisibleVersionReq visible_version_req
    36: optional TCleanUDFCacheReq clean_udf_cache_req

    // For cloud
    1000: optional TCalcDeleteBitmapRequest calc_delete_bitmap_req
}

struct TAgentResult {
    1: required Status.TStatus status
    2: optional string snapshot_path
    3: optional bool allow_incremental_clone
    // the snapshot that be has done according 
    // to the preferred snapshot version that client requests
    4: optional i32 snapshot_version  = 1
}

struct TTopicItem {
    1: required string key
    2: optional i64 int_value
    3: optional double double_value
    4: optional string string_value
}

enum TTopicType {
    RESOURCE = 0
}

struct TTopicUpdate {
    1: required TTopicType type
    2: optional list<TTopicItem> updates
    3: optional list<string> deletes
}

struct TAgentPublishRequest {
    1: required TAgentServiceVersion protocol_version
    2: required list<TTopicUpdate> updates
}
