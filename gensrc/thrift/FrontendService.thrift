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
include "Planner.thrift"
include "Descriptors.thrift"
include "Data.thrift"
include "Exprs.thrift"
include "RuntimeProfile.thrift"
include "MasterService.thrift"
include "AgentService.thrift"

// These are supporting structs for JniFrontend.java, which serves as the glue
// between our C++ execution environment and the Java frontend.

struct TSetSessionParams {
    1: required string user
}

struct TAuthenticateParams {
    1: required string user
    2: required string passwd
}

struct TColumnDesc {
  1: required string columnName
  2: required Types.TPrimitiveType columnType
  3: optional i32 columnLength
  4: optional i32 columnPrecision
  5: optional i32 columnScale
  6: optional bool isAllowNull
  7: optional string columnKey
  8: optional list<TColumnDesc> children
}

// A column definition; used by CREATE TABLE and DESCRIBE <table> statements. A column
// definition has a different meaning (and additional fields) from a column descriptor,
// so this is a separate struct from TColumnDesc.
struct TColumnDef {
  1: required TColumnDesc columnDesc
  2: optional string comment
}

// Arguments to DescribeTable, which returns a list of column descriptors for a
// given table
struct TDescribeTableParams {
  1: optional string db
  2: required string table_name
  3: optional string user   // deprecated
  4: optional string user_ip    // deprecated
  5: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
  6: optional bool show_hidden_columns = false
  7: optional string catalog
}

// Results of a call to describeTable()
struct TDescribeTableResult {
  1: required list<TColumnDef> columns
}

// Arguments to DescribeTables, which returns a list of column descriptors for
// given tables
struct TDescribeTablesParams {
  1: optional string db
  2: required list<string> tables_name
  3: optional string user   // deprecated
  4: optional string user_ip    // deprecated
  5: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
  6: optional bool show_hidden_columns = false
  7: optional string catalog
}

// Results of a call to describeTable()
struct TDescribeTablesResult {
  // tables_offset means that the offset for each table in columns
  1: required list<i32> tables_offset
  2: required list<TColumnDef> columns
}

struct TShowVariableRequest {
    1: required i64 threadId
    2: required Types.TVarType varType
}

// Results of a call to describeTable()
struct TShowVariableResult {
    1: required map<string, string> variables
}

// Valid table file formats
enum TFileFormat {
  PARQUETFILE,
  RCFILE,
  SEQUENCEFILE,
  TEXTFILE,
}

// set type
enum TSetType {
  OPT_DEFAULT,
  OPT_GLOBAL,
  OPT_SESSION,
}

// The row format specifies how to interpret the fields (columns) and lines (rows) in a
// data file when creating a new table.
struct TTableRowFormat {
  // Optional terminator string used to delimit fields (columns) in the table
  1: optional string field_terminator

  // Optional terminator string used to delimit lines (rows) in a table
  2: optional string line_terminator

  // Optional string used to specify a special escape character sequence
  3: optional string escaped_by
}


// Represents a single item in a partition spec (column name + value)
struct TPartitionKeyValue {
  // Partition column name
  1: required string name,

  // Partition value
  2: required string value
}

// Per-client session state
struct TSessionState {
  // The default database, changed by USE <database> queries.
  1: required string database

  // The user who this session belongs to.
  2: required string user

  // The user who this session belongs to.
  3: required i64 connection_id
}

struct TClientRequest {
  // select stmt to be executed
  1: required string stmt

  // query options
  2: required PaloInternalService.TQueryOptions queryOptions

  // session state
  3: required TSessionState sessionState;
}


// Parameters for SHOW DATABASES commands
struct TExplainParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: required string explain
}

struct TSetVar{
    1: required TSetType type
    2: required string variable
    3: required Exprs.TExpr value
}
// Parameters for Set commands
struct TSetParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: required list<TSetVar> set_vars
}

struct TKillParams {
  // Optional pattern to match database names. If not set, all databases are returned.
  1: required bool is_kill_connection
  2: required i64 connection_id
}

struct TCommonDdlParams {
  //1: required Ddl.TCommonDdlType ddl_type
  //2: optional Ddl.TCreateDbParams create_db_params
  //3: optional Ddl.TCreateTableParams create_table_params
  //4: optional Ddl.TLoadParams load_params
}

// Parameters for the USE db command
struct TUseDbParams {
  1: required string db
}

struct TResultSetMetadata {
  1: required list<TColumnDesc> columnDescs
}

// Result of call to PaloPlanService/JniFrontend.CreateQueryRequest()
struct TQueryExecRequest {
  // global descriptor tbl for all fragments
  1: optional Descriptors.TDescriptorTable desc_tbl

  // fragments[i] may consume the output of fragments[j > i];
  // fragments[0] is the root fragment and also the coordinator fragment, if
  // it is unpartitioned.
  2: required list<Planner.TPlanFragment> fragments

  // Specifies the destination fragment of the output of each fragment.
  // parent_fragment_idx.size() == fragments.size() - 1 and
  // fragments[i] sends its output to fragments[dest_fragment_idx[i-1]]
  3: optional list<i32> dest_fragment_idx

  // A map from scan node ids to a list of scan range locations.
  // The node ids refer to scan nodes in fragments[].plan_tree
  4: optional map<Types.TPlanNodeId, list<Planner.TScanRangeLocations>>
      per_node_scan_ranges

  // Metadata of the query result set (only for select)
  5: optional TResultSetMetadata result_set_metadata

  7: required PaloInternalService.TQueryGlobals query_globals

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be.
  9: required Types.TStmtType stmt_type

  // The statement type governs when the coordinator can judge a query to be finished.
  // DML queries are complete after Wait(), SELECTs may not be.
  10: optional bool is_block_query;
}

enum TDdlType {
  USE,
  DESCRIBE,
  SET,
  EXPLAIN,
  KILL,
  COMMON
}

struct TDdlExecRequest {
  1: required TDdlType ddl_type

  // Parameters for USE commands
  2: optional TUseDbParams use_db_params;

  // Parameters for DESCRIBE table commands
  3: optional TDescribeTableParams describe_table_params

  10: optional TExplainParams explain_params

  11: optional TSetParams set_params
  12: optional TKillParams kill_params
  //13: optional Ddl.TMasterDdlRequest common_params
}

// Results of an EXPLAIN
struct TExplainResult {
    // each line in the explain plan occupies an entry in the list
    1: required list<Data.TResultRow> results
}

// Result of call to createExecRequest()
struct TExecRequest {
  1: required Types.TStmtType stmt_type;

  2: optional string sql_stmt;

  // Globally unique id for this request. Assigned by the planner.
  3: required Types.TUniqueId request_id

  // Copied from the corresponding TClientRequest
  4: required PaloInternalService.TQueryOptions query_options;

  // TQueryExecRequest for the backend
  // Set iff stmt_type is QUERY or DML
  5: optional TQueryExecRequest query_exec_request

  // Set iff stmt_type is DDL
  6: optional TDdlExecRequest ddl_exec_request

  // Metadata of the query result set (not set for DML)
  7: optional TResultSetMetadata result_set_metadata

  // Result of EXPLAIN. Set iff stmt_type is EXPLAIN
  8: optional TExplainResult explain_result
}

// Arguments to getDbNames, which returns a list of dbs that match an optional
// pattern
struct TGetDbsParams {
  // If not set, match every database
  1: optional string pattern
  2: optional string user   // deprecated
  3: optional string user_ip    // deprecated
  4: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
  5: optional string catalog
  6: optional bool get_null_catalog  //if catalog is empty , get dbName ="NULL" and dbId = -1.
}

// getDbNames returns a list of database names , database ids and catalog names ,catalog ids
struct TGetDbsResult {
  1: optional list<string> dbs
  2: optional list<string> catalogs
  3: optional list<i64> db_ids
  4: optional list<i64> catalog_ids
}

// Arguments to getTableNames, which returns a list of tables that match an
// optional pattern.
struct TGetTablesParams {
  // If not set, match tables in all DBs
  1: optional string db

  // If not set, match every table
  2: optional string pattern
  3: optional string user   // deprecated
  4: optional string user_ip    // deprecated
  5: optional Types.TUserIdentity current_user_ident // to replace the user and user ip
  6: optional string type
  7: optional string catalog
}

struct TTableStatus {
    1: required string name
    2: required string type
    3: required string comment
    4: optional string engine
    5: optional i64 last_check_time
    6: optional i64 create_time
    7: optional string ddl_sql
    8: optional i64 update_time
    9: optional i64 check_time
    10: optional string collation
    11: optional i64 rows;
    12: optional i64 avg_row_length
    13: optional i64 data_length;
}

struct TListTableStatusResult {
    1: required list<TTableStatus> tables
}

struct TTableMetadataNameIds {
    1: optional string name
    2: optional i64 id 
}

struct TListTableMetadataNameIdsResult {
    1: optional list<TTableMetadataNameIds> tables 
}

// getTableNames returns a list of unqualified table names
struct TGetTablesResult {
  1: list<string> tables
}

struct TPrivilegeStatus {
    1: optional string table_name
    2: optional string privilege_type
    3: optional string grantee
    4: optional string schema
    5: optional string is_grantable
}

struct TListPrivilegesResult{
  1: required list<TPrivilegeStatus> privileges
}

struct TReportExecStatusResult {
  // required in V1
  1: optional Status.TStatus status
}

// Service Protocol Details
enum FrontendServiceVersion {
  V1
}

struct TDetailedReportParams {
  1: optional Types.TUniqueId fragment_instance_id
  2: optional RuntimeProfile.TRuntimeProfileTree profile
  3: optional RuntimeProfile.TRuntimeProfileTree loadChannelProfile
}

// The results of an INSERT query, sent to the coordinator as part of
// TReportExecStatusParams
struct TReportExecStatusParams {
  1: required FrontendServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId query_id

  // passed into ExecPlanFragment() as TExecPlanFragmentParams.backend_num
  // required in V1
  3: optional i32 backend_num

  // required in V1
  // Move to TDetailedReportParams for pipelineX
  4: optional Types.TUniqueId fragment_instance_id

  // Status of fragment execution; any error status means it's done.
  // required in V1
  5: optional Status.TStatus status

  // If true, fragment finished executing.
  // required in V1
  6: optional bool done

  // cumulative profile
  // required in V1
  // Move to TDetailedReportParams for pipelineX
  7: optional RuntimeProfile.TRuntimeProfileTree profile

  // New errors that have not been reported to the coordinator
  // optional in V1
  9: optional list<string> error_log

  // URL of files need to load
  // optional
  10: optional list<string> delta_urls
  11: optional map<string, string> load_counters
  12: optional string tracking_url

  // export files
  13: optional list<string> export_files

  14: optional list<Types.TTabletCommitInfo> commitInfos

  15: optional i64 loaded_rows

  16: optional i64 backend_id

  17: optional i64 loaded_bytes

  18: optional list<Types.TErrorTabletInfo> errorTabletInfos

  19: optional i32 fragment_id

  20: optional PaloInternalService.TQueryType query_type

  // Move to TDetailedReportParams for pipelineX
  21: optional RuntimeProfile.TRuntimeProfileTree loadChannelProfile

  22: optional i32 finished_scan_ranges

  23: optional list<TDetailedReportParams> detailed_report
}

struct TFeResult {
    1: required FrontendServiceVersion protocolVersion
    2: required Status.TStatus status
}

struct TMasterOpRequest {
    1: required string user
    2: required string db
    3: required string sql
    // Deprecated
    4: optional Types.TResourceInfo resourceInfo
    5: optional string cluster
    6: optional i64 execMemLimit // deprecated, move into query_options
    7: optional i32 queryTimeout // deprecated, move into query_options
    8: optional string user_ip
    9: optional string time_zone // deprecated, move into session_variables
    10: optional i64 stmt_id
    11: optional i64 sqlMode // deprecated, move into session_variables
    12: optional i64 loadMemLimit // deprecated, move into query_options
    13: optional bool enableStrictMode // deprecated, move into session_variables
    // this can replace the "user" field
    14: optional Types.TUserIdentity current_user_ident
    15: optional i32 stmtIdx  // the idx of the sql in multi statements
    16: optional PaloInternalService.TQueryOptions query_options
    17: optional Types.TUniqueId query_id // when this is a query, we translate this query id to master
    18: optional i64 insert_visible_timeout_ms // deprecated, move into session_variables
    19: optional map<string, string> session_variables
    20: optional bool foldConstantByBe
    21: optional map<string, string> trace_carrier
    22: optional string clientNodeHost
    23: optional i32 clientNodePort
    24: optional bool syncJournalOnly // if set to true, this request means to do nothing but just sync max journal id of master
    25: optional string defaultCatalog
    26: optional string defaultDatabase
}

struct TColumnDefinition {
    1: required string columnName;
    2: required Types.TColumnType columnType;
    3: optional Types.TAggregationType aggType;
    4: optional string defaultValue;
}

struct TShowResultSetMetaData {
    1: required list<TColumnDefinition> columns;
}

struct TShowResultSet {
    1: required TShowResultSetMetaData metaData;
    2: required list<list<string>> resultRows;
}

struct TMasterOpResult {
    1: required i64 maxJournalId;
    2: required binary packet;
    3: optional TShowResultSet resultSet;
    4: optional Types.TUniqueId queryId;
    5: optional string status;
}

struct TUpdateExportTaskStatusRequest {
    1: required FrontendServiceVersion protocolVersion
    2: required Types.TUniqueId taskId
    3: required PaloInternalService.TExportStatusResult taskStatus
}

struct TLoadTxnBeginRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required string label
    8: optional i64 timestamp   // deprecated, use request_id instead
    9: optional i64 auth_code
    // The real value of timeout should be i32. i64 ensures the compatibility of interface.
    10: optional i64 timeout
    11: optional Types.TUniqueId request_id
    12: optional string token
}

struct TLoadTxnBeginResult {
    1: required Status.TStatus status
    2: optional i64 txnId
    3: optional string job_status // if label already used, set status of existing job
    4: optional i64 db_id
}

struct TBeginTxnRequest {
    1: optional string cluster
    2: optional string user
    3: optional string passwd
    4: optional string db
    5: optional list<i64> table_ids
    6: optional string user_ip
    7: optional string label
    8: optional i64 auth_code
    // The real value of timeout should be i32. i64 ensures the compatibility of interface.
    9: optional i64 timeout
    10: optional Types.TUniqueId request_id
    11: optional string token
}

struct TBeginTxnResult {
    1: optional Status.TStatus status
    2: optional i64 txn_id
    3: optional string job_status // if label already used, set status of existing job
    4: optional i64 db_id
}

// StreamLoad request, used to load a streaming to engine
struct TStreamLoadPutRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip

    // and use this to assgin to OlapTableSink
    7: required Types.TUniqueId loadId
    8: required i64 txnId

    9: required Types.TFileType fileType
    10: required PlanNodes.TFileFormatType formatType

    // only valid when file_type is FILE_LOCAL
    11: optional string path

    // describe how table's column map to field in source file
    // slot descriptor stands for field of source file
    12: optional string columns
    // filters that applied on data
    13: optional string where
    // only valid when file type is CSV
    14: optional string columnSeparator

    15: optional string partitions
    16: optional i64 auth_code
    17: optional bool negative
    18: optional i32 timeout
    19: optional bool strictMode
    20: optional string timezone
    21: optional i64 execMemLimit
    22: optional bool isTempPartition
    23: optional bool strip_outer_array
    24: optional string jsonpaths
    25: optional i64 thrift_rpc_timeout_ms
    26: optional string json_root
    27: optional Types.TMergeType merge_type
    28: optional string delete_condition
    29: optional string sequence_col
    30: optional bool num_as_string
    31: optional bool fuzzy_parse
    32: optional string line_delimiter
    33: optional bool read_json_by_line
    34: optional string token
    35: optional i32 send_batch_parallelism
    36: optional double max_filter_ratio
    37: optional bool load_to_single_tablet
    38: optional string header_type
    39: optional string hidden_columns
    40: optional PlanNodes.TFileCompressType compress_type
    41: optional i64 file_size // only for stream load with parquet or orc
    42: optional bool trim_double_quotes // trim double quotes for csv
    43: optional i32 skip_lines // csv skip line num, only used when csv header_type is not set.
    44: optional bool enable_profile
    45: optional bool partial_update
    46: optional list<string> table_names
    47: optional string load_sql // insert into sql used by stream load
    48: optional i64 backend_id
    49: optional i32 version // version 1 means use load_sql
    50: optional string label
    // only valid when file type is CSV
    51: optional i8 enclose
    // only valid when file type is CSV
    52: optional i8 escape
    53: optional bool memtable_on_sink_node;
    54: optional bool group_commit
}

struct TStreamLoadPutResult {
    1: required Status.TStatus status
    // valid when status is OK
    2: optional PaloInternalService.TExecPlanFragmentParams params
    3: optional PaloInternalService.TPipelineFragmentParams pipeline_params
    // used for group commit
    4: optional i64 base_schema_version
    5: optional i64 db_id
    6: optional i64 table_id
}

struct TStreamLoadMultiTablePutResult {
    1: required Status.TStatus status
    // valid when status is OK
    2: optional list<PaloInternalService.TExecPlanFragmentParams> params
    3: optional list<PaloInternalService.TPipelineFragmentParams> pipeline_params
}

struct TStreamLoadWithLoadStatusResult {
    1: optional Status.TStatus status
    2: optional i64 txn_id
    3: optional i64 total_rows
    4: optional i64 loaded_rows
    5: optional i64 filtered_rows
    6: optional i64 unselected_rows
}

struct TCheckWalRequest {
    1: optional i64 wal_id
    2: optional i64 db_id
}

struct TCheckWalResult {
    1: optional Status.TStatus status
    2: optional bool need_recovery
}

struct TKafkaRLTaskProgress {
    1: required map<i32,i64> partitionCmtOffset
}

struct TRLTaskTxnCommitAttachment {
    1: required Types.TLoadSourceType loadSourceType
    2: required Types.TUniqueId id
    3: required i64 jobId
    4: optional i64 loadedRows
    5: optional i64 filteredRows
    6: optional i64 unselectedRows
    7: optional i64 receivedBytes
    8: optional i64 loadedBytes
    9: optional i64 loadCostMs
    10: optional TKafkaRLTaskProgress kafkaRLTaskProgress
    11: optional string errorLogUrl
}

struct TTxnCommitAttachment {
    1: required Types.TLoadType loadType
    2: optional TRLTaskTxnCommitAttachment rlTaskTxnCommitAttachment
//    3: optional TMiniLoadTxnCommitAttachment mlTxnCommitAttachment
}

struct TLoadTxnCommitRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required i64 txnId
    8: required bool sync
    9: optional list<Types.TTabletCommitInfo> commitInfos
    10: optional i64 auth_code
    11: optional TTxnCommitAttachment txnCommitAttachment
    12: optional i64 thrift_rpc_timeout_ms
    13: optional string token
    14: optional i64 db_id
    15: optional list<string> tbls
    16: optional i64 table_id
}

struct TLoadTxnCommitResult {
    1: required Status.TStatus status
}

struct TCommitTxnRequest {
    1: optional string cluster
    2: optional string user
    3: optional string passwd
    4: optional string db
    5: optional string user_ip
    6: optional i64 txn_id
    7: optional list<Types.TTabletCommitInfo> commit_infos
    8: optional i64 auth_code
    9: optional TTxnCommitAttachment txn_commit_attachment
    10: optional i64 thrift_rpc_timeout_ms
    11: optional string token
    12: optional i64 db_id
}

struct TCommitTxnResult {
    1: optional Status.TStatus status
}

struct TLoadTxn2PCRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: optional string db
    5: optional string user_ip
    6: optional i64 txnId
    7: optional string operation
    8: optional i64 auth_code
    9: optional string token
    10: optional i64 thrift_rpc_timeout_ms
    11: optional string label
}

struct TLoadTxn2PCResult {
    1: required Status.TStatus status
}

struct TRollbackTxnRequest {
    1: optional string cluster
    2: optional string user
    3: optional string passwd
    4: optional string db
    5: optional string user_ip
    6: optional i64 txn_id
    7: optional string reason
    9: optional i64 auth_code
    10: optional TTxnCommitAttachment txn_commit_attachment
    11: optional string token
    12: optional i64 db_id
}

struct TRollbackTxnResult {
    1: optional Status.TStatus status
}

struct TLoadTxnRollbackRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: required string db
    5: required string tbl
    6: optional string user_ip
    7: required i64 txnId
    8: optional string reason
    9: optional i64 auth_code
    10: optional TTxnCommitAttachment txnCommitAttachment
    11: optional string token
    12: optional i64 db_id
    13: optional list<string> tbls
}

struct TLoadTxnRollbackResult {
    1: required Status.TStatus status
}

struct TSnapshotLoaderReportRequest {
    1: required i64 job_id
    2: required i64 task_id
    3: required Types.TTaskType task_type
    4: optional i32 finished_num
    5: optional i32 total_num
}

enum TFrontendPingFrontendStatusCode {
   OK = 0,
   FAILED = 1
}

struct TFrontendPingFrontendRequest {
   1: required i32 clusterId
   2: required string token
}

struct TDiskInfo {
    1: required string dirType
    2: required string dir
    3: required string filesystem
    4: required i64 blocks
    5: required i64 used
    6: required i64 available
    7: required i32 useRate
    8: required string mountedOn
}

struct TFrontendPingFrontendResult {
    1: required TFrontendPingFrontendStatusCode status
    2: required string msg
    3: required i32 queryPort
    4: required i32 rpcPort
    5: required i64 replayedJournalId
    6: required string version
    7: optional i64 lastStartupTime
    8: optional list<TDiskInfo> diskInfos
    9: optional i64 processUUID
    10: optional i32 arrowFlightSqlPort
}

struct TPropertyVal {
    1: optional string strVal
    2: optional i32 intVal
    3: optional i64 longVal
    4: optional bool boolVal
}

struct TWaitingTxnStatusRequest {
    1: optional i64 db_id
    2: optional i64 txn_id
    3: optional string label
}

struct TWaitingTxnStatusResult {
    1: optional Status.TStatus status
    2: optional i32 txn_status_id
}

struct TInitExternalCtlMetaRequest {
    1: optional i64 catalogId
    2: optional i64 dbId
    3: optional i64 tableId
}

struct TInitExternalCtlMetaResult {
    1: optional i64 maxJournalId;
    2: optional string status;
}

enum TSchemaTableName {
  // BACKENDS = 0,
  METADATA_TABLE = 1,
}

struct TMetadataTableRequestParams {
  1: optional Types.TMetadataType metadata_type
  2: optional PlanNodes.TIcebergMetadataParams iceberg_metadata_params
  3: optional PlanNodes.TBackendsMetadataParams backends_metadata_params
  4: optional list<string> columns_name
  5: optional PlanNodes.TFrontendsMetadataParams frontends_metadata_params
  6: optional Types.TUserIdentity current_user_ident
}

struct TFetchSchemaTableDataRequest {
  1: optional string cluster_name
  2: optional TSchemaTableName schema_table_name
  3: optional TMetadataTableRequestParams metada_table_params
}

struct TFetchSchemaTableDataResult {
  1: required Status.TStatus status
  2: optional list<Data.TRow> data_batch;
}

// Only support base table add columns
struct TAddColumnsRequest {
    1: optional i64 table_id
    2: optional list<TColumnDef> addColumns
    3: optional string table_name
    4: optional string db_name
    5: optional bool allow_type_conflict
}

// Only support base table add columns
struct TAddColumnsResult {
    1: optional Status.TStatus status
    2: optional i64 table_id
    3: optional list<Descriptors.TColumn> allColumns
    4: optional i32 schema_version
}

struct TMySqlLoadAcquireTokenResult {
    1: optional Status.TStatus status
    2: optional string token
}

struct TTabletCooldownInfo {
    1: optional Types.TTabletId tablet_id
    2: optional Types.TReplicaId cooldown_replica_id
    3: optional Types.TUniqueId cooldown_meta_id
}

struct TConfirmUnusedRemoteFilesRequest {
    1: optional list<TTabletCooldownInfo> confirm_list
}

struct TConfirmUnusedRemoteFilesResult {
    1: optional list<Types.TTabletId> confirmed_tablets
}

enum TPrivilegeHier {
  GLOBAL = 0,
  CATALOG = 1,
  DATABASE = 2,
  TABLE = 3,
  COLUMNS = 4,
  RESOURSE = 5,
}

struct TPrivilegeCtrl {
    1: required TPrivilegeHier priv_hier
    2: optional string ctl
    3: optional string db
    4: optional string tbl
    5: optional set<string> cols
    6: optional string res
}

enum TPrivilegeType {
  NONE = -1,
  SHOW = 0,
  SHOW_RESOURCES = 1,
  GRANT = 2,
  ADMIN = 3,
  LOAD = 4,
  ALTER = 5,
  USAGE = 6,
  CREATE = 7,
  ALL = 8,
  OPERATOR = 9,
  DROP = 10
}

struct TCheckAuthRequest {
    1: optional string cluster
    2: required string user
    3: required string passwd
    4: optional string user_ip
    5: optional TPrivilegeCtrl priv_ctrl
    6: optional TPrivilegeType priv_type
    7: optional i64 thrift_rpc_timeout_ms
}

struct TCheckAuthResult {
    1: required Status.TStatus status
}

enum TQueryStatsType {
    CATALOG = 0,
    DATABASE = 1,
    TABLE = 2,
    TABLE_ALL = 3,
    TABLE_ALL_VERBOSE = 4,
    TABLET = 5,
    TABLETS = 6
}

struct TGetQueryStatsRequest {
    1: optional TQueryStatsType type
    2: optional string catalog
    3: optional string db
    4: optional string tbl
    5: optional i64 replica_id
    6: optional list<i64> replica_ids
}

struct TTableQueryStats {
    1: optional string field
    2: optional i64 query_stats
    3: optional i64 filter_stats
}

struct TTableIndexQueryStats {
    1: optional string index_name
    2: optional list<TTableQueryStats> table_stats
}

struct TQueryStatsResult {
    1: optional Status.TStatus status
    2: optional map<string, i64> simple_result
    3: optional list<TTableQueryStats> table_stats
    4: optional list<TTableIndexQueryStats> table_verbos_stats
    5: optional map<i64, i64> tablet_stats
}

struct TGetBinlogRequest {
    1: optional string cluster
    2: optional string user
    3: optional string passwd
    4: optional string db
    5: optional string table
    6: optional i64 table_id
    7: optional string user_ip
    8: optional string token
    9: optional i64 prev_commit_seq
}

enum TBinlogType {
  UPSERT = 0,
  ADD_PARTITION = 1,
  CREATE_TABLE = 2,
  DROP_PARTITION = 3,
  DROP_TABLE = 4,
  ALTER_JOB = 5,
  MODIFY_TABLE_ADD_OR_DROP_COLUMNS = 6,
  DUMMY = 7,
  ALTER_DATABASE_PROPERTY = 8,
  MODIFY_TABLE_PROPERTY = 9,
  BARRIER = 10,
  MODIFY_PARTITIONS = 11,
  REPLACE_PARTITIONS = 12,
  TRUNCATE_TABLE = 13,
}

struct TBinlog {
    1: optional i64 commit_seq
    2: optional i64 timestamp
    3: optional TBinlogType type
    4: optional i64 db_id
    5: optional list<i64> table_ids
    6: optional string data
    7: optional i64 belong  // belong == -1 if type is not DUMMY
    8: optional i64 table_ref // only use for gc
    9: optional bool remove_enable_cache
}

struct TGetBinlogResult {
    1: optional Status.TStatus status
    2: optional i64 next_commit_seq
    3: optional list<TBinlog> binlogs
    4: optional string fe_version
    5: optional i64 fe_meta_version
}

struct TGetTabletReplicaInfosRequest {
    1: required list<i64> tablet_ids
}

struct TGetTabletReplicaInfosResult {
    1: optional Status.TStatus status
    2: optional map<i64, list<Types.TReplicaInfo>> tablet_replica_infos
    3: optional string token
}

enum TSnapshotType {
    REMOTE = 0,
    LOCAL  = 1,
}

struct TGetSnapshotRequest {
    1: optional string cluster
    2: optional string user
    3: optional string passwd
    4: optional string db
    5: optional string table
    6: optional string token
    7: optional string label_name
    8: optional string snapshot_name
    9: optional TSnapshotType snapshot_type
}

struct TGetSnapshotResult {
    1: optional Status.TStatus status
    2: optional binary meta
    3: optional binary job_info
}

struct TTableRef {
    1: optional string table
    3: optional string alias_name
}

struct TRestoreSnapshotRequest {
    1: optional string cluster
    2: optional string user
    3: optional string passwd
    4: optional string db
    5: optional string table
    6: optional string token
    7: optional string label_name
    8: optional string repo_name
    9: optional list<TTableRef> table_refs
    10: optional map<string, string> properties
    11: optional binary meta
    12: optional binary job_info
}

struct TRestoreSnapshotResult {
    1: optional Status.TStatus status
}

struct TGetMasterTokenRequest {
    1: optional string cluster
    2: optional string user
    3: optional string password
}

struct TGetMasterTokenResult {
    1: optional Status.TStatus status
    2: optional string token
}

typedef TGetBinlogRequest TGetBinlogLagRequest

struct TGetBinlogLagResult {
    1: optional Status.TStatus status
    2: optional i64 lag
}

struct TUpdateFollowerStatsCacheRequest {
    1: optional string key;
    2: list<string> statsRows;
}

struct TAutoIncrementRangeRequest {
    1: optional i64 db_id;
    2: optional i64 table_id;
    3: optional i64 column_id;
    4: optional i64 length
    5: optional i64 lower_bound // if set, values in result range must larger than `lower_bound`
}

struct TAutoIncrementRangeResult {
    1: optional Status.TStatus status
    2: optional i64 start
    3: optional i64 length
}

struct TCreatePartitionRequest {
    1: optional i64 txn_id
    2: optional i64 db_id
    3: optional i64 table_id
    // for each partition column's partition values. [missing_rows, partition_keys]->Left bound(for range) or Point(for list)
    4: optional list<list<Exprs.TStringLiteral>> partitionValues
}

struct TCreatePartitionResult {
    1: optional Status.TStatus status
    2: optional list<Descriptors.TOlapTablePartition> partitions
    3: optional list<Descriptors.TTabletLocation> tablets
    4: optional list<Descriptors.TNodeInfo> nodes
}

service FrontendService {
    TGetDbsResult getDbNames(1: TGetDbsParams params)
    TGetTablesResult getTableNames(1: TGetTablesParams params)
    TDescribeTableResult describeTable(1: TDescribeTableParams params)
    TDescribeTablesResult describeTables(1: TDescribeTablesParams params)
    TShowVariableResult showVariables(1: TShowVariableRequest params)
    TReportExecStatusResult reportExecStatus(1: TReportExecStatusParams params)

    MasterService.TMasterResult finishTask(1: MasterService.TFinishTaskRequest request)
    MasterService.TMasterResult report(1: MasterService.TReportRequest request)
    // Deprecated
    MasterService.TFetchResourceResult fetchResource()

    TMasterOpResult forward(1: TMasterOpRequest params)

    TListTableStatusResult listTableStatus(1: TGetTablesParams params)
    TListTableMetadataNameIdsResult listTableMetadataNameIds(1: TGetTablesParams params)
    TListPrivilegesResult listTablePrivilegeStatus(1: TGetTablesParams params)
    TListPrivilegesResult listSchemaPrivilegeStatus(1: TGetTablesParams params)
    TListPrivilegesResult listUserPrivilegeStatus(1: TGetTablesParams params)

    TFeResult updateExportTaskStatus(1: TUpdateExportTaskStatusRequest request)

    TLoadTxnBeginResult loadTxnBegin(1: TLoadTxnBeginRequest request)
    TLoadTxnCommitResult loadTxnPreCommit(1: TLoadTxnCommitRequest request)
    TLoadTxn2PCResult loadTxn2PC(1: TLoadTxn2PCRequest request)
    TLoadTxnCommitResult loadTxnCommit(1: TLoadTxnCommitRequest request)
    TLoadTxnRollbackResult loadTxnRollback(1: TLoadTxnRollbackRequest request)

    TBeginTxnResult beginTxn(1: TBeginTxnRequest request)
    TCommitTxnResult commitTxn(1: TCommitTxnRequest request)
    TRollbackTxnResult rollbackTxn(1: TRollbackTxnRequest request)
    TGetBinlogResult getBinlog(1: TGetBinlogRequest request)
    TGetSnapshotResult getSnapshot(1: TGetSnapshotRequest request)
    TRestoreSnapshotResult restoreSnapshot(1: TRestoreSnapshotRequest request)

    TWaitingTxnStatusResult waitingTxnStatus(1: TWaitingTxnStatusRequest request)

    TStreamLoadPutResult streamLoadPut(1: TStreamLoadPutRequest request)

    TStreamLoadMultiTablePutResult streamLoadMultiTablePut(1: TStreamLoadPutRequest request)

    Status.TStatus snapshotLoaderReport(1: TSnapshotLoaderReportRequest request)

    TFrontendPingFrontendResult ping(1: TFrontendPingFrontendRequest request)

    TAddColumnsResult addColumns(1: TAddColumnsRequest request)

    TInitExternalCtlMetaResult initExternalCtlMeta(1: TInitExternalCtlMetaRequest request)

    TFetchSchemaTableDataResult fetchSchemaTableData(1: TFetchSchemaTableDataRequest request)

    TMySqlLoadAcquireTokenResult acquireToken()

    TConfirmUnusedRemoteFilesResult confirmUnusedRemoteFiles(1: TConfirmUnusedRemoteFilesRequest request)

    TCheckAuthResult checkAuth(1: TCheckAuthRequest request)

    TQueryStatsResult getQueryStats(1: TGetQueryStatsRequest request)

    TGetTabletReplicaInfosResult getTabletReplicaInfos(1: TGetTabletReplicaInfosRequest request)

    TGetMasterTokenResult getMasterToken(1: TGetMasterTokenRequest request)

    TGetBinlogLagResult getBinlogLag(1: TGetBinlogLagRequest request)

    Status.TStatus updateStatsCache(1: TUpdateFollowerStatsCacheRequest request)

    TAutoIncrementRangeResult getAutoIncrementRange(1: TAutoIncrementRangeRequest request)

    TCreatePartitionResult createPartition(1: TCreatePartitionRequest request)
}
