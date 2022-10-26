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
}

// getDbNames returns a list of database names and catalog names
struct TGetDbsResult {
  1: optional list<string> dbs
  2: optional list<string> catalogs
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
  4: optional Types.TUniqueId fragment_instance_id

  // Status of fragment execution; any error status means it's done.
  // required in V1
  5: optional Status.TStatus status

  // If true, fragment finished executing.
  // required in V1
  6: optional bool done

  // cumulative profile
  // required in V1
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
}

struct TFeResult {
    1: required FrontendServiceVersion protocolVersion
    2: required Status.TStatus status
}

struct TMasterOpRequest {
    1: required string user
    2: required string db
    3: required string sql 
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
    12: optional string auth_code_uuid
}

struct TLoadTxnBeginResult {
    1: required Status.TStatus status
    2: optional i64 txnId
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
    34: optional string auth_code_uuid
    35: optional i32 send_batch_parallelism
    36: optional double max_filter_ratio
    37: optional bool load_to_single_tablet
    38: optional string header_type
    39: optional string hidden_columns
    40: optional PlanNodes.TFileCompressType compress_type
}

struct TStreamLoadPutResult {
    1: required Status.TStatus status
    // valid when status is OK
    2: optional PaloInternalService.TExecPlanFragmentParams params
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
    13: optional string auth_code_uuid
    14: optional i64 db_id
}

struct TLoadTxnCommitResult {
    1: required Status.TStatus status
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
    9: optional string auth_code_uuid
    10: optional i64 thrift_rpc_timeout_ms
}

struct TLoadTxn2PCResult {
    1: required Status.TStatus status
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
    11: optional string auth_code_uuid
    12: optional i64 db_id
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

struct TFrontendPingFrontendResult {
    1: required TFrontendPingFrontendStatusCode status
    2: required string msg
    3: required i32 queryPort
    4: required i32 rpcPort
    5: required i64 replayedJournalId
    6: required string version
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

service FrontendService {
    TGetDbsResult getDbNames(1: TGetDbsParams params)
    TGetTablesResult getTableNames(1: TGetTablesParams params)
    TDescribeTableResult describeTable(1: TDescribeTableParams params)
    TShowVariableResult showVariables(1: TShowVariableRequest params)
    TReportExecStatusResult reportExecStatus(1: TReportExecStatusParams params)

    MasterService.TMasterResult finishTask(1: MasterService.TFinishTaskRequest request)
    MasterService.TMasterResult report(1: MasterService.TReportRequest request)
    MasterService.TFetchResourceResult fetchResource()

    TMasterOpResult forward(1: TMasterOpRequest params)

    TListTableStatusResult listTableStatus(1: TGetTablesParams params)
    TListPrivilegesResult listTablePrivilegeStatus(1: TGetTablesParams params)
    TListPrivilegesResult listSchemaPrivilegeStatus(1: TGetTablesParams params)
    TListPrivilegesResult listUserPrivilegeStatus(1: TGetTablesParams params)

    TFeResult updateExportTaskStatus(1: TUpdateExportTaskStatusRequest request)

    TLoadTxnBeginResult loadTxnBegin(1: TLoadTxnBeginRequest request)
    TLoadTxnCommitResult loadTxnPreCommit(1: TLoadTxnCommitRequest request)
    TLoadTxn2PCResult loadTxn2PC(1: TLoadTxn2PCRequest request)
    TLoadTxnCommitResult loadTxnCommit(1: TLoadTxnCommitRequest request)
    TLoadTxnRollbackResult loadTxnRollback(1: TLoadTxnRollbackRequest request)

    TWaitingTxnStatusResult waitingTxnStatus(1: TWaitingTxnStatusRequest request)

    TStreamLoadPutResult streamLoadPut(1: TStreamLoadPutRequest request)

    Status.TStatus snapshotLoaderReport(1: TSnapshotLoaderReportRequest request)

    TFrontendPingFrontendResult ping(1: TFrontendPingFrontendRequest request)

    AgentService.TGetStoragePolicyResult refreshStoragePolicy()
}
