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


typedef i64 TTimestamp
typedef i32 TPlanNodeId
typedef i32 TTupleId
typedef i32 TSlotId
typedef i64 TTableId
typedef i64 TTabletId
typedef i64 TReplicaId
typedef i64 TVersion
typedef i64 TVersionHash
typedef i32 TSchemaHash
typedef i32 TPort
typedef i64 TCount
typedef i64 TSize
typedef i32 TClusterId
typedef i64 TEpoch

// add for real time load, partitionid is not defined previously, define it here
typedef i64 TTransactionId
typedef i64 TPartitionId

enum TStorageType {
    ROW = 0,
    COLUMN = 1,
}

enum TStorageMedium {
    HDD = 0,
    SSD = 1,
    S3 = 2,
    REMOTE_CACHE = 3,
}

enum TVarType {
    SESSION = 0,
    GLOBAL = 1
}

enum TPrimitiveType {
  INVALID_TYPE = 0,
  NULL_TYPE = 1,
  BOOLEAN = 2,
  TINYINT = 3,
  SMALLINT = 4,
  INT = 5,
  BIGINT = 6,
  FLOAT = 7,
  DOUBLE = 8,
  DATE = 9,
  DATETIME = 10,
  BINARY = 11,
  DECIMAL_DEPRACTED = 12, // not used now, only for place holder
  // CHAR(n). Currently only supported in UDAs
  CHAR = 13,
  LARGEINT = 14,
  VARCHAR = 15,
  HLL = 16,
  DECIMALV2 = 17,
  // TIME = 18, deprecated, use TIMEV2 instead
  BITMAP = 19,
  ARRAY = 20,
  MAP = 21,
  STRUCT = 22,
  STRING = 23,
  ALL = 24,
  QUANTILE_STATE = 25,
  DATEV2 = 26,
  DATETIMEV2 = 27,
  TIMEV2 = 28,
  DECIMAL32 = 29,
  DECIMAL64 = 30,
  DECIMAL128I = 31,
  JSONB = 32,
  UNSUPPORTED = 33,
  VARIANT = 34,
  LAMBDA_FUNCTION = 35,
  AGG_STATE = 36,
  DECIMAL256 = 37,
  IPV4 = 38,
  IPV6 = 39,
  UINT32 = 40, // only used in BE to represent offsets
  UINT64 = 41,  // only used in BE to represent offsets
  FIXED_LENGTH_OBJECT = 42 // only used in BE to represent fixed-length object
}

enum TTypeNodeType {
    SCALAR = 0,
    ARRAY = 1,
    MAP = 2,
    STRUCT = 3,
    VARIANT = 4,
}

enum TStorageBackendType {
    BROKER = 0,
    S3 = 1,
    HDFS = 2,
    JFS = 3,
    LOCAL = 4,
    OFS = 5,
    AZURE = 6
}

// Enumerates the storage formats for inverted indexes in src_backends.
// This enum is used to distinguish between different organizational methods
// of inverted index data, affecting how the index is stored and accessed.
enum TInvertedIndexFileStorageFormat {
    DEFAULT = 0, // Default format, unspecified storage method.
    V1 = 1,      // Index per idx: Each index is stored separately based on its identifier.
    V2 = 2,      // Segment id per idx: Indexes are organized based on segment identifiers, grouping indexes by their associated segment.
    V3 = 3       // Position and dictionary compression
}

struct TScalarType {
    1: required TPrimitiveType type

    // Only set if type == CHAR or type == VARCHAR
    2: optional i32 len

    // Only set for DECIMAL
    3: optional i32 precision
    4: optional i32 scale

    // Only set for VARIANT
    5: optional i32 variant_max_subcolumns_count = 0;
}

// Represents a field in a STRUCT type.
// TODO: Model column stats for struct fields.
struct TStructField {
    1: required string name
    2: optional string comment
    3: optional bool contains_null
}

struct TTypeNode {
    1: required TTypeNodeType type

    // only set for scalar types
    2: optional TScalarType scalar_type

    // only used for structs; has struct_fields.size() corresponding child types
    3: optional list<TStructField> struct_fields

    // old version used for array
    4: optional bool contains_null

    // update for map/struct type
    5: optional list<bool> contains_nulls
}

// A flattened representation of a tree of column types obtained by depth-first
// traversal. Complex types such as map, array and struct have child types corresponding
// to the map key/value, array item type, and struct fields, respectively.
// For scalar types the list contains only a single node.
// Note: We cannot rename this to TType because it conflicts with Thrift's internal TType
// and the generated Python thrift files will not work.
// Note: TTypeDesc in impala is TColumnType, but we already use TColumnType, so we name this
// to TTypeDesc. In future, we merge these two to one
struct TTypeDesc {
    1: list<TTypeNode> types
    2: optional bool is_nullable
    3: optional i64  byte_size
    4: optional list<TTypeDesc> sub_types
    5: optional bool result_is_nullable
    6: optional string function_name
    7: optional i32 be_exec_version
}

enum TAggregationType {
    SUM = 0,
    MAX = 1,
    MIN = 2,
    REPLACE = 3,
    HLL_UNION = 4,
    NONE = 5,
    BITMAP_UNION = 6,
    REPLACE_IF_NOT_NULL = 7,
    QUANTILE_UNION = 8
}

enum TPushType {
    LOAD = 0, // deprecated, it is used for old hadoop dpp load
    DELETE = 1,
    LOAD_DELETE = 2,
    // for spark load push request
    LOAD_V2 = 3
}

enum TTaskType {
    CREATE = 0,
    DROP = 1,
    PUSH = 2,
    CLONE = 3,
    STORAGE_MEDIUM_MIGRATE = 4,
    ROLLUP = 5, // Deprecated
    SCHEMA_CHANGE = 6,  // Deprecated
    CANCEL_DELETE = 7,  // Deprecated
    MAKE_SNAPSHOT = 8,
    RELEASE_SNAPSHOT = 9,
    CHECK_CONSISTENCY = 10,
    UPLOAD = 11,
    DOWNLOAD = 12,
    CLEAR_REMOTE_FILE = 13,
    MOVE = 14,
    REALTIME_PUSH = 15,
    PUBLISH_VERSION = 16,
    CLEAR_ALTER_TASK = 17,
    CLEAR_TRANSACTION_TASK = 18,
    RECOVER_TABLET = 19, // deprecated
    STREAM_LOAD = 20,
    UPDATE_TABLET_META_INFO = 21,
    // this type of task will replace both ROLLUP and SCHEMA_CHANGE
    ALTER = 22,
    INSTALL_PLUGIN = 23,
    UNINSTALL_PLUGIN = 24,
    COMPACTION = 25,
    STORAGE_MEDIUM_MIGRATE_V2 = 26,
    NOTIFY_UPDATE_STORAGE_POLICY = 27, // deprecated
    PUSH_COOLDOWN_CONF = 28,
    PUSH_STORAGE_POLICY = 29,
    ALTER_INVERTED_INDEX = 30,
    GC_BINLOG = 31,
    CLEAN_TRASH = 32,
    UPDATE_VISIBLE_VERSION = 33,
    CLEAN_UDF_CACHE = 34,
    PUSH_INDEX_POLICY = 35,

    // CLOUD
    CALCULATE_DELETE_BITMAP = 1000
}

// level of verboseness for "explain" output
// TODO: should this go somewhere else?
enum TExplainLevel {
  BRIEF = 0,
  NORMAL = 1,
  VERBOSE = 2
}

enum TRuntimeFilterMode {
  // No filters are computed in the FE or the BE.
  OFF = 0

  // Only broadcast filters are computed in the BE, and are only published to the local
  // fragment.
  LOCAL = 1

  // Only shuffle filters are computed in the BE, and are only published globally.
  REMOTE = 2

  // All fiters are computed in the BE, and are published globally.
  GLOBAL = 3
}

struct TColumnType {
  1: required TPrimitiveType type
  // Only set if type == CHAR_ARRAY
  2: optional i32 len
  3: optional i32 index_len
  4: optional i32 precision
  5: optional i32 scale
  6: optional i32 variant_max_subcolumns_count = 0;
}

// A TNetworkAddress is the standard host, port representation of a
// network address. The hostname field must be resolvable to an IPv4
// address.
struct TNetworkAddress {
  1: required string hostname
  2: required i32 port
}

// Wire format for UniqueId
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

enum QueryState {
  CREATED = 0,
  INITIALIZED = 1,
  COMPILED = 2,
  RUNNING = 3,
  FINISHED = 4,
  EXCEPTION = 5
}

enum TFunctionType {
  SCALAR = 0,
  AGGREGATE = 1,
}

enum TFunctionBinaryType {
  // Doris builtin. We can either run this interpreted or via codegen
  // depending on the query option.
  BUILTIN = 0,

  // Hive UDFs, loaded from *.jar
  HIVE = 1,

  // Native-interface, precompiled UDFs loaded from *.so
  NATIVE = 2,

  // Native-interface, precompiled to IR; loaded from *.ll
  IR = 3,

  // call udfs by rpc service
  RPC = 4,

  JAVA_UDF = 5,

  AGG_STATE = 6
}

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database. Not set if in global
  // namespace (e.g. builtins)
  1: optional string db_name

  // Name of the function
  2: required string function_name
}

struct TScalarFunction {
    // Symbol for the function
    1: required string symbol
    2: optional string prepare_fn_symbol
    3: optional string close_fn_symbol
}

struct TAggregateFunction {
  1: required TTypeDesc intermediate_type
  2: optional string update_fn_symbol
  3: optional string init_fn_symbol
  4: optional string serialize_fn_symbol
  5: optional string merge_fn_symbol
  6: optional string finalize_fn_symbol
  8: optional string get_value_fn_symbol
  9: optional string remove_fn_symbol
  10: optional bool is_analytic_only_fn = false
  // used for java-udaf to point user defined class
  11: optional string symbol
}

struct TDictFunction {
  1: optional i64 dictionary_id
  2: optional i64 version_id
}

// Represents a function in the Catalog.
struct TFunction {
  // Fully qualified function name.
  1: required TFunctionName name

  // Type of the udf. e.g. hive, native, ir
  2: required TFunctionBinaryType binary_type

  // The types of the arguments to the function
  3: required list<TTypeDesc> arg_types

  // Return type for the function.
  4: required TTypeDesc ret_type

  // If true, this function takes var args.
  5: required bool has_var_args

  // Optional comment to attach to the function
  6: optional string comment

  7: optional string signature

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  8: optional string hdfs_location

  // One of these should be set.
  9: optional TScalarFunction scalar_fn
  10: optional TAggregateFunction aggregate_fn

  11: optional i64 id
  12: optional string checksum
  13: optional bool vectorized = false
  14: optional bool is_udtf_function = false
  15: optional bool is_static_load = false
  16: optional i64 expiration_time //minutes
  17: optional TDictFunction dict_function
}

enum TJdbcOperation {
    READ = 0,
    WRITE = 1
}

enum TOdbcTableType {
    MYSQL = 0,
    ORACLE = 1,
    POSTGRESQL = 2,
    SQLSERVER = 3,
    REDIS = 4,
    MONGODB = 5,
    CLICKHOUSE = 6,
    SAP_HANA = 7,
    TRINO = 8,
    PRESTO = 9,
    OCEANBASE = 10,
    OCEANBASE_ORACLE = 11,
    NEBULA = 12, // Deprecated
    DB2 = 13,
    GBASE = 14
}

struct TJdbcExecutorCtorParams {
  1: optional string statement

  // "jdbc:mysql://127.0.0.1:3307/test";
  2: optional string jdbc_url

  // root
  3: optional string jdbc_user

  // password
  4: optional string jdbc_password

  // "com.mysql.jdbc.Driver"
  5: optional string jdbc_driver_class

  6: optional i32 batch_size

  7: optional TJdbcOperation op

  // "/home/user/mysql-connector-java-5.1.47.jar"
  8: optional string driver_path

  9: optional TOdbcTableType table_type

  10: optional i32 connection_pool_min_size
  11: optional i32 connection_pool_max_size
  12: optional i32 connection_pool_max_wait_time
  13: optional i32 connection_pool_max_life_time
  14: optional i32 connection_pool_cache_clear_time
  15: optional bool connection_pool_keep_alive
  16: optional i64 catalog_id
  17: optional string jdbc_driver_checksum
  18: optional bool is_tvf
}

struct TJavaUdfExecutorCtorParams {
  1: optional TFunction fn

  // Local path to the UDF's jar file
  2: optional string location

  // The byte offset for each argument in the input buffer. The BE will
  // call the Java executor with a buffer for all the inputs.
  // input_byte_offsets[0] is the byte offset in the buffer for the first
  // argument; input_byte_offsets[1] is the second, etc.
  3: optional i64 input_offsets_ptrs

  // Native input buffer ptr (cast as i64) for the inputs. The input arguments
  // are written to this buffer directly and read from java with no copies
  // input_null_ptr[i] is true if the i-th input is null.
  // input_buffer_ptr[input_byte_offsets[i]] is the value of the i-th input.
  4: optional i64 input_nulls_ptrs
  5: optional i64 input_buffer_ptrs

  // Native output buffer ptr. For non-variable length types, the output is
  // written here and read from the native side with no copies.
  // The UDF should set *output_null_ptr to true, if the result of the UDF is
  // NULL.
  6: optional i64 output_null_ptr
  7: optional i64 output_buffer_ptr
  8: optional i64 output_offsets_ptr
  9: optional i64 output_intermediate_state_ptr

  10: optional i64 batch_size_ptr

  // this is used to pass place or places to FE, which could help us call jni
  // only once and can process a batch size data in JAVA-Udaf
  11: optional i64 input_places_ptr

  // for array type about nested column null map
  12: optional i64 input_array_nulls_buffer_ptr

  // used for array type of nested string column offset
  13: optional i64 input_array_string_offsets_ptrs

  // for array type about nested column null map when output
  14: optional i64 output_array_null_ptr

  // used for array type of nested string column offset when output
  15: optional i64 output_array_string_offsets_ptr
}

// Contains all interesting statistics from a single 'memory pool' in the JVM.
// All numeric values are measured in bytes.
struct TJvmMemoryPool {
  // Memory committed by the operating system to this pool (i.e. not just virtual address
  // space)
  1: required i64 committed

  // The initial amount of memory committed to this pool
  2: required i64 init

  // The maximum amount of memory this pool will use.
  3: required i64 max

  // The amount of memory currently in use by this pool (will be <= committed).
  4: required i64 used

  // Maximum committed memory over time
  5: required i64 peak_committed

  // Should be always == init
  6: required i64 peak_init

  // Peak maximum memory over time (usually will not change)
  7: required i64 peak_max

  // Peak consumed memory over time
  8: required i64 peak_used

  // Name of this pool, defined by the JVM
  9: required string name
}

// Response from JniUtil::GetJvmMemoryMetrics()
struct TGetJvmMemoryMetricsResponse {
  // One entry for every pool tracked by the Jvm, plus a synthetic aggregate pool called
  // 'total'
  1: required list<TJvmMemoryPool> memory_pools

  // Metrics from JvmPauseMonitor, measuring how much time is spend
  // pausing, presumably because of Garbage Collection. These
  // names are consistent with Hadoop's metric names.
  2: required i64 gc_num_warn_threshold_exceeded
  3: required i64 gc_num_info_threshold_exceeded
  4: required i64 gc_total_extra_sleep_time_millis

  // Metrics for JVM Garbage Collection, from the management beans;
  // these are cumulative across all types of GCs.
  5: required i64 gc_count
  6: required i64 gc_time_millis
}

// Contains information about a JVM thread
struct TJvmThreadInfo {
  // Summary of a JVM thread. Includes stacktraces, locked monitors
  // and synchronizers.
  1: required string summary

  // The total CPU time for this thread in nanoseconds
  2: required i64 cpu_time_in_ns

  // The CPU time that this thread has executed in user mode in nanoseconds
  3: required i64 user_time_in_ns

  // The number of times this thread blocked to enter or reenter a monitor
  4: required i64 blocked_count

  // Approximate accumulated elapsed time (in milliseconds) that this thread has blocked
  // to enter or reenter a monitor
  5: required i64 blocked_time_in_ms

  // True if this thread is executing native code via the Java Native Interface (JNI)
  6: required bool is_in_native
}

// Request to get information about JVM threads
struct TGetJvmThreadsInfoRequest {
  // If set, return complete info about JVM threads. Otherwise, return only
  // the total number of live JVM threads.
  1: required bool get_complete_info
}

struct TGetJvmThreadsInfoResponse {
  // The current number of live threads including both daemon and non-daemon threads
  1: required i32 total_thread_count

  // The current number of live daemon threads
  2: required i32 daemon_thread_count

  // The peak live thread count since the Java virtual machine started
  3: required i32 peak_thread_count

  // Information about JVM threads. It is not included when
  // TGetJvmThreadsInfoRequest.get_complete_info is false.
  4: optional list<TJvmThreadInfo> threads
}

struct TGetJMXJsonResponse {
  // JMX of the JVM serialized to a json string.
  1: required string jmx_json
}

enum TLoadJobState {
    PENDING = 0,
    ETL = 1,
    LOADING = 2,
    FINISHED = 3,
    CANCELLED = 4
}

enum TEtlState {
	RUNNING = 0,
	FINISHED = 1,
	CANCELLED = 2,
  UNKNOWN = 3
}

enum TTableType {
    MYSQL_TABLE = 0, // Deprecated
    OLAP_TABLE = 1,
    SCHEMA_TABLE = 2,
    KUDU_TABLE = 3, // Deprecated
    BROKER_TABLE = 4,
    ES_TABLE = 5,
    ODBC_TABLE = 6,
    HIVE_TABLE = 7,
    ICEBERG_TABLE = 8,
    HUDI_TABLE = 9,
    JDBC_TABLE = 10,
    TEST_EXTERNAL_TABLE = 11,
    MAX_COMPUTE_TABLE = 12,
    LAKESOUL_TABLE = 13,
    TRINO_CONNECTOR_TABLE = 14,
    DICTIONARY_TABLE = 15
}

enum TKeysType {
    PRIMARY_KEYS = 0,
    DUP_KEYS = 1,
    UNIQUE_KEYS = 2,
    AGG_KEYS = 3
}

enum TPriority {
    NORMAL = 0,
    HIGH = 1
}

struct TBackend {
    1: required string host
    2: required TPort be_port
    3: required TPort http_port
    4: optional TPort brpc_port
    5: optional bool is_alive
    6: optional i64 id
}

struct TReplicaInfo {
    1: required string host
    2: required TPort  be_port
    3: required TPort  http_port
    4: required TPort  brpc_port
    5: required TReplicaId replica_id
    6: optional bool is_alive
    7: optional i64 backend_id
}

struct TResourceInfo {
    1: required string user
    2: required string group
}

enum TExportState {
    RUNNING = 0,
    FINISHED = 1,
    CANCELLED = 2,
    UNKNOWN = 3
}

enum TFileType {
    FILE_LOCAL = 0,
    FILE_BROKER = 1,
    FILE_STREAM = 2,    // file content is streaming in the buffer
    FILE_S3 = 3,
    FILE_HDFS = 4,
    FILE_NET = 5,       // read file by network, such as http
}

struct TTabletCommitInfo {
    1: required i64 tabletId
    2: required i64 backendId
    // Every load job should check if the global dict is valid, if the global dict
    // is invalid then should sent the invalid column names to FE
    3: optional list<string> invalid_dict_cols
}

struct TErrorTabletInfo {
    1: optional i64 tabletId
    2: optional string msg
}

enum TLoadType {
    MANUL_LOAD = 0,
    ROUTINE_LOAD = 1,
    MINI_LOAD = 2
}

enum TLoadSourceType {
    RAW = 0,
    KAFKA = 1,
    MULTI_TABLE = 2,
}

enum TMergeType {
  APPEND = 0,
  MERGE = 1,
  DELETE = 2
}

enum TUniqueKeyUpdateMode {
  UPSERT = 0,
  UPDATE_FIXED_COLUMNS = 1,
  UPDATE_FLEXIBLE_COLUMNS = 2
}

enum TSortType {
    LEXICAL = 0,
    ZORDER = 1,
}

enum TMetadataType {
  ICEBERG = 0,
  BACKENDS = 1,
  FRONTENDS = 2,
  CATALOGS = 3,
  FRONTENDS_DISKS = 4,
  MATERIALIZED_VIEWS = 5,
  JOBS = 6,
  TASKS = 7,
  WORKLOAD_SCHED_POLICY = 8,
  PARTITIONS = 9,
  PARTITION_VALUES = 10,
  HUDI = 11,
  PAIMON = 12,
}

enum THudiQueryType {
  TIMELINE = 0
}

// represent a user identity
struct TUserIdentity {
    1: optional string username
    2: optional string host
    3: optional bool is_domain
}

const i32 TSNAPSHOT_REQ_VERSION1 = 3; // corresponding to alpha rowset
const i32 TSNAPSHOT_REQ_VERSION2 = 4; // corresponding to beta rowset
// the snapshot request should always set prefer snapshot version to TPREFER_SNAPSHOT_REQ_VERSION
const i32 TPREFER_SNAPSHOT_REQ_VERSION = TSNAPSHOT_REQ_VERSION2;

