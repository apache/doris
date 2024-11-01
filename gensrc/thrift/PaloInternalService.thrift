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
include "Exprs.thrift"
include "Descriptors.thrift"
include "PlanNodes.thrift"
include "Planner.thrift"
include "DataSinks.thrift"
include "Data.thrift"
include "RuntimeProfile.thrift"
include "PaloService.thrift"

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

enum TQueryType {
    SELECT,
    LOAD,
    EXTERNAL
}

enum TErrorHubType {
    MYSQL,
    BROKER,
    NULL_TYPE
}

enum TPrefetchMode {
    NONE,
    HT_BUCKET
}

struct TMysqlErrorHubInfo {
    1: required string host;
    2: required i32 port;
    3: required string user;
    4: required string passwd;
    5: required string db;
    6: required string table;
}

struct TBrokerErrorHubInfo {
    1: required Types.TNetworkAddress broker_addr;
    2: required string path;
    3: required map<string, string> prop;
}

struct TLoadErrorHubInfo {
    1: required TErrorHubType type = TErrorHubType.NULL_TYPE;
    2: optional TMysqlErrorHubInfo mysql_info;
    3: optional TBrokerErrorHubInfo broker_info;
}

struct TResourceLimit {
    1: optional i32 cpu_limit
}

enum TSerdeDialect {
  DORIS = 0,
  PRESTO = 1
}

// Query options that correspond to PaloService.PaloQueryOptions,
// with their respective defaults
struct TQueryOptions {
  1: optional bool abort_on_error = 0
  2: optional i32 max_errors = 0
  3: optional bool disable_codegen = 1
  4: optional i32 batch_size = 0
  5: optional i32 num_nodes = NUM_NODES_ALL
  6: optional i64 max_scan_range_length = 0
  7: optional i32 num_scanner_threads = 0
  8: optional i32 max_io_buffers = 0
  9: optional bool allow_unsupported_formats = 0
  10: optional i64 default_order_by_limit = -1
  // 11: optional string debug_action = "" // Never used
  12: optional i64 mem_limit = 2147483648
  13: optional bool abort_on_default_limit_exceeded = 0
  14: optional i32 query_timeout = 3600
  15: optional bool is_report_success = 0
  16: optional i32 codegen_level = 0
  // INT64::MAX
  17: optional i64 kudu_latest_observed_ts = 9223372036854775807 // Deprecated
  18: optional TQueryType query_type = TQueryType.SELECT
  19: optional i64 min_reservation = 0
  20: optional i64 max_reservation = 107374182400
  21: optional i64 initial_reservation_total_claims = 2147483647 // TODO chenhao
  22: optional i64 buffer_pool_limit = 2147483648

  // The default spillable buffer size in bytes, which may be overridden by the planner.
  // Defaults to 2MB.
  23: optional i64 default_spillable_buffer_size = 2097152;

  // The minimum spillable buffer to use. The planner will not choose a size smaller than
  // this. Defaults to 64KB.
  24: optional i64 min_spillable_buffer_size = 65536;

  // The maximum size of row that the query will reserve memory to process. Processing
  // rows larger than this may result in a query failure. Defaults to 512KB, e.g.
  // enough for a row with 15 32KB strings or many smaller columns.
  //
  // Different operators handle this option in different ways. E.g. some simply increase
  // the size of all their buffers to fit this row size, whereas others may use more
  // sophisticated strategies - e.g. reserving a small number of buffers large enough to
  // fit maximum-sized rows.
  25: optional i64 max_row_size = 524288;

  // stream preaggregation
  26: optional bool disable_stream_preaggregations = false;

  // multithreaded degree of intra-node parallelism
  27: optional i32 mt_dop = 0;
  // if this is a query option for LOAD, load_mem_limit should be set to limit the mem comsuption
  // of load channel.
  28: optional i64 load_mem_limit = 0;
  // see BE config `doris_max_scan_key_num` for details
  // if set, this will overwrite the BE config.
  29: optional i32 max_scan_key_num;
  // see BE config `max_pushdown_conditions_per_column` for details
  // if set, this will overwrite the BE config.
  30: optional i32 max_pushdown_conditions_per_column
  // whether enable spilling to disk
  31: optional bool enable_spilling = false;
  // whether enable parallel merge in exchange node
  32: optional bool enable_enable_exchange_node_parallel_merge = false; // deprecated

  // Time in ms to wait until runtime filters are delivered.
  33: optional i32 runtime_filter_wait_time_ms = 1000

  // if the right table is greater than this value in the hash join,  we will ignore IN filter
  34: optional i32 runtime_filter_max_in_num = 1024;

  // the resource limitation of this query
  42: optional TResourceLimit resource_limit

  // show bitmap data in result, if use this in mysql cli may make the terminal
  // output corrupted character
  43: optional bool return_object_data_as_binary = false

  // trim tailing spaces while querying external table and stream load
  44: optional bool trim_tailing_spaces_for_external_table_query = false

  45: optional bool enable_function_pushdown;

  46: optional string fragment_transmission_compression_codec;

  48: optional bool enable_local_exchange;

  // For debug purpose, dont' merge unique key and agg key when reading data.
  49: optional bool skip_storage_engine_merge = false

  // For debug purpose, skip delete predicates when reading data
  50: optional bool skip_delete_predicate = false

  51: optional bool enable_new_shuffle_hash_method

  52: optional i32 be_exec_version = 0

  53: optional i32 partitioned_hash_join_rows_threshold = 0

  54: optional bool enable_share_hash_table_for_broadcast_join

  55: optional bool check_overflow_for_decimal = true

  // For debug purpose, skip delete bitmap when reading data
  56: optional bool skip_delete_bitmap = false
  // non-pipelinex engine removed. always true.
  57: optional bool enable_pipeline_engine = true

  58: optional i32 repeat_max_num = 0 // Deprecated

  59: optional i64 external_sort_bytes_threshold = 0

  // deprecated
  60: optional i32 partitioned_hash_agg_rows_threshold = 0

  61: optional bool enable_file_cache = false

  62: optional i32 insert_timeout = 14400

  63: optional i32 execution_timeout = 3600

  64: optional bool dry_run_query = false

  65: optional bool enable_common_expr_pushdown = false;

  66: optional i32 parallel_instance = 1
  // Indicate where useServerPrepStmts enabled
  67: optional bool mysql_row_binary_format = false;
  68: optional i64 external_agg_bytes_threshold = 0

  // partition count(1 << external_agg_partition_bits) when spill aggregation data into disk
  69: optional i32 external_agg_partition_bits = 4

  // Specify base path for file cache
  70: optional string file_cache_base_path

  71: optional bool enable_parquet_lazy_mat = true

  72: optional bool enable_orc_lazy_mat = true

  73: optional i64 scan_queue_mem_limit
  // deprecated
  74: optional bool enable_scan_node_run_serial = false;

  75: optional bool enable_insert_strict = false;

  76: optional bool enable_inverted_index_query = true;

  77: optional bool truncate_char_or_varchar_columns = false

  78: optional bool enable_hash_join_early_start_probe = false
  // non-pipelinex engine removed. always true.
  79: optional bool enable_pipeline_x_engine = true;

  80: optional bool enable_memtable_on_sink_node = false;

  81: optional bool enable_delete_sub_predicate_v2 = false;

  // A tag used to distinguish fe start epoch.
  82: optional i64 fe_process_uuid = 0;

  83: optional i32 inverted_index_conjunction_opt_threshold = 1000;
  // A seperate flag to indicate whether to enable profile, not
  // use is_report_success any more
  84: optional bool enable_profile = false;
  85: optional bool enable_page_cache = false;
  86: optional i32 analyze_timeout = 43200;

  87: optional bool faster_float_convert = false; // deprecated

  88: optional bool enable_decimal256 = false;

  89: optional bool enable_local_shuffle = false;
  // For emergency use, skip missing version when reading rowsets
  90: optional bool skip_missing_version = false;

  91: optional bool runtime_filter_wait_infinitely = false;

  92: optional i32 wait_full_block_schedule_times = 1;
  
  93: optional i32 inverted_index_max_expansions = 50;

  94: optional i32 inverted_index_skip_threshold = 50;

  95: optional bool enable_parallel_scan = false;

  96: optional i32 parallel_scan_max_scanners_count = 0;

  97: optional i64 parallel_scan_min_rows_per_scanner = 0;

  98: optional bool skip_bad_tablet = false;
  // Increase concurrency of scanners adaptively, the maxinum times to scale up
  99: optional double scanner_scale_up_ratio = 0;

  100: optional bool enable_distinct_streaming_aggregation = true;

  101: optional bool enable_join_spill = false

  102: optional bool enable_sort_spill = false

  103: optional bool enable_agg_spill = false

  104: optional i64 min_revocable_mem = 0

  105: optional i64 spill_streaming_agg_mem_limit = 0;

  // max rows of each sub-queue in DataQueue.
  106: optional i64 data_queue_max_blocks = 0;
  
  // expr pushdown for index filter rows
  107: optional bool enable_common_expr_pushdown_for_inverted_index = false;
  108: optional i64 local_exchange_free_blocks_limit;

  109: optional bool enable_force_spill = false;

  110: optional bool enable_parquet_filter_by_min_max = true
  111: optional bool enable_orc_filter_by_min_max = true

  112: optional i32 max_column_reader_num = 0

  113: optional bool enable_local_merge_sort = false;

  114: optional bool enable_parallel_result_sink = false;

  115: optional bool enable_short_circuit_query_access_column_store = false;

  116: optional bool enable_no_need_read_data_opt = true;
  
  117: optional bool read_csv_empty_line_as_null = false;

  118: optional TSerdeDialect serde_dialect = TSerdeDialect.DORIS;

  119: optional bool enable_match_without_inverted_index = true;

  120: optional bool enable_fallback_on_missing_inverted_index = true;

  121: optional bool keep_carriage_return = false; // \n,\r\n split line in CSV.

  122: optional i32 runtime_bloom_filter_min_size = 1048576;

  //Access Parquet/ORC columns by name by default. Set this property to `false` to access columns
  //by their ordinal position in the Hive table definition.  
  123: optional bool hive_parquet_use_column_names = true;
  124: optional bool hive_orc_use_column_names = true;

  125: optional bool enable_segment_cache = true;

  126: optional i32 runtime_bloom_filter_max_size = 16777216;

  127: optional i32 in_list_value_count_threshold = 10;

  // We need this two fields to make sure thrift id on master is compatible with other branch.
  128: optional bool enable_verbose_profile = false;
  129: optional i32 rpc_verbose_profile_max_instance_count = 0;

  130: optional bool enable_adaptive_pipeline_task_serial_read_on_limit = true;
  131: optional i32 adaptive_pipeline_task_serial_read_on_limit = 10000;

  132: optional i32 parallel_prepare_threshold = 0;
  133: optional i32 partition_topn_max_partitions = 1024;
  134: optional i32 partition_topn_pre_partition_rows = 1000;

  135: optional bool enable_parallel_outfile = false;

  136: optional bool enable_phrase_query_sequential_opt = true;

  137: optional bool enable_auto_create_when_overwrite = false;
  // For cloud, to control if the content would be written into file cache
  // In write path, to control if the content would be written into file cache.
  // In read path, read from file cache or remote storage when execute query.
  1000: optional bool disable_file_cache = false
}


// A scan range plus the parameters needed to execute that scan.
struct TScanRangeParams {
  1: required PlanNodes.TScanRange scan_range
  2: optional i32 volume_id = -1
}

struct TRuntimeFilterTargetParams {
  1: required Types.TUniqueId target_fragment_instance_id
  // The address of the instance where the fragment is expected to run
  2: required Types.TNetworkAddress target_fragment_instance_addr
}

struct TRuntimeFilterTargetParamsV2 {
  1: required list<Types.TUniqueId> target_fragment_instance_ids
  // The address of the instance where the fragment is expected to run
  2: required Types.TNetworkAddress target_fragment_instance_addr
  3: optional list<i32> target_fragment_ids
}

struct TRuntimeFilterParams {
  // Runtime filter merge instance address
  1: optional Types.TNetworkAddress runtime_filter_merge_addr

  // Runtime filter ID to the instance address of the fragment,
  // that is expected to use this runtime filter
  2: optional map<i32, list<TRuntimeFilterTargetParams>> rid_to_target_param

  // Runtime filter ID to the runtime filter desc
  3: optional map<i32, PlanNodes.TRuntimeFilterDesc> rid_to_runtime_filter

  // Number of Runtime filter producers
  4: optional map<i32, i32> runtime_filter_builder_num

  5: optional map<i32, list<TRuntimeFilterTargetParamsV2>> rid_to_target_paramv2
}

// Parameters for a single execution instance of a particular TPlanFragment
// TODO: for range partitioning, we also need to specify the range boundaries
struct TPlanFragmentExecParams {
  // a globally unique id assigned to the entire query
  1: required Types.TUniqueId query_id

  // a globally unique id assigned to this particular execution instance of
  // a TPlanFragment
  2: required Types.TUniqueId fragment_instance_id

  // initial scan ranges for each scan node in TPlanFragment.plan_tree
  3: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges

  // number of senders for ExchangeNodes contained in TPlanFragment.plan_tree;
  // needed to create a DataStreamRecvr
  4: required map<Types.TPlanNodeId, i32> per_exch_num_senders

  // Output destinations, one per output partition.
  // The partitioning of the output is specified by
  // TPlanFragment.output_sink.output_partition.
  // The number of output partitions is destinations.size().
  5: list<DataSinks.TPlanFragmentDestination> destinations

  // Debug options: perform some action in a particular phase of a particular node
  // 6: optional Types.TPlanNodeId debug_node_id // Never used
  // 7: optional PlanNodes.TExecNodePhase debug_phase // Never used
  // 8: optional PlanNodes.TDebugAction debug_action // Never used

  // Id of this fragment in its role as a sender.
  9: optional i32 sender_id
  10: optional i32 num_senders
  11: optional bool send_query_statistics_with_every_batch
  // Used to merge and send runtime filter
  12: optional TRuntimeFilterParams runtime_filter_params
  13: optional bool group_commit // deprecated
  14: optional list<i32> topn_filter_source_node_ids
}

// Global query parameters assigned by the coordinator.
struct TQueryGlobals {
  // String containing a timestamp set as the current time.
  // Format is yyyy-MM-dd HH:mm:ss
  1: required string now_string

  // To support timezone in Doris. timestamp_ms is the millisecond uinix timestamp for
  // this query to calculate time zone relative function
  2: optional i64 timestamp_ms

  // time_zone is the timezone this query used.
  // If this value is set, BE will ignore now_string
  3: optional string time_zone

  // Set to true if in a load plan, the max_filter_ratio is 0.0
  4: optional bool load_zero_tolerance = false

  5: optional i32 nano_seconds
}


// Service Protocol Details

enum PaloInternalServiceVersion {
  V1
}

struct TTxnParams {
  1: optional bool need_txn
  2: optional string token
  3: optional i64 thrift_rpc_timeout_ms
  4: optional string db
  5: optional string tbl
  6: optional string user_ip
  7: optional i64 txn_id
  8: optional Types.TUniqueId fragment_instance_id
  9: optional i64 db_id
  10: optional double max_filter_ratio
  // For load task with transaction, use this to indicate we use pipeline or not
  // non-pipelinex engine removed. always true.
  11: optional bool enable_pipeline_txn_load = true;
}

// Definition of global dict, global dict is used to accelerate query performance of low cardinality data
struct TColumnDict {
  1: optional Types.TPrimitiveType type
  2: list<string> str_dict  // map one string to a integer, using offset as id
}

struct TGlobalDict {
  1: optional map<i32, TColumnDict> dicts,  // map dict_id to column dict
  2: optional map<i32, i32> slot_dicts // map from slot id to column dict id, because 2 or more column may share the dict
}

struct TPipelineWorkloadGroup {
  1: optional i64 id
  2: optional string name
  3: optional map<string, string> properties
  4: optional i64 version
}

// ExecPlanFragment
struct TExecPlanFragmentParams {
  1: required PaloInternalServiceVersion protocol_version

  // required in V1
  2: optional Planner.TPlanFragment fragment

  // required in V1
  // @Common components
  3: optional Descriptors.TDescriptorTable desc_tbl

  // required in V1
  4: optional TPlanFragmentExecParams params

  // Initiating coordinator.
  // TODO: determine whether we can get this somehow via the Thrift rpc mechanism.
  // required in V1
  // @Common components
  5: optional Types.TNetworkAddress coord

  // backend number assigned by coord to identify backend
  // required in V1
  6: optional i32 backend_num

  // Global query parameters assigned by coordinator.
  // required in V1
  // @Common components
  7: optional TQueryGlobals query_globals

  // options for the query
  // required in V1
  8: optional TQueryOptions query_options

  // Whether reportd when the backend fails
  // required in V1
  9: optional bool is_report_success

  // required in V1
  // @Common components
  // Deprecated
  10: optional Types.TResourceInfo resource_info

  // load job related
  11: optional string import_label
  12: optional string db_name
  13: optional i64 load_job_id
  14: optional TLoadErrorHubInfo load_error_hub_info

  // The total number of fragments on same BE host
  15: optional i32 fragment_num_on_host

  // If true, all @Common components is unset and should be got from BE's cache
  // If this field is unset or it set to false, all @Common components is set.
  16: optional bool is_simplified_param = false;
  17: optional TTxnParams txn_conf
  18: optional i64 backend_id
  19: optional TGlobalDict global_dict  // scan node could use the global dict to encode the string value to an integer

  // If it is true, after this fragment is prepared on the BE side,
  // it will wait for the FE to send the "start execution" command before it is actually executed.
  // Otherwise, the fragment will start executing directly on the BE side.
  20: optional bool need_wait_execution_trigger = false;

  // deprecated
  21: optional bool build_hash_table_for_broadcast_join = false;

  22: optional list<Types.TUniqueId> instances_sharing_hash_table;
  23: optional string table_name;

  // scan node id -> scan range params, only for external file scan
  24: optional map<Types.TPlanNodeId, PlanNodes.TFileScanRangeParams> file_scan_params

  25: optional i64 wal_id

  // num load stream for each sink backend
  26: optional i32 load_stream_per_node

  // total num of load streams the downstream backend will see
  27: optional i32 total_load_streams

  28: optional i32 num_local_sink

  29: optional i64 content_length

  30: optional list<TPipelineWorkloadGroup> workload_groups

  31: optional bool is_nereids = true;

  32: optional Types.TNetworkAddress current_connect_fe

  // For cloud
  1000: optional bool is_mow_table;
}

struct TExecPlanFragmentParamsList {
    1: optional list<TExecPlanFragmentParams> paramsList;
}

struct TExecPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}

// CancelPlanFragment
struct TCancelPlanFragmentParams {
  1: required PaloInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId fragment_instance_id
}

struct TCancelPlanFragmentResult {
  // required in V1
  1: optional Status.TStatus status
}

// fold constant expr
struct TExprMap {
  1: required map<string, Exprs.TExpr> expr_map
}

struct TFoldConstantParams {
  1: required map<string, map<string, Exprs.TExpr>> expr_map
  2: required TQueryGlobals query_globals
  3: optional bool vec_exec
  4: optional TQueryOptions query_options
  5: optional Types.TUniqueId query_id
  6: optional bool is_nereids
}

// TransmitData
struct TTransmitDataParams {
  1: required PaloInternalServiceVersion protocol_version

  // required in V1
  2: optional Types.TUniqueId dest_fragment_instance_id

  // for debugging purposes; currently ignored
  //3: optional Types.TUniqueId src_fragment_instance_id

  // required in V1
  4: optional Types.TPlanNodeId dest_node_id

  // required in V1
  5: optional Data.TRowBatch row_batch

  // if set to true, indicates that no more row batches will be sent
  // for this dest_node_id
  6: optional bool eos

  7: optional i32 be_number
  8: optional i64 packet_seq

  // Id of this fragment in its role as a sender.
  9: optional i32 sender_id
}

struct TTransmitDataResult {
  // required in V1
  1: optional Status.TStatus status
  2: optional i64 packet_seq
  3: optional Types.TUniqueId dest_fragment_instance_id
  4: optional Types.TPlanNodeId dest_node_id
}

struct TTabletWithPartition {
    1: required i64 partition_id
    2: required i64 tablet_id
}

// open a tablet writer
struct TTabletWriterOpenParams {
    1: required Types.TUniqueId id
    2: required i64 index_id
    3: required i64 txn_id
    4: required Descriptors.TOlapTableSchemaParam schema
    5: required list<TTabletWithPartition> tablets

    6: required i32 num_senders
}

struct TTabletWriterOpenResult {
    1: required Status.TStatus status
}

// add batch to tablet writer
struct TTabletWriterAddBatchParams {
    1: required Types.TUniqueId id
    2: required i64 index_id

    3: required i64 packet_seq
    4: required list<Types.TTabletId> tablet_ids
    5: required Data.TRowBatch row_batch

    6: required i32 sender_no
}

struct TTabletWriterAddBatchResult {
    1: required Status.TStatus status
}

struct TTabletWriterCloseParams {
    1: required Types.TUniqueId id
    2: required i64 index_id

    3: required i32 sender_no
}

struct TTabletWriterCloseResult {
    1: required Status.TStatus status
}

//
struct TTabletWriterCancelParams {
    1: required Types.TUniqueId id
    2: required i64 index_id

    3: required i32 sender_no
}

struct TTabletWriterCancelResult {
}

struct TFetchDataParams {
  1: required PaloInternalServiceVersion protocol_version
  // required in V1
  // query id which want to fetch data
  2: required Types.TUniqueId fragment_instance_id
}

struct TFetchDataResult {
    // result batch
    1: required Data.TResultBatch result_batch
    // end of stream flag
    2: required bool eos
    // packet num used check lost of packet
    3: required i32 packet_num
    // Operation result
    4: optional Status.TStatus status
}

// For cloud
enum TCompoundType {
    UNKNOWN = 0,
    AND = 1,
    OR = 2,
    NOT = 3,
}

struct TCondition {
    1:  required string column_name
    2:  required string condition_op
    3:  required list<string> condition_values
    // In delete condition, the different column may have same column name, need
    // using unique id to distinguish them
    4:  optional i32 column_unique_id
    5:  optional bool marked_by_runtime_filter = false

    // For cloud
    1000: optional TCompoundType compound_type = TCompoundType.UNKNOWN
}

struct TExportStatusResult {
    1: required Status.TStatus status
    2: required Types.TExportState state
    3: optional list<string> files
}

struct TPipelineInstanceParams {
  1: required Types.TUniqueId fragment_instance_id
  // deprecated
  2: optional bool build_hash_table_for_broadcast_join = false;
  3: required map<Types.TPlanNodeId, list<TScanRangeParams>> per_node_scan_ranges
  4: optional i32 sender_id
  5: optional TRuntimeFilterParams runtime_filter_params
  6: optional i32 backend_num
  7: optional map<Types.TPlanNodeId, bool> per_node_shared_scans
  8: optional list<i32> topn_filter_source_node_ids // deprecated after we set topn_filter_descs
  9: optional list<PlanNodes.TTopnFilterDesc> topn_filter_descs
}

// ExecPlanFragment
struct TPipelineFragmentParams {
  1: required PaloInternalServiceVersion protocol_version
  2: required Types.TUniqueId query_id
  3: optional i32 fragment_id
  4: required map<Types.TPlanNodeId, i32> per_exch_num_senders
  5: optional Descriptors.TDescriptorTable desc_tbl
  // Deprecated
  6: optional Types.TResourceInfo resource_info
  7: list<DataSinks.TPlanFragmentDestination> destinations
  8: optional i32 num_senders
  9: optional bool send_query_statistics_with_every_batch
  10: optional Types.TNetworkAddress coord
  11: optional TQueryGlobals query_globals
  12: optional TQueryOptions query_options
  // load job related
  13: optional string import_label
  14: optional string db_name
  15: optional i64 load_job_id
  16: optional TLoadErrorHubInfo load_error_hub_info
  17: optional i32 fragment_num_on_host
  18: optional i64 backend_id
  19: optional bool need_wait_execution_trigger = false
  20: optional list<Types.TUniqueId> instances_sharing_hash_table
  21: optional bool is_simplified_param = false;
  22: optional TGlobalDict global_dict  // scan node could use the global dict to encode the string value to an integer
  23: optional Planner.TPlanFragment fragment
  24: list<TPipelineInstanceParams> local_params
  26: optional list<TPipelineWorkloadGroup> workload_groups
  27: optional TTxnParams txn_conf
  28: optional string table_name
  // scan node id -> scan range params, only for external file scan
  29: optional map<Types.TPlanNodeId, PlanNodes.TFileScanRangeParams> file_scan_params
  30: optional bool group_commit = false;
  31: optional i32 load_stream_per_node // num load stream for each sink backend
  32: optional i32 total_load_streams // total num of load streams the downstream backend will see
  33: optional i32 num_local_sink
  34: optional i32 num_buckets
  35: optional map<i32, i32> bucket_seq_to_instance_idx
  36: optional map<Types.TPlanNodeId, bool> per_node_shared_scans
  37: optional i32 parallel_instances
  38: optional i32 total_instances
  39: optional map<i32, i32> shuffle_idx_to_instance_idx
  40: optional bool is_nereids = true;
  41: optional i64 wal_id
  42: optional i64 content_length
  43: optional Types.TNetworkAddress current_connect_fe
  // Used by 2.1
  44: optional list<i32> topn_filter_source_node_ids

  // For cloud
  1000: optional bool is_mow_table;
}

struct TPipelineFragmentParamsList {
  1: optional list<TPipelineFragmentParams> params_list;
  2: optional Descriptors.TDescriptorTable desc_tbl;
  // scan node id -> scan range params, only for external file scan
  3: optional map<Types.TPlanNodeId, PlanNodes.TFileScanRangeParams> file_scan_params;
  4: optional Types.TNetworkAddress coord;
  5: optional TQueryGlobals query_globals;
  6: optional Types.TResourceInfo resource_info;
  // The total number of fragments on same BE host
  7: optional i32 fragment_num_on_host
  8: optional TQueryOptions query_options
  9: optional bool is_nereids = true;
  10: optional list<TPipelineWorkloadGroup> workload_groups
  11: optional Types.TUniqueId query_id
  12: optional list<i32> topn_filter_source_node_ids
  13: optional Types.TNetworkAddress runtime_filter_merge_addr
}
