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

include "Exprs.thrift"
include "Types.thrift"
include "Opcodes.thrift"
include "Partitions.thrift"
include "Descriptors.thrift"

enum TPlanNodeType {
  OLAP_SCAN_NODE,
  MYSQL_SCAN_NODE,
  CSV_SCAN_NODE, // deprecated
  SCHEMA_SCAN_NODE,
  HASH_JOIN_NODE,
  MERGE_JOIN_NODE, // deprecated
  AGGREGATION_NODE,
  PRE_AGGREGATION_NODE,
  SORT_NODE,
  EXCHANGE_NODE,
  MERGE_NODE,
  SELECT_NODE,
  CROSS_JOIN_NODE,
  META_SCAN_NODE,
  ANALYTIC_EVAL_NODE,
  OLAP_REWRITE_NODE, // deprecated
  KUDU_SCAN_NODE, // Deprecated
  BROKER_SCAN_NODE,
  EMPTY_SET_NODE, 
  UNION_NODE,
  ES_SCAN_NODE,
  ES_HTTP_SCAN_NODE,
  REPEAT_NODE,
  ASSERT_NUM_ROWS_NODE,
  INTERSECT_NODE,
  EXCEPT_NODE,
  ODBC_SCAN_NODE,
  TABLE_FUNCTION_NODE,
  DATA_GEN_SCAN_NODE,
  FILE_SCAN_NODE,
  JDBC_SCAN_NODE,
  TEST_EXTERNAL_SCAN_NODE,
  PARTITION_SORT_NODE,
  GROUP_COMMIT_SCAN_NODE
}

// phases of an execution node
enum TExecNodePhase {
  PREPARE,
  OPEN,
  GETNEXT,
  CLOSE,
  INVALID
}

// what to do when hitting a debug point (TPaloQueryOptions.DEBUG_ACTION)
enum TDebugAction {
  WAIT,
  FAIL
}

struct TKeyRange {
  1: required i64 begin_key
  2: required i64 end_key
  3: required Types.TPrimitiveType column_type
  4: required string column_name
}

// The information contained in subclasses of ScanNode captured in two separate
// Thrift structs:
// - TScanRange: the data range that's covered by the scan (which varies with the
//   particular partition of the plan fragment of which the scan node is a part)
// - T<subclass>: all other operational parameters that are the same across
//   all plan fragments

struct TPaloScanRange {
  1: required list<Types.TNetworkAddress> hosts
  2: required string schema_hash
  3: required string version
  4: required string version_hash // Deprecated
  5: required Types.TTabletId tablet_id
  6: required string db_name
  7: optional list<TKeyRange> partition_column_ranges
  8: optional string index_name
  9: optional string table_name
}

enum TFileFormatType {
    FORMAT_UNKNOWN = -1,
    FORMAT_CSV_PLAIN = 0,
    FORMAT_CSV_GZ,
    FORMAT_CSV_LZO,
    FORMAT_CSV_BZ2,
    FORMAT_CSV_LZ4FRAME,
    FORMAT_CSV_LZOP,
    FORMAT_PARQUET,
    FORMAT_CSV_DEFLATE,
    FORMAT_ORC,
    FORMAT_JSON,
    FORMAT_PROTO,
    FORMAT_JNI,
    FORMAT_AVRO,
    FORMAT_CSV_LZ4BLOCK,
    FORMAT_CSV_SNAPPYBLOCK,
    FORMAT_WAL,
    FORMAT_ARROW
}

// In previous versions, the data compression format and file format were stored together, as TFileFormatType,
// which was inconvenient for flexible combination of file format and compression format.
// Therefore, the compressed format is separately added here.
// In order to ensure forward compatibility, if this type is set, the type shall prevail,
// otherwise, the TFileFormatType shall prevail
enum TFileCompressType {
    UNKNOWN,
    PLAIN,
    GZ,
    LZO,
    BZ2,
    LZ4FRAME,
    DEFLATE,
    LZOP,
    LZ4BLOCK,
    SNAPPYBLOCK,
    ZLIB,
    ZSTD
}

struct THdfsConf {
    1: required string key
    2: required string value
}

struct THdfsParams {
    1: optional string fs_name
    2: optional string user
    3: optional string hdfs_kerberos_principal
    4: optional string hdfs_kerberos_keytab
    5: optional list<THdfsConf> hdfs_conf
    // Used for Cold Heat Separation to specify the root path
    6: optional string root_path
}

// One broker range information.
struct TBrokerRangeDesc {
    1: required Types.TFileType file_type
    2: required TFileFormatType format_type
    3: required bool splittable;
    // Path of this range
    4: required string path
    // Offset of this file start
    5: required i64 start_offset;
    // Size of this range, if size = -1, this means that will read to then end of file
    6: required i64 size
    // used to get stream for this load
    7: optional Types.TUniqueId load_id
    // total size of the file
    8: optional i64 file_size
    // number of columns from file
    9: optional i32 num_of_columns_from_file
    // columns parsed from file path should be after the columns read from file
    10: optional list<string> columns_from_path
    //  it's usefull when format_type == FORMAT_JSON
    11: optional bool strip_outer_array;
    12: optional string jsonpaths;
    13: optional string json_root;
    //  it's usefull when format_type == FORMAT_JSON
    14: optional bool num_as_string;
    15: optional bool fuzzy_parse;
    16: optional THdfsParams hdfs_params
    17: optional bool read_json_by_line;
    // Whether read line by column defination, only for Hive
    18: optional bool read_by_column_def;
    // csv with header type
    19: optional string header_type;
    // csv skip line num, only used when csv header_type is not set.
    20: optional i32 skip_lines;
}

struct TBrokerScanRangeParams {
    1: required i8 column_separator;
    2: required i8 line_delimiter;

    // We construct one line in file to a tuple. And each field of line
    // correspond to a slot in this tuple.
    // src_tuple_id is the tuple id of the input file
    3: required Types.TTupleId src_tuple_id
    // src_slot_ids is the slot_ids of the input file
    // we use this id to find the slot descriptor
    4: required list<Types.TSlotId> src_slot_ids

    // dest_tuple_id is the tuple id that need by scan node
    5: required Types.TTupleId dest_tuple_id
    // This is expr that convert the content read from file
    // the format that need by the compute layer.
    6: optional map<Types.TSlotId, Exprs.TExpr> expr_of_dest_slot

    // properties need to access broker.
    7: optional map<string, string> properties;

    // If partition_ids is set, data that doesn't in this partition will be filtered.
    8: optional list<i64> partition_ids
    
    // This is the mapping of dest slot id and src slot id in load expr
    // It excludes the slot id which has the transform expr
    9: optional map<Types.TSlotId, Types.TSlotId> dest_sid_to_src_sid_without_trans
    // strictMode is a boolean
    // if strict mode is true, the incorrect data (the result of cast is null) will not be loaded
    10: optional bool strict_mode
    // for multibytes separators
    11: optional i32 column_separator_length = 1;
    12: optional i32 line_delimiter_length = 1;
    13: optional string column_separator_str;
    14: optional string line_delimiter_str;
    // trim double quotes for csv
    15: optional bool trim_double_quotes;

}

// Broker scan range
struct TBrokerScanRange {
    1: required list<TBrokerRangeDesc> ranges
    2: required TBrokerScanRangeParams params
    3: required list<Types.TNetworkAddress> broker_addresses
}

// Es scan range
struct TEsScanRange {
  1: required list<Types.TNetworkAddress> es_hosts  //  es hosts is used by be scan node to connect to es
  // has to set index and type here, could not set it in scannode
  // because on scan node maybe scan an es alias then it contains one or more indices
  2: required string index   
  3: optional string type
  4: required i32 shard_id
}

struct TFileTextScanRangeParams {
    1: optional string column_separator;
    2: optional string line_delimiter;
    3: optional string collection_delimiter;// array ,map ,struct delimiter 
    4: optional string mapkv_delimiter;
    5: optional i8 enclose;
    6: optional i8 escape;
}

struct TFileScanSlotInfo {
    1: optional Types.TSlotId slot_id;
    2: optional bool is_file_slot;
}

// descirbe how to read file
struct TFileAttributes {
    1: optional TFileTextScanRangeParams text_params;
    //  it's usefull when format_type == FORMAT_JSON
    2: optional bool strip_outer_array;
    3: optional string jsonpaths;
    4: optional string json_root;
    5: optional bool num_as_string;
    6: optional bool fuzzy_parse;
    7: optional bool read_json_by_line;
    // Whether read line by column defination, only for Hive
    8: optional bool read_by_column_def;
    // csv with header type
    9: optional string header_type;
    // trim double quotes for csv
    10: optional bool trim_double_quotes;
    // csv skip line num, only used when csv header_type is not set.
    11: optional i32 skip_lines;
    // for cloud copy into
    1001: optional bool ignore_csv_redundant_col;
}

struct TIcebergDeleteFileDesc {
    1: optional string path;
    2: optional i64 position_lower_bound;
    3: optional i64 position_upper_bound;
    4: optional list<i32> field_ids;
    // Iceberg file type, 0: data, 1: position delete, 2: equality delete.
    5: optional i32 content;
}

struct TIcebergFileDesc {
    1: optional i32 format_version;
    // Iceberg file type, 0: data, 1: position delete, 2: equality delete.
    // deprecated, a data file can have both position and delete files
    2: optional i32 content;
    // When open a delete file, filter the data file path with the 'file_path' property
    3: optional list<TIcebergDeleteFileDesc> delete_files;
    // Deprecated
    4: optional Types.TTupleId delete_table_tuple_id;
    // Deprecated
    5: optional Exprs.TExpr file_select_conjunct;
    6: optional string original_file_path;
}

struct TPaimonDeletionFileDesc {
    1: optional string path;
    2: optional i64 offset;
    3: optional i64 length;
}

struct TPaimonFileDesc {
    1: optional string paimon_split
    2: optional string paimon_column_names
    3: optional string db_name
    4: optional string table_name
    5: optional string paimon_predicate
    6: optional map<string, string> paimon_options
    7: optional i64 ctl_id
    8: optional i64 db_id
    9: optional i64 tbl_id
    10: optional i64 last_update_time
    11: optional string file_format
    12: optional TPaimonDeletionFileDesc deletion_file;
    13: optional map<string, string> hadoop_conf
}

struct TTrinoConnectorFileDesc {
    1: optional string catalog_name
    2: optional string db_name
    3: optional string table_name
    4: optional map<string, string> trino_connector_options
    5: optional string trino_connector_table_handle
    6: optional string trino_connector_column_handles
    7: optional string trino_connector_column_metadata
    8: optional string trino_connector_column_names // not used
    9: optional string trino_connector_split
    10: optional string trino_connector_predicate
    11: optional string trino_connector_trascation_handle
}

struct TMaxComputeFileDesc {
    1: optional string partition_spec // deprecated 
    2: optional string session_id 
    3: optional string table_batch_read_session

}

struct THudiFileDesc {
    1: optional string instant_time;
    2: optional string serde;
    3: optional string input_format;
    4: optional string base_path;
    5: optional string data_file_path;
    6: optional i64 data_file_length;
    7: optional list<string> delta_logs;
    8: optional list<string> column_names;
    9: optional list<string> column_types;
    10: optional list<string> nested_fields;
}

struct TLakeSoulFileDesc {
    1: optional list<string> file_paths;
    2: optional list<string> primary_keys;
    3: optional list<string> partition_descs;
    4: optional string table_schema;
    5: optional string options;
}

struct TTransactionalHiveDeleteDeltaDesc {
    1: optional string directory_location
    2: optional list<string> file_names
}

struct TTransactionalHiveDesc {
    1: optional string partition
    2: optional list<TTransactionalHiveDeleteDeltaDesc> delete_deltas
}

struct TTableFormatFileDesc {
    1: optional string table_format_type
    2: optional TIcebergFileDesc iceberg_params
    3: optional THudiFileDesc hudi_params
    4: optional TPaimonFileDesc paimon_params
    5: optional TTransactionalHiveDesc transactional_hive_params
    6: optional TMaxComputeFileDesc max_compute_params
    7: optional TTrinoConnectorFileDesc trino_connector_params
    8: optional TLakeSoulFileDesc lakesoul_params
}

enum TTextSerdeType {
    JSON_TEXT_SERDE = 0,
    HIVE_TEXT_SERDE = 1,
}

struct TFileScanRangeParams {
    // deprecated, move to TFileScanRange
    1: optional Types.TFileType file_type;
    2: optional TFileFormatType format_type;
    // deprecated, move to TFileScanRange
    3: optional TFileCompressType compress_type;
    // If this is for load job, src point to the source table and dest point to the doris table.
    // If this is for query, only dest_tuple_id is set, including both file slot and partition slot.
    4: optional Types.TTupleId src_tuple_id;
    5: optional Types.TTupleId dest_tuple_id
    // num_of_columns_from_file can spilt the all_file_slot and all_partition_slot
    6: optional i32 num_of_columns_from_file;
    // all selected slots which may compose from file and partition value.
    7: optional list<TFileScanSlotInfo> required_slots;

    8: optional THdfsParams hdfs_params;
    // properties for file such as s3 information
    9: optional map<string, string> properties;

    // The convert exprt map for load job
    // desc slot id -> expr
    10: optional map<Types.TSlotId, Exprs.TExpr> expr_of_dest_slot
    11: optional map<Types.TSlotId, Exprs.TExpr> default_value_of_src_slot
    // This is the mapping of dest slot id and src slot id in load expr
    // It excludes the slot id which has the transform expr
    12: optional map<Types.TSlotId, Types.TSlotId> dest_sid_to_src_sid_without_trans

    // strictMode is a boolean
    // if strict mode is true, the incorrect data (the result of cast is null) will not be loaded
    13: optional bool strict_mode

    14: optional list<Types.TNetworkAddress> broker_addresses
    15: optional TFileAttributes file_attributes
    16: optional Exprs.TExpr pre_filter_exprs
    // Deprecated, For data lake table format
    17: optional TTableFormatFileDesc table_format_params
    // For csv query task, same the column index in file, order by dest_tuple
    18: optional list<i32> column_idxs
    // Map of slot to its position in table schema. Only for Hive external table.
    19: optional map<string, i32> slot_name_to_schema_pos
    20: optional list<Exprs.TExpr> pre_filter_exprs_list
    21: optional Types.TUniqueId load_id
    22: optional TTextSerdeType  text_serde_type 
}

struct TFileRangeDesc {
    // If load_id is set, this is for stream/routine load.
    // If path is set, this is for bulk load.
    1: optional Types.TUniqueId load_id
    // Path of this range
    2: optional string path;
    // Offset of this file start
    3: optional i64 start_offset;
    // Size of this range, if size = -1, this means that will read to the end of file
    4: optional i64 size;
    // total size of file this range belongs to, -1 means unset
    5: optional i64 file_size = -1;
    // columns parsed from file path should be after the columns read from file
    6: optional list<string> columns_from_path;
    // column names from file path, in the same order with columns_from_path
    7: optional list<string> columns_from_path_keys;
    // For data lake table format
    8: optional TTableFormatFileDesc table_format_params
    // Use modification time to determine whether the file is changed
    9: optional i64 modification_time
    10: optional Types.TFileType file_type;
    11: optional TFileCompressType compress_type;
    // for hive table, different files may have different fs,
    // so fs_name should be with TFileRangeDesc
    12: optional string fs_name
}

struct TSplitSource {
    1: optional i64 split_source_id
    2: optional i32 num_splits
}

// TFileScanRange represents a set of descriptions of a file and the rules for reading and converting it.
//  TFileScanRangeParams: describe how to read and convert file
//  list<TFileRangeDesc>: file location and range
struct TFileScanRange {
    1: optional list<TFileRangeDesc> ranges
    // If file_scan_params in TExecPlanFragmentParams is set in TExecPlanFragmentParams
    // will use that field, otherwise, use this field.
    // file_scan_params in TExecPlanFragmentParams will always be set in query request,
    // and TFileScanRangeParams here is used for some other request such as fetch table schema for tvf. 
    2: optional TFileScanRangeParams params
    3: optional TSplitSource split_source
}

// Scan range for external datasource, such as file on hdfs, es datanode, etc.
struct TExternalScanRange {
    1: optional TFileScanRange file_scan_range
}

enum TDataGenFunctionName {
    NUMBERS = 0,
}

// Every table valued function should have a scan range definition to save its
// running parameters
struct TTVFNumbersScanRange {
  1: optional i64 totalNumbers
  2: optional bool useConst
  3: optional i64 constValue
}

struct TDataGenScanRange {
  1: optional TTVFNumbersScanRange numbers_params
}


struct TIcebergMetadataParams {
  1: optional Types.TIcebergQueryType iceberg_query_type
  2: optional string catalog
  3: optional string database
  4: optional string table
}

struct TBackendsMetadataParams {
  1: optional string cluster_name
}

struct TFrontendsMetadataParams {
  1: optional string cluster_name
}

struct TMaterializedViewsMetadataParams {
  1: optional string database
  2: optional Types.TUserIdentity current_user_ident
}

struct TPartitionsMetadataParams {
  1: optional string catalog
  2: optional string database
  3: optional string table
}

struct TJobsMetadataParams {
  1: optional string type
  2: optional Types.TUserIdentity current_user_ident
}

struct TTasksMetadataParams {
  1: optional string type
  2: optional Types.TUserIdentity current_user_ident
}

struct TQueriesMetadataParams {
  1: optional string cluster_name
  2: optional bool relay_to_other_fe
  3: optional TMaterializedViewsMetadataParams materialized_views_params
  4: optional TJobsMetadataParams jobs_params
  5: optional TTasksMetadataParams tasks_params
  6: optional TPartitionsMetadataParams partitions_params
}

struct TMetaCacheStatsParams {
}

struct TMetaScanRange {
  1: optional Types.TMetadataType metadata_type
  2: optional TIcebergMetadataParams iceberg_params
  3: optional TBackendsMetadataParams backends_params
  4: optional TFrontendsMetadataParams frontends_params
  5: optional TQueriesMetadataParams queries_params
  6: optional TMaterializedViewsMetadataParams materialized_views_params
  7: optional TJobsMetadataParams jobs_params
  8: optional TTasksMetadataParams tasks_params
  9: optional TPartitionsMetadataParams partitions_params
  10: optional TMetaCacheStatsParams meta_cache_stats_params
}

// Specification of an individual data range which is held in its entirety
// by a storage server
struct TScanRange {
  // one of these must be set for every TScanRange2
  4: optional TPaloScanRange palo_scan_range
  5: optional binary kudu_scan_token // Decrepated
  6: optional TBrokerScanRange broker_scan_range
  7: optional TEsScanRange es_scan_range
  8: optional TExternalScanRange ext_scan_range
  9: optional TDataGenScanRange data_gen_scan_range
  10: optional TMetaScanRange meta_scan_range
}

struct TMySQLScanNode {
  1: required Types.TTupleId tuple_id
  2: required string table_name
  3: required list<string> columns
  4: required list<string> filters
}

struct TOdbcScanNode {
  1: optional Types.TTupleId tuple_id
  2: optional string table_name

  //Deprecated
  3: optional string driver
  4: optional Types.TOdbcTableType type
  5: optional list<string> columns
  6: optional list<string> filters

  //Use now
  7: optional string connect_string
  8: optional string query_string
}

struct TJdbcScanNode {
  1: optional Types.TTupleId tuple_id
  2: optional string table_name
  3: optional string query_string
  4: optional Types.TOdbcTableType table_type
}

struct TBrokerScanNode {
    1: required Types.TTupleId tuple_id

    // Partition info used to process partition select in broker load
    2: optional list<Exprs.TExpr> partition_exprs
    3: optional list<Partitions.TRangePartition> partition_infos
    4: optional list<Exprs.TExpr> pre_filter_exprs
}

struct TFileScanNode {
    1: optional Types.TTupleId tuple_id
    2: optional string table_name
}

struct TEsScanNode {
    1: required Types.TTupleId tuple_id
    2: optional map<string,string> properties
    // used to indicate which fields can get from ES docavalue
    // because elasticsearch can have "fields" feature, field can have
    // two or more types, the first type maybe have not docvalue but other
    // can have, such as (text field not have docvalue, but keyword can have):
    // "properties": {
    //      "city": {
    //        "type": "text",
    //        "fields": {
    //          "raw": {
    //            "type":  "keyword"
    //          }
    //        }
    //      }
    //    }
    // then the docvalue context provided the mapping between the select field and real request field :
    // {"city": "city.raw"}
    // use select city from table, if enable the docvalue, we will fetch the `city` field value from `city.raw`
    3: optional map<string, string> docvalue_context
    // used to indicate which string-type field predicate should used xxx.keyword etc.
    // "k1": {
    //    "type": "text",
    //    "fields": {
    //        "keyword": {
    //            "type": "keyword",
    //            "ignore_above": 256
    //           }
    //    }
    // }
    // k1 > 'abc' -> k1.keyword > 'abc'
    4: optional map<string, string> fields_context
}

struct TMiniLoadEtlFunction {
  1: required string function_name
  2: required i32 param_column_index
}

struct TCsvScanNode {
  1: required Types.TTupleId tuple_id
  2: required list<string> file_paths

  3: optional string column_separator
  4: optional string line_delimiter

  // <column_name, ColumnType>
  5: optional map<string, Types.TColumnType> column_type_mapping

  // columns specified in load command
  6: optional list<string> columns
  // <column_name, default_value_in_string>
  7: optional list<string> unspecified_columns
  // always string type, and only contain columns which are not specified
  8: optional list<string> default_values

  9: optional double max_filter_ratio
  10:optional map<string, TMiniLoadEtlFunction> column_function_mapping
}

struct TSchemaScanNode {
  1: required Types.TTupleId tuple_id

  2: required string table_name
  3: optional string db
  4: optional string table
  5: optional string wild
  6: optional string user   // deprecated
  7: optional string ip // frontend ip
  8: optional i32 port  // frontend thrift server port
  9: optional i64 thread_id
  10: optional string user_ip   // deprecated
  11: optional Types.TUserIdentity current_user_ident   // to replace the user and user_ip
  12: optional bool show_hidden_cloumns = false
  // 13: optional list<TSchemaTableStructure> table_structure // deprecated
  14: optional string catalog
  15: optional list<Types.TNetworkAddress> fe_addr_list
}

struct TMetaScanNode {
  1: required Types.TTupleId tuple_id
  2: optional Types.TMetadataType metadata_type
  3: optional Types.TUserIdentity current_user_ident
}

struct TTestExternalScanNode {
  1: optional Types.TTupleId tuple_id
  2: optional string table_name
}

struct TSortInfo {
  1: required list<Exprs.TExpr> ordering_exprs
  2: required list<bool> is_asc_order
  // Indicates, for each expr, if nulls should be listed first or last. This is
  // independent of is_asc_order.
  3: required list<bool> nulls_first
  // Expressions evaluated over the input row that materialize the tuple to be sorted.
  // Contains one expr per slot in the materialized tuple.
  4: optional list<Exprs.TExpr> sort_tuple_slot_exprs

  // Indicates the nullable info of sort_tuple_slot_exprs is changed after substitute by child's smap
  5: optional list<bool> slot_exprs_nullability_changed_flags
  // Indicates whether topn query using two phase read
  6: optional bool use_two_phase_read
}

enum TPushAggOp {
	NONE = 0,
	MINMAX = 1,
	COUNT = 2,
	MIX = 3,
	COUNT_ON_INDEX = 4
}

struct TOlapScanNode {
  1: required Types.TTupleId tuple_id
  2: required list<string> key_column_name
  3: required list<Types.TPrimitiveType> key_column_type
  4: required bool is_preaggregation
  5: optional string sort_column
  6: optional Types.TKeysType keyType
  7: optional string table_name
  8: optional list<Descriptors.TColumn> columns_desc
  9: optional TSortInfo sort_info
  // When scan match sort_info, we can push limit into OlapScanNode.
  // It's limit for scanner instead of scanNode so we add a new limit.
  10: optional i64 sort_limit
  11: optional bool enable_unique_key_merge_on_write
  12: optional TPushAggOp push_down_agg_type_opt //Deprecated
  13: optional bool use_topn_opt // Deprecated
  14: optional list<Descriptors.TOlapTableIndex> indexes_desc
  15: optional set<i32> output_column_unique_ids
  16: optional list<i32> distribute_column_ids
  17: optional i32 schema_version
  18: optional list<i32> topn_filter_source_node_ids //deprecated, move to TPlanNode.106
}

struct TEqJoinCondition {
  // left-hand side of "<a> = <b>"
  1: required Exprs.TExpr left;
  // right-hand side of "<a> = <b>"
  2: required Exprs.TExpr right;
  // operator of equal join
  3: optional Opcodes.TExprOpcode opcode;
}

enum TJoinOp {
  INNER_JOIN,
  LEFT_OUTER_JOIN,
  LEFT_SEMI_JOIN,
  RIGHT_OUTER_JOIN,
  FULL_OUTER_JOIN,
  CROSS_JOIN,
  MERGE_JOIN, // deprecated

  RIGHT_SEMI_JOIN,
  LEFT_ANTI_JOIN,
  RIGHT_ANTI_JOIN,

  // Similar to LEFT_ANTI_JOIN with special handling for NULLs for the join conjuncts
  // on the build side. Those NULLs are considered candidate matches, and therefore could
  // be rejected (ANTI-join), based on the other join conjuncts. This is in contrast
  // to LEFT_ANTI_JOIN where NULLs are not matches and therefore always returned.
  NULL_AWARE_LEFT_ANTI_JOIN,
  NULL_AWARE_LEFT_SEMI_JOIN
}

enum TJoinDistributionType {
  NONE,
  BROADCAST,
  PARTITIONED,
  BUCKET_SHUFFLE,
  COLOCATE,
}

struct THashJoinNode {
  1: required TJoinOp join_op

  // anything from the ON, USING or WHERE clauses that's an equi-join predicate
  2: required list<TEqJoinCondition> eq_join_conjuncts

  // anything from the ON or USING clauses (but *not* the WHERE clause) that's not an
  // equi-join predicate
  3: optional list<Exprs.TExpr> other_join_conjuncts

  // If true, this join node can (but may choose not to) generate slot filters
  // after constructing the build side that can be applied to the probe side.
  4: optional bool add_probe_filters

  // anything from the ON or USING clauses (but *not* the WHERE clause) that's not an
  // equi-join predicate, only use in vec exec engine
  5: optional Exprs.TExpr vother_join_conjunct

  // hash output column
  6: optional list<Types.TSlotId> hash_output_slot_ids

  // TODO: remove 7 and 8 in the version after the version include projection on ExecNode
  7: optional list<Exprs.TExpr> srcExprList

  8: optional Types.TTupleId voutput_tuple_id

  9: optional list<Types.TTupleId> vintermediate_tuple_id_list

  10: optional bool is_broadcast_join

  11: optional bool is_mark
  12: optional TJoinDistributionType dist_type
  13: optional list<Exprs.TExpr> mark_join_conjuncts
  // use_specific_projections true, if output exprssions is denoted by srcExprList represents, o.w. PlanNode.projections
  14: optional bool use_specific_projections
}

struct TNestedLoopJoinNode {
  1: required TJoinOp join_op
  // TODO: remove 2 and 3 in the version after the version include projection on ExecNode
  2: optional list<Exprs.TExpr> srcExprList

  3: optional Types.TTupleId voutput_tuple_id

  4: optional list<Types.TTupleId> vintermediate_tuple_id_list

  // for bitmap filer, don't need to join, but output left child tuple
  5: optional bool is_output_left_side_only

  6: optional Exprs.TExpr vjoin_conjunct

  7: optional bool is_mark

  8: optional list<Exprs.TExpr> join_conjuncts

  9: optional list<Exprs.TExpr> mark_join_conjuncts
  // use_specific_projections true, if output exprssions is denoted by srcExprList represents, o.w. PlanNode.projections
  10: optional bool use_specific_projections
}

struct TMergeJoinNode {
  // anything from the ON, USING or WHERE clauses that's an equi-join predicate
  1: required list<TEqJoinCondition> cmp_conjuncts

  // anything from the ON or USING clauses (but *not* the WHERE clause) that's not an
  // equi-join predicate
  2: optional list<Exprs.TExpr> other_join_conjuncts
}

enum TAggregationOp {
  INVALID,
  COUNT,
  MAX,
  DISTINCT_PC,
  DISTINCT_PCSA,
  MIN,
  SUM,
  GROUP_CONCAT,
  HLL,
  COUNT_DISTINCT,
  SUM_DISTINCT,
  LEAD,
  FIRST_VALUE,
  LAST_VALUE,
  RANK,
  DENSE_RANK,
  ROW_NUMBER,
  LAG,
  HLL_C,
  BITMAP_UNION,
  NTILE,
}

//struct TAggregateFunctionCall {
  // The aggregate function to call.
//  1: required Types.TFunction fn

  // The input exprs to this aggregate function
//  2: required list<Exprs.TExpr> input_exprs

  // If set, this aggregate function udf has varargs and this is the index for the
  // first variable argument.
//  3: optional i32 vararg_start_idx
//}

struct TAggregationNode {
  1: optional list<Exprs.TExpr> grouping_exprs
  // aggregate exprs. The root of each expr is the aggregate function. The
  // other exprs are the inputs to the aggregate function.
  2: required list<Exprs.TExpr> aggregate_functions

  // Tuple id used for intermediate aggregations (with slots of agg intermediate types)
  3: required Types.TTupleId intermediate_tuple_id

  // Tupld id used for the aggregation output (with slots of agg output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // aggregate functions.
  4: required Types.TTupleId output_tuple_id

  // Set to true if this aggregation function requires finalization to complete after all
  // rows have been aggregated, and this node is not an intermediate node.
  5: required bool need_finalize
  6: optional bool use_streaming_preaggregation
  7: optional list<TSortInfo> agg_sort_infos
  8: optional bool is_first_phase
  9: optional bool is_colocate
  10: optional TSortInfo agg_sort_info_by_group_key
}

struct TRepeatNode {
 // Tulple id used for output, it has new slots.
  1: required Types.TTupleId output_tuple_id
  // Slot id set used to indicate those slots need to set to null.
  2: required list<set<Types.TSlotId>> slot_id_set_list
  // An integer bitmap list, it indicates the bit position of the exprs not null.
  3: required list<i64> repeat_id_list
  // A list of integer list, it indicates the position of the grouping virtual slot.
  4: required list<list<i64>> grouping_list
  // A list of all slot
  5: required set<Types.TSlotId> all_slot_ids
  6: required list<Exprs.TExpr> exprs
}

struct TPreAggregationNode {
  1: required list<Exprs.TExpr> group_exprs
  2: required list<Exprs.TExpr> aggregate_exprs
}

enum TSortAlgorithm {
   HEAP_SORT,
   TOPN_SORT,
   FULL_SORT
 }

struct TSortNode {
  1: required TSortInfo sort_info
  // Indicates whether the backend service should use topn vs. sorting
  2: required bool use_top_n;
  // This is the number of rows to skip before returning results
  3: optional i64 offset

  // Indicates whether the imposed limit comes DEFAULT_ORDER_BY_LIMIT.           
  6: optional bool is_default_limit                                              
  7: optional bool use_topn_opt // Deprecated
  8: optional bool merge_by_exchange
  9: optional bool is_analytic_sort
  10: optional bool is_colocate
  11: optional TSortAlgorithm algorithm
}

enum TopNAlgorithm {
   RANK,
   DENSE_RANK,
   ROW_NUMBER
 }

enum TPartTopNPhase {
  UNKNOWN,
  ONE_PHASE_GLOBAL,
  TWO_PHASE_LOCAL,
  TWO_PHASE_GLOBAL
}

 struct TPartitionSortNode {
   1: optional list<Exprs.TExpr> partition_exprs
   2: optional TSortInfo sort_info
   3: optional bool has_global_limit
   4: optional TopNAlgorithm top_n_algorithm
   5: optional i64 partition_inner_limit
   6: optional TPartTopNPhase ptopn_phase
 }
enum TAnalyticWindowType {
  // Specifies the window as a logical offset
  RANGE,

  // Specifies the window in physical units
  ROWS
}

enum TAnalyticWindowBoundaryType {
  // The window starts/ends at the current row.
  CURRENT_ROW,

  // The window starts/ends at an offset preceding current row.
  PRECEDING,

  // The window starts/ends at an offset following current row.
  FOLLOWING
}

struct TAnalyticWindowBoundary {
  1: required TAnalyticWindowBoundaryType type

  // Predicate that checks: child tuple '<=' buffered tuple + offset for the orderby expr
  2: optional Exprs.TExpr range_offset_predicate

  // Offset from the current row for ROWS windows.
  3: optional i64 rows_offset_value
}

struct TAnalyticWindow {
  // Specifies the window type for the start and end bounds.
  1: required TAnalyticWindowType type

  // Absence indicates window start is UNBOUNDED PRECEDING.
  2: optional TAnalyticWindowBoundary window_start

  // Absence indicates window end is UNBOUNDED FOLLOWING.
  3: optional TAnalyticWindowBoundary window_end
}

// Defines a group of one or more analytic functions that share the same window,
// partitioning expressions and order-by expressions and are evaluated by a single
// ExecNode.
struct TAnalyticNode {
  // Exprs on which the analytic function input is partitioned. Input is already sorted
  // on partitions and order by clauses, partition_exprs is used to identify partition
  // boundaries. Empty if no partition clause is specified.
  1: required list<Exprs.TExpr> partition_exprs

  // Exprs specified by an order-by clause for RANGE windows. Used to evaluate RANGE
  // window boundaries. Empty if no order-by clause is specified or for windows
  // specifying ROWS.
  2: required list<Exprs.TExpr> order_by_exprs

  // Functions evaluated over the window for each input row. The root of each expr is
  // the aggregate function. Child exprs are the inputs to the function.
  3: required list<Exprs.TExpr> analytic_functions

  // Window specification
  4: optional TAnalyticWindow window

  // Tuple used for intermediate results of analytic function evaluations
  // (with slots of analytic intermediate types)
  5: required Types.TTupleId intermediate_tuple_id

  // Tupld used for the analytic function output (with slots of analytic output types)
  // Equal to intermediate_tuple_id if intermediate type == output type for all
  // analytic functions.
  6: required Types.TTupleId output_tuple_id

  // id of the buffered tuple (identical to the input tuple, which is assumed
  // to come from a single SortNode); not set if both partition_exprs and
  // order_by_exprs are empty
  7: optional Types.TTupleId buffered_tuple_id

  // predicate that checks: child tuple is in the same partition as the buffered tuple,
  // i.e. each partition expr is equal or both are not null. Only set if
  // buffered_tuple_id is set; should be evaluated over a row that is composed of the
  // child tuple and the buffered tuple
  8: optional Exprs.TExpr partition_by_eq

  // predicate that checks: the order_by_exprs are equal or both NULL when evaluated
  // over the child tuple and the buffered tuple. only set if buffered_tuple_id is set;
  // should be evaluated over a row that is composed of the child tuple and the buffered
  // tuple
  9: optional Exprs.TExpr order_by_eq

  10: optional bool is_colocate
}

struct TMergeNode {
  // A MergeNode could be the left input of a join and needs to know which tuple to write.
  1: required Types.TTupleId tuple_id
  // List or expr lists materialized by this node.
  // There is one list of exprs per query stmt feeding into this merge node.
  2: required list<list<Exprs.TExpr>> result_expr_lists
  // Separate list of expr lists coming from a constant select stmts.
  3: required list<list<Exprs.TExpr>> const_expr_lists
}

struct TUnionNode {
    // A UnionNode materializes all const/result exprs into this tuple.
    1: required Types.TTupleId tuple_id
    // List or expr lists materialized by this node.
    // There is one list of exprs per query stmt feeding into this union node.
    2: required list<list<Exprs.TExpr>> result_expr_lists
    // Separate list of expr lists coming from a constant select stmts.
    3: required list<list<Exprs.TExpr>> const_expr_lists
    // Index of the first child that needs to be materialized.
    4: required i64 first_materialized_child_idx
}

struct TIntersectNode {
    // A IntersectNode materializes all const/result exprs into this tuple.
    1: required Types.TTupleId tuple_id
    // List or expr lists materialized by this node.
    // There is one list of exprs per query stmt feeding into this union node.
    2: required list<list<Exprs.TExpr>> result_expr_lists
    // Separate list of expr lists coming from a constant select stmts.
    3: required list<list<Exprs.TExpr>> const_expr_lists
    // Index of the first child that needs to be materialized.
    4: required i64 first_materialized_child_idx
    5: optional bool is_colocate
}

struct TExceptNode {
    // A ExceptNode materializes all const/result exprs into this tuple.
    1: required Types.TTupleId tuple_id
    // List or expr lists materialized by this node.
    // There is one list of exprs per query stmt feeding into this union node.
    2: required list<list<Exprs.TExpr>> result_expr_lists
    // Separate list of expr lists coming from a constant select stmts.
    3: required list<list<Exprs.TExpr>> const_expr_lists
    // Index of the first child that needs to be materialized.
    4: required i64 first_materialized_child_idx
    5: optional bool is_colocate
}


struct TExchangeNode {
  // The ExchangeNode's input rows form a prefix of the output rows it produces;
  // this describes the composition of that prefix
  1: required list<Types.TTupleId> input_row_tuples
  // For a merging exchange, the sort information.
  2: optional TSortInfo sort_info
  // This is tHe number of rows to skip before returning results
  3: optional i64 offset
  // Shuffle partition type
  4: optional Partitions.TPartitionType partition_type
}

struct TOlapRewriteNode {
    1: required list<Exprs.TExpr> columns
    2: required list<Types.TColumnType> column_types
    3: required Types.TTupleId output_tuple_id
}

struct TTableFunctionNode {
    1: optional list<Exprs.TExpr> fnCallExprList
    2: optional list<Types.TSlotId> outputSlotIds
}

// This contains all of the information computed by the plan as part of the resource
// profile that is needed by the backend to execute.
struct TBackendResourceProfile {
// The minimum reservation for this plan node in bytes.
1: required i64 min_reservation = 0; // no support reservation

// The maximum reservation for this plan node in bytes. MAX_INT64 means effectively
// unlimited.
2: required i64 max_reservation = 12188490189880;  // no max reservation limit 

// The spillable buffer size in bytes to use for this node, chosen by the planner.
// Set iff the node uses spillable buffers.
3: optional i64 spillable_buffer_size = 2097152

// The buffer size in bytes that is large enough to fit the largest row to be processed.
// Set if the node allocates buffers for rows from the buffer pool.
// Deprecated after support string type
4: optional i64 max_row_buffer_size = 4294967296  // 4G
}

enum TAssertion {
  EQ, // val1 == val2
  NE, // val1 != val2
  LT, // val1 < val2
  LE, // val1 <= val2
  GT, // val1 > val2
  GE // val1 >= val2
}

struct TAssertNumRowsNode {
    1: optional i64 desired_num_rows;
    2: optional string subquery_string;
    3: optional TAssertion assertion;
    4: optional bool should_convert_output_to_nullable;
}

enum TRuntimeFilterType {
  IN = 1
  BLOOM = 2
  MIN_MAX = 4
  IN_OR_BLOOM = 8
  BITMAP = 16
}

// generate min-max runtime filter for non-equal condition or equal condition. 
enum TMinMaxRuntimeFilterType {
  // only min is valid, RF generated according to condition: n < col_A
  MIN = 1
  // only max is valid, RF generated according to condition: m > col_A
  MAX = 2
  // both min/max are valid, 
  // support hash join condition: col_A = col_B
  // support other join condition: n < col_A and col_A < m
  MIN_MAX = 4
}

struct TTopnFilterDesc {
  // topn node id
  1: required i32 source_node_id 
  2: required bool is_asc
  3: required bool null_first 
  // scan node id -> expr on scan node
  4: required map<Types.TPlanNodeId, Exprs.TExpr> target_node_id_to_target_expr
}

// Specification of a runtime filter.
struct TRuntimeFilterDesc {
  // Filter unique id (within a query)
  1: required i32 filter_id

  // Expr on which the filter is built on a hash join.
  2: required Exprs.TExpr src_expr

  // The order of Expr in join predicate
  3: required i32 expr_order

  // Map of target node id to the target expr
  4: required map<Types.TPlanNodeId, Exprs.TExpr> planId_to_target_expr

  // Indicates if the source join node of this filter is a broadcast or
  // a partitioned join.
  5: required bool is_broadcast_join

  // Indicates if there is at least one target scan node that is in the
  // same fragment as the broadcast join that produced the runtime filter
  6: required bool has_local_targets

  // Indicates if there is at least one target scan node that is not in the same
  // fragment as the broadcast join that produced the runtime filter
  7: required bool has_remote_targets

  // The type of runtime filter to build.
  8: required TRuntimeFilterType type

  // The size of the filter based on the ndv estimate and the min/max limit specified in
  // the query options. Should be greater than zero for bloom filters, zero otherwise.
  9: optional i64 bloom_filter_size_bytes

  // for bitmap filter target expr
  10: optional Exprs.TExpr bitmap_target_expr

  // for bitmap filter
  11: optional bool bitmap_filter_not_in

  12: optional bool opt_remote_rf; // Deprecated
  
  // for min/max rf
  13: optional TMinMaxRuntimeFilterType min_max_type;

  // true, if bloom filter size is calculated by ndv
  // if bloom_filter_size_calculated_by_ndv=false, BE could calculate filter size according to the actural row count, and 
  // ignore bloom_filter_size_bytes
  14: optional bool bloom_filter_size_calculated_by_ndv;

  // true, if join type is null aware like <=>. rf should dispose the case
  15: optional bool null_aware;

  16: optional bool sync_filter_size;
}



struct TDataGenScanNode {
	1: optional Types.TTupleId tuple_id
  2: optional TDataGenFunctionName func_name
}

struct TGroupCommitScanNode {
    1: optional i64 table_id;
}

// This is essentially a union of all messages corresponding to subclasses
// of PlanNode.
struct TPlanNode {
  // node id, needed to reassemble tree structure
  1: required Types.TPlanNodeId node_id
  2: required TPlanNodeType node_type
  3: required i32 num_children
  4: required i64 limit
  5: required list<Types.TTupleId> row_tuples

  // nullable_tuples[i] is true if row_tuples[i] is nullable
  6: required list<bool> nullable_tuples
  7: optional list<Exprs.TExpr> conjuncts

  // Produce data in compact format.
  8: required bool compact_data

  // one field per PlanNode subclass
  11: optional THashJoinNode hash_join_node
  12: optional TAggregationNode agg_node
  13: optional TSortNode sort_node
  14: optional TMergeNode merge_node
  15: optional TExchangeNode exchange_node
  17: optional TMySQLScanNode mysql_scan_node
  18: optional TOlapScanNode olap_scan_node  
  19: optional TCsvScanNode csv_scan_node  
  20: optional TBrokerScanNode broker_scan_node  
  21: optional TPreAggregationNode pre_agg_node
  22: optional TSchemaScanNode schema_scan_node
  23: optional TMergeJoinNode merge_join_node
  24: optional TMetaScanNode meta_scan_node
  25: optional TAnalyticNode analytic_node
  26: optional TOlapRewriteNode olap_rewrite_node
  28: optional TUnionNode union_node
  29: optional TBackendResourceProfile resource_profile
  30: optional TEsScanNode es_scan_node
  31: optional TRepeatNode repeat_node
  32: optional TAssertNumRowsNode assert_num_rows_node
  33: optional TIntersectNode intersect_node
  34: optional TExceptNode except_node
  35: optional TOdbcScanNode odbc_scan_node
  // Runtime filters assigned to this plan node, exist in HashJoinNode and ScanNode
  36: optional list<TRuntimeFilterDesc> runtime_filters
  37: optional TGroupCommitScanNode group_commit_scan_node

  // Use in vec exec engine
  40: optional Exprs.TExpr vconjunct

  41: optional TTableFunctionNode table_function_node

  // output column
  42: optional list<Types.TSlotId> output_slot_ids
  43: optional TDataGenScanNode data_gen_scan_node

  // file scan node
  44: optional TFileScanNode file_scan_node
  45: optional TJdbcScanNode jdbc_scan_node
  46: optional TNestedLoopJoinNode nested_loop_join_node
  47: optional TTestExternalScanNode test_external_scan_node

  48: optional TPushAggOp push_down_agg_type_opt

  49: optional i64 push_down_count

  50: optional list<list<Exprs.TExpr>> distribute_expr_lists
  // projections is final projections, which means projecting into results and materializing them into the output block.
  101: optional list<Exprs.TExpr> projections
  102: optional Types.TTupleId output_tuple_id
  103: optional TPartitionSortNode partition_sort_node
  // Intermediate projections will not materialize into the output block.
  104: optional list<list<Exprs.TExpr>> intermediate_projections_list
  105: optional list<Types.TTupleId> intermediate_output_tuple_id_list

  106: optional list<i32> topn_filter_source_node_ids
  107: optional i32 nereids_id
}

// A flattened representation of a tree of PlanNodes, obtained by depth-first
// traversal.
struct TPlan {
  1: required list<TPlanNode> nodes
}
