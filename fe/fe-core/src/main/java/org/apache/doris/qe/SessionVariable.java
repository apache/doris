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

package org.apache.doris.qe;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.VariableMgr.VarAttr;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TResourceLimit;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * System variable.
 **/
public class SessionVariable implements Serializable, Writable {
    public static final Logger LOG = LogManager.getLogger(SessionVariable.class);

    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String QUERY_TIMEOUT = "query_timeout";
    public static final String ENABLE_PROFILE = "enable_profile";
    public static final String SQL_MODE = "sql_mode";
    public static final String RESOURCE_VARIABLE = "resource_group";
    public static final String AUTO_COMMIT = "autocommit";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String TX_READ_ONLY = "tx_read_only";
    public static final String TRANSACTION_READ_ONLY = "transaction_read_only";
    public static final String TRANSACTION_ISOLATION = "transaction_isolation";
    public static final String CHARACTER_SET_CLIENT = "character_set_client";
    public static final String CHARACTER_SET_CONNNECTION = "character_set_connection";
    public static final String CHARACTER_SET_RESULTS = "character_set_results";
    public static final String CHARACTER_SET_SERVER = "character_set_server";
    public static final String COLLATION_CONNECTION = "collation_connection";
    public static final String COLLATION_DATABASE = "collation_database";
    public static final String COLLATION_SERVER = "collation_server";
    public static final String SQL_AUTO_IS_NULL = "SQL_AUTO_IS_NULL";
    public static final String SQL_SELECT_LIMIT = "sql_select_limit";
    public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";
    public static final String AUTO_INCREMENT_INCREMENT = "auto_increment_increment";
    public static final String QUERY_CACHE_TYPE = "query_cache_type";
    public static final String INTERACTIVE_TIMTOUT = "interactive_timeout";
    public static final String WAIT_TIMEOUT = "wait_timeout";
    public static final String NET_WRITE_TIMEOUT = "net_write_timeout";
    public static final String NET_READ_TIMEOUT = "net_read_timeout";
    public static final String TIME_ZONE = "time_zone";
    public static final String SQL_SAFE_UPDATES = "sql_safe_updates";
    public static final String NET_BUFFER_LENGTH = "net_buffer_length";
    public static final String CODEGEN_LEVEL = "codegen_level";
    // mem limit can't smaller than bufferpool's default page size
    public static final int MIN_EXEC_MEM_LIMIT = 2097152;
    public static final String BATCH_SIZE = "batch_size";
    public static final String DISABLE_STREAMING_PREAGGREGATIONS = "disable_streaming_preaggregations";
    public static final String DISABLE_COLOCATE_PLAN = "disable_colocate_plan";
    public static final String ENABLE_BUCKET_SHUFFLE_JOIN = "enable_bucket_shuffle_join";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM = "parallel_fragment_exec_instance_num";
    public static final String ENABLE_INSERT_STRICT = "enable_insert_strict";
    public static final String ENABLE_SPILLING = "enable_spilling";
    public static final String ENABLE_EXCHANGE_NODE_PARALLEL_MERGE = "enable_exchange_node_parallel_merge";
    public static final String PREFER_JOIN_METHOD = "prefer_join_method";

    public static final String ENABLE_FOLD_CONSTANT_BY_BE = "enable_fold_constant_by_be";
    public static final String ENABLE_ODBC_TRANSCATION = "enable_odbc_transcation";
    public static final String ENABLE_SQL_CACHE = "enable_sql_cache";
    public static final String ENABLE_PARTITION_CACHE = "enable_partition_cache";

    public static final String ENABLE_COST_BASED_JOIN_REORDER = "enable_cost_based_join_reorder";

    public static final int MIN_EXEC_INSTANCE_NUM = 1;
    public static final int MAX_EXEC_INSTANCE_NUM = 32;
    // if set to true, some of stmt will be forwarded to master FE to get result
    public static final String FORWARD_TO_MASTER = "forward_to_master";
    // user can set instance num after exchange, no need to be equal to nums of before exchange
    public static final String PARALLEL_EXCHANGE_INSTANCE_NUM = "parallel_exchange_instance_num";
    public static final String SHOW_HIDDEN_COLUMNS = "show_hidden_columns";
    public static final String USE_V2_ROLLUP = "use_v2_rollup";
    public static final String TEST_MATERIALIZED_VIEW = "test_materialized_view";
    public static final String REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL = "rewrite_count_distinct_to_bitmap_hll";
    public static final String EVENT_SCHEDULER = "event_scheduler";
    public static final String STORAGE_ENGINE = "storage_engine";
    // Compatible with datagrip mysql
    public static final String DEFAULT_STORAGE_ENGINE = "default_storage_engine";
    public static final String DEFAULT_TMP_STORAGE_ENGINE = "default_tmp_storage_engine";

    public static final String DIV_PRECISION_INCREMENT = "div_precision_increment";

    // see comment of `doris_max_scan_key_num` and `max_pushdown_conditions_per_column` in BE config
    public static final String MAX_SCAN_KEY_NUM = "max_scan_key_num";
    public static final String MAX_PUSHDOWN_CONDITIONS_PER_COLUMN = "max_pushdown_conditions_per_column";

    // when true, the partition column must be set to NOT NULL.
    public static final String ALLOW_PARTITION_COLUMN_NULLABLE = "allow_partition_column_nullable";

    // runtime filter run mode
    public static final String RUNTIME_FILTER_MODE = "runtime_filter_mode";
    // Size in bytes of Bloom Filters used for runtime filters. Actual size of filter will
    // be rounded up to the nearest power of two.
    public static final String RUNTIME_BLOOM_FILTER_SIZE = "runtime_bloom_filter_size";
    // Minimum runtime bloom filter size, in bytes
    public static final String RUNTIME_BLOOM_FILTER_MIN_SIZE = "runtime_bloom_filter_min_size";
    // Maximum runtime bloom filter size, in bytes
    public static final String RUNTIME_BLOOM_FILTER_MAX_SIZE = "runtime_bloom_filter_max_size";
    // Time in ms to wait until runtime filters are delivered.
    public static final String RUNTIME_FILTER_WAIT_TIME_MS = "runtime_filter_wait_time_ms";
    // Maximum number of bloom runtime filters allowed per query
    public static final String RUNTIME_FILTERS_MAX_NUM = "runtime_filters_max_num";
    // Runtime filter type used, For testing, Corresponds to TRuntimeFilterType
    public static final String RUNTIME_FILTER_TYPE = "runtime_filter_type";
    // if the right table is greater than this value in the hash join,  we will ignore IN filter
    public static final String RUNTIME_FILTER_MAX_IN_NUM = "runtime_filter_max_in_num";

    // max ms to wait transaction publish finish when exec insert stmt.
    public static final String INSERT_VISIBLE_TIMEOUT_MS = "insert_visible_timeout_ms";

    public static final String DELETE_WITHOUT_PARTITION = "delete_without_partition";

    // set the default parallelism for send batch when execute InsertStmt operation,
    // if the value for parallelism exceed `max_send_batch_parallelism_per_job` in BE config,
    // then the coordinator be will use the value of `max_send_batch_parallelism_per_job`
    public static final String SEND_BATCH_PARALLELISM = "send_batch_parallelism";

    // turn off all automatic join reorder algorithms
    public static final String DISABLE_JOIN_REORDER = "disable_join_reorder";

    public static final String ENABLE_INFER_PREDICATE = "enable_infer_predicate";

    public static final long DEFAULT_INSERT_VISIBLE_TIMEOUT_MS = 10_000;

    public static final String EXTRACT_WIDE_RANGE_EXPR = "extract_wide_range_expr";

    public static final String PARTITION_PRUNE_ALGORITHM_VERSION = "partition_prune_algorithm_version";

    // If user set a very small value, use this value instead.
    public static final long MIN_INSERT_VISIBLE_TIMEOUT_MS = 1000;

    public static final String ENABLE_VECTORIZED_ENGINE = "enable_vectorized_engine";

    public static final String ENABLE_SINGLE_DISTINCT_COLUMN_OPT = "enable_single_distinct_column_opt";

    public static final String CPU_RESOURCE_LIMIT = "cpu_resource_limit";

    public static final String ENABLE_PARALLEL_OUTFILE = "enable_parallel_outfile";

    public static final String ENABLE_LATERAL_VIEW = "enable_lateral_view";

    public static final String SQL_QUOTE_SHOW_CREATE = "sql_quote_show_create";

    public static final String RETURN_OBJECT_DATA_AS_BINARY = "return_object_data_as_binary";

    public static final String BLOCK_ENCRYPTION_MODE = "block_encryption_mode";

    public static final String AUTO_BROADCAST_JOIN_THRESHOLD = "auto_broadcast_join_threshold";

    public static final String ENABLE_PROJECTION = "enable_projection";

    public static final String TRIM_TAILING_SPACES_FOR_EXTERNAL_TABLE_QUERY
            = "trim_tailing_spaces_for_external_table_query";

    public static final String ENABLE_NEREIDS_PLANNER = "enable_nereids_planner";

    public static final String ENABLE_FALLBACK_TO_ORIGINAL_PLANNER = "enable_fallback_to_original_planner";

    public static final String ENABLE_NEREIDS_RUNTIME_FILTER = "enable_nereids_runtime_filter";

    public static final String NEREIDS_STAR_SCHEMA_SUPPORT = "nereids_star_schema_support";
    public static final String ENABLE_NEREIDS_TRACE = "enable_nereids_trace";
    public static final String ENABLE_NEREIDS_REORDER_TO_ELIMINATE_CROSS_JOIN =
            "enable_nereids_reorder_to_eliminate_cross_join";

    public static final String ENABLE_REMOVE_NO_CONJUNCTS_RUNTIME_FILTER =
            "enable_remove_no_conjuncts_runtime_filter_policy";

    static final String SESSION_CONTEXT = "session_context";

    public static final String ENABLE_SINGLE_REPLICA_INSERT = "enable_single_replica_insert";

    public static final String ENABLE_FUNCTION_PUSHDOWN = "enable_function_pushdown";

    public static final String FRAGMENT_TRANSMISSION_COMPRESSION_CODEC = "fragment_transmission_compression_codec";

    public static final String ENABLE_LOCAL_EXCHANGE = "enable_local_exchange";

    public static final String SKIP_STORAGE_ENGINE_MERGE = "skip_storage_engine_merge";

    public static final String SKIP_DELETE_PREDICATE = "skip_delete_predicate";

    public static final String ENABLE_NEW_SHUFFLE_HASH_METHOD = "enable_new_shuffle_hash_method";

    public static final String ENABLE_PUSH_DOWN_NO_GROUP_AGG = "enable_push_down_no_group_agg";

    public static final String ENABLE_CBO_STATISTICS = "enable_cbo_statistics";

    public static final String ENABLE_NEREIDS_STATS_DERIVE_V2 = "enable_nereids_stats_derive_v2";

    // session origin value
    public Map<Field, String> sessionOriginValue = new HashMap<Field, String>();
    // check stmt is or not [select /*+ SET_VAR(...)*/ ...]
    // if it is setStmt, we needn't collect session origin value
    public boolean isSingleSetVar = false;

    @VariableMgr.VarAttr(name = INSERT_VISIBLE_TIMEOUT_MS, needForward = true)
    public long insertVisibleTimeoutMs = DEFAULT_INSERT_VISIBLE_TIMEOUT_MS;

    // max memory used on every backend.
    @VariableMgr.VarAttr(name = EXEC_MEM_LIMIT)
    public long maxExecMemByte = 2147483648L;

    @VariableMgr.VarAttr(name = ENABLE_SPILLING)
    public boolean enableSpilling = false;

    @VariableMgr.VarAttr(name = ENABLE_EXCHANGE_NODE_PARALLEL_MERGE)
    public boolean enableExchangeNodeParallelMerge = false;

    // query timeout in second.
    @VariableMgr.VarAttr(name = QUERY_TIMEOUT)
    public int queryTimeoutS = 300;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = ENABLE_PROFILE, needForward = true)
    public boolean enableProfile = false;

    // using hashset intead of group by + count can improve performance
    //        but may cause rpc failed when cluster has less BE
    // Whether this switch is turned on depends on the BE number
    @VariableMgr.VarAttr(name = ENABLE_SINGLE_DISTINCT_COLUMN_OPT)
    public boolean enableSingleDistinctColumnOpt = false;

    // Set sqlMode to empty string
    @VariableMgr.VarAttr(name = SQL_MODE, needForward = true)
    public long sqlMode = 0L;

    @VariableMgr.VarAttr(name = RESOURCE_VARIABLE)
    public String resourceGroup = "normal";

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = AUTO_COMMIT)
    public boolean autoCommit = true;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = TX_ISOLATION)
    public String txIsolation = "REPEATABLE-READ";

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = TX_READ_ONLY)
    public boolean txReadonly = false;

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = TRANSACTION_READ_ONLY)
    public boolean transactionReadonly = false;

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = TRANSACTION_ISOLATION)
    public String transactionIsolation = "REPEATABLE-READ";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = CHARACTER_SET_CLIENT)
    public String charsetClient = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_CONNNECTION)
    public String charsetConnection = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_RESULTS)
    public String charsetResults = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_SERVER)
    public String charsetServer = "utf8";
    @VariableMgr.VarAttr(name = COLLATION_CONNECTION)
    public String collationConnection = "utf8_general_ci";
    @VariableMgr.VarAttr(name = COLLATION_DATABASE)
    public String collationDatabase = "utf8_general_ci";

    @VariableMgr.VarAttr(name = COLLATION_SERVER)
    public String collationServer = "utf8_general_ci";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = SQL_AUTO_IS_NULL)
    public boolean sqlAutoIsNull = false;

    @VariableMgr.VarAttr(name = SQL_SELECT_LIMIT)
    public long sqlSelectLimit = 9223372036854775807L;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = MAX_ALLOWED_PACKET)
    public int maxAllowedPacket = 1048576;

    @VariableMgr.VarAttr(name = AUTO_INCREMENT_INCREMENT)
    public int autoIncrementIncrement = 1;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = QUERY_CACHE_TYPE)
    public int queryCacheType = 0;

    // The number of seconds the server waits for activity on an interactive connection before closing it
    @VariableMgr.VarAttr(name = INTERACTIVE_TIMTOUT)
    public int interactiveTimeout = 3600;

    // The number of seconds the server waits for activity on a noninteractive connection before closing it.
    @VariableMgr.VarAttr(name = WAIT_TIMEOUT)
    public int waitTimeoutS = 28800;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_WRITE_TIMEOUT)
    public int netWriteTimeout = 60;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_READ_TIMEOUT)
    public int netReadTimeout = 60;

    // The current time zone
    @VariableMgr.VarAttr(name = TIME_ZONE, needForward = true)
    public String timeZone = TimeUtils.getSystemTimeZone().getID();

    @VariableMgr.VarAttr(name = PARALLEL_EXCHANGE_INSTANCE_NUM)
    public int exchangeInstanceParallel = -1;

    @VariableMgr.VarAttr(name = SQL_SAFE_UPDATES)
    public int sqlSafeUpdates = 0;

    // only
    @VariableMgr.VarAttr(name = NET_BUFFER_LENGTH, flag = VariableMgr.READ_ONLY)
    public int netBufferLength = 16384;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = CODEGEN_LEVEL)
    public int codegenLevel = 0;

    // 1024 minus 16 + 16 bytes padding that in padding pod array
    @VariableMgr.VarAttr(name = BATCH_SIZE)
    public int batchSize = 992;

    @VariableMgr.VarAttr(name = DISABLE_STREAMING_PREAGGREGATIONS)
    public boolean disableStreamPreaggregations = false;

    @VariableMgr.VarAttr(name = DISABLE_COLOCATE_PLAN)
    public boolean disableColocatePlan = false;

    @VariableMgr.VarAttr(name = ENABLE_BUCKET_SHUFFLE_JOIN)
    public boolean enableBucketShuffleJoin = true;

    @VariableMgr.VarAttr(name = PREFER_JOIN_METHOD)
    public String preferJoinMethod = "broadcast";

    @VariableMgr.VarAttr(name = FRAGMENT_TRANSMISSION_COMPRESSION_CODEC)
    public String fragmentTransmissionCompressionCodec = "lz4";

    /*
     * the parallel exec instance num for one Fragment in one BE
     * 1 means disable this feature
     */
    @VariableMgr.VarAttr(name = PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM)
    public int parallelExecInstanceNum = 1;

    @VariableMgr.VarAttr(name = ENABLE_INSERT_STRICT, needForward = true)
    public boolean enableInsertStrict = true;

    @VariableMgr.VarAttr(name = ENABLE_ODBC_TRANSCATION)
    public boolean enableOdbcTransaction = false;

    @VariableMgr.VarAttr(name = ENABLE_SQL_CACHE)
    public boolean enableSqlCache = false;

    @VariableMgr.VarAttr(name = ENABLE_PARTITION_CACHE)
    public boolean enablePartitionCache = false;

    @VariableMgr.VarAttr(name = FORWARD_TO_MASTER)
    public boolean forwardToMaster = true;

    @VariableMgr.VarAttr(name = USE_V2_ROLLUP)
    public boolean useV2Rollup = false;

    // TODO(ml): remove it after test
    @VariableMgr.VarAttr(name = TEST_MATERIALIZED_VIEW)
    public boolean testMaterializedView = false;

    @VariableMgr.VarAttr(name = REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL)
    public boolean rewriteCountDistinct = true;

    // compatible with some mysql client connect, say DataGrip of JetBrains
    @VariableMgr.VarAttr(name = EVENT_SCHEDULER)
    public String eventScheduler = "OFF";
    @VariableMgr.VarAttr(name = STORAGE_ENGINE)
    public String storageEngine = "olap";
    @VariableMgr.VarAttr(name = DEFAULT_STORAGE_ENGINE)
    public String defaultStorageEngine = "olap";
    @VariableMgr.VarAttr(name = DEFAULT_TMP_STORAGE_ENGINE)
    public String defaultTmpStorageEngine = "olap";
    @VariableMgr.VarAttr(name = DIV_PRECISION_INCREMENT)
    public int divPrecisionIncrement = 4;

    // -1 means unset, BE will use its config value
    @VariableMgr.VarAttr(name = MAX_SCAN_KEY_NUM)
    public int maxScanKeyNum = -1;
    @VariableMgr.VarAttr(name = MAX_PUSHDOWN_CONDITIONS_PER_COLUMN)
    public int maxPushdownConditionsPerColumn = -1;
    @VariableMgr.VarAttr(name = SHOW_HIDDEN_COLUMNS, flag = VariableMgr.SESSION_ONLY)
    public boolean showHiddenColumns = false;

    @VariableMgr.VarAttr(name = ALLOW_PARTITION_COLUMN_NULLABLE)
    public boolean allowPartitionColumnNullable = true;

    @VariableMgr.VarAttr(name = DELETE_WITHOUT_PARTITION, needForward = true)
    public boolean deleteWithoutPartition = false;

    @VariableMgr.VarAttr(name = SEND_BATCH_PARALLELISM, needForward = true)
    public int sendBatchParallelism = 1;

    @VariableMgr.VarAttr(name = EXTRACT_WIDE_RANGE_EXPR, needForward = true)
    public boolean extractWideRangeExpr = true;

    @VariableMgr.VarAttr(name = PARTITION_PRUNE_ALGORITHM_VERSION, needForward = true)
    public int partitionPruneAlgorithmVersion = 2;

    @VariableMgr.VarAttr(name = ENABLE_VECTORIZED_ENGINE)
    public boolean enableVectorizedEngine = true;

    @VariableMgr.VarAttr(name = ENABLE_PARALLEL_OUTFILE)
    public boolean enableParallelOutfile = false;

    @VariableMgr.VarAttr(name = CPU_RESOURCE_LIMIT)
    public int cpuResourceLimit = -1;

    @VariableMgr.VarAttr(name = SQL_QUOTE_SHOW_CREATE)
    public boolean sqlQuoteShowCreate = true;

    @VariableMgr.VarAttr(name = TRIM_TAILING_SPACES_FOR_EXTERNAL_TABLE_QUERY, needForward = true)
    public boolean trimTailingSpacesForExternalTableQuery = false;


    // the maximum size in bytes for a table that will be broadcast to all be nodes
    // when performing a join, By setting this value to -1 broadcasting can be disabled.
    // Default value is 1Gto
    @VariableMgr.VarAttr(name = AUTO_BROADCAST_JOIN_THRESHOLD)
    public double autoBroadcastJoinThreshold = 0.8;

    @VariableMgr.VarAttr(name = ENABLE_COST_BASED_JOIN_REORDER)
    private boolean enableJoinReorderBasedCost = false;

    @VariableMgr.VarAttr(name = ENABLE_FOLD_CONSTANT_BY_BE)
    private boolean enableFoldConstantByBe = false;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_MODE)
    private String runtimeFilterMode = "GLOBAL";

    @VariableMgr.VarAttr(name = RUNTIME_BLOOM_FILTER_SIZE)
    private int runtimeBloomFilterSize = 2097152;

    @VariableMgr.VarAttr(name = RUNTIME_BLOOM_FILTER_MIN_SIZE)
    private int runtimeBloomFilterMinSize = 1048576;

    @VariableMgr.VarAttr(name = RUNTIME_BLOOM_FILTER_MAX_SIZE)
    private int runtimeBloomFilterMaxSize = 16777216;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_WAIT_TIME_MS)
    private int runtimeFilterWaitTimeMs = 1000;

    @VariableMgr.VarAttr(name = RUNTIME_FILTERS_MAX_NUM)
    private int runtimeFiltersMaxNum = 10;

    // Set runtimeFilterType to IN_OR_BLOOM filter
    @VariableMgr.VarAttr(name = RUNTIME_FILTER_TYPE)
    private int runtimeFilterType = 8;

    @VariableMgr.VarAttr(name = RUNTIME_FILTER_MAX_IN_NUM)
    private int runtimeFilterMaxInNum = 1024;

    @VariableMgr.VarAttr(name = DISABLE_JOIN_REORDER)
    private boolean disableJoinReorder = false;

    @VariableMgr.VarAttr(name = ENABLE_INFER_PREDICATE)
    private boolean enableInferPredicate = true;

    @VariableMgr.VarAttr(name = RETURN_OBJECT_DATA_AS_BINARY)
    private boolean returnObjectDataAsBinary = false;

    @VariableMgr.VarAttr(name = BLOCK_ENCRYPTION_MODE)
    private String blockEncryptionMode = "";


    @VariableMgr.VarAttr(name = ENABLE_PROJECTION)
    private boolean enableProjection = true;

    /**
     * as the new optimizer is not mature yet, use this var
     * to control whether to use new optimizer, remove it when
     * the new optimizer is fully developed. I hope that day
     * would be coming soon.
     */
    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_PLANNER)
    private boolean enableNereidsPlanner = false;

    @VariableMgr.VarAttr(name = NEREIDS_STAR_SCHEMA_SUPPORT)
    private boolean nereidsStarSchemaSupport = true;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_TRACE)
    private boolean enableNereidsTrace = true;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_RUNTIME_FILTER)
    private boolean enableNereidsRuntimeFilter = true;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_REORDER_TO_ELIMINATE_CROSS_JOIN)
    private boolean enableNereidsReorderToEliminateCrossJoin = true;

    @VariableMgr.VarAttr(name = ENABLE_REMOVE_NO_CONJUNCTS_RUNTIME_FILTER)
    public boolean enableRemoveNoConjunctsRuntimeFilterPolicy = false;

    /**
     * The client can pass some special information by setting this session variable in the format: "k1:v1;k2:v2".
     * For example, trace_id can be passed to trace the query request sent by the user.
     * set session_context="trace_id:1234565678";
     */
    @VariableMgr.VarAttr(name = SESSION_CONTEXT, needForward = true)
    public String sessionContext = "";

    @VariableMgr.VarAttr(name = ENABLE_SINGLE_REPLICA_INSERT, needForward = true)
    public boolean enableSingleReplicaInsert = false;

    @VariableMgr.VarAttr(name = ENABLE_FUNCTION_PUSHDOWN)
    public boolean enableFunctionPushdown;

    @VariableMgr.VarAttr(name = ENABLE_LOCAL_EXCHANGE)
    public boolean enableLocalExchange = true;

    /**
     * For debugg purpose, dont' merge unique key and agg key when reading data.
     */
    @VariableMgr.VarAttr(name = SKIP_STORAGE_ENGINE_MERGE)
    public boolean skipStorageEngineMerge = false;

    /**
     * For debugg purpose, skip delte predicate when reading data.
     */
    @VariableMgr.VarAttr(name = SKIP_DELETE_PREDICATE)
    public boolean skipDeletePredicate = false;

    // This variable is used to avoid FE fallback to the original parser. When we execute SQL in regression tests
    // for nereids, fallback will cause the Doris return the correct result although the syntax is unsupported
    // in nereids for some mistaken modification. You should set it on the
    @VariableMgr.VarAttr(name = ENABLE_FALLBACK_TO_ORIGINAL_PLANNER)
    public boolean enableFallbackToOriginalPlanner = true;

    @VariableMgr.VarAttr(name = ENABLE_NEW_SHUFFLE_HASH_METHOD)
    public boolean enableNewShuffleHashMethod = true;

    @VariableMgr.VarAttr(name = ENABLE_PUSH_DOWN_NO_GROUP_AGG)
    public boolean enablePushDownNoGroupAgg = true;

    /**
     * The current statistics are only used for CBO test,
     * and are not available to users. (work in progress)
     */
    @VariableMgr.VarAttr(name = ENABLE_CBO_STATISTICS)
    public boolean enableCboStatistics = false;

    @VariableMgr.VarAttr(name = ENABLE_NEREIDS_STATS_DERIVE_V2)
    public boolean enableNereidsStatsDeriveV2 = false;

    public String getBlockEncryptionMode() {
        return blockEncryptionMode;
    }

    public void setBlockEncryptionMode(String blockEncryptionMode) {
        this.blockEncryptionMode = blockEncryptionMode;
    }

    public long getMaxExecMemByte() {
        return maxExecMemByte;
    }

    public int getQueryTimeoutS() {
        return queryTimeoutS;
    }

    public boolean enableProfile() {
        return enableProfile;
    }

    public boolean enableSingleDistinctColumnOpt() {
        return enableSingleDistinctColumnOpt;
    }

    public int getWaitTimeoutS() {
        return waitTimeoutS;
    }

    public long getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    public boolean isEnableJoinReorderBasedCost() {
        return enableJoinReorderBasedCost;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public boolean isTxReadonly() {
        return txReadonly;
    }

    public boolean isTransactionReadonly() {
        return transactionReadonly;
    }

    public String getTransactionIsolation() {
        return transactionIsolation;
    }

    public String getTxIsolation() {
        return txIsolation;
    }

    public String getCharsetClient() {
        return charsetClient;
    }

    public String getCharsetConnection() {
        return charsetConnection;
    }

    public String getCharsetResults() {
        return charsetResults;
    }

    public String getCharsetServer() {
        return charsetServer;
    }

    public String getCollationConnection() {
        return collationConnection;
    }

    public String getCollationDatabase() {
        return collationDatabase;
    }

    public String getCollationServer() {
        return collationServer;
    }

    public boolean isSqlAutoIsNull() {
        return sqlAutoIsNull;
    }

    public long getSqlSelectLimit() {
        return sqlSelectLimit;
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public int getQueryCacheType() {
        return queryCacheType;
    }

    public int getInteractiveTimeout() {
        return interactiveTimeout;
    }

    public int getNetWriteTimeout() {
        return netWriteTimeout;
    }

    public int getNetReadTimeout() {
        return netReadTimeout;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public int getSqlSafeUpdates() {
        return sqlSafeUpdates;
    }

    public int getNetBufferLength() {
        return netBufferLength;
    }

    public int getCodegenLevel() {
        return codegenLevel;
    }

    /**
     * setMaxExecMemByte.
     **/
    public void setMaxExecMemByte(long maxExecMemByte) {
        if (maxExecMemByte < MIN_EXEC_MEM_LIMIT) {
            this.maxExecMemByte = MIN_EXEC_MEM_LIMIT;
        } else {
            this.maxExecMemByte = maxExecMemByte;
        }
    }

    public boolean isSqlQuoteShowCreate() {
        return sqlQuoteShowCreate;
    }

    public void setSqlQuoteShowCreate(boolean sqlQuoteShowCreate) {
        this.sqlQuoteShowCreate = sqlQuoteShowCreate;
    }

    public void setQueryTimeoutS(int queryTimeoutS) {
        this.queryTimeoutS = queryTimeoutS;
    }

    public String getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public boolean isDisableColocatePlan() {
        return disableColocatePlan;
    }

    public boolean isEnableBucketShuffleJoin() {
        return enableBucketShuffleJoin;
    }

    public boolean isEnableOdbcTransaction() {
        return enableOdbcTransaction;
    }

    public String getPreferJoinMethod() {
        return preferJoinMethod;
    }

    public void setPreferJoinMethod(String preferJoinMethod) {
        this.preferJoinMethod = preferJoinMethod;
    }

    public boolean isEnableFoldConstantByBe() {
        return enableFoldConstantByBe;
    }

    public void setEnableFoldConstantByBe(boolean foldConstantByBe) {
        this.enableFoldConstantByBe = foldConstantByBe;
    }

    public int getParallelExecInstanceNum() {
        return parallelExecInstanceNum;
    }

    public int getExchangeInstanceParallel() {
        return exchangeInstanceParallel;
    }

    public boolean getEnableInsertStrict() {
        return enableInsertStrict;
    }

    public void setEnableInsertStrict(boolean enableInsertStrict) {
        this.enableInsertStrict = enableInsertStrict;
    }

    public boolean isEnableSqlCache() {
        return enableSqlCache;
    }

    public void setEnableSqlCache(boolean enableSqlCache) {
        this.enableSqlCache = enableSqlCache;
    }

    public boolean isEnablePartitionCache() {
        return enablePartitionCache;
    }

    public void setEnablePartitionCache(boolean enablePartitionCache) {
        this.enablePartitionCache = enablePartitionCache;
    }

    // Serialize to thrift object
    public boolean getForwardToMaster() {
        return forwardToMaster;
    }

    public boolean isUseV2Rollup() {
        return useV2Rollup;
    }

    // for unit test
    public void setUseV2Rollup(boolean useV2Rollup) {
        this.useV2Rollup = useV2Rollup;
    }

    public boolean getTestMaterializedView() {
        return this.testMaterializedView;
    }

    public void setTestMaterializedView(boolean testMaterializedView) {
        this.testMaterializedView = testMaterializedView;
    }

    public boolean isRewriteCountDistinct() {
        return rewriteCountDistinct;
    }

    public void setRewriteCountDistinct(boolean rewriteCountDistinct) {
        this.rewriteCountDistinct = rewriteCountDistinct;
    }

    public String getEventScheduler() {
        return eventScheduler;
    }

    public void setEventScheduler(String eventScheduler) {
        this.eventScheduler = eventScheduler;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public int getDivPrecisionIncrement() {
        return divPrecisionIncrement;
    }

    public int getMaxScanKeyNum() {
        return maxScanKeyNum;
    }

    public void setMaxScanKeyNum(int maxScanKeyNum) {
        this.maxScanKeyNum = maxScanKeyNum;
    }

    public int getMaxPushdownConditionsPerColumn() {
        return maxPushdownConditionsPerColumn;
    }

    public void setMaxPushdownConditionsPerColumn(int maxPushdownConditionsPerColumn) {
        this.maxPushdownConditionsPerColumn = maxPushdownConditionsPerColumn;
    }

    public boolean showHiddenColumns() {
        return showHiddenColumns;
    }

    public void setShowHiddenColumns(boolean showHiddenColumns) {
        this.showHiddenColumns = showHiddenColumns;
    }

    public boolean skipStorageEngineMerge() {
        return skipStorageEngineMerge;
    }

    public boolean isAllowPartitionColumnNullable() {
        return allowPartitionColumnNullable;
    }

    public String getRuntimeFilterMode() {
        return runtimeFilterMode;
    }

    public void setRuntimeFilterMode(String runtimeFilterMode) {
        this.runtimeFilterMode = runtimeFilterMode;
    }

    public int getRuntimeBloomFilterSize() {
        return runtimeBloomFilterSize;
    }

    public void setRuntimeBloomFilterSize(int runtimeBloomFilterSize) {
        this.runtimeBloomFilterSize = runtimeBloomFilterSize;
    }

    public int getRuntimeBloomFilterMinSize() {
        return runtimeBloomFilterMinSize;
    }

    public void setRuntimeBloomFilterMinSize(int runtimeBloomFilterMinSize) {
        this.runtimeBloomFilterMinSize = runtimeBloomFilterMinSize;
    }

    public int getRuntimeBloomFilterMaxSize() {
        return runtimeBloomFilterMaxSize;
    }

    public void setRuntimeBloomFilterMaxSize(int runtimeBloomFilterMaxSize) {
        this.runtimeBloomFilterMaxSize = runtimeBloomFilterMaxSize;
    }

    public int getRuntimeFilterWaitTimeMs() {
        return runtimeFilterWaitTimeMs;
    }

    public void setRuntimeFilterWaitTimeMs(int runtimeFilterWaitTimeMs) {
        this.runtimeFilterWaitTimeMs = runtimeFilterWaitTimeMs;
    }

    public int getRuntimeFiltersMaxNum() {
        return runtimeFiltersMaxNum;
    }

    public void setRuntimeFiltersMaxNum(int runtimeFiltersMaxNum) {
        this.runtimeFiltersMaxNum = runtimeFiltersMaxNum;
    }

    public int getRuntimeFilterType() {
        return runtimeFilterType;
    }

    public void setRuntimeFilterType(int runtimeFilterType) {
        this.runtimeFilterType = runtimeFilterType;
    }

    public int getRuntimeFilterMaxInNum() {
        return runtimeFilterMaxInNum;
    }

    public void setRuntimeFilterMaxInNum(int runtimeFilterMaxInNum) {
        this.runtimeFilterMaxInNum = runtimeFilterMaxInNum;
    }

    public boolean enableVectorizedEngine() {
        return enableVectorizedEngine;
    }

    public void setEnableVectorizedEngine(boolean enableVectorizedEngine) {
        this.enableVectorizedEngine = enableVectorizedEngine;
    }

    public boolean enablePushDownNoGroupAgg() {
        return enablePushDownNoGroupAgg;
    }

    public boolean getEnableFunctionPushdown() {
        return this.enableFunctionPushdown;
    }

    public boolean getEnableLocalExchange() {
        return enableLocalExchange;
    }

    public boolean getEnableCboStatistics() {
        return enableCboStatistics;
    }

    /**
     * getInsertVisibleTimeoutMs.
     **/
    public long getInsertVisibleTimeoutMs() {
        if (insertVisibleTimeoutMs < MIN_INSERT_VISIBLE_TIMEOUT_MS) {
            return MIN_INSERT_VISIBLE_TIMEOUT_MS;
        } else {
            return insertVisibleTimeoutMs;
        }
    }

    /**
     * setInsertVisibleTimeoutMs.
     **/
    public void setInsertVisibleTimeoutMs(long insertVisibleTimeoutMs) {
        if (insertVisibleTimeoutMs < MIN_INSERT_VISIBLE_TIMEOUT_MS) {
            this.insertVisibleTimeoutMs = MIN_INSERT_VISIBLE_TIMEOUT_MS;
        } else {
            this.insertVisibleTimeoutMs = insertVisibleTimeoutMs;
        }
    }

    public boolean getIsSingleSetVar() {
        return isSingleSetVar;
    }

    public void setIsSingleSetVar(boolean issinglesetvar) {
        this.isSingleSetVar = issinglesetvar;
    }

    public Map<Field, String> getSessionOriginValue() {
        return sessionOriginValue;
    }

    public void addSessionOriginValue(Field key, String value) {
        sessionOriginValue.put(key, value);
    }

    public void clearSessionOriginValue() {
        sessionOriginValue.clear();
    }

    public boolean isDeleteWithoutPartition() {
        return deleteWithoutPartition;
    }

    public boolean isExtractWideRangeExpr() {
        return extractWideRangeExpr;
    }

    public int getPartitionPruneAlgorithmVersion() {
        return partitionPruneAlgorithmVersion;
    }

    public int getCpuResourceLimit() {
        return cpuResourceLimit;
    }

    public int getSendBatchParallelism() {
        return sendBatchParallelism;
    }

    public boolean isEnableParallelOutfile() {
        return enableParallelOutfile;
    }

    public boolean isDisableJoinReorder() {
        return disableJoinReorder;
    }

    public boolean isReturnObjectDataAsBinary() {
        return returnObjectDataAsBinary;
    }

    public void setReturnObjectDataAsBinary(boolean returnObjectDataAsBinary) {
        this.returnObjectDataAsBinary = returnObjectDataAsBinary;
    }

    public boolean isEnableInferPredicate() {
        return enableInferPredicate;
    }

    public void setEnableInferPredicate(boolean enableInferPredicate) {
        this.enableInferPredicate = enableInferPredicate;
    }

    public boolean isEnableProjection() {
        return enableProjection;
    }

    public boolean isTrimTailingSpacesForExternalTableQuery() {
        return trimTailingSpacesForExternalTableQuery;
    }

    public void setTrimTailingSpacesForExternalTableQuery(boolean trimTailingSpacesForExternalTableQuery) {
        this.trimTailingSpacesForExternalTableQuery = trimTailingSpacesForExternalTableQuery;
    }

    public void setEnableJoinReorderBasedCost(boolean enableJoinReorderBasedCost) {
        this.enableJoinReorderBasedCost = enableJoinReorderBasedCost;
    }

    public void setDisableJoinReorder(boolean disableJoinReorder) {
        this.disableJoinReorder = disableJoinReorder;
    }

    /**
     * Nereids only support vectorized engine.
     *
     * @return true if both nereids and vectorized engine are enabled
     */
    public boolean isEnableNereidsPlanner() {
        return enableNereidsPlanner && enableVectorizedEngine;
    }

    public void setEnableNereidsPlanner(boolean enableNereidsPlanner) {
        this.enableNereidsPlanner = enableNereidsPlanner;
    }

    public boolean isNereidsStarSchemaSupport() {
        return isEnableNereidsPlanner() && nereidsStarSchemaSupport;
    }

    public boolean isEnableNereidsTrace() {
        return isEnableNereidsPlanner() && enableNereidsTrace;
    }

    public boolean isEnableNereidsRuntimeFilter() {
        return enableNereidsRuntimeFilter;
    }

    public void setEnableNereidsRuntimeFilter(boolean enableNereidsRuntimeFilter) {
        this.enableNereidsRuntimeFilter = enableNereidsRuntimeFilter;
    }

    public boolean isEnableNereidsReorderToEliminateCrossJoin() {
        return enableNereidsReorderToEliminateCrossJoin;
    }

    public void setEnableNereidsReorderToEliminateCrossJoin(boolean value) {
        enableNereidsReorderToEliminateCrossJoin = value;
    }

    public boolean isEnableSingleReplicaInsert() {
        return enableSingleReplicaInsert;
    }

    public void setEnableSingleReplicaInsert(boolean enableSingleReplicaInsert) {
        this.enableSingleReplicaInsert = enableSingleReplicaInsert;
    }

    public boolean isEnableNereidsStatsDeriveV2() {
        return enableNereidsStatsDeriveV2;
    }

    public void setEnableNereidsStatsDeriveV2(boolean enableNereidsStatsDeriveV2) {
        this.enableNereidsStatsDeriveV2 = enableNereidsStatsDeriveV2;
    }

    /**
     * Serialize to thrift object.
     * Used for rest api.
     **/
    public boolean isEnableRemoveNoConjunctsRuntimeFilterPolicy() {
        return enableRemoveNoConjunctsRuntimeFilterPolicy;
    }

    public void setEnableRemoveNoConjunctsRuntimeFilterPolicy(boolean enableRemoveNoConjunctsRuntimeFilterPolicy) {
        this.enableRemoveNoConjunctsRuntimeFilterPolicy = enableRemoveNoConjunctsRuntimeFilterPolicy;
    }

    public void setFragmentTransmissionCompressionCodec(String codec) {
        this.fragmentTransmissionCompressionCodec = codec;
    }

    // Serialize to thrift object
    // used for rest api
    public TQueryOptions toThrift() {
        TQueryOptions tResult = new TQueryOptions();
        tResult.setMemLimit(maxExecMemByte);

        // TODO chenhao, reservation will be calculated by cost
        tResult.setMinReservation(0);
        tResult.setMaxReservation(maxExecMemByte);
        tResult.setInitialReservationTotalClaims(maxExecMemByte);
        tResult.setBufferPoolLimit(maxExecMemByte);

        tResult.setQueryTimeout(queryTimeoutS);
        tResult.setIsReportSuccess(enableProfile);
        tResult.setCodegenLevel(codegenLevel);
        tResult.setEnableVectorizedEngine(enableVectorizedEngine);
        tResult.setReturnObjectDataAsBinary(returnObjectDataAsBinary);
        tResult.setTrimTailingSpacesForExternalTableQuery(trimTailingSpacesForExternalTableQuery);

        tResult.setBatchSize(batchSize);
        tResult.setDisableStreamPreaggregations(disableStreamPreaggregations);

        if (maxScanKeyNum > -1) {
            tResult.setMaxScanKeyNum(maxScanKeyNum);
        }
        if (maxPushdownConditionsPerColumn > -1) {
            tResult.setMaxPushdownConditionsPerColumn(maxPushdownConditionsPerColumn);
        }

        tResult.setEnableSpilling(enableSpilling);
        tResult.setEnableEnableExchangeNodeParallelMerge(enableExchangeNodeParallelMerge);

        tResult.setRuntimeFilterWaitTimeMs(runtimeFilterWaitTimeMs);
        tResult.setRuntimeFilterMaxInNum(runtimeFilterMaxInNum);

        if (cpuResourceLimit > 0) {
            TResourceLimit resourceLimit = new TResourceLimit();
            resourceLimit.setCpuLimit(cpuResourceLimit);
            tResult.setResourceLimit(resourceLimit);
        }

        tResult.setEnableFunctionPushdown(enableFunctionPushdown);
        tResult.setFragmentTransmissionCompressionCodec(fragmentTransmissionCompressionCodec);
        tResult.setEnableLocalExchange(enableLocalExchange);
        tResult.setEnableNewShuffleHashMethod(enableNewShuffleHashMethod);

        tResult.setSkipStorageEngineMerge(skipStorageEngineMerge);

        tResult.setSkipDeletePredicate(skipDeletePredicate);

        return tResult;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JSONObject root = new JSONObject();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }
                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        root.put(attr.name(), (Boolean) field.get(this));
                        break;
                    case "int":
                        root.put(attr.name(), (Integer) field.get(this));
                        break;
                    case "long":
                        root.put(attr.name(), (Long) field.get(this));
                        break;
                    case "float":
                        root.put(attr.name(), (Float) field.get(this));
                        break;
                    case "double":
                        root.put(attr.name(), (Double) field.get(this));
                        break;
                    case "String":
                        root.put(attr.name(), (String) field.get(this));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
        Text.writeString(out, root.toString());
    }


    public void readFields(DataInput in) throws IOException {
        readFromJson(in);
    }

    private void readFromJson(DataInput in) throws IOException {
        String json = Text.readString(in);
        JSONObject root = (JSONObject) JSONValue.parse(json);
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }

                if (!root.containsKey(attr.name())) {
                    continue;
                }

                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        field.set(this, root.get(attr.name()));
                        break;
                    case "int":
                        // root.get(attr.name()) always return Long type, so need to convert it.
                        field.set(this, Integer.valueOf(root.get(attr.name()).toString()));
                        break;
                    case "long":
                        field.set(this, (Long) root.get(attr.name()));
                        break;
                    case "float":
                        field.set(this, root.get(attr.name()));
                        break;
                    case "double":
                        field.set(this, root.get(attr.name()));
                        break;
                    case "String":
                        field.set(this, root.get(attr.name()));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to read session variable: " + e.getMessage());
        }
    }

    /**
     * Get all variables which need to forward along with statement.
     **/
    public Map<String, String> getForwardVariables() {
        HashMap<String, String> map = new HashMap<String, String>();
        try {
            Field[] fields = SessionVariable.class.getDeclaredFields();
            for (Field f : fields) {
                VarAttr varAttr = f.getAnnotation(VarAttr.class);
                if (varAttr == null || !varAttr.needForward()) {
                    continue;
                }
                map.put(varAttr.name(), String.valueOf(f.get(this)));
            }
        } catch (IllegalAccessException e) {
            LOG.error("failed to get forward variables", e);
        }
        return map;
    }

    /**
     * Set forwardedSessionVariables for variables.
     **/
    public void setForwardedSessionVariables(Map<String, String> variables) {
        try {
            Field[] fields = SessionVariable.class.getFields();
            for (Field f : fields) {
                VarAttr varAttr = f.getAnnotation(VarAttr.class);
                if (varAttr == null || !varAttr.needForward()) {
                    continue;
                }
                String val = variables.get(varAttr.name());
                if (val == null) {
                    continue;
                }

                LOG.debug("set forward variable: {} = {}", varAttr.name(), val);

                // set config field
                switch (f.getType().getSimpleName()) {
                    case "short":
                        f.setShort(this, Short.parseShort(val));
                        break;
                    case "int":
                        f.setInt(this, Integer.parseInt(val));
                        break;
                    case "long":
                        f.setLong(this, Long.parseLong(val));
                        break;
                    case "double":
                        f.setDouble(this, Double.parseDouble(val));
                        break;
                    case "boolean":
                        f.setBoolean(this, Boolean.parseBoolean(val));
                        break;
                    case "String":
                        f.set(this, val);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown field type: " + f.getType().getSimpleName());
                }
            }
        } catch (IllegalAccessException e) {
            LOG.error("failed to set forward variables", e);
        }
    }

    /**
     * Set forwardedSessionVariables for queryOptions.
     **/
    public void setForwardedSessionVariables(TQueryOptions queryOptions) {
        if (queryOptions.isSetMemLimit()) {
            setMaxExecMemByte(queryOptions.getMemLimit());
        }
        if (queryOptions.isSetQueryTimeout()) {
            setQueryTimeoutS(queryOptions.getQueryTimeout());
        }
    }

    /**
     * Get all variables which need to be set in TQueryOptions.
     **/
    public TQueryOptions getQueryOptionVariables() {
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setMemLimit(maxExecMemByte);
        queryOptions.setQueryTimeout(queryTimeoutS);
        return queryOptions;
    }

    /**
     * The sessionContext is as follows:
     * "k1:v1;k2:v2;..."
     * Here we want to get value with key named "trace_id",
     * Return empty string is not found.
     *
     * @return
     */
    public String getTraceId() {
        if (Strings.isNullOrEmpty(sessionContext)) {
            return "";
        }
        String[] parts = sessionContext.split(";");
        for (String part : parts) {
            String[] innerParts = part.split(":");
            if (innerParts.length != 2) {
                continue;
            }
            if (innerParts[0].equals("trace_id")) {
                return innerParts[1];
            }
        }
        return "";
    }
}
