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

package org.apache.doris.common.profile;

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.SafeStringBuilder;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TUnit;
import org.apache.doris.transaction.TransactionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * SummaryProfile is part of a query profile.
 * It contains the summary information of a query.
 */
public class SummaryProfile {
    // Summary
    public static final String SUMMARY_PROFILE_NAME = "Summary";
    public static final String PROFILE_ID = "Profile ID";
    public static final String DORIS_VERSION = "Doris Version";
    public static final String TASK_TYPE = "Task Type";
    public static final String START_TIME = "Start Time";
    public static final String END_TIME = "End Time";
    public static final String TOTAL_TIME = "Total";
    public static final String TASK_STATE = "Task State";
    public static final String USER = "User";
    public static final String DEFAULT_CATALOG = "Default Catalog";
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";
    public static final String IS_NEREIDS = "Is Nereids";
    public static final String TOTAL_INSTANCES_NUM = "Total Instances Num";
    public static final String INSTANCES_NUM_PER_BE = "Instances Num Per BE";
    public static final String SCHEDULE_TIME_PER_BE = "Schedule Time Of BE";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE = "Parallel Fragment Exec Instance Num";
    public static final String TRACE_ID = "Trace ID";
    public static final String WORKLOAD_GROUP = "Workload Group";
    public static final String DISTRIBUTED_PLAN = "Distributed Plan";
    public static final String SYSTEM_MESSAGE = "System Message";
    public static final String EXECUTED_BY_FRONTEND = "Executed By Frontend";
    // Execution Summary
    public static final String EXECUTION_SUMMARY_PROFILE_NAME = "Execution Summary";
    public static final String INIT_SCAN_NODE_TIME = "Init Scan Node Time";
    public static final String FINALIZE_SCAN_NODE_TIME = "Finalize Scan Node Time";
    public static final String GET_SPLITS_TIME = "Get Splits Time";
    public static final String GET_PARTITIONS_TIME = "Get Partitions Time";
    public static final String GET_PARTITION_FILES_TIME = "Get Partition Files Time";
    public static final String CREATE_SCAN_RANGE_TIME = "Create Scan Range Time";
    public static final String SINK_SET_PARTITION_VALUES_TIME = "Sink Set Partition Values Time";
    public static final String PLAN_TIME = "Plan Time";
    public static final String SCHEDULE_TIME = "Schedule Time";
    public static final String ASSIGN_FRAGMENT_TIME = "Fragment Assign Time";
    public static final String FRAGMENT_SERIALIZE_TIME = "Fragment Serialize Time";
    public static final String SEND_FRAGMENT_PHASE1_TIME = "Fragment RPC Phase1 Time";
    public static final String SEND_FRAGMENT_PHASE2_TIME = "Fragment RPC Phase2 Time";
    public static final String WAIT_FETCH_RESULT_TIME = "Wait and Fetch Result Time";
    public static final String FETCH_RESULT_TIME = "Fetch Result Time";
    public static final String WRITE_RESULT_TIME = "Write Result Time";
    public static final String GET_META_VERSION_TIME = "Get Meta Version Time";
    public static final String GET_PARTITION_VERSION_TIME = "Get Partition Version Time";
    public static final String GET_PARTITION_VERSION_COUNT = "Get Partition Version Count";
    public static final String GET_PARTITION_VERSION_BY_HAS_DATA_COUNT = "Get Partition Version Count (hasData)";
    public static final String GET_TABLE_VERSION_TIME = "Get Table Version Time";
    public static final String GET_TABLE_VERSION_COUNT = "Get Table Version Count";
    public static final String MAX_CONCURRENCY = "Max Concurrency";
    public static final String MAX_QUEUE_SIZE = "Max Queue Size";
    public static final String QUEUE_TIMEOUT = "Queue Timeout";
    public static final String SCAN_THREAD_NUM = "Scan Thread Num";
    public static final String MAX_REMOTE_SCAN_THREAD_NUM = "Max Remote Scan Thread Num";
    public static final String MIN_REMOTE_SCAN_THREAD_NUM = "Min Remote Scan Thread Num";
    public static final String MEMORY_LOW_WATERMARK = "Memory Low Watermark";
    public static final String MEMORY_HIGH_WATERMARK = "Memory High Watermark";
    public static final String TAG = "Tag";
    public static final String READ_BYTES_PER_SECOND = "Read Bytes Per Second";
    public static final String REMOTE_READ_BYTES_PER_SECOND = "Remote Read Bytes Per Second";

    public static final String PARSE_SQL_TIME = "Parse SQL Time";
    public static final String NEREIDS_LOCK_TABLE_TIME = "Nereids Lock Table Time";
    public static final String NEREIDS_ANALYSIS_TIME = "Nereids Analysis Time";
    public static final String NEREIDS_REWRITE_TIME = "Nereids Rewrite Time";
    public static final String NEREIDS_COLLECT_TABLE_PARTITION_TIME = "Nereids Collect Table Partition Time";
    public static final String NEREIDS_PRE_REWRITE_BY_MV_TIME = "Nereids Pre Rewrite By Mv Time";
    public static final String NEREIDS_OPTIMIZE_TIME = "Nereids Optimize Time";
    public static final String NEREIDS_TRANSLATE_TIME = "Nereids Translate Time";
    public static final String NEREIDS_DISTRIBUTE_TIME = "Nereids Distribute Time";
    public static final String NEREIDS_GARBAGE_COLLECT_TIME = "Garbage Collect During Plan Time";
    public static final String NEREIDS_BE_FOLD_CONST_TIME = "Nereids Fold Const By BE Time";

    public static final String FRAGMENT_COMPRESSED_SIZE = "Fragment Compressed Size";
    public static final String FRAGMENT_RPC_COUNT = "Fragment RPC Count";
    public static final String TRANSACTION_COMMIT_TIME = "Transaction Commit Time";
    public static final String FILESYSTEM_OPT_TIME = "FileSystem Operator Time";
    public static final String FILESYSTEM_OPT_RENAME_FILE_CNT = "Rename File Count";
    public static final String FILESYSTEM_OPT_RENAME_DIR_CNT = "Rename Dir Count";

    public static final String FILESYSTEM_OPT_DELETE_FILE_CNT = "Delete File Count";
    public static final String FILESYSTEM_OPT_DELETE_DIR_CNT = "Delete Dir Count";
    public static final String HMS_ADD_PARTITION_TIME = "HMS Add Partition Time";
    public static final String HMS_ADD_PARTITION_CNT = "HMS Add Partition Count";
    public static final String HMS_UPDATE_PARTITION_TIME = "HMS Update Partition Time";
    public static final String HMS_UPDATE_PARTITION_CNT = "HMS Update Partition Count";
    public static final String LATENCY_FROM_FE_TO_BE = "RPC Latency From FE To BE";
    public static final String RPC_QUEUE_TIME = "RPC Work Queue Time";
    public static final String RPC_WORK_TIME = "RPC Work Time";
    public static final String LATENCY_FROM_BE_TO_FE = "RPC Latency From BE To FE";
    public static final String SPLITS_ASSIGNMENT_WEIGHT = "Splits Assignment Weight";
    public static final String ICEBERG_SCAN_METRICS = "Iceberg Scan Metrics";
    public static final String PAIMON_SCAN_METRICS = "Paimon Scan Metrics";
    private boolean isWarmUp = false;

    public void setWarmup(boolean isWarmUp) {
        this.isWarmUp = isWarmUp;
    }

    public boolean isWarmup() {
        return isWarmUp;
    }
    // These info will display on FE's web ui table, every one will be displayed as
    // a column, so that should not
    // add many columns here. Add to ExecutionSummary list.

    public static final ImmutableList<String> SUMMARY_CAPTIONS = ImmutableList.of(PROFILE_ID, TASK_TYPE,
            START_TIME, END_TIME, TOTAL_TIME, TASK_STATE, USER, DEFAULT_CATALOG, DEFAULT_DB, SQL_STATEMENT);
    public static final ImmutableList<String> SUMMARY_KEYS = new ImmutableList.Builder<String>()
            .addAll(SUMMARY_CAPTIONS)
            .add(DISTRIBUTED_PLAN)
            .build();

    // The display order of execution summary items.
    public static final ImmutableList<String> EXECUTION_SUMMARY_KEYS = ImmutableList.of(
            WORKLOAD_GROUP,
            MAX_CONCURRENCY,
            MAX_QUEUE_SIZE,
            SCAN_THREAD_NUM,
            MAX_REMOTE_SCAN_THREAD_NUM,
            MIN_REMOTE_SCAN_THREAD_NUM,
            MEMORY_LOW_WATERMARK,
            MEMORY_HIGH_WATERMARK,
            TAG,
            READ_BYTES_PER_SECOND,
            REMOTE_READ_BYTES_PER_SECOND,
            PARSE_SQL_TIME,
            PLAN_TIME,
            NEREIDS_GARBAGE_COLLECT_TIME,
            NEREIDS_LOCK_TABLE_TIME,
            NEREIDS_ANALYSIS_TIME,
            NEREIDS_REWRITE_TIME,
            NEREIDS_BE_FOLD_CONST_TIME,
            NEREIDS_COLLECT_TABLE_PARTITION_TIME,
            NEREIDS_PRE_REWRITE_BY_MV_TIME,
            NEREIDS_OPTIMIZE_TIME,
            NEREIDS_TRANSLATE_TIME,
            INIT_SCAN_NODE_TIME,
            FINALIZE_SCAN_NODE_TIME,
            GET_SPLITS_TIME,
            GET_PARTITIONS_TIME,
            GET_PARTITION_FILES_TIME,
            SINK_SET_PARTITION_VALUES_TIME,
            CREATE_SCAN_RANGE_TIME,
            ICEBERG_SCAN_METRICS,
            PAIMON_SCAN_METRICS,
            NEREIDS_DISTRIBUTE_TIME,
            GET_META_VERSION_TIME,
            GET_PARTITION_VERSION_TIME,
            GET_PARTITION_VERSION_BY_HAS_DATA_COUNT,
            GET_PARTITION_VERSION_COUNT,
            GET_TABLE_VERSION_TIME,
            GET_TABLE_VERSION_COUNT,
            SCHEDULE_TIME,
            ASSIGN_FRAGMENT_TIME,
            FRAGMENT_SERIALIZE_TIME,
            SEND_FRAGMENT_PHASE1_TIME,
            SEND_FRAGMENT_PHASE2_TIME,
            FRAGMENT_COMPRESSED_SIZE,
            FRAGMENT_RPC_COUNT,
            SCHEDULE_TIME_PER_BE,
            WAIT_FETCH_RESULT_TIME,
            FETCH_RESULT_TIME,
            WRITE_RESULT_TIME,
            DORIS_VERSION,
            IS_NEREIDS,
            IS_CACHED,
            TOTAL_INSTANCES_NUM,
            INSTANCES_NUM_PER_BE,
            PARALLEL_FRAGMENT_EXEC_INSTANCE,
            TRACE_ID,
            TRANSACTION_COMMIT_TIME,
            SYSTEM_MESSAGE,
            EXECUTED_BY_FRONTEND,
            SPLITS_ASSIGNMENT_WEIGHT
    );

    // Ident of each item. Default is 0, which doesn't need to present in this Map.
    // Please set this map for new profile items if they need ident.
    public static ImmutableMap<String, Integer> EXECUTION_SUMMARY_KEYS_INDENTATION
            = ImmutableMap.<String, Integer>builder()
            .put(NEREIDS_GARBAGE_COLLECT_TIME, 1)
            .put(NEREIDS_LOCK_TABLE_TIME, 1)
            .put(NEREIDS_ANALYSIS_TIME, 1)
            .put(NEREIDS_REWRITE_TIME, 1)
            .put(NEREIDS_COLLECT_TABLE_PARTITION_TIME, 1)
            .put(NEREIDS_OPTIMIZE_TIME, 1)
            .put(NEREIDS_TRANSLATE_TIME, 1)
            .put(INIT_SCAN_NODE_TIME, 2)
            .put(FINALIZE_SCAN_NODE_TIME, 2)
            .put(GET_SPLITS_TIME, 3)
            .put(NEREIDS_DISTRIBUTE_TIME, 1)
            .put(NEREIDS_BE_FOLD_CONST_TIME, 2)
            .put(GET_PARTITIONS_TIME, 3)
            .put(GET_PARTITION_FILES_TIME, 3)
            .put(SINK_SET_PARTITION_VALUES_TIME, 3)
            .put(CREATE_SCAN_RANGE_TIME, 2)
            .put(ICEBERG_SCAN_METRICS, 3)
            .put(PAIMON_SCAN_METRICS, 3)
            .put(GET_PARTITION_VERSION_TIME, 1)
            .put(GET_PARTITION_VERSION_COUNT, 1)
            .put(GET_PARTITION_VERSION_BY_HAS_DATA_COUNT, 1)
            .put(GET_TABLE_VERSION_TIME, 1)
            .put(GET_TABLE_VERSION_COUNT, 1)
            .put(ASSIGN_FRAGMENT_TIME, 1)
            .put(FRAGMENT_SERIALIZE_TIME, 1)
            .put(SEND_FRAGMENT_PHASE1_TIME, 1)
            .put(SEND_FRAGMENT_PHASE2_TIME, 1)
            .put(FRAGMENT_COMPRESSED_SIZE, 1)
            .put(FRAGMENT_RPC_COUNT, 1)
            .put(FILESYSTEM_OPT_TIME, 1)
            .put(FILESYSTEM_OPT_RENAME_FILE_CNT, 2)
            .put(FILESYSTEM_OPT_RENAME_DIR_CNT, 2)
            .put(FILESYSTEM_OPT_DELETE_FILE_CNT, 2)
            .put(FILESYSTEM_OPT_DELETE_DIR_CNT, 2)
            .put(HMS_ADD_PARTITION_TIME, 1)
            .put(HMS_ADD_PARTITION_CNT, 2)
            .put(HMS_UPDATE_PARTITION_TIME, 1)
            .put(HMS_UPDATE_PARTITION_CNT, 2)
            .put(MAX_QUEUE_SIZE, 1)
            .put(QUEUE_TIMEOUT, 1)
            .put(MAX_CONCURRENCY, 1)
            .put(MAX_REMOTE_SCAN_THREAD_NUM, 1)
            .put(SCAN_THREAD_NUM, 1)
            .put(MIN_REMOTE_SCAN_THREAD_NUM, 1)
            .put(MEMORY_LOW_WATERMARK, 1)
            .put(MEMORY_HIGH_WATERMARK, 1)
            .put(REMOTE_READ_BYTES_PER_SECOND, 1)
            .put(READ_BYTES_PER_SECOND, 1)
            .put(TAG, 1)
            .build();
    public boolean parsedByConnectionProcess = false;
    @SerializedName(value = "summaryProfile")
    private RuntimeProfile summaryProfile = new RuntimeProfile(SUMMARY_PROFILE_NAME);
    @SerializedName(value = "executionSummaryProfile")
    private RuntimeProfile executionSummaryProfile = new RuntimeProfile(EXECUTION_SUMMARY_PROFILE_NAME);

    // New QueryTrace for dynamic span/counter/text tracking.
    // This will progressively replace the legacy timestamp fields below.
    private final QueryTrace queryTrace = new QueryTrace();
    // Legacy timestamp fields kept for backward compatibility during deserialization.
    // New code records via QueryTrace; these are only read as fallbacks in getters.
    @SerializedName(value = "parseSqlStartTime")
    private long parseSqlStartTime = -1;
    @SerializedName(value = "parseSqlFinishTime")
    private long parseSqlFinishTime = -1;
    // timestamp of query begin
    @SerializedName(value = "queryBeginTime")
    private long queryBeginTime = -1;

    // Plan end time
    @SerializedName(value = "queryPlanFinishTime")
    private long queryPlanFinishTime = -1;
    @SerializedName(value = "assignFragmentTime")
    private long assignFragmentTime = -1;
    @SerializedName(value = "fragmentSerializeTime")
    private long fragmentSerializeTime = -1;
    @SerializedName(value = "fragmentSendPhase1Time")
    private long fragmentSendPhase1Time = -1;
    @SerializedName(value = "fragmentSendPhase2Time")
    private long fragmentSendPhase2Time = -1;
    @SerializedName(value = "fragmentCompressedSize")
    private long fragmentCompressedSize = 0;
    @SerializedName(value = "fragmentRpcCount")
    private long fragmentRpcCount = 0;
    // Fragment schedule and send end time
    @SerializedName(value = "queryScheduleFinishTime")
    private long queryScheduleFinishTime = -1;
    // Query result fetch end time
    @SerializedName(value = "queryFetchResultFinishTime")
    private long queryFetchResultFinishTime = -1;
    @SerializedName(value = "tempStarTime")
    private long tempStarTime = -1;
    @SerializedName(value = "queryFetchResultConsumeTime")
    private long queryFetchResultConsumeTime = 0;
    @SerializedName(value = "queryWriteResultConsumeTime")
    private long queryWriteResultConsumeTime = 0;
    @SerializedName(value = "getPartitionVersionTime")
    private long getPartitionVersionTime = 0;
    @SerializedName(value = "getPartitionVersionCount")
    private long getPartitionVersionCount = 0;
    @SerializedName(value = "getPartitionVersionByHasDataCount")
    private long getPartitionVersionByHasDataCount = 0;
    @SerializedName(value = "getTableVersionTime")
    private long getTableVersionTime = 0;
    @SerializedName(value = "getTableVersionCount")
    private long getTableVersionCount = 0;
    @SerializedName(value = "transactionCommitBeginTime")
    private long transactionCommitBeginTime = -1;
    @SerializedName(value = "transactionCommitEndTime")
    private long transactionCommitEndTime = -1;
    @SerializedName(value = "transactionType")
    private TransactionType transactionType = TransactionType.UNKNOWN;
    @SerializedName("maxConcurrency")
    private int maxConcurrency = 0;
    @SerializedName("maxQueueSize")
    private int maxQueueSize = 0;
    @SerializedName("queueTimeout")
    private int queueTimeout = 0;
    @SerializedName("scanThreadNum")
    private int scanThreadNum = -1;
    @SerializedName("maxRemoteScanThreadNum")
    private int maxRemoteScanThreadNum = -1;
    @SerializedName("minRemoteScanThreadNum")
    private int minRemoteScanThreadNum = -1;
    @SerializedName("memoryLowWatermark")
    private int memoryLowWatermark = 0;
    @SerializedName("memoryHighWatermark")
    private int memoryHighWatermark = 0;
    @SerializedName("tag")
    private String tag = "";
    @SerializedName("readBytesPerSecond")
    private long readBytesPerSecond = -1L;
    @SerializedName("remoteReadBytesPerSecond")
    private long remoteReadBytesPerSecond = -1L;
    // BE -> (RPC latency from FE to BE, Execution latency on bthread, Duration of doing work, RPC latency from BE
    // to FE)
    private Map<TNetworkAddress, List<Long>> rpcPhase1Latency;
    private Map<TNetworkAddress, List<Long>> rpcPhase2Latency;
    private Map<Backend, Long> assignedWeightPerBackend;

    public SummaryProfile() {
        init();
    }

    public static SummaryProfile read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), SummaryProfile.class);
    }

    public static SummaryProfile getSummaryProfile(ConnectContext connectContext) {
        ConnectContext ctx = connectContext == null ? ConnectContext.get() : connectContext;
        if (ctx != null) {
            StmtExecutor executor = ctx.getExecutor();
            if (executor != null) {
                return executor.getSummaryProfile();
            }
        }
        return null;
    }

    private void init() {
        for (String key : SUMMARY_KEYS) {
            summaryProfile.addInfoString(key, "N/A");
        }
        for (String key : EXECUTION_SUMMARY_KEYS) {
            executionSummaryProfile.addInfoString(key, "N/A");
        }
    }

    // For UT usage
    public void fuzzyInit() {
        for (String key : SUMMARY_KEYS) {
            String randomId = String.valueOf(TimeUtils.getStartTimeMs());
            summaryProfile.addInfoString(key, randomId);
        }
        for (String key : EXECUTION_SUMMARY_KEYS) {
            String randomId = String.valueOf(TimeUtils.getStartTimeMs());
            executionSummaryProfile.addInfoString(key, randomId);
        }
    }

    public String getProfileId() {
        return this.summaryProfile.getInfoString(PROFILE_ID);
    }

    public RuntimeProfile getSummary() {
        return summaryProfile;
    }

    public RuntimeProfile getExecutionSummary() {
        return executionSummaryProfile;
    }

    /**
     * Get the QueryTrace for dynamic span/counter/text tracking.
     * Callers should use this to record timing and metrics via the new Span-based API.
     */
    public QueryTrace getQueryTrace() {
        return queryTrace;
    }

    public void prettyPrint(SafeStringBuilder builder) {
        summaryProfile.prettyPrint(builder, "");
        executionSummaryProfile.prettyPrint(builder, "");
    }

    public Map<String, String> getAsInfoStings() {
        Map<String, String> infoStrings = Maps.newHashMap();
        for (String header : SummaryProfile.SUMMARY_CAPTIONS) {
            infoStrings.put(header, summaryProfile.getInfoString(header));
        }
        return infoStrings;
    }

    public void update(Map<String, String> summaryInfo) {
        updateSummaryProfile(summaryInfo);
        updateExecutionSummaryProfile();
    }

    // This method is used to display the final data status when the overall query ends.
    // This can avoid recalculating some strings and so on every time during the update process.
    public void queryFinished() {
        if (assignedWeightPerBackend != null) {
            Map<String, Long> m = assignedWeightPerBackend.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .collect(Collectors.toMap(
                            entry -> entry.getKey().getAddress(),
                            Entry::getValue,
                            (v1, v2) -> v1,
                            LinkedHashMap::new
                    ));
            executionSummaryProfile.addInfoString(
                    SPLITS_ASSIGNMENT_WEIGHT,
                    new GsonBuilder().create().toJson(m));
        }
    }

    private void updateSummaryProfile(Map<String, String> infos) {
        for (String key : infos.keySet()) {
            if (SUMMARY_KEYS.contains(key)) {
                summaryProfile.addInfoString(key, infos.get(key));
            } else if (EXECUTION_SUMMARY_KEYS.contains(key)) {
                // Some static value is build in summary profile, should add
                // them to execution summary profile during update.
                executionSummaryProfile.addInfoString(key, infos.get(key));
            }
        }
    }

    private void updateExecutionSummaryProfile() {
        // All timing metrics (parse, plan, scan-node, schedule, fetch, write) are now
        // populated by queryTrace.populateProfile() which is called separately.
        // Only the metrics NOT YET migrated to QueryTrace are emitted here.

        // --- Metrics NOT YET migrated to QueryTrace (always emitted) ---
        executionSummaryProfile.addInfoString(SCHEDULE_TIME_PER_BE, getRpcLatency());
        executionSummaryProfile.addInfoString(ASSIGN_FRAGMENT_TIME,
                getPrettyTime(assignFragmentTime, queryPlanFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(FRAGMENT_SERIALIZE_TIME,
                getPrettyTime(fragmentSerializeTime, assignFragmentTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(SEND_FRAGMENT_PHASE1_TIME,
                getPrettyTime(fragmentSendPhase1Time, fragmentSerializeTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(SEND_FRAGMENT_PHASE2_TIME,
                getPrettyTime(fragmentSendPhase2Time, fragmentSendPhase1Time, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(FRAGMENT_COMPRESSED_SIZE,
                RuntimeProfile.printCounter(fragmentCompressedSize, TUnit.BYTES));
        executionSummaryProfile.addInfoString(FRAGMENT_RPC_COUNT, "" + fragmentRpcCount);
        executionSummaryProfile.addInfoString(MAX_CONCURRENCY, RuntimeProfile.printCounter(maxConcurrency, TUnit.UNIT));
        executionSummaryProfile.addInfoString(QUEUE_TIMEOUT, RuntimeProfile.printCounter(queueTimeout, TUnit.UNIT));
        executionSummaryProfile.addInfoString(MAX_QUEUE_SIZE, RuntimeProfile.printCounter(maxQueueSize, TUnit.UNIT));
        executionSummaryProfile.addInfoString(MEMORY_HIGH_WATERMARK,
                RuntimeProfile.printCounter(memoryHighWatermark, TUnit.UNIT));
        executionSummaryProfile.addInfoString(SCAN_THREAD_NUM, RuntimeProfile.printCounter(scanThreadNum, TUnit.UNIT));
        executionSummaryProfile.addInfoString(MAX_REMOTE_SCAN_THREAD_NUM,
                RuntimeProfile.printCounter(maxRemoteScanThreadNum, TUnit.UNIT));
        executionSummaryProfile.addInfoString(MIN_REMOTE_SCAN_THREAD_NUM,
                RuntimeProfile.printCounter(minRemoteScanThreadNum, TUnit.UNIT));
        executionSummaryProfile.addInfoString(MEMORY_LOW_WATERMARK,
                RuntimeProfile.printCounter(memoryLowWatermark, TUnit.UNIT));
        executionSummaryProfile.addInfoString(READ_BYTES_PER_SECOND,
                RuntimeProfile.printCounter(readBytesPerSecond, TUnit.BYTES_PER_SECOND));
        executionSummaryProfile.addInfoString(REMOTE_READ_BYTES_PER_SECOND,
                RuntimeProfile.printCounter(remoteReadBytesPerSecond, TUnit.BYTES_PER_SECOND));
        setTransactionSummary();

        if (Config.isCloudMode()) {
            executionSummaryProfile.addInfoString(GET_META_VERSION_TIME, getPrettyGetMetaVersionTime());
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_TIME, getPrettyGetPartitionVersionTime());
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_COUNT, getPrettyGetPartitionVersionCount());
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_BY_HAS_DATA_COUNT,
                    getPrettyGetPartitionVersionByHasDataCount());
            executionSummaryProfile.addInfoString(GET_TABLE_VERSION_TIME, getPrettyGetTableVersionTime());
            executionSummaryProfile.addInfoString(GET_TABLE_VERSION_COUNT, getPrettyGetTableVersionCount());
        }

        // Populate dynamically registered metrics from QueryTrace
        if (queryTrace != null) {
            queryTrace.populateProfile(executionSummaryProfile);
        }
    }

    public void setTransactionSummary() {
        executionSummaryProfile.addInfoString(TRANSACTION_COMMIT_TIME,
                getPrettyTime(transactionCommitEndTime, transactionCommitBeginTime, TUnit.TIME_MS));
        // HMS filesystem/partition metrics are now emitted via QueryTrace.populateProfile()
    }

    public void setParseSqlStartTime(long parseSqlStartTime) {
        this.parseSqlStartTime = parseSqlStartTime;
    }

    public void setParseSqlFinishTime(long parseSqlFinishTime) {
        this.parseSqlFinishTime = parseSqlFinishTime;
    }

    // Legacy Nereids timing setters and data source scan node setters have been removed.
    // All timing is now recorded via QueryTrace.startSpan() or QueryTrace.recordDuration().

    public void setQueryPlanFinishTime(long planFinishTime) {
        if (queryPlanFinishTime == -1) {
            this.queryPlanFinishTime = planFinishTime;
        }
    }

    public void setQueryScheduleFinishTime(long scheduleFinishTime) {
        this.queryScheduleFinishTime = scheduleFinishTime;
    }

    public void setQueryFetchResultFinishTime(long fetchResultFinishTime) {
        this.queryFetchResultFinishTime = fetchResultFinishTime;
    }

    public void setTempStartTime() {
        this.tempStarTime = TimeUtils.getStartTimeMs();
    }

    public void freshFetchResultConsumeTime() {
        this.queryFetchResultConsumeTime += TimeUtils.getStartTimeMs() - tempStarTime;
    }

    public void freshWriteResultConsumeTime() {
        this.queryWriteResultConsumeTime += TimeUtils.getStartTimeMs() - tempStarTime;
    }

    public void setAssignFragmentTime() {
        this.assignFragmentTime = TimeUtils.getStartTimeMs();
    }

    public void setFragmentSerializeTime() {
        this.fragmentSerializeTime = TimeUtils.getStartTimeMs();
    }

    public void setFragmentSendPhase1Time() {
        this.fragmentSendPhase1Time = TimeUtils.getStartTimeMs();
    }

    public void setFragmentSendPhase2Time() {
        this.fragmentSendPhase2Time = TimeUtils.getStartTimeMs();
    }

    public void updateFragmentCompressedSize(long size) {
        this.fragmentCompressedSize += size;
    }

    public void updateFragmentRpcCount(long count) {
        this.fragmentRpcCount += count;
    }

    public void addGetPartitionVersionTime(long ns) {
        this.getPartitionVersionTime += ns;
        this.getPartitionVersionCount += 1;
    }

    public void addGetTableVersionTime(long ns) {
        this.getTableVersionTime += ns;
        this.getTableVersionCount += 1;
    }

    public void incGetPartitionVersionByHasDataCount() {
        this.getPartitionVersionByHasDataCount += 1;
    }

    public long getQueryBeginTime() {
        return queryBeginTime;
    }

    public void setQueryBeginTime(long queryBeginTime) {
        this.queryBeginTime = queryBeginTime;
    }

    public void setRpcPhase1Latency(Map<TNetworkAddress, List<Long>> rpcPhase1Latency) {
        this.rpcPhase1Latency = rpcPhase1Latency;
    }

    public void setRpcPhase2Latency(Map<TNetworkAddress, List<Long>> rpcPhase2Latency) {
        this.rpcPhase2Latency = rpcPhase2Latency;
    }

    public int getParseSqlTimeMs() {
        return (int) getTraceDurationMs("Parse SQL Time",
                getTimeMs(parseSqlFinishTime, parseSqlStartTime));
    }

    public int getPlanTimeMs() {
        return (int) getTraceDurationMs("Plan Time",
                getTimeMs(queryPlanFinishTime, parseSqlFinishTime));
    }

    /**
     * Read a duration from QueryTrace if available; otherwise fall back to the legacy value.
     * This bridges the transition period where some metrics are produced by QueryTrace
     * and some are still computed from legacy timestamp pairs.
     */
    private long getTraceDurationMs(String spanName, long legacyValueMs) {
        if (queryTrace != null) {
            long traceDuration = queryTrace.getDurationMs(spanName);
            if (traceDuration >= 0) {
                return traceDuration;
            }
        }
        return legacyValueMs;
    }

    public int getNereidsLockTableTimeMs() {
        return (int) getTraceDurationMs("Nereids Lock Table Time", -1);
    }

    public int getNereidsAnalysisTimeMs() {
        return (int) getTraceDurationMs("Nereids Analysis Time", -1);
    }

    public int getNereidsRewriteTimeMs() {
        return (int) getTraceDurationMs("Nereids Rewrite Time", -1);
    }

    public int getNereidsCollectTablePartitionTimeMs() {
        return (int) getTraceDurationMs("Nereids Collect Table Partition Time", -1);
    }

    public int getNereidsOptimizeTimeMs() {
        return (int) getTraceDurationMs("Nereids Optimize Time", -1);
    }

    public int getNereidsTranslateTimeMs() {
        return (int) getTraceDurationMs("Nereids Translate Time", -1);
    }

    public int getNereidsGarbageCollectionTimeMs() {
        return (int) getTraceDurationMs("Garbage Collect During Plan Time", -1);
    }

    public int getNereidsBeFoldConstTimeMs() {
        return (int) getTraceDurationMs(NEREIDS_BE_FOLD_CONST_TIME, -1);
    }

    public int getNereidsDistributeTimeMs() {
        return (int) getTraceDurationMs("Nereids Distribute Time", -1);
    }

    public int getInitScanNodeTimeMs() {
        return (int) getTraceDurationMs("Init Scan Node Time", -1);
    }

    public int getFinalizeScanNodeTimeMs() {
        return (int) getTraceDurationMs("Finalize Scan Node Time", -1);
    }

    public int getCreateScanRangeTimeMs() {
        return (int) getTraceDurationMs("Create Scan Range Time", -1);
    }

    public int getScheduleTimeMs() {
        return (int) getTraceDurationMs("Schedule Time",
                getTimeMs(queryScheduleFinishTime, queryPlanFinishTime));
    }

    public int getFragmentAssignTimsMs() {
        return getTimeMs(assignFragmentTime, queryPlanFinishTime);
    }

    public int getFragmentSerializeTimeMs() {
        return getTimeMs(fragmentSerializeTime, assignFragmentTime);
    }

    public int getFragmentRPCPhase1TimeMs() {
        return getTimeMs(fragmentSendPhase1Time, fragmentSerializeTime);
    }

    public int getFragmentRPCPhase2TimeMs() {
        return getTimeMs(fragmentSendPhase2Time, fragmentSendPhase1Time);
    }

    public double getFragmentCompressedSizeByte() {
        return fragmentCompressedSize;
    }

    public long getFragmentRPCCount() {
        return fragmentRpcCount;
    }

    public int getTimeMs(long end, long start) {
        if (end == -1 || start == -1) {
            return -1;
        } else {
            return (int) (end - start);
        }
    }

    public String getPrettyParseSqlTime() {
        return getPrettyTime(parseSqlFinishTime, parseSqlStartTime, TUnit.TIME_MS);
    }

    public String getPrettyNereidsLockTableTime() {
        return RuntimeProfile.printCounter(getNereidsLockTableTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsAnalysisTime() {
        return RuntimeProfile.printCounter(getNereidsAnalysisTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsRewriteTime() {
        return RuntimeProfile.printCounter(getNereidsRewriteTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsCollectTablePartitionTime() {
        return RuntimeProfile.printCounter(getNereidsCollectTablePartitionTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsPreRewriteByMvTime() {
        return RuntimeProfile.printCounter(
                getTraceDurationMs(NEREIDS_PRE_REWRITE_BY_MV_TIME, -1), TUnit.TIME_MS);
    }

    public String getPrettyNereidsOptimizeTime() {
        return RuntimeProfile.printCounter(getNereidsOptimizeTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsTranslateTime() {
        return RuntimeProfile.printCounter(getNereidsTranslateTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsGarbageCollectionTime() {
        return RuntimeProfile.printCounter(getNereidsGarbageCollectionTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsBeFoldConstTime() {
        return RuntimeProfile.printCounter(getNereidsBeFoldConstTimeMs(), TUnit.TIME_MS);
    }

    public String getPrettyNereidsDistributeTime() {
        return RuntimeProfile.printCounter(getNereidsDistributeTimeMs(), TUnit.TIME_MS);
    }

    private String getPrettyGetMetaVersionTime() {
        long getMetaVersionTime = getPartitionVersionTime + getTableVersionTime;
        return RuntimeProfile.printCounter(getMetaVersionTime, TUnit.TIME_NS);
    }

    private String getPrettyGetPartitionVersionTime() {
        if (getPartitionVersionTime == 0) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(getPartitionVersionTime, TUnit.TIME_NS);
    }

    private String getPrettyGetPartitionVersionByHasDataCount() {
        return RuntimeProfile.printCounter(getPartitionVersionByHasDataCount, TUnit.UNIT);
    }

    private String getPrettyGetPartitionVersionCount() {
        return RuntimeProfile.printCounter(getPartitionVersionCount, TUnit.UNIT);
    }

    private String getPrettyCount(long cnt) {
        return RuntimeProfile.printCounter(cnt, TUnit.UNIT);
    }

    private String getPrettyGetTableVersionTime() {
        if (getTableVersionTime == 0) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(getTableVersionTime, TUnit.TIME_NS);
    }

    private String getPrettyGetTableVersionCount() {
        return RuntimeProfile.printCounter(getTableVersionCount, TUnit.UNIT);
    }

    public long getGetPartitionVersionTime() {
        return getPartitionVersionTime;
    }

    public long getGetPartitionVersionCount() {
        return getPartitionVersionCount;
    }

    public long getGetPartitionVersionByHasDataCount() {
        return getPartitionVersionByHasDataCount;
    }

    public long getGetTableVersionTime() {
        return getTableVersionTime;
    }

    public long getGetTableVersionCount() {
        return getTableVersionCount;
    }

    private String getPrettyTime(long end, long start, TUnit unit) {
        if (start == -1 || end == -1) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(end - start, unit);
    }

    public void setTransactionBeginTime(TransactionType type) {
        this.transactionCommitBeginTime = TimeUtils.getStartTimeMs();
        this.transactionType = type;
    }

    public void setTransactionEndTime() {
        this.transactionCommitEndTime = TimeUtils.getStartTimeMs();
    }

    // Legacy freshFilesystemOptTime, setHmsAddPartitionTime, etc. have been removed.
    // All HMS/filesystem metrics are now recorded via QueryTrace in HMSTransaction.

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public void setMaxConcurrency(int maxConcurrency) {
        this.maxConcurrency = maxConcurrency;
    }

    public void setScanThreadNum(int scanThreadNum) {
        this.scanThreadNum = scanThreadNum;
    }

    public void setMaxRemoteScanThreadNum(int maxRemoteScanThreadNum) {
        this.maxRemoteScanThreadNum = maxRemoteScanThreadNum;
    }

    public void setMinRemoteScanThreadNum(int minRemoteScanThreadNum) {
        this.minRemoteScanThreadNum = minRemoteScanThreadNum;
    }

    public void setMemoryLowWatermark(int memoryLowWatermark) {
        this.memoryLowWatermark = memoryLowWatermark;
    }

    public void setMemoryHighWatermark(int memoryHighWatermark) {
        this.memoryHighWatermark = memoryHighWatermark;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void setReadBytesPerSecond(int readBytesPerSecond) {
        this.readBytesPerSecond = readBytesPerSecond;
    }

    public void setRemoteReadBytesPerSecond(int remoteReadBytesPerSecond) {
        this.remoteReadBytesPerSecond = remoteReadBytesPerSecond;
    }

    public void setQueueTimeout(int queueTimeout) {
        this.queueTimeout = queueTimeout;
    }

    private String getRpcLatency() {
        Map<String, Map<String, Map<String, String>>> jsonObject = new HashMap<>();
        if (rpcPhase1Latency != null) {
            Map<String, Map<String, String>> latencyForPhase1 = new HashMap<>();
            for (TNetworkAddress key : rpcPhase1Latency.keySet()) {
                Preconditions.checkState(rpcPhase1Latency.get(key).size() == 4, "rpc latency should have 4 elements");
                Map<String, String> latency = new HashMap<>();
                latency.put(LATENCY_FROM_FE_TO_BE, RuntimeProfile.printCounter(rpcPhase1Latency.get(key).get(0),
                        TUnit.TIME_MS));
                latency.put(RPC_QUEUE_TIME, RuntimeProfile.printCounter(rpcPhase1Latency.get(key).get(1),
                        TUnit.TIME_MS));
                latency.put(RPC_WORK_TIME, RuntimeProfile.printCounter(rpcPhase1Latency.get(key).get(2),
                        TUnit.TIME_MS));
                latency.put(LATENCY_FROM_BE_TO_FE, RuntimeProfile.printCounter(rpcPhase1Latency.get(key).get(3),
                        TUnit.TIME_MS));
                latencyForPhase1.put(key.getHostname() + ": " + key.getPort(), latency);
            }
            jsonObject.put("phase1", latencyForPhase1);
        }
        if (rpcPhase2Latency != null) {
            Map<String, Map<String, String>> latencyForPhase2 = new HashMap<>();
            for (TNetworkAddress key : rpcPhase2Latency.keySet()) {
                Preconditions.checkState(rpcPhase2Latency.get(key).size() == 4, "rpc latency should have 4 elements");
                Map<String, String> latency = new HashMap<>();
                latency.put(LATENCY_FROM_FE_TO_BE, RuntimeProfile.printCounter(rpcPhase2Latency.get(key).get(0),
                        TUnit.TIME_MS));
                latency.put(RPC_QUEUE_TIME, RuntimeProfile.printCounter(rpcPhase2Latency.get(key).get(1),
                        TUnit.TIME_MS));
                latency.put(RPC_WORK_TIME, RuntimeProfile.printCounter(rpcPhase2Latency.get(key).get(2),
                        TUnit.TIME_MS));
                latency.put(LATENCY_FROM_BE_TO_FE, RuntimeProfile.printCounter(rpcPhase2Latency.get(key).get(3),
                        TUnit.TIME_MS));
                latencyForPhase2.put(key.getHostname() + ": " + key.getPort(), latency);
            }
            jsonObject.put("phase2", latencyForPhase2);
        }
        return new Gson().toJson(jsonObject);
    }

    public void setSystemMessage(String msg) {
        executionSummaryProfile.addInfoString(SYSTEM_MESSAGE, msg);
    }

    public void setExecutedByFrontend(boolean executedByFrontend) {
        executionSummaryProfile.addInfoString(EXECUTED_BY_FRONTEND, String.valueOf(executedByFrontend));
    }

    // Legacy addNereidsMvRewriteTime, addExternalCatalogMetaTime etc. have been removed.
    // All cumulative timings are now recorded via QueryTrace.recordDuration().

    public long getNereidsMvRewriteTimeMs() {
        return getTraceDurationMs("Nereids MV Rewrite Time", -1);
    }

    public long getCloudMetaTimeMs() {
        return TimeUnit.NANOSECONDS.toMillis(getPartitionVersionTime + getTableVersionTime);
    }

    public long getExternalCatalogMetaTimeMs() {
        return getTraceDurationMs("External Catalog Meta Time", -1);
    }

    public long getExternalTvfInitTimeMs() {
        return getTraceDurationMs("External TVF Init Time", -1);
    }

    public long getNereidsPartitiionPruneTimeMs() {
        return getTraceDurationMs("Nereids Partition Prune Time", -1);
    }

    public void write(DataOutput output) throws IOException {
        Text.writeString(output, GsonUtils.GSON.toJson(this));
    }

    public void setAssignedWeightPerBackend(Map<Backend, Long> assignedWeightPerBackend) {
        this.assignedWeightPerBackend = assignedWeightPerBackend;
    }

    public String getPlanTime() {
        String planTimesMs = "{"
                + "\"plan\"" + ":" + this.getPlanTimeMs() + ","
                + "\"garbage_collect\"" + ":" + this.getNereidsGarbageCollectionTimeMs() + ","
                + "\"lock_tables\"" + ":" + this.getNereidsLockTableTimeMs() + ","
                + "\"analyze\"" + ":" + this.getNereidsAnalysisTimeMs() + ","
                + "\"rewrite\"" + ":" + this.getNereidsRewriteTimeMs() + ","
                + "\"fold_const_by_be\"" + ":" + this.getNereidsBeFoldConstTimeMs() + ","
                + "\"collect_partitions\"" + ":" + this.getNereidsCollectTablePartitionTimeMs() + ","
                + "\"optimize\"" + ":" + this.getNereidsOptimizeTimeMs() + ","
                + "\"translate\"" + ":" + this.getNereidsTranslateTimeMs() + ","
                + "\"init_scan_node\"" + ":" + this.getInitScanNodeTimeMs() + ","
                + "\"finalize_scan_node\"" + ":" + this.getFinalizeScanNodeTimeMs() + ","
                + "\"create_scan_range\"" + ":" + this.getCreateScanRangeTimeMs() + ","
                + "\"distribute\"" + ":" + this.getNereidsDistributeTimeMs()
                + "}";
        return planTimesMs;
    }

    public String getMetaTime() {
        return "{"
                + "\"get_partition_version_time_ms\"" + ":" + this.getGetPartitionVersionTime() + ","
                + "\"get_partition_version_count_has_data\"" + ":" + this.getGetPartitionVersionByHasDataCount() + ","
                + "\"get_partition_version_count\"" + ":" + this.getGetPartitionVersionCount() + ","
                + "\"get_table_version_time_ms\"" + ":" + this.getGetTableVersionTime() + ","
                + "\"get_table_version_count\"" + ":" + this.getGetTableVersionCount()
                + "}";
    }

    public String getScheduleTime() {
        return "{"
                + "\"schedule_time_ms\"" + ":" + this.getScheduleTimeMs() + ","
                + "\"fragment_assign_time_ms\"" + ":" + this.getFragmentAssignTimsMs() + ","
                + "\"fragment_serialize_time_ms\"" + ":" + this.getFragmentSerializeTimeMs() + ","
                + "\"fragment_rpc_phase_1_time_ms\"" + ":" + this.getFragmentRPCPhase1TimeMs() + ","
                + "\"fragment_rpc_phase_2_time_ms\"" + ":" + this.getFragmentRPCPhase2TimeMs() + ","
                + "\"fragment_compressed_size_byte\"" + ":" + this.getFragmentCompressedSizeByte() + ","
                + "\"fragment_rpc_count\"" + ":" + this.getFragmentRPCCount()
                + "}";
    }

    public static class SummaryBuilder {
        private Map<String, String> map = Maps.newHashMap();

        public SummaryBuilder profileId(String val) {
            map.put(PROFILE_ID, val);
            return this;
        }

        public SummaryBuilder dorisVersion(String val) {
            map.put(DORIS_VERSION, val);
            return this;
        }

        public SummaryBuilder taskType(String val) {
            map.put(TASK_TYPE, val);
            return this;
        }

        public SummaryBuilder startTime(String val) {
            map.put(START_TIME, val);
            return this;
        }

        public SummaryBuilder endTime(String val) {
            map.put(END_TIME, val);
            return this;
        }

        public SummaryBuilder totalTime(String val) {
            map.put(TOTAL_TIME, val);
            return this;
        }

        public SummaryBuilder taskState(String val) {
            map.put(TASK_STATE, val);
            return this;
        }

        public SummaryBuilder user(String val) {
            map.put(USER, val);
            return this;
        }

        public SummaryBuilder defaultCatalog(String val) {
            map.put(DEFAULT_CATALOG, val);
            return this;
        }

        public SummaryBuilder defaultDb(String val) {
            map.put(DEFAULT_DB, val);
            return this;
        }

        public SummaryBuilder workloadGroup(String workloadGroup) {
            map.put(WORKLOAD_GROUP, workloadGroup);
            return this;
        }

        public SummaryBuilder sqlStatement(String val) {
            map.put(SQL_STATEMENT, val);
            return this;
        }

        public SummaryBuilder isCached(String val) {
            map.put(IS_CACHED, val);
            return this;
        }

        public SummaryBuilder totalInstancesNum(String val) {
            map.put(TOTAL_INSTANCES_NUM, val);
            return this;
        }

        public SummaryBuilder instancesNumPerBe(String val) {
            map.put(INSTANCES_NUM_PER_BE, val);
            return this;
        }

        public SummaryBuilder parallelFragmentExecInstance(String val) {
            map.put(PARALLEL_FRAGMENT_EXEC_INSTANCE, val);
            return this;
        }

        public SummaryBuilder traceId(String val) {
            map.put(TRACE_ID, val);
            return this;
        }

        public SummaryBuilder isNereids(String isNereids) {
            map.put(IS_NEREIDS, isNereids);
            return this;
        }

        public Map<String, String> build() {
            return map;
        }
    }
}
