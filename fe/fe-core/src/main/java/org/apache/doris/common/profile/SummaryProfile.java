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
    // Span name constants (used as counter names in the profile)
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
    public static final String FETCH_RESULT_CONSUME_TIME = "Fetch Result Consume Time";
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
    public static final String BEFORE_TRANSACTION_COMMIT_TIME = "Before Transaction Commit Time";
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
    public static final String EXECUTION_GPLOT = "Execution Gplot";
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

    // Info string items only (time metrics are now counters managed by ProfileTracer).
    public static final ImmutableList<String> EXECUTION_SUMMARY_KEYS = ImmutableList.of(
            WORKLOAD_GROUP,
            SCHEDULE_TIME_PER_BE,
            DORIS_VERSION,
            IS_NEREIDS,
            IS_CACHED,
            TOTAL_INSTANCES_NUM,
            INSTANCES_NUM_PER_BE,
            PARALLEL_FRAGMENT_EXEC_INSTANCE,
            TRACE_ID,
            SYSTEM_MESSAGE,
            EXECUTED_BY_FRONTEND,
            SPLITS_ASSIGNMENT_WEIGHT,
            ICEBERG_SCAN_METRICS,
            PAIMON_SCAN_METRICS,
            EXECUTION_GPLOT
    );

    public boolean parsedByConnectionProcess = false;
    @SerializedName(value = "summaryProfile")
    private RuntimeProfile summaryProfile = new RuntimeProfile(SUMMARY_PROFILE_NAME);
    @SerializedName(value = "executionSummaryProfile")
    private RuntimeProfile executionSummaryProfile = new RuntimeProfile(EXECUTION_SUMMARY_PROFILE_NAME);

    private transient ProfileTracer tracer;

    // Kept for query duration calculation in Profile.java
    @SerializedName(value = "queryBeginTime")
    private long queryBeginTime = -1;

    // Kept for transaction summary info strings
    @SerializedName(value = "transactionType")
    private TransactionType transactionType = TransactionType.UNKNOWN;
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
        executionSummaryProfile.setCounterFirst(true);
        tracer = new ProfileTracer(executionSummaryProfile);
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

    public ProfileTracer getTracer() {
        return tracer;
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
    }

    // This method is used to display the final data status when the overall query ends.
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
        // Write RPC latency and transaction info strings at query finish
        executionSummaryProfile.addInfoString(SCHEDULE_TIME_PER_BE, getRpcLatency());
        setTransactionSummary();
        // Write cloud-mode info strings
        if (Config.isCloudMode()) {
            writeCloudMetaInfoStrings();
        }
    }

    private void updateSummaryProfile(Map<String, String> infos) {
        for (String key : infos.keySet()) {
            if (SUMMARY_KEYS.contains(key)) {
                summaryProfile.addInfoString(key, infos.get(key));
            } else if (EXECUTION_SUMMARY_KEYS.contains(key)) {
                executionSummaryProfile.addInfoString(key, infos.get(key));
            }
        }
    }

    public void setTransactionType(TransactionType type) {
        this.transactionType = type;
    }

    public void setTransactionSummary() {
        if (transactionType.equals(TransactionType.HMS)) {
            // HMS filesystem and partition info strings are written via spans/counters now.
            // The counter hierarchy under Transaction Commit Time handles the values.
        }
    }

    private void writeCloudMetaInfoStrings() {
        long partVersionTime = getSpanValue(GET_PARTITION_VERSION_TIME);
        long tableVersionTime = getSpanValue(GET_TABLE_VERSION_TIME);
        long totalMetaTime = partVersionTime + tableVersionTime;
        executionSummaryProfile.addInfoString(GET_META_VERSION_TIME,
                RuntimeProfile.printCounter(totalMetaTime, TUnit.TIME_NS));
        if (partVersionTime > 0) {
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_TIME,
                    RuntimeProfile.printCounter(partVersionTime, TUnit.TIME_NS));
        }
        executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_COUNT,
                RuntimeProfile.printCounter(getSpanValue(GET_PARTITION_VERSION_COUNT), TUnit.UNIT));
        executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_BY_HAS_DATA_COUNT,
                RuntimeProfile.printCounter(getSpanValue(GET_PARTITION_VERSION_BY_HAS_DATA_COUNT), TUnit.UNIT));
        if (tableVersionTime > 0) {
            executionSummaryProfile.addInfoString(GET_TABLE_VERSION_TIME,
                    RuntimeProfile.printCounter(tableVersionTime, TUnit.TIME_NS));
        }
        executionSummaryProfile.addInfoString(GET_TABLE_VERSION_COUNT,
                RuntimeProfile.printCounter(getSpanValue(GET_TABLE_VERSION_COUNT), TUnit.UNIT));
    }

    // --- Info string setters (write directly to executionSummaryProfile) ---

    public void setSystemMessage(String msg) {
        executionSummaryProfile.addInfoString(SYSTEM_MESSAGE, msg);
    }

    public void setExecutedByFrontend(boolean executedByFrontend) {
        executionSummaryProfile.addInfoString(EXECUTED_BY_FRONTEND, String.valueOf(executedByFrontend));
    }

    public void setMaxQueueSize(int maxQueueSize) {
        executionSummaryProfile.addInfoString(MAX_QUEUE_SIZE, RuntimeProfile.printCounter(maxQueueSize, TUnit.UNIT));
    }

    public void setMaxConcurrency(int maxConcurrency) {
        executionSummaryProfile.addInfoString(MAX_CONCURRENCY, RuntimeProfile.printCounter(maxConcurrency, TUnit.UNIT));
    }

    public void setScanThreadNum(int scanThreadNum) {
        executionSummaryProfile.addInfoString(SCAN_THREAD_NUM, RuntimeProfile.printCounter(scanThreadNum, TUnit.UNIT));
    }

    public void setMaxRemoteScanThreadNum(int maxRemoteScanThreadNum) {
        executionSummaryProfile.addInfoString(MAX_REMOTE_SCAN_THREAD_NUM,
                RuntimeProfile.printCounter(maxRemoteScanThreadNum, TUnit.UNIT));
    }

    public void setMinRemoteScanThreadNum(int minRemoteScanThreadNum) {
        executionSummaryProfile.addInfoString(MIN_REMOTE_SCAN_THREAD_NUM,
                RuntimeProfile.printCounter(minRemoteScanThreadNum, TUnit.UNIT));
    }

    public void setMemoryLowWatermark(int memoryLowWatermark) {
        executionSummaryProfile.addInfoString(MEMORY_LOW_WATERMARK,
                RuntimeProfile.printCounter(memoryLowWatermark, TUnit.UNIT));
    }

    public void setMemoryHighWatermark(int memoryHighWatermark) {
        executionSummaryProfile.addInfoString(MEMORY_HIGH_WATERMARK,
                RuntimeProfile.printCounter(memoryHighWatermark, TUnit.UNIT));
    }

    public void setTag(String tag) {
        executionSummaryProfile.addInfoString(TAG, tag);
    }

    public void setReadBytesPerSecond(int readBytesPerSecond) {
        executionSummaryProfile.addInfoString(READ_BYTES_PER_SECOND,
                RuntimeProfile.printCounter(readBytesPerSecond, TUnit.BYTES_PER_SECOND));
    }

    public void setRemoteReadBytesPerSecond(int remoteReadBytesPerSecond) {
        executionSummaryProfile.addInfoString(REMOTE_READ_BYTES_PER_SECOND,
                RuntimeProfile.printCounter(remoteReadBytesPerSecond, TUnit.BYTES_PER_SECOND));
    }

    public void setQueueTimeout(int queueTimeout) {
        executionSummaryProfile.addInfoString(QUEUE_TIMEOUT, RuntimeProfile.printCounter(queueTimeout, TUnit.UNIT));
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

    public void setAssignedWeightPerBackend(Map<Backend, Long> assignedWeightPerBackend) {
        this.assignedWeightPerBackend = assignedWeightPerBackend;
    }

    // --- Span value helpers (read counter values from tracer) ---

    private long getSpanValue(String name) {
        ProfileSpan span = tracer.getSpan(name);
        return span != null ? span.getValue() : -1;
    }

    private int getSpanValueAsInt(String name) {
        long v = getSpanValue(name);
        return v >= 0 ? (int) v : -1;
    }

    /**
     * Get a formatted time string for a span counter (e.g. "42ms").
     * Returns "N/A" if the span does not exist.
     */
    public String getPrettySpanTime(String name) {
        ProfileSpan span = tracer.getSpan(name);
        if (span == null) {
            return "N/A";
        }
        return RuntimeProfile.printCounter(span.getValue(), TUnit.TIME_MS);
    }

    // --- getXxxTimeMs() methods (read from tracer counters) ---

    public int getParseSqlTimeMs() {
        return getSpanValueAsInt(PARSE_SQL_TIME);
    }

    public int getPlanTimeMs() {
        return getSpanValueAsInt(PLAN_TIME);
    }

    public int getNereidsLockTableTimeMs() {
        return getSpanValueAsInt(NEREIDS_LOCK_TABLE_TIME);
    }

    public int getNereidsAnalysisTimeMs() {
        return getSpanValueAsInt(NEREIDS_ANALYSIS_TIME);
    }

    public int getNereidsRewriteTimeMs() {
        return getSpanValueAsInt(NEREIDS_REWRITE_TIME);
    }

    public int getNereidsCollectTablePartitionTimeMs() {
        return getSpanValueAsInt(NEREIDS_COLLECT_TABLE_PARTITION_TIME);
    }

    public int getNereidsOptimizeTimeMs() {
        return getSpanValueAsInt(NEREIDS_OPTIMIZE_TIME);
    }

    public int getNereidsTranslateTimeMs() {
        return getSpanValueAsInt(NEREIDS_TRANSLATE_TIME);
    }

    public int getNereidsGarbageCollectionTimeMs() {
        return getSpanValueAsInt(NEREIDS_GARBAGE_COLLECT_TIME);
    }

    public int getNereidsBeFoldConstTimeMs() {
        return getSpanValueAsInt(NEREIDS_BE_FOLD_CONST_TIME);
    }

    public int getNereidsDistributeTimeMs() {
        return getSpanValueAsInt(NEREIDS_DISTRIBUTE_TIME);
    }

    public int getInitScanNodeTimeMs() {
        return getSpanValueAsInt(INIT_SCAN_NODE_TIME);
    }

    public int getFinalizeScanNodeTimeMs() {
        return getSpanValueAsInt(FINALIZE_SCAN_NODE_TIME);
    }

    public int getCreateScanRangeTimeMs() {
        return getSpanValueAsInt(CREATE_SCAN_RANGE_TIME);
    }

    public int getScheduleTimeMs() {
        return getSpanValueAsInt(SCHEDULE_TIME);
    }

    public int getFragmentAssignTimsMs() {
        return getSpanValueAsInt(ASSIGN_FRAGMENT_TIME);
    }

    public int getFragmentSerializeTimeMs() {
        return getSpanValueAsInt(FRAGMENT_SERIALIZE_TIME);
    }

    public int getFragmentRPCPhase1TimeMs() {
        return getSpanValueAsInt(SEND_FRAGMENT_PHASE1_TIME);
    }

    public int getFragmentRPCPhase2TimeMs() {
        return getSpanValueAsInt(SEND_FRAGMENT_PHASE2_TIME);
    }

    public double getFragmentCompressedSizeByte() {
        long v = getSpanValue(FRAGMENT_COMPRESSED_SIZE);
        return v >= 0 ? v : 0;
    }

    public long getFragmentRPCCount() {
        long v = getSpanValue(FRAGMENT_RPC_COUNT);
        return v >= 0 ? v : 0;
    }

    public long getGetPartitionVersionTime() {
        return getSpanValue(GET_PARTITION_VERSION_TIME);
    }

    public long getGetPartitionVersionCount() {
        return getSpanValue(GET_PARTITION_VERSION_COUNT);
    }

    public long getGetPartitionVersionByHasDataCount() {
        return getSpanValue(GET_PARTITION_VERSION_BY_HAS_DATA_COUNT);
    }

    public long getGetTableVersionTime() {
        return getSpanValue(GET_TABLE_VERSION_TIME);
    }

    public long getGetTableVersionCount() {
        return getSpanValue(GET_TABLE_VERSION_COUNT);
    }

    public long getCloudMetaTimeMs() {
        long partTime = getSpanValue(GET_PARTITION_VERSION_TIME);
        long tableTime = getSpanValue(GET_TABLE_VERSION_TIME);
        if (partTime < 0) {
            partTime = 0;
        }
        if (tableTime < 0) {
            tableTime = 0;
        }
        return TimeUnit.NANOSECONDS.toMillis(partTime + tableTime);
    }

    public long getNereidsMvRewriteTimeMs() {
        return getSpanValue(NEREIDS_PRE_REWRITE_BY_MV_TIME);
    }

    public long getExternalCatalogMetaTimeMs() {
        // External catalog meta time is tracked via accumulative span
        long v = getSpanValue("External Catalog Meta Time");
        return v >= 0 ? v : 0;
    }

    public long getExternalTvfInitTimeMs() {
        long v = getSpanValue("External TVF Init Time");
        return v >= 0 ? v : 0;
    }

    public long getNereidsPartitiionPruneTimeMs() {
        long v = getSpanValue("Nereids Partition Prune Time");
        return v >= 0 ? v : 0;
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

    public void write(DataOutput output) throws IOException {
        Text.writeString(output, GsonUtils.GSON.toJson(this));
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
