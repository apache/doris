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
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TUnit;
import org.apache.doris.transaction.TransactionType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;

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
    public static final String DEFAULT_DB = "Default Db";
    public static final String SQL_STATEMENT = "Sql Statement";
    public static final String IS_CACHED = "Is Cached";
    public static final String IS_NEREIDS = "Is Nereids";
    public static final String TOTAL_INSTANCES_NUM = "Total Instances Num";
    public static final String INSTANCES_NUM_PER_BE = "Instances Num Per BE";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE = "Parallel Fragment Exec Instance Num";
    public static final String TRACE_ID = "Trace ID";
    public static final String WORKLOAD_GROUP = "Workload Group";
    public static final String PHYSICAL_PLAN = "Physical Plan";
    public static final String DISTRIBUTED_PLAN = "Distributed Plan";
    // Execution Summary
    public static final String EXECUTION_SUMMARY_PROFILE_NAME = "Execution Summary";
    public static final String ANALYSIS_TIME = "Analysis Time";
    public static final String JOIN_REORDER_TIME = "JoinReorder Time";
    public static final String CREATE_SINGLE_NODE_TIME = "CreateSingleNode Time";
    public static final String QUERY_DISTRIBUTED_TIME = "QueryDistributed Time";
    public static final String INIT_SCAN_NODE_TIME = "Init Scan Node Time";
    public static final String FINALIZE_SCAN_NODE_TIME = "Finalize Scan Node Time";
    public static final String GET_SPLITS_TIME = "Get Splits Time";
    public static final String GET_PARTITIONS_TIME = "Get Partitions Time";
    public static final String GET_PARTITION_FILES_TIME = "Get Partition Files Time";
    public static final String CREATE_SCAN_RANGE_TIME = "Create Scan Range Time";
    public static final String PLAN_TIME = "Plan Time";
    public static final String SCHEDULE_TIME = "Schedule Time";
    public static final String ASSIGN_FRAGMENT_TIME = "Fragment Assign Time";
    public static final String FRAGMENT_SERIALIZE_TIME = "Fragment Serialize Time";
    public static final String SEND_FRAGMENT_PHASE1_TIME = "Fragment RPC Phase1 Time";
    public static final String SEND_FRAGMENT_PHASE2_TIME = "Fragment RPC Phase2 Time";
    public static final String WAIT_FETCH_RESULT_TIME = "Wait and Fetch Result Time";
    public static final String FETCH_RESULT_TIME = "Fetch Result Time";
    public static final String WRITE_RESULT_TIME = "Write Result Time";
    public static final String GET_PARTITION_VERSION_TIME = "Get Partition Version Time";
    public static final String GET_PARTITION_VERSION_COUNT = "Get Partition Version Count";
    public static final String GET_PARTITION_VERSION_BY_HAS_DATA_COUNT = "Get Partition Version Count (hasData)";
    public static final String GET_TABLE_VERSION_TIME = "Get Table Version Time";
    public static final String GET_TABLE_VERSION_COUNT = "Get Table Version Count";

    public static final String PARSE_SQL_TIME = "Parse SQL Time";
    public static final String NEREIDS_ANALYSIS_TIME = "Nereids Analysis Time";
    public static final String NEREIDS_REWRITE_TIME = "Nereids Rewrite Time";
    public static final String NEREIDS_OPTIMIZE_TIME = "Nereids Optimize Time";
    public static final String NEREIDS_TRANSLATE_TIME = "Nereids Translate Time";
    public static final String NEREIDS_DISTRIBUTE_TIME = "Nereids Distribute Time";

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

    // These info will display on FE's web ui table, every one will be displayed as
    // a column, so that should not
    // add many columns here. Add to ExecutionSummary list.
    public static final ImmutableList<String> SUMMARY_CAPTIONS = ImmutableList.of(PROFILE_ID, TASK_TYPE,
            START_TIME, END_TIME, TOTAL_TIME, TASK_STATE, USER, DEFAULT_DB, SQL_STATEMENT);
    public static final ImmutableList<String> SUMMARY_KEYS = new ImmutableList.Builder<String>()
            .addAll(SUMMARY_CAPTIONS)
            .add(PHYSICAL_PLAN)
            .add(DISTRIBUTED_PLAN)
            .build();

    // The display order of execution summary items.
    public static final ImmutableList<String> EXECUTION_SUMMARY_KEYS = ImmutableList.of(
            PARSE_SQL_TIME,
            NEREIDS_ANALYSIS_TIME,
            NEREIDS_REWRITE_TIME,
            NEREIDS_OPTIMIZE_TIME,
            NEREIDS_TRANSLATE_TIME,
            WORKLOAD_GROUP,
            ANALYSIS_TIME,
            PLAN_TIME,
            JOIN_REORDER_TIME,
            CREATE_SINGLE_NODE_TIME,
            QUERY_DISTRIBUTED_TIME,
            INIT_SCAN_NODE_TIME,
            FINALIZE_SCAN_NODE_TIME,
            GET_SPLITS_TIME,
            GET_PARTITIONS_TIME,
            GET_PARTITION_FILES_TIME,
            CREATE_SCAN_RANGE_TIME,
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
            TRANSACTION_COMMIT_TIME
    );

    // Ident of each item. Default is 0, which doesn't need to present in this Map.
    // Please set this map for new profile items if they need ident.
    public static ImmutableMap<String, Integer> EXECUTION_SUMMARY_KEYS_IDENTATION
            = ImmutableMap.<String, Integer>builder()
            .put(JOIN_REORDER_TIME, 1)
            .put(CREATE_SINGLE_NODE_TIME, 1)
            .put(QUERY_DISTRIBUTED_TIME, 1)
            .put(INIT_SCAN_NODE_TIME, 1)
            .put(FINALIZE_SCAN_NODE_TIME, 1)
            .put(GET_SPLITS_TIME, 2)
            .put(GET_PARTITIONS_TIME, 3)
            .put(GET_PARTITION_FILES_TIME, 3)
            .put(CREATE_SCAN_RANGE_TIME, 2)
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
            .build();

    private RuntimeProfile summaryProfile;
    private RuntimeProfile executionSummaryProfile;

    private long parseSqlStartTime = -1;
    private long parseSqlFinishTime = -1;
    private long nereidsAnalysisFinishTime = -1;
    private long nereidsRewriteFinishTime = -1;
    private long nereidsOptimizeFinishTime = -1;
    private long nereidsTranslateFinishTime = -1;
    private long nereidsDistributeFinishTime = -1;
    // timestamp of query begin
    private long queryBeginTime = -1;
    // Analysis end time
    private long queryAnalysisFinishTime = -1;
    // Join reorder end time
    private long queryJoinReorderFinishTime = -1;
    // Create single node plan end time
    private long queryCreateSingleNodeFinishTime = -1;
    // Create distribute plan end time
    private long queryDistributedFinishTime = -1;
    private long initScanNodeStartTime = -1;
    private long initScanNodeFinishTime = -1;
    private long finalizeScanNodeStartTime = -1;
    private long finalizeScanNodeFinishTime = -1;
    private long getSplitsStartTime = -1;
    private long getPartitionsFinishTime = -1;
    private long getPartitionFilesFinishTime = -1;
    private long getSplitsFinishTime = -1;
    private long createScanRangeFinishTime = -1;
    // Plan end time
    private long queryPlanFinishTime = -1;
    private long assignFragmentTime = -1;
    private long fragmentSerializeTime = -1;
    private long fragmentSendPhase1Time = -1;
    private long fragmentSendPhase2Time = -1;
    private long fragmentCompressedSize = 0;
    private long fragmentRpcCount = 0;
    // Fragment schedule and send end time
    private long queryScheduleFinishTime = -1;
    // Query result fetch end time
    private long queryFetchResultFinishTime = -1;
    private long tempStarTime = -1;
    private long queryFetchResultConsumeTime = 0;
    private long queryWriteResultConsumeTime = 0;
    private long getPartitionVersionTime = 0;
    private long getPartitionVersionCount = 0;
    private long getPartitionVersionByHasDataCount = 0;
    private long getTableVersionTime = 0;
    private long getTableVersionCount = 0;
    private long transactionCommitBeginTime = -1;
    private long transactionCommitEndTime = -1;
    private long filesystemOptTime = -1;
    private long hmsAddPartitionTime = -1;
    private long hmsAddPartitionCnt = 0;
    private long hmsUpdatePartitionTime = -1;
    private long hmsUpdatePartitionCnt = 0;
    private long filesystemRenameFileCnt = 0;
    private long filesystemRenameDirCnt = 0;

    private long filesystemDeleteFileCnt = 0;
    private long filesystemDeleteDirCnt = 0;
    private TransactionType transactionType = TransactionType.UNKNOWN;

    public SummaryProfile() {
        summaryProfile = new RuntimeProfile(SUMMARY_PROFILE_NAME);
        executionSummaryProfile = new RuntimeProfile(EXECUTION_SUMMARY_PROFILE_NAME);
        init();
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

    private void init() {
        for (String key : SUMMARY_KEYS) {
            summaryProfile.addInfoString(key, "N/A");
        }
        for (String key : EXECUTION_SUMMARY_KEYS) {
            executionSummaryProfile.addInfoString(key, "N/A");
        }
    }

    public void prettyPrint(StringBuilder builder) {
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
        executionSummaryProfile.addInfoString(PARSE_SQL_TIME, getPrettyParseSqlTime());
        executionSummaryProfile.addInfoString(NEREIDS_ANALYSIS_TIME, getPrettyNereidsAnalysisTime());
        executionSummaryProfile.addInfoString(NEREIDS_REWRITE_TIME, getPrettyNereidsRewriteTime());
        executionSummaryProfile.addInfoString(NEREIDS_OPTIMIZE_TIME, getPrettyNereidsOptimizeTime());
        executionSummaryProfile.addInfoString(NEREIDS_TRANSLATE_TIME, getPrettyNereidsTranslateTime());
        executionSummaryProfile.addInfoString(NEREIDS_DISTRIBUTE_TIME, getPrettyNereidsDistributeTime());
        executionSummaryProfile.addInfoString(ANALYSIS_TIME,
                getPrettyTime(queryAnalysisFinishTime, queryBeginTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(PLAN_TIME,
                getPrettyTime(queryPlanFinishTime, queryAnalysisFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(JOIN_REORDER_TIME,
                getPrettyTime(queryJoinReorderFinishTime, queryAnalysisFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(CREATE_SINGLE_NODE_TIME,
                getPrettyTime(queryCreateSingleNodeFinishTime, queryJoinReorderFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(QUERY_DISTRIBUTED_TIME,
                getPrettyTime(queryDistributedFinishTime, queryCreateSingleNodeFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(INIT_SCAN_NODE_TIME,
                getPrettyTime(initScanNodeFinishTime, initScanNodeStartTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(FINALIZE_SCAN_NODE_TIME,
                getPrettyTime(finalizeScanNodeFinishTime, finalizeScanNodeStartTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(GET_SPLITS_TIME,
                getPrettyTime(getSplitsFinishTime, getSplitsStartTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(GET_PARTITIONS_TIME,
                getPrettyTime(getPartitionsFinishTime, getSplitsStartTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(GET_PARTITION_FILES_TIME,
                getPrettyTime(getPartitionFilesFinishTime, getPartitionsFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(CREATE_SCAN_RANGE_TIME,
                getPrettyTime(createScanRangeFinishTime, getSplitsFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(SCHEDULE_TIME,
                getPrettyTime(queryScheduleFinishTime, queryPlanFinishTime, TUnit.TIME_MS));
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
        executionSummaryProfile.addInfoString(WAIT_FETCH_RESULT_TIME,
                getPrettyTime(queryFetchResultFinishTime, queryScheduleFinishTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(FETCH_RESULT_TIME,
                RuntimeProfile.printCounter(queryFetchResultConsumeTime, TUnit.TIME_MS));
        executionSummaryProfile.addInfoString(WRITE_RESULT_TIME,
                RuntimeProfile.printCounter(queryWriteResultConsumeTime, TUnit.TIME_MS));
        setTransactionSummary();

        if (Config.isCloudMode()) {
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_TIME, getPrettyGetPartitionVersionTime());
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_COUNT, getPrettyGetPartitionVersionCount());
            executionSummaryProfile.addInfoString(GET_PARTITION_VERSION_BY_HAS_DATA_COUNT,
                    getPrettyGetPartitionVersionByHasDataCount());
            executionSummaryProfile.addInfoString(GET_TABLE_VERSION_TIME, getPrettyGetTableVersionTime());
            executionSummaryProfile.addInfoString(GET_TABLE_VERSION_COUNT, getPrettyGetTableVersionCount());
        }
    }

    public void setTransactionSummary() {
        executionSummaryProfile.addInfoString(TRANSACTION_COMMIT_TIME,
                getPrettyTime(transactionCommitEndTime, transactionCommitBeginTime, TUnit.TIME_MS));

        if (transactionType.equals(TransactionType.HMS)) {
            executionSummaryProfile.addInfoString(FILESYSTEM_OPT_TIME,
                    getPrettyTime(filesystemOptTime, 0, TUnit.TIME_MS));
            executionSummaryProfile.addInfoString(FILESYSTEM_OPT_RENAME_FILE_CNT,
                    getPrettyCount(filesystemRenameFileCnt));
            executionSummaryProfile.addInfoString(FILESYSTEM_OPT_RENAME_DIR_CNT,
                    getPrettyCount(filesystemRenameDirCnt));
            executionSummaryProfile.addInfoString(FILESYSTEM_OPT_DELETE_FILE_CNT,
                    getPrettyCount(filesystemDeleteFileCnt));
            executionSummaryProfile.addInfoString(FILESYSTEM_OPT_DELETE_DIR_CNT,
                    getPrettyCount(filesystemDeleteDirCnt));

            executionSummaryProfile.addInfoString(HMS_ADD_PARTITION_TIME,
                    getPrettyTime(hmsAddPartitionTime, 0, TUnit.TIME_MS));
            executionSummaryProfile.addInfoString(HMS_ADD_PARTITION_CNT,
                    getPrettyCount(hmsAddPartitionCnt));
            executionSummaryProfile.addInfoString(HMS_UPDATE_PARTITION_TIME,
                    getPrettyTime(hmsUpdatePartitionTime, 0, TUnit.TIME_MS));
            executionSummaryProfile.addInfoString(HMS_UPDATE_PARTITION_CNT,
                    getPrettyCount(hmsUpdatePartitionCnt));
        }
    }

    public void setParseSqlStartTime(long parseSqlStartTime) {
        this.parseSqlStartTime = parseSqlStartTime;
    }

    public void setParseSqlFinishTime(long parseSqlFinishTime) {
        this.parseSqlFinishTime = parseSqlFinishTime;
    }

    public void setNereidsAnalysisTime() {
        this.nereidsAnalysisFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsRewriteTime() {
        this.nereidsRewriteFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsOptimizeTime() {
        this.nereidsOptimizeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsTranslateTime() {
        this.nereidsTranslateFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setNereidsDistributeTime() {
        this.nereidsDistributeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryBeginTime() {
        this.queryBeginTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryAnalysisFinishTime() {
        this.queryAnalysisFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryJoinReorderFinishTime() {
        this.queryJoinReorderFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setCreateSingleNodeFinishTime() {
        this.queryCreateSingleNodeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setInitScanNodeStartTime() {
        this.initScanNodeStartTime = TimeUtils.getStartTimeMs();
    }

    public void setInitScanNodeFinishTime() {
        this.initScanNodeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setFinalizeScanNodeStartTime() {
        this.finalizeScanNodeStartTime = TimeUtils.getStartTimeMs();
    }

    public void setFinalizeScanNodeFinishTime() {
        this.finalizeScanNodeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setGetSplitsStartTime() {
        this.getSplitsStartTime = TimeUtils.getStartTimeMs();
    }

    public void setGetPartitionsFinishTime() {
        this.getPartitionsFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setGetPartitionFilesFinishTime() {
        this.getPartitionFilesFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setGetSplitsFinishTime() {
        this.getSplitsFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setCreateScanRangeFinishTime() {
        this.createScanRangeFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryDistributedFinishTime() {
        this.queryDistributedFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryPlanFinishTime() {
        this.queryPlanFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryScheduleFinishTime() {
        this.queryScheduleFinishTime = TimeUtils.getStartTimeMs();
    }

    public void setQueryFetchResultFinishTime() {
        this.queryFetchResultFinishTime = TimeUtils.getStartTimeMs();
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

    public String getPrettyParseSqlTime() {
        return getPrettyTime(parseSqlFinishTime, parseSqlStartTime, TUnit.TIME_MS);
    }

    public String getPrettyNereidsAnalysisTime() {
        return getPrettyTime(nereidsAnalysisFinishTime, queryBeginTime, TUnit.TIME_MS);
    }

    public String getPrettyNereidsRewriteTime() {
        return getPrettyTime(nereidsRewriteFinishTime, nereidsAnalysisFinishTime, TUnit.TIME_MS);
    }

    public String getPrettyNereidsOptimizeTime() {
        return getPrettyTime(nereidsOptimizeFinishTime, nereidsRewriteFinishTime, TUnit.TIME_MS);
    }

    public String getPrettyNereidsTranslateTime() {
        return getPrettyTime(nereidsTranslateFinishTime, nereidsOptimizeFinishTime, TUnit.TIME_MS);
    }

    public String getPrettyNereidsDistributeTime() {
        return getPrettyTime(nereidsDistributeFinishTime, nereidsTranslateFinishTime, TUnit.TIME_MS);
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

    public void freshFilesystemOptTime() {
        if (this.filesystemOptTime == -1) {
            // Because this value needs to be summed up.
            // If it is not set zero here:
            //     1. If the detection time is longer than 1ms,
            //        the final cumulative value will be 1 ms less due to -1 initialization.
            //     2. if the detection time is no longer than 1ms,
            //        the final cumulative value will be -1 always.
            //        This is considered to be the indicator's not being detected,
            //        Apparently not, it's just that the value detected is 0.
            this.filesystemOptTime = 0;
        }
        this.filesystemOptTime += System.currentTimeMillis() - tempStarTime;
    }

    public void setHmsAddPartitionTime() {
        this.hmsAddPartitionTime = TimeUtils.getStartTimeMs() - tempStarTime;
    }

    public void addHmsAddPartitionCnt(long c) {
        this.hmsAddPartitionCnt = c;
    }

    public void setHmsUpdatePartitionTime() {
        this.hmsUpdatePartitionTime = TimeUtils.getStartTimeMs() - tempStarTime;
    }

    public void addHmsUpdatePartitionCnt(long c) {
        this.hmsUpdatePartitionCnt = c;
    }

    public void addRenameFileCnt(long c) {
        this.filesystemRenameFileCnt += c;
    }

    public void incRenameDirCnt() {
        this.filesystemRenameDirCnt += 1;
    }

    public void incDeleteDirRecursiveCnt() {
        this.filesystemDeleteDirCnt += 1;
    }

    public void incDeleteFileCnt() {
        this.filesystemDeleteFileCnt += 1;
    }
}
