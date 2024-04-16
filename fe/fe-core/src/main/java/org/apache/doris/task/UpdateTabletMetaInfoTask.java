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

package org.apache.doris.task;

import org.apache.doris.catalog.BinlogConfig;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.Pair;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTabletMetaInfo;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.thrift.TUpdateTabletMetaInfoReq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateTabletMetaInfoTask extends AgentTask {

    private static final Logger LOG = LogManager.getLogger(UpdateTabletMetaInfoTask.class);

    // used for synchronous process
    private MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch;

    private Set<Pair<Long, Integer>> tableIdWithSchemaHash;
    private int inMemory = -1; // < 0 means not to update inMemory property, > 0 means true, == 0 means false
    private long storagePolicyId = -1; // < 0 means not to update storage policy, == 0 means to reset storage policy
    private BinlogConfig binlogConfig = null; // null means not to update binlog config
    private String compactionPolicy = null; // null means not to update compaction policy
    private Map<String, Long> timeSeriesCompactionConfig = null; // null means not to update compaction policy config
    // For ReportHandler
    private List<TTabletMetaInfo> tabletMetaInfos;
    // < 0 means not to update property, > 0 means true, == 0 means false
    private int enableSingleReplicaCompaction = -1;
    private int skipWriteIndexOnLoad = -1;
    private int disableAutoCompaction = -1;

    public UpdateTabletMetaInfoTask(long backendId, Set<Pair<Long, Integer>> tableIdWithSchemaHash) {
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO,
                -1L, -1L, -1L, -1L, -1L, Math.abs(new SecureRandom().nextLong()));
        this.tableIdWithSchemaHash = tableIdWithSchemaHash;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    int inMemory, long storagePolicyId,
                                    BinlogConfig binlogConfig,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch) {
        this(backendId, tableIdWithSchemaHash);
        this.storagePolicyId = storagePolicyId;
        this.inMemory = inMemory;
        this.binlogConfig = binlogConfig;
        this.latch = latch;
    }

    public UpdateTabletMetaInfoTask(long backendId, List<TTabletMetaInfo> tabletMetaInfos) {
        // For ReportHandler, never add to AgentTaskQueue, so signature is useless.
        super(null, backendId, TTaskType.UPDATE_TABLET_META_INFO,
                -1L, -1L, -1L, -1L, -1L);
        this.tabletMetaInfos = tabletMetaInfos;
    }

    public UpdateTabletMetaInfoTask(long backendId,
                                    Set<Pair<Long, Integer>> tableIdWithSchemaHash,
                                    int inMemory, long storagePolicyId,
                                    BinlogConfig binlogConfig,
                                    MarkedCountDownLatch<Long, Set<Pair<Long, Integer>>> latch,
                                    String compactionPolicy,
                                    Map<String, Long> timeSeriesCompactionConfig,
                                    int enableSingleReplicaCompaction,
                                    int skipWriteIndexOnLoad,
                                    int disableAutoCompaction) {
        this(backendId, tableIdWithSchemaHash, inMemory, storagePolicyId, binlogConfig, latch);
        this.compactionPolicy = compactionPolicy;
        this.timeSeriesCompactionConfig = timeSeriesCompactionConfig;
        this.enableSingleReplicaCompaction = enableSingleReplicaCompaction;
        this.skipWriteIndexOnLoad = skipWriteIndexOnLoad;
        this.disableAutoCompaction = disableAutoCompaction;
    }

    public void countDownLatch(long backendId, Set<Pair<Long, Integer>> tablets) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, tablets)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("UpdateTabletMetaInfoTask current latch count: {}, backend: {}, tablets:{}",
                            latch.getCount(), backendId, tablets);
                }
            }
        }
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            if (LOG.isDebugEnabled()) {
                LOG.debug("UpdateTabletMetaInfoTask count down to zero. error msg: {}", errMsg);
            }
        }
    }

    public Set<Pair<Long, Integer>> getTablets() {
        return tableIdWithSchemaHash;
    }

    public TUpdateTabletMetaInfoReq toThrift() {
        TUpdateTabletMetaInfoReq updateTabletMetaInfoReq = new TUpdateTabletMetaInfoReq();
        if (latch != null) {
            // for schema change
            for (Pair<Long, Integer> pair : tableIdWithSchemaHash) {
                TTabletMetaInfo metaInfo = new TTabletMetaInfo();
                metaInfo.setTabletId(pair.first);
                metaInfo.setSchemaHash(pair.second);
                if (inMemory >= 0) {
                    metaInfo.setIsInMemory(inMemory > 0);
                }
                if (storagePolicyId >= 0) {
                    metaInfo.setStoragePolicyId(storagePolicyId);
                }
                if (binlogConfig != null) {
                    metaInfo.setBinlogConfig(binlogConfig.toThrift());
                }
                if (compactionPolicy != null) {
                    metaInfo.setCompactionPolicy(compactionPolicy);
                }
                if (timeSeriesCompactionConfig != null && !timeSeriesCompactionConfig.isEmpty()) {
                    if (timeSeriesCompactionConfig
                            .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES)) {
                        metaInfo.setTimeSeriesCompactionGoalSizeMbytes(timeSeriesCompactionConfig
                                    .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_GOAL_SIZE_MBYTES));
                    }
                    if (timeSeriesCompactionConfig
                            .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD)) {
                        metaInfo.setTimeSeriesCompactionFileCountThreshold(timeSeriesCompactionConfig
                                        .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_FILE_COUNT_THRESHOLD));
                    }
                    if (timeSeriesCompactionConfig
                            .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS)) {
                        metaInfo.setTimeSeriesCompactionTimeThresholdSeconds(timeSeriesCompactionConfig
                                    .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_TIME_THRESHOLD_SECONDS));
                    }
                    if (timeSeriesCompactionConfig
                            .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD)) {
                        metaInfo.setTimeSeriesCompactionEmptyRowsetsThreshold(timeSeriesCompactionConfig
                                    .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_EMPTY_ROWSETS_THRESHOLD));
                    }
                    if (timeSeriesCompactionConfig
                            .containsKey(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD)) {
                        metaInfo.setTimeSeriesCompactionLevelThreshold(timeSeriesCompactionConfig
                                    .get(PropertyAnalyzer.PROPERTIES_TIME_SERIES_COMPACTION_LEVEL_THRESHOLD));
                    }
                }
                if (enableSingleReplicaCompaction >= 0) {
                    metaInfo.setEnableSingleReplicaCompaction(enableSingleReplicaCompaction > 0);
                }
                if (skipWriteIndexOnLoad >= 0) {
                    metaInfo.setSkipWriteIndexOnLoad(skipWriteIndexOnLoad > 0);
                }
                if (disableAutoCompaction >= 0) {
                    metaInfo.setDisableAutoCompaction(disableAutoCompaction > 0);
                }
                updateTabletMetaInfoReq.addToTabletMetaInfos(metaInfo);
            }
        } else {
            // for ReportHandler
            updateTabletMetaInfoReq.setTabletMetaInfos(tabletMetaInfos);
        }
        return updateTabletMetaInfoReq;
    }
}
