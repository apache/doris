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

package org.apache.doris.catalog;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.OlapTableFactory.MTMVParams;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.mtmv.EnvInfo;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVJobInfo;
import org.apache.doris.mtmv.MTMVJobManager;
import org.apache.doris.mtmv.MTMVPartitionInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.mtmv.MTMVRefreshInfo;
import org.apache.doris.mtmv.MTMVRefreshPartitionSnapshot;
import org.apache.doris.mtmv.MTMVRefreshSnapshot;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MTMV extends OlapTable {
    private static final Logger LOG = LogManager.getLogger(MTMV.class);
    private ReentrantReadWriteLock mvRwLock;

    @SerializedName("ri")
    private MTMVRefreshInfo refreshInfo;
    @SerializedName("qs")
    private String querySql;
    @SerializedName("s")
    private MTMVStatus status;
    @SerializedName("ei")
    private EnvInfo envInfo;
    @SerializedName("ji")
    private MTMVJobInfo jobInfo;
    @SerializedName("mp")
    private Map<String, String> mvProperties;
    @SerializedName("r")
    private MTMVRelation relation;
    @SerializedName("mpi")
    private MTMVPartitionInfo mvPartitionInfo;
    @SerializedName("rs")
    private MTMVRefreshSnapshot refreshSnapshot;
    // Should update after every fresh, not persist
    private MTMVCache cache;

    // For deserialization
    public MTMV() {
        type = TableType.MATERIALIZED_VIEW;
        mvRwLock = new ReentrantReadWriteLock(true);
    }

    MTMV(MTMVParams params) {
        super(
                params.tableId,
                params.tableName,
                params.schema,
                params.keysType,
                params.partitionInfo,
                params.distributionInfo
        );
        this.type = TableType.MATERIALIZED_VIEW;
        this.querySql = params.querySql;
        this.refreshInfo = params.refreshInfo;
        this.envInfo = params.envInfo;
        this.status = new MTMVStatus();
        this.jobInfo = new MTMVJobInfo(MTMVJobManager.MTMV_JOB_PREFIX + params.tableId);
        this.mvProperties = params.mvProperties;
        this.mvPartitionInfo = params.mvPartitionInfo;
        this.relation = params.relation;
        this.refreshSnapshot = new MTMVRefreshSnapshot();
        mvRwLock = new ReentrantReadWriteLock(true);
    }

    public MTMVRefreshInfo getRefreshInfo() {
        readMvLock();
        try {
            return refreshInfo;
        } finally {
            readMvUnlock();
        }
    }

    public String getQuerySql() {
        return querySql;
    }

    public MTMVStatus getStatus() {
        readMvLock();
        try {
            return status;
        } finally {
            readMvUnlock();
        }
    }

    public EnvInfo getEnvInfo() {
        return envInfo;
    }

    public MTMVJobInfo getJobInfo() {
        readMvLock();
        try {
            return jobInfo;
        } finally {
            readMvUnlock();
        }
    }

    public MTMVRelation getRelation() {
        readMvLock();
        try {
            return relation;
        } finally {
            readMvUnlock();
        }
    }

    public void setCache(MTMVCache cache) {
        this.cache = cache;
    }

    public MTMVRefreshInfo alterRefreshInfo(MTMVRefreshInfo newRefreshInfo) {
        writeMvLock();
        try {
            return refreshInfo.updateNotNull(newRefreshInfo);
        } finally {
            writeMvUnlock();
        }
    }

    public MTMVStatus alterStatus(MTMVStatus newStatus) {
        writeMvLock();
        try {
            return this.status.updateNotNull(newStatus);
        } finally {
            writeMvUnlock();
        }
    }

    public void addTaskResult(MTMVTask task, MTMVRelation relation,
            Map<String, MTMVRefreshPartitionSnapshot> partitionSnapshots) {
        writeMvLock();
        try {
            if (task.getStatus() == TaskStatus.SUCCESS) {
                this.status.setState(MTMVState.NORMAL);
                this.status.setSchemaChangeDetail(null);
                this.status.setRefreshState(MTMVRefreshState.SUCCESS);
                this.relation = relation;
                if (!Env.isCheckpointThread()) {
                    try {
                        this.cache = MTMVCache.from(this, MTMVPlanUtil.createMTMVContext(this));
                    } catch (Throwable e) {
                        this.cache = null;
                        LOG.warn("generate cache failed", e);
                    }
                }
            } else {
                this.status.setRefreshState(MTMVRefreshState.FAIL);
            }
            this.jobInfo.addHistoryTask(task);
            this.refreshSnapshot.updateSnapshots(partitionSnapshots, getPartitionNames());
        } finally {
            writeMvUnlock();
        }
    }

    public Map<String, String> alterMvProperties(Map<String, String> mvProperties) {
        writeMvLock();
        try {
            this.mvProperties.putAll(mvProperties);
            return this.mvProperties;
        } finally {
            writeMvUnlock();
        }
    }

    public long getGracePeriod() {
        readMvLock();
        try {
            if (!StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD))) {
                return Long.parseLong(mvProperties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD)) * 1000;
            } else {
                return 0L;
            }
        } finally {
            readMvUnlock();
        }
    }

    public Optional<String> getWorkloadGroup() {
        readMvLock();
        try {
            if (mvProperties.containsKey(PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP) && !StringUtils
                    .isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP))) {
                return Optional.of(mvProperties.get(PropertyAnalyzer.PROPERTIES_WORKLOAD_GROUP));
            }
            return Optional.empty();
        } finally {
            readMvUnlock();
        }
    }

    public int getRefreshPartitionNum() {
        readMvLock();
        try {
            if (!StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM))) {
                int value = Integer.parseInt(mvProperties.get(PropertyAnalyzer.PROPERTIES_REFRESH_PARTITION_NUM));
                return value < 1 ? MTMVTask.DEFAULT_REFRESH_PARTITION_NUM : value;
            } else {
                return MTMVTask.DEFAULT_REFRESH_PARTITION_NUM;
            }
        } finally {
            readMvUnlock();
        }
    }

    public Set<String> getExcludedTriggerTables() {
        readMvLock();
        try {
            if (StringUtils.isEmpty(mvProperties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES))) {
                return Sets.newHashSet();
            }
            String[] split = mvProperties.get(PropertyAnalyzer.PROPERTIES_EXCLUDED_TRIGGER_TABLES).split(",");
            return Sets.newHashSet(split);
        } finally {
            readMvUnlock();
        }
    }

    public MTMVCache getOrGenerateCache() throws AnalysisException {
        if (cache == null) {
            writeMvLock();
            try {
                if (cache == null) {
                    this.cache = MTMVCache.from(this, MTMVPlanUtil.createMTMVContext(this));
                }
            } finally {
                writeMvUnlock();
            }
        }
        return cache;
    }

    public Map<String, String> getMvProperties() {
        readMvLock();
        try {
            return mvProperties;
        } finally {
            readMvUnlock();
        }
    }

    public MTMVPartitionInfo getMvPartitionInfo() {
        return mvPartitionInfo;
    }

    public MTMVRefreshSnapshot getRefreshSnapshot() {
        return refreshSnapshot;
    }

    /**
     * generateMvPartitionDescs
     *
     * @return mvPartitionId ==> mvPartitionKeyDesc
     */
    public Map<Long, PartitionKeyDesc> generateMvPartitionDescs() {
        Map<Long, PartitionItem> mtmvItems = getAndCopyPartitionItems();
        Map<Long, PartitionKeyDesc> result = Maps.newHashMap();
        for (Entry<Long, PartitionItem> entry : mtmvItems.entrySet()) {
            result.put(entry.getKey(), entry.getValue().toPartitionKeyDesc());
        }
        return result;
    }

    /**
     * generateRelatedPartitionDescs
     * <p>
     * Different partitions may generate the same PartitionKeyDesc through logical calculations
     * (such as selecting only one column, or rolling up partitions), so it is a one to many relationship
     *
     * @return related PartitionKeyDesc ==> relatedPartitionIds
     * @throws AnalysisException
     */
    public Map<PartitionKeyDesc, Set<Long>> generateRelatedPartitionDescs() throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Maps.newHashMap();
        }
        Map<PartitionKeyDesc, Set<Long>> res = new HashMap<>();
        int relatedColPos = mvPartitionInfo.getRelatedColPos();
        Map<Long, PartitionItem> relatedPartitionItems = mvPartitionInfo.getRelatedTable()
                .getPartitionItemsByTimeFilter(relatedColPos,
                        MTMVUtil.generateMTMVPartitionSyncConfigByProperties(mvProperties));
        for (Entry<Long, PartitionItem> entry : relatedPartitionItems.entrySet()) {
            PartitionKeyDesc partitionKeyDesc = entry.getValue().toPartitionKeyDesc(relatedColPos);
            if (res.containsKey(partitionKeyDesc)) {
                res.get(partitionKeyDesc).add(entry.getKey());
            } else {
                res.put(partitionKeyDesc, Sets.newHashSet(entry.getKey()));
            }
        }
        return res;
    }

    /**
     * Calculate the partition and associated partition mapping relationship of the MTMV
     * It is the result of real-time comparison calculation, so there may be some costs,
     * so it should be called with caution
     *
     * @return mvPartitionId ==> relationPartitionIds
     * @throws AnalysisException
     */
    public Map<Long, Set<Long>> calculatePartitionMappings() throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Maps.newHashMap();
        }
        Map<Long, Set<Long>> res = Maps.newHashMap();
        Map<PartitionKeyDesc, Set<Long>> relatedPartitionDescs = generateRelatedPartitionDescs();
        Map<Long, PartitionItem> mvPartitionItems = getAndCopyPartitionItems();
        for (Entry<Long, PartitionItem> entry : mvPartitionItems.entrySet()) {
            res.put(entry.getKey(),
                    relatedPartitionDescs.getOrDefault(entry.getValue().toPartitionKeyDesc(), Sets.newHashSet()));
        }
        return res;
    }

    public void readMvLock() {
        this.mvRwLock.readLock().lock();
    }

    public void readMvUnlock() {
        this.mvRwLock.readLock().unlock();
    }

    public void writeMvLock() {
        this.mvRwLock.writeLock().lock();
    }

    public void writeMvUnlock() {
        this.mvRwLock.writeLock().unlock();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        MTMV materializedView = GsonUtils.GSON.fromJson(Text.readString(in), this.getClass());
        refreshInfo = materializedView.refreshInfo;
        querySql = materializedView.querySql;
        status = materializedView.status;
        envInfo = materializedView.envInfo;
        jobInfo = materializedView.jobInfo;
        mvProperties = materializedView.mvProperties;
        relation = materializedView.relation;
        mvPartitionInfo = materializedView.mvPartitionInfo;
        refreshSnapshot = materializedView.refreshSnapshot;
        // For compatibility
        if (refreshSnapshot == null) {
            refreshSnapshot = new MTMVRefreshSnapshot();
        }
    }

    @Override
    public String toString() {
        return "MTMV{"
                + ", refreshInfo=" + refreshInfo
                + ", querySql='" + querySql + '\''
                + ", status=" + status
                + ", envInfo=" + envInfo
                + ", jobInfo=" + jobInfo
                + ", mvProperties=" + mvProperties
                + ", relation=" + relation
                + ", mvPartitionInfo=" + mvPartitionInfo
                + ", refreshSnapshot=" + refreshSnapshot
                + ", cache=" + cache
                + ", id=" + id
                + ", name='" + name + '\''
                + ", qualifiedDbName='" + qualifiedDbName + '\''
                + ", comment='" + comment + '\''
                + '}';
    }
}
