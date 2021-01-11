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

package org.apache.doris.load;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Replica;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TResourceInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoadJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    // QUORUM_FINISHED state is internal
    // To user, it should be transformed to FINISHED
    public enum JobState {
        UNKNOWN, // only for show load state value check, details, see LoadJobV2's JobState
        PENDING,
        ETL,
        LOADING,
        FINISHED,
        QUORUM_FINISHED,
        CANCELLED
    }

    private static final int DEFAULT_TIMEOUT_S = 0;
    private static final long DEFAULT_EXEC_MEM_LIMIT = 2147483648L; // 2GB

    private long id;
    private long dbId;
    private long tableId;
    private String label;
    // when this job is a real time load job, the job is attach with a transaction
    private long transactionId = -1;
    long timestamp;
    private int timeoutSecond;
    private double maxFilterRatio = Config.default_max_filter_ratio;
    private JobState state;

    private BrokerDesc brokerDesc;
    private BrokerFileGroupAggInfo pullLoadSourceInfo;

    // progress has two functions at ETL stage:
    // 1. when progress < 100, it indicates ETL progress
    // 2. set progress = 100 ONLY when ETL progress is completely done
    // 
    // when at LOADING stage, use it normally (as real progress)
    private int progress;

    private long createTimeMs;
    private long etlStartTimeMs;
    private long etlFinishTimeMs;
    private long loadStartTimeMs;
    private long loadFinishTimeMs;
    // not serialize it
    private long quorumFinishTimeMs;
    private FailMsg failMsg;

    private EtlJobType etlJobType;
    private EtlJobInfo etlJobInfo;
    
    private Map<Long, TableLoadInfo> idToTableLoadInfo;
    private Map<Long, TabletLoadInfo> idToTabletLoadInfo;
    private Set<Long> quorumTablets;
    private Set<Long> fullTablets;
    private List<Long> unfinishedTablets;
    private Set<PushTask> pushTasks;
    private Map<Long, ReplicaPersistInfo> replicaPersistInfos;
    
    private Map<Long, Replica> finishedReplicas;
    
    private List<Predicate> conditions = null;
    private DeleteInfo deleteInfo;

    private TResourceInfo resourceInfo;
    
    private TPriority priority;

    private long execMemLimit;

    // save table names for auth check
    private Set<String> tableNames = Sets.newHashSet();

    public LoadJob() {
        this("");
    }

    public LoadJob(String label) {
        this(label, DEFAULT_TIMEOUT_S, Config.default_max_filter_ratio);
    }
    
    // convert an async delete job to load job
    public LoadJob(long id, long dbId, long tableId, long partitionId, String label, 
            Map<Long, Integer> indexIdToSchemaHash, List<Predicate> deleteConditions, 
            DeleteInfo deleteInfo) {
        this.id = id;
        this.dbId = dbId;
        this.tableId = tableId;
        this.label = label; 
        this.transactionId = -1;
        this.timestamp = -1;
        this.timeoutSecond = DEFAULT_TIMEOUT_S;
        this.state = JobState.LOADING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.etlStartTimeMs = -1;
        this.etlFinishTimeMs = -1;
        this.loadStartTimeMs = -1;
        this.loadFinishTimeMs = -1;
        this.quorumFinishTimeMs = -1;
        this.failMsg = new FailMsg(CancelType.UNKNOWN, "");
        this.etlJobType = EtlJobType.DELETE;
        EtlStatus etlStatus = new EtlStatus();
        etlStatus.setState(TEtlState.FINISHED);
        // has to use hadoop etl job info, because replay thread use hadoop job info
        HadoopEtlJobInfo hadoopEtlJobInfo = new HadoopEtlJobInfo();
        hadoopEtlJobInfo.setCluster("");
        hadoopEtlJobInfo.setEtlOutputDir("");
        this.etlJobInfo = hadoopEtlJobInfo;
        this.etlJobInfo.setJobStatus(etlStatus);
        this.idToTableLoadInfo = Maps.newHashMap();;
        this.idToTabletLoadInfo = Maps.newHashMap();;
        this.quorumTablets = new HashSet<Long>();
        this.fullTablets = new HashSet<Long>();
        this.unfinishedTablets = new ArrayList<>();
        this.pushTasks = new HashSet<PushTask>();
        this.replicaPersistInfos = Maps.newHashMap();
        this.resourceInfo = null;
        this.priority = TPriority.NORMAL;
        this.execMemLimit = DEFAULT_EXEC_MEM_LIMIT;
        this.finishedReplicas = Maps.newHashMap();
        
        // generate table load info
        PartitionLoadInfo partitionLoadInfo = new PartitionLoadInfo(null);
        Map<Long, PartitionLoadInfo> idToPartitionLoadInfo = new HashMap<>();
        idToPartitionLoadInfo.put(partitionId, partitionLoadInfo);
        TableLoadInfo tableLoadInfo = new TableLoadInfo(idToPartitionLoadInfo);
        tableLoadInfo.addAllSchemaHash(indexIdToSchemaHash);
        idToTableLoadInfo.put(tableId, tableLoadInfo);
        
        // add delete conditions to load job
        this.conditions = deleteConditions;
        this.deleteInfo = deleteInfo;
    }

    public LoadJob(String label, int timeoutSecond, double maxFilterRatio) {
        this.id = -1;
        this.dbId = -1;
        this.label = label;
        this.transactionId = -1;
        this.timestamp = -1;
        this.timeoutSecond = timeoutSecond;
        this.maxFilterRatio = maxFilterRatio;
        this.state = JobState.PENDING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.etlStartTimeMs = -1;
        this.etlFinishTimeMs = -1;
        this.loadStartTimeMs = -1;
        this.loadFinishTimeMs = -1;
        this.quorumFinishTimeMs = -1;
        this.failMsg = new FailMsg(CancelType.UNKNOWN, "");
        this.etlJobType = EtlJobType.HADOOP;
        this.etlJobInfo = new HadoopEtlJobInfo();
        this.idToTableLoadInfo = null;
        this.idToTabletLoadInfo = null;
        this.quorumTablets = new HashSet<Long>();
        this.fullTablets = new HashSet<Long>();
        this.unfinishedTablets = new ArrayList<>();
        this.pushTasks = new HashSet<PushTask>();
        this.replicaPersistInfos = Maps.newHashMap();
        this.resourceInfo = null;
        this.priority = TPriority.NORMAL;
        this.execMemLimit = DEFAULT_EXEC_MEM_LIMIT;
        this.finishedReplicas = Maps.newHashMap();
    }
    
    public void addTableName(String tableName) {
        tableNames.add(tableName);
    }
    
    public Set<String> getTableNames() {
        return tableNames;
    }

    public long getId() {
        return id;
    }

    public void setId(long jobId) {
        this.id = jobId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        // table id may be 0 for some load job, eg, hadoop load job.
        // use it carefully.
        return tableId;
    }

    public void setDbId(long dbId) {
        this.dbId = dbId;
    }
    
    public long getTransactionId() {
        return transactionId;
    }
    
    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public String getLabel() {
        return label;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimeoutSecond(int timeoutSecond) {
        this.timeoutSecond = timeoutSecond;
    }
    
    public int getTimeoutSecond() {
        return timeoutSecond;
    }
    
    public void setMaxFilterRatio(double maxFilterRatio) {
        this.maxFilterRatio = maxFilterRatio;
    }

    public double getMaxFilterRatio() {
        return maxFilterRatio;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public int getProgress() {
        return progress;
    }

    public void setProgress(int progress) {
        this.progress = progress;
    }

    public long getCreateTimeMs() {
        return createTimeMs;
    }
    
    public void setCreateTimeMs(long createTimeMs) {
        this.createTimeMs = createTimeMs;
    }

    public long getEtlStartTimeMs() {
        return etlStartTimeMs;
    }

    public void setEtlStartTimeMs(long etlStartTimeMs) {
        this.etlStartTimeMs = etlStartTimeMs;
    }

    public long getEtlFinishTimeMs() {
        return etlFinishTimeMs;
    }

    public void setEtlFinishTimeMs(long etlFinishTimeMs) {
        this.etlFinishTimeMs = etlFinishTimeMs;
        if (etlStartTimeMs > -1) {
            long etlCostMs = etlFinishTimeMs - etlStartTimeMs;
            switch (etlJobType) {
                case HADOOP:
                    break;
                case MINI:
                    break;
                case BROKER:
                    break;
                default:
                    break;
            }
        }
    }

    public long getLoadStartTimeMs() {
        return loadStartTimeMs;
    }

    public void setLoadStartTimeMs(long loadStartTimeMs) {
        this.loadStartTimeMs = loadStartTimeMs;
    }

    public long getLoadFinishTimeMs() {
        return loadFinishTimeMs;
    }

    public void setLoadFinishTimeMs(long loadFinishTimeMs) {
        this.loadFinishTimeMs = loadFinishTimeMs;
        long loadCostMs = loadFinishTimeMs - loadStartTimeMs;
        long totalCostMs = loadFinishTimeMs - createTimeMs;
        switch (etlJobType) {
            case HADOOP:
                break;
            case MINI:
                break;
            case BROKER:
                break;
            default:
                break;
        }
    }
    
    public long getQuorumFinishTimeMs() {
        return quorumFinishTimeMs;
    }

    public void setQuorumFinishTimeMs(long quorumFinishTimeMs) {
        this.quorumFinishTimeMs = quorumFinishTimeMs;
    }

    public FailMsg getFailMsg() {
        return failMsg;
    }

    public void setFailMsg(FailMsg failMsg) {
        this.failMsg = failMsg;
    }
    
    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public void setBrokerDesc(BrokerDesc brokerDesc) {
        this.brokerDesc = brokerDesc;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public void setPullLoadSourceInfo(BrokerFileGroupAggInfo sourceInfo) {
        this.pullLoadSourceInfo = sourceInfo;
    }

    public BrokerFileGroupAggInfo getPullLoadSourceInfo() {
        return pullLoadSourceInfo;
    }

    public void setExecMemLimit(long execMemLimit) { this.execMemLimit = execMemLimit; }

    public long getExecMemLimit() { return execMemLimit; }

    public void setEtlJobType(EtlJobType etlJobType) {
        this.etlJobType = etlJobType;
        switch (etlJobType) {
            case HADOOP:
                etlJobInfo = new HadoopEtlJobInfo();
                break;
            case MINI:
                etlJobInfo = new MiniEtlJobInfo();
                break;
            case INSERT:
                etlJobInfo = new EtlJobInfo();
                break;
            default:
                break;
        }
    }

    public EtlJobInfo getEtlJobInfo() {
        return etlJobInfo;
    }

    public String getHadoopCluster() {
        if (etlJobType == EtlJobType.HADOOP) {
            return ((HadoopEtlJobInfo) etlJobInfo).getCluster();
        }
        return FeConstants.null_string;
    }

    public DppConfig getHadoopDppConfig() {
        if (etlJobType == EtlJobType.HADOOP) {
            return ((HadoopEtlJobInfo) etlJobInfo).getDppConfig();
        }
        return null;
    }

    public void setClusterInfo(String cluster, DppConfig dppConfig) {
        if (etlJobType == EtlJobType.HADOOP) {
            HadoopEtlJobInfo hadoopEtlJobInfo = (HadoopEtlJobInfo) etlJobInfo;
            hadoopEtlJobInfo.setCluster(cluster);
            hadoopEtlJobInfo.setDppConfig(dppConfig);
        }
    }

    public void setPriority(TPriority priority) {
        this.priority = priority;
    }

    public TPriority getPriority() {
        return this.priority;
    }

    public String getHadoopEtlOutputDir() {
        if (etlJobType == EtlJobType.HADOOP) {
            return ((HadoopEtlJobInfo) etlJobInfo).getEtlOutputDir();
        }
        return null;
    }

    public void setHadoopEtlOutputDir(String etlOutputDir) {
        if (etlJobType == EtlJobType.HADOOP) {
            ((HadoopEtlJobInfo) etlJobInfo).setEtlOutputDir(etlOutputDir);
        }
    }

    public String getHadoopEtlJobId() {
        if (etlJobType == EtlJobType.HADOOP) {
            return ((HadoopEtlJobInfo) etlJobInfo).getEtlJobId();
        }
        return null;
    }

    public void setHadoopEtlJobId(String etlJobId) {
        if (etlJobType == EtlJobType.HADOOP) {
            ((HadoopEtlJobInfo) etlJobInfo).setEtlJobId(etlJobId);;
        }
    }
        
    public Map<Long, MiniEtlTaskInfo> getMiniEtlTasks() {
        if (etlJobType == EtlJobType.MINI) {
            return ((MiniEtlJobInfo) etlJobInfo).getEtlTasks();
        }
        return null;
    }
    
    public MiniEtlTaskInfo getMiniEtlTask(long taskId) {
        if (etlJobType == EtlJobType.MINI) {
            return ((MiniEtlJobInfo) etlJobInfo).getEtlTask(taskId);
        }
        return null;
    }

    public void setMiniEtlTasks(Map<Long, MiniEtlTaskInfo> idToEtlTask) {
        if (etlJobType == EtlJobType.MINI) {
            ((MiniEtlJobInfo) etlJobInfo).setEtlTasks(idToEtlTask);
        }
    }
    
    public boolean miniNeedGetTaskStatus() {
        if (etlJobType == EtlJobType.MINI) {
            return ((MiniEtlJobInfo) etlJobInfo).needGetTaskStatus();
        }
        return true;
    }

    public EtlStatus getEtlJobStatus() {
        return etlJobInfo.getJobStatus();
    }
    
    public void setEtlJobStatus(EtlStatus etlStatus) {
        etlJobInfo.setJobStatus(etlStatus);
    }
    
    public Map<Long, TableLoadInfo> getIdToTableLoadInfo() {
        return idToTableLoadInfo;
    }
    
    public TableLoadInfo getTableLoadInfo(long tableId) {
        return idToTableLoadInfo.get(tableId);
    }

    public PartitionLoadInfo getPartitionLoadInfo(long tableId, long partitionId) {
        if (!idToTableLoadInfo.containsKey(tableId)) {
            return null;
        }

        TableLoadInfo tableLoadInfo = getTableLoadInfo(tableId);
        return tableLoadInfo.getPartitionLoadInfo(partitionId);
    }

    public void setIdToTableLoadInfo(Map<Long, TableLoadInfo> idToTableLoadInfo) {
        this.idToTableLoadInfo = idToTableLoadInfo;
    }

    public List<Long> getAllTableIds() {
        List<Long> tblIds = Lists.newArrayList();
        if (idToTableLoadInfo != null) {
            tblIds.addAll(idToTableLoadInfo.keySet());
        }
        return tblIds;
    }

    public Map<Long, TabletLoadInfo> getIdToTabletLoadInfo() {
        return idToTabletLoadInfo;
    }
    
    public TabletLoadInfo getTabletLoadInfo(long tabletId) {
        return idToTabletLoadInfo.get(tabletId);
    }

    public void setIdToTabletLoadInfo(Map<Long, TabletLoadInfo> idTotabletLoadInfo) {
        this.idToTabletLoadInfo = idTotabletLoadInfo;
    }
 
    public void addQuorumTablet(long tabletId) {
        quorumTablets.add(tabletId);
    }
    
    public Set<Long> getQuorumTablets() {
        return quorumTablets;
    }
    
    public void clearQuorumTablets() {
        quorumTablets.clear();
    }
   
    public void addFullTablet(long tabletId) {
        fullTablets.add(tabletId);
    }

    public Set<Long> getFullTablets() {
        return fullTablets;
    }
    
    public void setUnfinishedTablets(Set<Long> unfinishedTablets) {
        this.unfinishedTablets.clear();
        this.unfinishedTablets.addAll(unfinishedTablets);
    }
    
    public void addPushTask(PushTask pushTask) {
        pushTasks.add(pushTask);
    }
    
    public Set<PushTask> getPushTasks() {
        return pushTasks;
    }
    
    public Map<Long, ReplicaPersistInfo> getReplicaPersistInfos() {
        return this.replicaPersistInfos;
    }
    
    public void addReplicaPersistInfos(ReplicaPersistInfo info) {
        if (!replicaPersistInfos.containsKey(info.getReplicaId())) {
            replicaPersistInfos.put(info.getReplicaId(), info);
        }
    }

    public void setResourceInfo(TResourceInfo resourceInfo) {
        this.resourceInfo = resourceInfo;
    }

    public TResourceInfo getResourceInfo() {
        return resourceInfo;
    }
    
    public boolean addFinishedReplica(Replica replica) {
        finishedReplicas.put(replica.getId(), replica);
        return true;
    }
    
    public boolean isReplicaFinished(long replicaId) {
        return finishedReplicas.containsKey(replicaId);
    }
    
    public Collection<Replica> getFinishedReplicas() {
        return finishedReplicas.values();
    }

    public List<Predicate> getConditions() {
        return conditions;
    }
    
    public boolean isSyncDeleteJob() {
        if (conditions != null) {
            return true;
        }
        return false;
    }
    
    public DeleteInfo getDeleteInfo() {
        return deleteInfo;
    }
    
    public long getDeleteJobTimeout() {
        // timeout is between 30 seconds to 5 min
        long timeout = Math.max(idToTabletLoadInfo.size() * Config.tablet_delete_timeout_second * 1000L, 30000L);
        return Math.min(timeout, Config.load_straggler_wait_second * 1000L);
    }
    
    @Override
    public String toString() {
        return "LoadJob [id=" + id + ", dbId=" + dbId + ", label=" + label + ", timeoutSecond=" + timeoutSecond
                + ", maxFilterRatio=" + maxFilterRatio + ", state=" + state
                + ", progress=" + progress + ", createTimeMs=" + createTimeMs + ", etlStartTimeMs=" + etlStartTimeMs
                + ", etlFinishTimeMs=" + etlFinishTimeMs + ", loadStartTimeMs=" + loadStartTimeMs
                + ", loadFinishTimeMs=" + loadFinishTimeMs + ", failMsg=" + failMsg + ", etlJobType=" + etlJobType
                + ", etlJobInfo=" + etlJobInfo + ", priority=" + priority + ", transactionId=" + transactionId 
                + ", quorumFinishTimeMs=" + quorumFinishTimeMs 
                + ", unfinished tablets=[" + this.unfinishedTablets.subList(0, Math.min(3, this.unfinishedTablets.size())) + "]" 
                + "]";
    }

    public void clearRedundantInfoForHistoryJob() {
        // retain 'etlJobStatus' for LoadJob proc
        if (idToTableLoadInfo != null) {
            idToTableLoadInfo.clear();
            idToTableLoadInfo = null;
        }
        
        if (idToTabletLoadInfo != null) {
            idToTabletLoadInfo.clear();
            idToTabletLoadInfo = null;
        }
        
        if (quorumTablets != null) {
            quorumTablets.clear();
            quorumTablets = null;
        }
        
        if (fullTablets != null) {
            fullTablets.clear();
            fullTablets = null;
        }
        
        if (replicaPersistInfos != null) {
            replicaPersistInfos.clear();
            replicaPersistInfos = null;
        }

        if (etlJobInfo != null && etlJobType == EtlJobType.HADOOP) {
            DppConfig dppConfig = ((HadoopEtlJobInfo) etlJobInfo).getDppConfig();
            if (dppConfig != null) {
                dppConfig.clear();
            }
        }

        if (pushTasks != null) {
            pushTasks.clear();
            pushTasks = null;
        }

        resourceInfo = null;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        out.writeLong(dbId);
        Text.writeString(out, label);
        out.writeLong(timestamp);
        out.writeInt(timeoutSecond);
        out.writeDouble(maxFilterRatio);
        out.writeBoolean(true); // delete flag, does not use anymore
        Text.writeString(out, state.name());
        out.writeInt(progress);
        out.writeLong(createTimeMs);
        out.writeLong(etlStartTimeMs);
        out.writeLong(etlFinishTimeMs);
        out.writeLong(loadStartTimeMs); 
        out.writeLong(loadFinishTimeMs);
        failMsg.write(out);
        Text.writeString(out, etlJobType.name());
        etlJobInfo.write(out);

        int count = 0;
        if (idToTableLoadInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = idToTableLoadInfo.size();
            out.writeInt(count);
            for (Map.Entry<Long, TableLoadInfo> entry : idToTableLoadInfo.entrySet()) {
                out.writeLong(entry.getKey());
                entry.getValue().write(out);
            }
        }
        
        if (idToTabletLoadInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = idToTabletLoadInfo.size();
            out.writeInt(count);
            for (Map.Entry<Long, TabletLoadInfo> entry : idToTabletLoadInfo.entrySet()) {
                out.writeLong(entry.getKey());
                entry.getValue().write(out);
            }
        }
        
        if (fullTablets == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = fullTablets.size();
            out.writeInt(count);
            for (long id : fullTablets) {
                out.writeLong(id);
            }
        }
        
        if (replicaPersistInfos == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = replicaPersistInfos.size();
            out.writeInt(count);
            for (ReplicaPersistInfo info : replicaPersistInfos.values()) {
                info.write(out);
            }
        }

        // resourceInfo
        if (resourceInfo == null || Strings.isNullOrEmpty(resourceInfo.getGroup())
                || Strings.isNullOrEmpty(resourceInfo.getUser())) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Text.writeString(out, resourceInfo.getUser());
            Text.writeString(out, resourceInfo.getGroup());
        }

        Text.writeString(out, priority.name());

        // Version 24
        if (brokerDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            brokerDesc.write(out);
        }

        if (pullLoadSourceInfo == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            pullLoadSourceInfo.write(out);
        }

        out.writeLong(execMemLimit);
        out.writeLong(transactionId);
        
        if (conditions != null) {
            out.writeBoolean(true);
            count = conditions.size();
            out.writeInt(count);
            for (Predicate predicate : conditions) {
                if (predicate instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) predicate;
                    SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                    String columnName = slotRef.getColumnName();
                    Text.writeString(out, columnName);
                    Text.writeString(out, binaryPredicate.getOp().name());
                    String value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                    Text.writeString(out, value);
                } else if (predicate instanceof IsNullPredicate) {
                    IsNullPredicate isNullPredicate = (IsNullPredicate) predicate;
                    SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                    String columnName = slotRef.getColumnName();
                    Text.writeString(out, columnName);
                    Text.writeString(out, "IS");
                    String value = null;
                    if (isNullPredicate.isNotNull()) {
                        value = "NOT NULL";
                    } else {
                        value = "NULL";
                    }
                    Text.writeString(out, value);
                }
            }
        } else {
            out.writeBoolean(false);
        }
        if (deleteInfo != null) {
            out.writeBoolean(true);
            deleteInfo.write(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeInt(tableNames.size());
        for (String tableName : tableNames) {
            Text.writeString(out, tableName);
        }
    }
    public void readFields(DataInput in) throws IOException {
        long version = Catalog.getCurrentCatalogJournalVersion();

        id = in.readLong();
        dbId = in.readLong();
        label = Text.readString(in);
        if (version >= FeMetaVersion.VERSION_23) {
            timestamp = in.readLong();
        } else {
            timestamp = -1;
        }
        timeoutSecond = in.readInt();
        maxFilterRatio = in.readDouble();
        
        boolean deleteFlag = false;
        if (version >= FeMetaVersion.VERSION_30) {
            deleteFlag = in.readBoolean();
        }

        state = JobState.valueOf(Text.readString(in));
        progress = in.readInt();
        createTimeMs = in.readLong();
        etlStartTimeMs = in.readLong();
        etlFinishTimeMs = in.readLong();
        loadStartTimeMs = in.readLong();
        loadFinishTimeMs = in.readLong();
        failMsg = new FailMsg();
        failMsg.readFields(in);
        String etlJobType = Text.readString(in);
        if (etlJobType.equals("BULK")) {
            setEtlJobType(EtlJobType.MINI);
        } else {
            setEtlJobType(EtlJobType.valueOf(etlJobType));
        }
        etlJobInfo.readFields(in);

        int count = 0;
        if (in.readBoolean()) {
            count = in.readInt();
            idToTableLoadInfo = new HashMap<Long, TableLoadInfo>();
            for (int i = 0; i < count; ++i) {
                long key = in.readLong();
                TableLoadInfo value = new TableLoadInfo();
                value.readFields(in);
                idToTableLoadInfo.put(key, value); 
            }
        }

        if (in.readBoolean()) {
            count = in.readInt();
            idToTabletLoadInfo = new HashMap<Long, TabletLoadInfo>();
            for (int i = 0; i < count; ++i) {
                long key = in.readLong();
                TabletLoadInfo tLoadInfo = new TabletLoadInfo();
                tLoadInfo.readFields(in);
                idToTabletLoadInfo.put(key, tLoadInfo);
            }
        }
        
        if (in.readBoolean()) {
            count = in.readInt();
            fullTablets = new HashSet<Long>();
            for (int i = 0; i < count; ++i) {
                long id = in.readLong();
                fullTablets.add(id);
            }
        }
        
        if (in.readBoolean()) {
            count = in.readInt();
            replicaPersistInfos = Maps.newHashMap();
            for (int i = 0; i < count; ++i) {
                ReplicaPersistInfo info = ReplicaPersistInfo.read(in);
                replicaPersistInfos.put(info.getReplicaId(), info);
            }
        }

        if (in.readBoolean()) {
            String user = Text.readString(in);
            String group = Text.readString(in);
            resourceInfo = new TResourceInfo(user, group);
        }

        if (version >= 3 && version < 7) {
            // bos 3 parameters
            String bosEndpoint = Text.readString(in);
            String bosAccessKey = Text.readString(in);
            String bosSecretAccessKey = Text.readString(in);
        }

        if (version >= FeMetaVersion.VERSION_15) {
            this.priority = TPriority.valueOf(Text.readString(in));
        } else {
            this.priority = TPriority.NORMAL;
        }

        if (version >= FeMetaVersion.VERSION_31) {
            // Broker description
            if (in.readBoolean()) {
                this.brokerDesc = BrokerDesc.read(in);
            }
            // Pull load
            if (in.readBoolean()) {
                this.pullLoadSourceInfo = BrokerFileGroupAggInfo.read(in);
            }
        }

        if (version >= FeMetaVersion.VERSION_34) {
            this.execMemLimit = in.readLong();
        }
        
        if (version >= FeMetaVersion.VERSION_45) {
            this.transactionId = in.readLong();
            if (in.readBoolean()) {
                count = in.readInt();
                conditions = Lists.newArrayList();
                for (int i = 0; i < count; i++) {
                    String key = Text.readString(in);
                    String opStr = Text.readString(in);
                    if (opStr.equalsIgnoreCase("IS")) {
                        String value = Text.readString(in);
                        IsNullPredicate predicate;
                        if (value.equalsIgnoreCase("NOT NULL")) {
                            predicate = new IsNullPredicate(new SlotRef(null, key), true);
                        } else {
                            predicate = new IsNullPredicate(new SlotRef(null, key), true);
                        }
                        conditions.add(predicate);
                    } else {
                        Operator op = Operator.valueOf(opStr);
                        String value = Text.readString(in);
                        BinaryPredicate predicate = new BinaryPredicate(op, new SlotRef(null, key), 
                                new StringLiteral(value));
                        conditions.add(predicate);
                    }
                }
            }
            if (in.readBoolean()) {
                this.deleteInfo = new DeleteInfo();
                this.deleteInfo.readFields(in);
            }
        }

        if (version >= FeMetaVersion.VERSION_43) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                tableNames.add(Text.readString(in));
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof LoadJob)) {
            return false;
        }
        
        LoadJob job = (LoadJob) obj;
        
        if (this.id == job.id) {
            return true;
        }

        return false;
    }

}
