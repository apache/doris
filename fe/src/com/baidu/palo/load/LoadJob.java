// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.load;

import com.baidu.palo.analysis.BrokerDesc;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.FeMetaVersion;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.load.FailMsg.CancelType;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.task.PushTask;
import com.baidu.palo.thrift.TPriority;
import com.baidu.palo.thrift.TResourceInfo;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LoadJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(LoadJob.class);

    // QUORUM_FINISHED state is internal
    // To user, it should be transformed to FINISHED
    public enum JobState {
        PENDING,
        ETL,
        LOADING,
        FINISHED,
        QUORUM_FINISHED,
        CANCELLED
    }
    
    public enum EtlJobType {
        HADOOP,
        MINI,
        INSERT,
        BROKER
    }

    private static final int DEFAULT_TIMEOUT_S = 0;
    private static final double DEFAULT_MAX_FILTER_RATIO = 0;
    private static final long DEFAULT_EXEC_MEM_LIMIT = 2147483648L; // 2GB

    private long id;
    private long dbId;
    private String label;
    long timestamp;
    private int timeoutSecond;
    private double maxFilterRatio;
    private boolean deleteFlag;
    private JobState state;

    private BrokerDesc brokerDesc;
    private PullLoadSourceInfo pullLoadSourceInfo;

    // progress has two functions at ETL stage:
    // 1. when progress < 100, it indicates ETL progress
    // 2. set progress = 100 ONLY when ETL progress is compeletly done
    // 
    // when at LOADING stage, use it normally (as real progress)
    private int progress;

    private long createTimeMs;
    private long etlStartTimeMs;
    private long etlFinishTimeMs;
    private long loadStartTimeMs;
    private long loadFinishTimeMs;
    private FailMsg failMsg;

    private EtlJobType etlJobType;
    private EtlJobInfo etlJobInfo;
    
    private Map<Long, TableLoadInfo> idToTableLoadInfo;
    private Map<Long, TabletLoadInfo> idToTabletLoadInfo;
    private Set<Long> quorumTablets;
    private Set<Long> fullTablets;
    private Set<PushTask> pushTasks;
    private Map<Long, ReplicaPersistInfo> replicaPersistInfos;

    private TResourceInfo resourceInfo;
    
    private TPriority priority;

    private long execMemLimit;

    // save table names for auth check
    private Set<String> tableNames;

    public LoadJob() {
        this("");
    }

    public LoadJob(String label) {
        this(label, DEFAULT_TIMEOUT_S, DEFAULT_MAX_FILTER_RATIO);
    }

    public LoadJob(String label, int timeoutSecond, double maxFilterRatio) {
        this.id = -1;
        this.dbId = -1;
        this.label = label;
        this.timestamp = -1;
        this.timeoutSecond = timeoutSecond;
        this.maxFilterRatio = maxFilterRatio;
        this.deleteFlag = false;
        this.state = JobState.PENDING;
        this.progress = 0;
        this.createTimeMs = System.currentTimeMillis();
        this.etlStartTimeMs = -1;
        this.etlFinishTimeMs = -1;
        this.loadStartTimeMs = -1;
        this.loadFinishTimeMs = -1;
        this.failMsg = new FailMsg(CancelType.UNKNOWN, "");
        this.etlJobType = EtlJobType.HADOOP;
        this.etlJobInfo = new HadoopEtlJobInfo();
        this.idToTableLoadInfo = null;
        this.idToTabletLoadInfo = null;
        this.quorumTablets = new HashSet<Long>();
        this.fullTablets = new HashSet<Long>();
        this.pushTasks = new HashSet<PushTask>();
        this.replicaPersistInfos = Maps.newHashMap();
        this.resourceInfo = null;
        this.priority = TPriority.NORMAL;
        this.execMemLimit = DEFAULT_EXEC_MEM_LIMIT;
        this.tableNames = Sets.newHashSet();
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

    public void setDbId(long dbId) {
        this.dbId = dbId;
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
    
    public void setDeleteFlag(boolean deleteFlag) {
        this.deleteFlag = deleteFlag;
    }

    public boolean getDeleteFlag() {
        return deleteFlag;
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
        } else {
            LOG.info("cmy debug: get load job: {}", toString());
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

    public void setPullLoadSourceInfo(PullLoadSourceInfo sourceInfo) {
        this.pullLoadSourceInfo = sourceInfo;
    }

    public PullLoadSourceInfo getPullLoadSourceInfo() {
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
        return "N/A";
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

    public void setPrority(TPriority priority) {
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
   
    public void addFullTablet(long tabletId) {
        fullTablets.add(tabletId);
    }

    public Set<Long> getFullTablets() {
        return fullTablets;
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

    @Override
    public String toString() {
        return "LoadJob [id=" + id + ", dbId=" + dbId + ", label=" + label + ", timeoutSecond=" + timeoutSecond
                + ", maxFilterRatio=" + maxFilterRatio + ", deleteFlag=" + deleteFlag + ", state=" + state
                + ", progress=" + progress + ", createTimeMs=" + createTimeMs + ", etlStartTimeMs=" + etlStartTimeMs
                + ", etlFinishTimeMs=" + etlFinishTimeMs + ", loadStartTimeMs=" + loadStartTimeMs
                + ", loadFinishTimeMs=" + loadFinishTimeMs + ", failMsg=" + failMsg + ", etlJobType=" + etlJobType
                + ", etlJobInfo=" + etlJobInfo + ", priority=" + priority + "]";
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
        out.writeBoolean(deleteFlag);
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
        if (resourceInfo == null) {
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
        
        deleteFlag = false;
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
                ReplicaPersistInfo info = new ReplicaPersistInfo();
                info.readFields(in);
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
                this.pullLoadSourceInfo = PullLoadSourceInfo.read(in);
            }
        }

        if (version >= FeMetaVersion.VERSION_34) {
            this.execMemLimit = in.readLong();
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
