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

package org.apache.doris.transaction;

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.task.PublishVersionTask;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransactionState implements Writable {
    
    public enum LoadJobSourceType {
        FRONTEND(1),        // old dpp load, mini load, insert stmt(not streaming type) use this type
        BACKEND_STREAMING(2),         // streaming load use this type
        INSERT_STREAMING(3); // insert stmt (streaming type) use this type
        
        private final int flag;
        
        private LoadJobSourceType(int flag) {
            this.flag = flag;
        }
        
        public int value() {
            return flag;
        }
        
        public static LoadJobSourceType valueOf(int flag) {
            switch (flag) {
                case 1:
                    return FRONTEND;
                case 2:
                    return BACKEND_STREAMING;
                case 3:
                    return INSERT_STREAMING;
                default:
                    return null;
            }
        }
        
        @Override
        public String toString() {
            switch (this) {
                case FRONTEND:
                    return "frontend";
                case BACKEND_STREAMING:
                    return "backend_streaming";
                case INSERT_STREAMING:
                    return "insert_streaming";
                default:
                    return null;
            }
        }
    }

    private long dbId;
    private long transactionId;
    private String label;
    private Map<Long, TableCommitInfo> idToTableCommitInfos;
    private String coordinator;
    private TransactionStatus transactionStatus;
    private LoadJobSourceType sourceType;
    private long prepareTime;
    private long commitTime;
    private long finishTime;
    private String reason;
    private Set<Long> errorReplicas;
    private CountDownLatch latch;
    
    // this state need not to be serialized
    private Map<Long, PublishVersionTask> publishVersionTasks;
    private boolean hasSendTask;
    private long publishVersionTime;
    private TransactionStatus preStatus = null;
    
    public TransactionState() {
        this.dbId = -1;
        this.transactionId = -1;
        this.label = "";
        this.idToTableCommitInfos = Maps.newHashMap();
        this.coordinator = "";
        this.transactionStatus = TransactionStatus.PREPARE;
        this.sourceType = LoadJobSourceType.FRONTEND;
        this.prepareTime = -1;
        this.commitTime = -1;
        this.finishTime = -1;
        this.reason = "";
        this.errorReplicas = Sets.newHashSet();
        this.publishVersionTasks = Maps.newHashMap();
        this.hasSendTask = false;
        this.latch = new CountDownLatch(1);
    }
    
    public TransactionState(long dbId, long transactionId, String label, LoadJobSourceType sourceType, String coordinator) {
        this.dbId = dbId;
        this.transactionId = transactionId;
        this.label = label;
        this.idToTableCommitInfos = Maps.newHashMap();
        this.coordinator = coordinator;
        this.transactionStatus = TransactionStatus.PREPARE;
        this.sourceType = sourceType;
        this.prepareTime = -1;
        this.commitTime = -1;
        this.finishTime = -1;
        this.reason = "";
        this.errorReplicas = Sets.newHashSet();
        this.publishVersionTasks = Maps.newHashMap();
        this.hasSendTask = false;
        this.latch = new CountDownLatch(1);
    }

    public void setErrorReplicas(Set<Long> newErrorReplicas) {
        this.errorReplicas = newErrorReplicas;
    }

    public boolean isRunning() {
        if (transactionStatus == TransactionStatus.PREPARE 
                || transactionStatus == TransactionStatus.COMMITTED) {
            return true;
        }
        return false;
    }
    
    public void addPublishVersionTask(Long backendId, PublishVersionTask task) {
        this.publishVersionTasks.put(backendId, task);
    }
    
    public void setHasSendTask(boolean hasSendTask) {
        this.hasSendTask = hasSendTask;
        this.publishVersionTime = System.currentTimeMillis();
    }
    
    public void updateSendTaskTime() {
        this.publishVersionTime = System.currentTimeMillis();
    }
    
    public long getPublishVersionTime() {
        return this.publishVersionTime;
    }
    
    public boolean hasSendTask() {
        return this.hasSendTask;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(transactionId);
        Text.writeString(out, label);
        out.writeLong(dbId);
        out.writeInt(idToTableCommitInfos.size());
        for (TableCommitInfo info : idToTableCommitInfos.values()) {
            info.write(out);
        }
        Text.writeString(out, coordinator);
        out.writeInt(transactionStatus.value());
        out.writeInt(sourceType.value());
        out.writeLong(prepareTime);
        out.writeLong(commitTime);
        out.writeLong(finishTime);
        Text.writeString(out, reason);
        out.writeInt(errorReplicas.size());
        for (long errorReplciaId : errorReplicas) {
            out.writeLong(errorReplciaId);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        transactionId = in.readLong();
        label = Text.readString(in);
        dbId = in.readLong();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TableCommitInfo info = new TableCommitInfo();
            info.readFields(in);
            idToTableCommitInfos.put(info.getTableId(), info);
        }
        coordinator = Text.readString(in);
        transactionStatus = TransactionStatus.valueOf(in.readInt());
        sourceType = LoadJobSourceType.valueOf(in.readInt());
        prepareTime = in.readLong();
        commitTime = in.readLong();
        finishTime = in.readLong();
        reason = Text.readString(in);
        int errorReplicaNum = in.readInt();
        for (int i = 0; i < errorReplicaNum; ++i) {
            errorReplicas.add(in.readLong());
        }
    }

    public long getTransactionId() {
        return transactionId;
    }

    public String getLabel() {
        return this.label;
    }
    
    public String getCoordinator() {
        return coordinator;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public long getPrepareTime() {
        return prepareTime;
    }

    public long getCommitTime() {
        return commitTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public String getReason() {
        return reason;
    }
    
    public TransactionStatus getPreStatus() {
        return this.preStatus;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        this.preStatus = this.transactionStatus;
        this.transactionStatus = transactionStatus;
        if (transactionStatus == TransactionStatus.VISIBLE) {
            this.latch.countDown();
            if (MetricRepo.isInit.get()) {
                MetricRepo.COUNTER_TXN_SUCCESS.increase(1L);
            }
        } else if (transactionStatus == TransactionStatus.ABORTED) {
            if (MetricRepo.isInit.get()) {
                MetricRepo.COUNTER_TXN_FAILED.increase(1L);
            }
        }
    }
    
    public void waitTransactionVisible(long timeoutMillis) throws InterruptedException {
        this.latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public void setPrepareTime(long prepareTime) {
        this.prepareTime = prepareTime;
    }

    public void setCommitTime(long commitTime) {
        this.commitTime = commitTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
    
    public Set<Long> getErrorReplicas() {
        return this.errorReplicas;
    }

    public long getDbId() {
        return dbId;
    }

    public Map<Long, TableCommitInfo> getIdToTableCommitInfos() {
        return idToTableCommitInfos;
    }

    public void putIdToTableCommitInfo(long tableId, TableCommitInfo tableCommitInfo) {
        idToTableCommitInfos.put(tableId, tableCommitInfo);
    }
    
    public TableCommitInfo getTableCommitInfo(long tableId) {
        return this.idToTableCommitInfos.get(tableId);
    }
    
    public void removeTable(long tableId) {
        this.idToTableCommitInfos.remove(tableId);
    }

    @Override
    public String toString() {
        return "TransactionState [transactionId=" + transactionId 
                + ", label=" + label 
                + ", dbId=" + dbId 
                + ", coordinator=" + coordinator 
                + ", loadjobsource=" + sourceType
                + ", transactionStatus=" + transactionStatus 
                + ", errorReplicas=" + errorReplicas 
                + ", prepareTime="
                + prepareTime + ", commitTime=" + commitTime + ", finishTime="
                + finishTime + ", reason=" + reason + "]";
    }

    public LoadJobSourceType getSourceType() {
        return sourceType;
    }

    public Map<Long, PublishVersionTask> getPublishVersionTasks() {
        return publishVersionTasks;
    }

    public boolean isPublishTimeout() {
        // timeout is between 3 to Config.max_txn_publish_waiting_time_ms seconds.
        long timeoutMillis = Math.min(Config.publish_version_timeout_second * publishVersionTasks.size() * 1000,
                                      Config.load_straggler_wait_second * 1000);
        timeoutMillis = Math.max(timeoutMillis, 3000);
        return System.currentTimeMillis() - publishVersionTime > timeoutMillis;
    }
}
