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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.task.PublishVersionTask;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransactionState implements Writable {
    private static final Logger LOG = LogManager.getLogger(TransactionState.class);
    
    public enum LoadJobSourceType {
        FRONTEND(1),        // old dpp load, mini load, insert stmt(not streaming type) use this type
        BACKEND_STREAMING(2),         // streaming load use this type
        INSERT_STREAMING(3), // insert stmt (streaming type) use this type
        ROUTINE_LOAD_TASK(4), // routine load task use this type
        BATCH_LOAD_JOB(5); // load job v2 for broker load
        
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
                case 4:
                    return ROUTINE_LOAD_TASK;
                case 5:
                    return BATCH_LOAD_JOB;
                default:
                    return null;
            }
        }
    }
    
    public enum TxnStatusChangeReason {
        DB_DROPPED,
        TIMEOUT,
        OFFSET_OUT_OF_RANGE,
        PAUSE;

        public static TxnStatusChangeReason fromString(String reasonString) {
            for (TxnStatusChangeReason txnStatusChangeReason : TxnStatusChangeReason.values()) {
                if (reasonString.contains(txnStatusChangeReason.toString())) {
                    return txnStatusChangeReason;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            switch (this) {
                case OFFSET_OUT_OF_RANGE:
                    return "Offset out of range";
                default:
                    return this.name();
            }
        }
    }
    
    private long dbId;
    private long transactionId;
    private String label;
    // timestamp is used to judge whether a begin request is a internal retry request.
    // no need to persist it
    private long timestamp;
    private Map<Long, TableCommitInfo> idToTableCommitInfos;
    // coordinator is show who begin this txn (FE, or one of BE, etc...)
    private String coordinator;
    private TransactionStatus transactionStatus;
    private LoadJobSourceType sourceType;
    private long prepareTime;
    private long commitTime;
    private long finishTime;
    private String reason = "";
    // error replica ids
    private Set<Long> errorReplicas;
    private CountDownLatch latch;
    
    // this state need not to be serialized
    private Map<Long, PublishVersionTask> publishVersionTasks;
    private boolean hasSendTask;
    private long publishVersionTime;
    private TransactionStatus preStatus = null;
    
    private long callbackId = -1;
    private long timeoutMs = Config.stream_load_default_timeout_second;

    // is set to true, we will double the publish timeout
    private boolean prolongPublishTimeout = false;

    // optional
    private TxnCommitAttachment txnCommitAttachment;
    
    private String errorLogUrl = null;

    public TransactionState() {
        this.dbId = -1;
        this.transactionId = -1;
        this.label = "";
        this.timestamp = -1;
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
    
    public TransactionState(long dbId, long transactionId, String label, long timestamp,
                            LoadJobSourceType sourceType, String coordinator, long callbackId, long timeoutMs) {
        this.dbId = dbId;
        this.transactionId = transactionId;
        this.label = label;
        this.timestamp = timestamp;
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
        this.callbackId = callbackId;
        this.timeoutMs = timeoutMs;
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
    
    public long getTimestamp() {
        return timestamp;
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
    
    public TxnCommitAttachment getTxnCommitAttachment() {
        return txnCommitAttachment;
    }

    public long getCallbackId() {
        return callbackId;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setErrorLogUrl(String errorLogUrl) {
        this.errorLogUrl = errorLogUrl;
    }

    public String getErrorLogUrl() {
        return errorLogUrl;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        // status changed
        this.preStatus = this.transactionStatus;
        this.transactionStatus = transactionStatus;
        
        // after status changed
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

    public void beforeStateTransform(TransactionStatus transactionStatus) throws TransactionException {
        // before status changed
        TxnStateChangeCallback callback = Catalog.getCurrentGlobalTransactionMgr()
                .getCallbackFactory().getCallback(callbackId);
        if (callback != null) {
            switch (transactionStatus) {
                case ABORTED:
                    callback.beforeAborted(this);
                    break;
                case COMMITTED:
                    callback.beforeCommitted(this);
                    break;
                default:
                    break;
            }
        } else if (callback == null && callbackId > 0) {
            switch (transactionStatus) {
                case COMMITTED:
                    // Maybe listener has been deleted. The txn need to be aborted later.
                    throw new TransactionException("Failed to commit txn when callback could not be found");
                default:
                    break;
            }
        }
    }

    public void afterStateTransform(TransactionStatus transactionStatus, boolean txnOperated) throws UserException {
        afterStateTransform(transactionStatus, txnOperated, null);
    }

    public void afterStateTransform(TransactionStatus transactionStatus, boolean txnOperated, String txnStatusChangeReason)
            throws UserException {
        // after status changed
        TxnStateChangeCallback callback = Catalog.getCurrentGlobalTransactionMgr()
                .getCallbackFactory().getCallback(callbackId);
        if (callback != null) {
            switch (transactionStatus) {
                case ABORTED:
                    callback.afterAborted(this, txnOperated, txnStatusChangeReason);
                    break;
                case COMMITTED:
                    callback.afterCommitted(this, txnOperated);
                    break;
                case VISIBLE:
                    callback.afterVisible(this, txnOperated);
                    break;
                default:
                    break;
            }
        }
    }
    
    public void replaySetTransactionStatus() {
        TxnStateChangeCallback callback = Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().getCallback(
                callbackId);
        if (callback != null) {
            if (transactionStatus == TransactionStatus.ABORTED) {
                callback.replayOnAborted(this);
            } else if (transactionStatus == TransactionStatus.COMMITTED) {
                callback.replayOnCommitted(this);
            } else if (transactionStatus == TransactionStatus.VISIBLE) {
                callback.replayOnVisible(this);
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
        this.reason = Strings.nullToEmpty(reason);
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
    
    public void setTxnCommitAttachment(TxnCommitAttachment txnCommitAttachment) {
        this.txnCommitAttachment = txnCommitAttachment;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TransactionState. ");
        sb.append("transaction id: ").append(transactionId);
        sb.append(", label: ").append(label);
        sb.append(", db id: ").append(dbId);
        sb.append(", coordinator: ").append(coordinator);
        sb.append(", transaction status: ").append(transactionStatus);
        sb.append(", error replicas num: ").append(errorReplicas.size());
        sb.append(", replica ids: ").append(Joiner.on(",").join(errorReplicas.stream().limit(5).toArray()));
        sb.append(", prepare time: ").append(prepareTime);
        sb.append(", commit time: ").append(commitTime);
        sb.append(", finish time: ").append(finishTime);
        sb.append(", reason: ").append(reason);
        if (txnCommitAttachment != null) {
            sb.append(" attactment: ").append(txnCommitAttachment);
        }
        return sb.toString();
    }
    
    public LoadJobSourceType getSourceType() {
        return sourceType;
    }
    
    public Map<Long, PublishVersionTask> getPublishVersionTasks() {
        return publishVersionTasks;
    }
    
    public boolean isPublishTimeout() {
        // the max timeout is Config.publish_version_timeout_second * 2;
        long timeoutMillis = Config.publish_version_timeout_second * 1000;
        if (prolongPublishTimeout) {
            timeoutMillis *= 2;
        }
        return System.currentTimeMillis() - publishVersionTime > timeoutMillis;
    }
    
    public void prolongPublishTimeout() {
        this.prolongPublishTimeout = true;
        LOG.info("prolong the timeout of publish version task for transaction: {}", transactionId);
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

        if (txnCommitAttachment == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            txnCommitAttachment.write(out);
        }
        out.writeLong(callbackId);
        out.writeLong(timeoutMs);
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

        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_49) {
            if (in.readBoolean()) {
                txnCommitAttachment = TxnCommitAttachment.read(in);
            }
            callbackId = in.readLong();
            timeoutMs = in.readLong();
        }
    }
}
