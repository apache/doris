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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransactionState implements Writable {
    private static final Logger LOG = LogManager.getLogger(TransactionState.class);

    // compare the TransactionState by txn id, desc
    public static class TxnStateComparator implements Comparator<TransactionState> {
        @Override
        public int compare(TransactionState t1, TransactionState t2) {
            return Long.compare(t2.getTransactionId(), t1.getTransactionId());
        }
    }

    public static final TxnStateComparator TXN_ID_COMPARATOR = new TxnStateComparator();

    public enum LoadJobSourceType {
        FRONTEND(1),        // old dpp load, mini load, insert stmt(not streaming type) use this type
        BACKEND_STREAMING(2),         // streaming load use this type
        INSERT_STREAMING(3), // insert stmt (streaming type), update stmt use this type
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
        PAUSE,
        NO_PARTITIONS;

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
                case NO_PARTITIONS:
                    return "all partitions have no load data";
                default:
                    return this.name();
            }
        }
    }

    public enum TxnSourceType {
        FE(1),
        BE(2);

        public int value() {
            return flag;
        }

        private int flag;

        TxnSourceType(int flag) {
            this.flag = flag;
        }

        public static TxnSourceType valueOf(int flag) {
            switch (flag) {
                case 1:
                    return FE;
                case 2:
                    return BE;
                default:
                    return null;
            }
        }
    }

    public static class TxnCoordinator {
        @SerializedName(value = "sourceType")
        public TxnSourceType sourceType;
        @SerializedName(value = "ip")
        public String ip;

        public TxnCoordinator() {
        }

        public TxnCoordinator(TxnSourceType sourceType, String ip) {
            this.sourceType = sourceType;
            this.ip = ip;
        }

        @Override
        public String toString() {
            return sourceType.toString() + ": " + ip;
        }
    }

    @SerializedName(value = "dbId")
    private long dbId;
    @SerializedName(value = "tableIdList")
    @Setter
    @Getter
    private List<Long> tableIdList;
    private int replicaNum = 0;
    @SerializedName(value = "txnId")
    private long transactionId;
    @SerializedName(value = "label")
    private String label;
    // requestId is used to judge whether a begin request is a internal retry request.
    // no need to persist it.
    private TUniqueId requestId;
    @SerializedName(value = "idToTableCommitInfos")
    private Map<Long, TableCommitInfo> idToTableCommitInfos;
    // coordinator is show who begin this txn (FE, or one of BE, etc...)
    @SerializedName(value = "txnCoordinator")
    private TxnCoordinator txnCoordinator;
    @SerializedName(value = "txnStatus")
    private TransactionStatus transactionStatus;
    @SerializedName(value = "sourceType")
    private LoadJobSourceType sourceType;
    @SerializedName(value = "prepareTime")
    private long prepareTime;
    @SerializedName(value = "preCommitTime")
    private long preCommitTime;
    @SerializedName(value = "commitTime")
    private long commitTime;
    @SerializedName(value = "finishTime")
    private long finishTime;
    @SerializedName(value = "reason")
    private String reason = "";
    // error replica ids
    @SerializedName(value = "errorReplicas")
    private Set<Long> errorReplicas;
    // this latch will be counted down when txn status change to VISIBLE
    private CountDownLatch visibleLatch;

    // this state need not be serialized
    private Map<Long, PublishVersionTask> publishVersionTasks;
    private boolean hasSendTask;
    private long publishVersionTime = -1;
    private TransactionStatus preStatus = null;

    // When publish txn, if every tablet has at least 1 replica published succ, but not quorum replicas succ,
    // and time since firstPublishOneSuccTime has exceeds Config.publish_wait_time_second,
    // then this transaction will become visible.
    private long firstPublishOneSuccTime = -1;

    @SerializedName(value = "callbackId")
    private long callbackId = -1;

    // In the beforeStateTransform() phase, we will get the callback object through the callbackId,
    // and if we get it, we will save it in this variable.
    // The main function of this variable is to retain a reference to this callback object.
    // In order to prevent in the afterStateTransform() phase the callback object may have been removed
    // from the CallbackFactory, resulting in the inability to obtain the object, causing some bugs
    // such as
    // 1. the write lock of callback object has been called in beforeStateTransform()
    // 2. callback object has been removed from CallbackFactory
    // 3. in afterStateTransform(), callback object can not be found, so the write lock can not be released.
    private TxnStateChangeCallback callback = null;
    @SerializedName(value = "timeoutMs")
    private long timeoutMs = Config.stream_load_default_timeout_second * 1000;
    private long preCommittedTimeoutMs = Config.stream_load_default_precommit_timeout_second * 1000;

    // is set to true, we will double the publish timeout
    private boolean prolongPublishTimeout = false;

    // optional
    @SerializedName(value = "txnCommitAttachment")
    private TxnCommitAttachment txnCommitAttachment;

    // this map should be set when load execution begin, so that when the txn commit, it will know
    // which tables and rollups it loaded.
    // tbl id -> (index ids)
    private Map<Long, Set<Long>> loadedTblIndexes = Maps.newHashMap();

    /**
     * the value is the num delta rows of all replicas in each table
     */
    private final Map<Long, Long> tableIdToTotalNumDeltaRows = Maps.newHashMap();

    private String errorLogUrl = null;

    // record some error msgs during the transaction operation.
    // this msg will be shown in show proc "/transactions/dbId/";
    // no need to persist.
    private String errMsg = "";

    public class SchemaInfo {
        public List<Column> schema;
        public int schemaVersion;

        public SchemaInfo(OlapTable olapTable) {
            Map<Long, MaterializedIndexMeta> indexIdToMeta = olapTable.getIndexIdToMeta();
            for (MaterializedIndexMeta indexMeta : indexIdToMeta.values()) {
                schema = indexMeta.getSchema();
                schemaVersion = indexMeta.getSchemaVersion();
                break;
            }
        }
    }

    private boolean isPartialUpdate = false;
    // table id -> schema info
    private Map<Long, SchemaInfo> txnSchemas = new HashMap<>();

    public TransactionState() {
        this.dbId = -1;
        this.tableIdList = Lists.newArrayList();
        this.transactionId = -1;
        this.label = "";
        this.idToTableCommitInfos = Maps.newHashMap();
        this.txnCoordinator = new TxnCoordinator(TxnSourceType.FE, "127.0.0.1"); // mocked, to avoid NPE
        this.transactionStatus = TransactionStatus.PREPARE;
        this.sourceType = LoadJobSourceType.FRONTEND;
        this.prepareTime = -1;
        this.preCommitTime = -1;
        this.commitTime = -1;
        this.finishTime = -1;
        this.reason = "";
        this.errorReplicas = Sets.newHashSet();
        this.publishVersionTasks = Maps.newHashMap();
        this.hasSendTask = false;
        this.visibleLatch = new CountDownLatch(1);
    }

    public TransactionState(long dbId, List<Long> tableIdList, long transactionId, String label, TUniqueId requestId,
            LoadJobSourceType sourceType, TxnCoordinator txnCoordinator, long callbackId, long timeoutMs) {
        this.dbId = dbId;
        this.tableIdList = (tableIdList == null ? Lists.newArrayList() : tableIdList);
        this.transactionId = transactionId;
        this.label = label;
        this.requestId = requestId;
        this.idToTableCommitInfos = Maps.newHashMap();
        this.txnCoordinator = txnCoordinator;
        this.transactionStatus = TransactionStatus.PREPARE;
        this.sourceType = sourceType;
        this.prepareTime = -1;
        this.preCommitTime = -1;
        this.commitTime = -1;
        this.finishTime = -1;
        this.reason = "";
        this.errorReplicas = Sets.newHashSet();
        this.publishVersionTasks = Maps.newHashMap();
        this.hasSendTask = false;
        this.visibleLatch = new CountDownLatch(1);
        this.callbackId = callbackId;
        this.timeoutMs = timeoutMs;
    }

    public void setErrorReplicas(Set<Long> newErrorReplicas) {
        this.errorReplicas = newErrorReplicas;
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

    public TUniqueId getRequestId() {
        return requestId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public String getLabel() {
        return this.label;
    }

    public TxnCoordinator getCoordinator() {
        return txnCoordinator;
    }

    public TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    public long getPrepareTime() {
        return prepareTime;
    }

    public long getPreCommitTime() {
        return preCommitTime;
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

    public long getFirstPublishOneSuccTime() {
        return firstPublishOneSuccTime;
    }

    public void setFirstPublishOneSuccTime(long firstPublishOneSuccTime) {
        this.firstPublishOneSuccTime = firstPublishOneSuccTime;
    }

    public void setTransactionStatus(TransactionStatus transactionStatus) {
        // status changed
        this.preStatus = this.transactionStatus;
        this.transactionStatus = transactionStatus;

        // after status changed
        if (transactionStatus == TransactionStatus.VISIBLE) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_SUCCESS.increase(1L);
            }
        } else if (transactionStatus == TransactionStatus.ABORTED) {
            if (MetricRepo.isInit) {
                MetricRepo.COUNTER_TXN_FAILED.increase(1L);
            }
        }
    }

    public void beforeStateTransform(TransactionStatus transactionStatus) throws TransactionException {
        // before status changed
        callback = Env.getCurrentGlobalTransactionMgr().getCallbackFactory().getCallback(callbackId);
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
                    throw new TransactionException(
                            "Failed to commit txn when callback " + callbackId + "could not be found");
                default:
                    break;
            }
        }
    }

    public void afterStateTransform(TransactionStatus transactionStatus, boolean txnOperated) throws UserException {
        afterStateTransform(transactionStatus, txnOperated, null);
    }

    public void afterStateTransform(TransactionStatus transactionStatus,
            boolean txnOperated, String txnStatusChangeReason) throws UserException {
        // after status changed
        if (callback == null) {
            callback = Env.getCurrentGlobalTransactionMgr().getCallbackFactory().getCallback(callbackId);
        }
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
        TxnStateChangeCallback callback = Env.getCurrentGlobalTransactionMgr().getCallbackFactory()
                .getCallback(callbackId);
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

    public void countdownVisibleLatch() {
        this.visibleLatch.countDown();
    }

    public void waitTransactionVisible(long timeoutMillis) throws InterruptedException {
        this.visibleLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public void setPrepareTime(long prepareTime) {
        this.prepareTime = prepareTime;
    }

    public void setPreCommitTime(long preCommitTime) {
        this.preCommitTime = preCommitTime;
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

    public List<Long> getTableIdList() {
        return tableIdList;
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

    public void setTxnCommitAttachment(TxnCommitAttachment txnCommitAttachment) {
        this.txnCommitAttachment = txnCommitAttachment;
    }

    // return true if txn is in final status and label is expired
    public boolean isExpired(long currentMillis) {
        if (!transactionStatus.isFinalStatus()) {
            return false;
        }
        long expireTime = Config.label_keep_max_second;
        if (isShortTxn()) {
            expireTime = Config.streaming_label_keep_max_second;
        }
        return (currentMillis - finishTime) / 1000 > expireTime;
    }

    // Return true if this txn is related to streaming load/insert/routine load task.
    // We call these tasks "Short" tasks because they will be cleaned up in a short time after they are finished.
    public boolean isShortTxn() {
        return sourceType == LoadJobSourceType.BACKEND_STREAMING || sourceType == LoadJobSourceType.INSERT_STREAMING
                || sourceType == LoadJobSourceType.ROUTINE_LOAD_TASK;
    }

    // return true if txn is running but timeout
    public boolean isTimeout(long currentMillis) {
        return (transactionStatus == TransactionStatus.PREPARE
                && currentMillis - prepareTime > timeoutMs)
                || (transactionStatus == TransactionStatus.PRECOMMITTED
                && currentMillis - preCommitTime > preCommittedTimeoutMs);
    }

    public synchronized void addTableIndexes(OlapTable table) {
        Set<Long> indexIds = loadedTblIndexes.computeIfAbsent(table.getId(), k -> Sets.newHashSet());
        // always equal the index ids
        indexIds.clear();
        indexIds.addAll(table.getIndexIdToMeta().keySet());
    }

    public Map<Long, Set<Long>> getLoadedTblIndexes() {
        return loadedTblIndexes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TransactionState. ");
        sb.append("transaction id: ").append(transactionId);
        sb.append(", label: ").append(label);
        sb.append(", db id: ").append(dbId);
        sb.append(", table id list: ").append(StringUtils.join(tableIdList, ","));
        sb.append(", callback id: ").append(callbackId);
        sb.append(", coordinator: ").append(txnCoordinator.toString());
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

    public String toJson() {
        return GsonUtils.GSON.toJson(this);
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
        out.writeInt(txnCoordinator.sourceType.value());
        Text.writeString(out, txnCoordinator.ip);
        out.writeInt(transactionStatus.value());
        out.writeInt(sourceType.value());
        out.writeLong(prepareTime);
        out.writeLong(preCommitTime);
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
        out.writeInt(tableIdList.size());
        for (Long aLong : tableIdList) {
            out.writeLong(aLong);
        }
    }

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
        txnCoordinator = new TxnCoordinator(TxnSourceType.valueOf(in.readInt()), Text.readString(in));
        transactionStatus = TransactionStatus.valueOf(in.readInt());
        sourceType = LoadJobSourceType.valueOf(in.readInt());
        prepareTime = in.readLong();
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_107) {
            preCommitTime = in.readLong();
        }
        commitTime = in.readLong();
        finishTime = in.readLong();
        reason = Text.readString(in);
        int errorReplicaNum = in.readInt();
        for (int i = 0; i < errorReplicaNum; ++i) {
            errorReplicas.add(in.readLong());
        }

        if (in.readBoolean()) {
            txnCommitAttachment = TxnCommitAttachment.read(in);
        }
        callbackId = in.readLong();
        timeoutMs = in.readLong();
        tableIdList = Lists.newArrayList();
        int tableListSize = in.readInt();
        for (int i = 0; i < tableListSize; i++) {
            tableIdList.add(in.readLong());
        }
    }

    public Map<Long, Long> getTableIdToTotalNumDeltaRows() {
        return tableIdToTotalNumDeltaRows;
    }

    public void setTableIdToTotalNumDeltaRows(Map<Long, Long> tableIdToTotalNumDeltaRows) {
        this.tableIdToTotalNumDeltaRows.putAll(tableIdToTotalNumDeltaRows);
    }

    public void setErrorMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public void clearErrorMsg() {
        this.errMsg = "";
    }

    public String getErrMsg() {
        return this.errMsg;
    }

    public void setSchemaForPartialUpdate(OlapTable olapTable) {
        // the caller should hold the read lock of the table
        isPartialUpdate = true;
        txnSchemas.put(olapTable.getId(), new SchemaInfo(olapTable));
    }

    public boolean isPartialUpdate() {
        return isPartialUpdate;
    }

    public SchemaInfo getTxnSchema(long id) {
        return txnSchemas.get(id);
    }

    public boolean checkSchemaCompatibility(OlapTable olapTable) {
        SchemaInfo currentSchemaInfo = new SchemaInfo(olapTable);
        SchemaInfo txnSchemaInfo = txnSchemas.get(olapTable.getId());
        if (txnSchemaInfo == null) {
            return true;
        }
        if (txnSchemaInfo.schemaVersion >= currentSchemaInfo.schemaVersion) {
            return true;
        }
        for (Column txnCol : txnSchemaInfo.schema) {
            if (!txnCol.isVisible() || !txnCol.getType().isStringType()) {
                continue;
            }
            int uniqueId = txnCol.getUniqueId();
            Optional<Column> currentCol = currentSchemaInfo.schema.stream()
                    .filter(col -> col.getUniqueId() == uniqueId).findFirst();
            // for now Doris's light schema change only supports adding columns,
            // dropping columns, and type conversions that increase the varchar length
            if (currentCol.isPresent() && currentCol.get().getType().isStringType()) {
                if (currentCol.get().getStrLen() != txnCol.getStrLen()) {
                    LOG.warn("Check schema compatibility failed, txnId={}, table={}",
                            transactionId, olapTable.getName());
                    return false;
                }
            }
        }
        return true;
    }
}
