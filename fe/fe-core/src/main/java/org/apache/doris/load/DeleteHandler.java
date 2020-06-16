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
import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.DeleteJob.DeleteState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.QueryStateException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class DeleteHandler implements Writable {
    private static final Logger LOG = LogManager.getLogger(DeleteHandler.class);

    // TransactionId -> DeleteJob
    private Map<Long, DeleteJob> idToDeleteJob;

    // Db -> DeleteInfo list
    @SerializedName(value = "dbToDeleteInfos")
    private Map<Long, List<DeleteInfo>> dbToDeleteInfos;

    public DeleteHandler() {
        idToDeleteJob = Maps.newConcurrentMap();
        dbToDeleteInfos = Maps.newConcurrentMap();
    }

    private enum CancelType {
        METADATA_MISSING,
        TIMEOUT,
        COMMIT_FAIL,
        UNKNOWN
    }

    public void process(DeleteStmt stmt) throws DdlException, QueryStateException, MetaNotFoundException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        String partitionName = stmt.getPartitionName();
        List<Predicate> conditions = stmt.getDeleteConditions();
        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + dbName);
        }

        DeleteJob deleteJob = null;
        try {
            MarkedCountDownLatch<Long, Long> countDownLatch;
            long transactionId = -1;
            Table table = db.getTableOrThrowException(tableName, Table.TableType.OLAP);
            table.readLock();
            try {
                OlapTable olapTable = (OlapTable) table;

                if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                    throw new DdlException("Table's state is not normal: " + tableName);
                }

                if (partitionName == null) {
                    if (olapTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                        throw new DdlException("This is a range partitioned table."
                                + " You should specify partition in delete stmt");
                    } else {
                        // this is a unpartitioned table, use table name as partition name
                        partitionName = olapTable.getName();
                    }
                }

                Partition partition = olapTable.getPartition(partitionName);
                if (partition == null) {
                    throw new DdlException("Partition does not exist. name: " + partitionName);
                }

                List<String> deleteConditions = Lists.newArrayList();

                // pre check
                checkDeleteV2(olapTable, partition, conditions, deleteConditions);

                // generate label
                String label = "delete_" + UUID.randomUUID();
                //generate jobId
                long jobId = Catalog.getCurrentCatalog().getNextId();
                // begin txn here and generate txn id
                transactionId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                        Lists.newArrayList(table.getId()), label, null,
                        new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                        TransactionState.LoadJobSourceType.FRONTEND, jobId, Config.stream_load_default_timeout_second);

                DeleteInfo deleteInfo = new DeleteInfo(db.getId(), olapTable.getId(), tableName,
                        partition.getId(), partitionName,
                        -1, 0, deleteConditions);
                deleteJob = new DeleteJob(jobId, transactionId, label, deleteInfo);
                idToDeleteJob.put(deleteJob.getTransactionId(), deleteJob);

                Catalog.getCurrentGlobalTransactionMgr().getCallbackFactory().addCallback(deleteJob);
                // task sent to be
                AgentBatchTask batchTask = new AgentBatchTask();
                // count total replica num
                int totalReplicaNum = 0;
                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        totalReplicaNum += tablet.getReplicas().size();
                    }
                }
                countDownLatch = new MarkedCountDownLatch<Long, Long>(totalReplicaNum);

                for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                    long indexId = index.getId();
                    int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                    for (Tablet tablet : index.getTablets()) {
                        long tabletId = tablet.getId();

                        // set push type
                        TPushType type = TPushType.DELETE;

                        for (Replica replica : tablet.getReplicas()) {
                            long replicaId = replica.getId();
                            long backendId = replica.getBackendId();
                            countDownLatch.addMark(backendId, tabletId);

                            // create push task for each replica
                            PushTask pushTask = new PushTask(null,
                                    replica.getBackendId(), db.getId(), olapTable.getId(),
                                    partition.getId(), indexId,
                                    tabletId, replicaId, schemaHash,
                                    -1, 0, "", -1, 0,
                                    -1, type, conditions,
                                    true, TPriority.NORMAL,
                                    TTaskType.REALTIME_PUSH,
                                    transactionId,
                                    Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId());
                            pushTask.setIsSchemaChanging(false);
                            pushTask.setCountDownLatch(countDownLatch);

                            if (AgentTaskQueue.addTask(pushTask)) {
                                batchTask.addTask(pushTask);
                                deleteJob.addPushTask(pushTask);
                                deleteJob.addTablet(tabletId);
                            }
                        }
                    }
                }

                // submit push tasks
                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }

            } catch (Throwable t) {
                LOG.warn("error occurred during delete process", t);
                // if transaction has been begun, need to abort it
                if (Catalog.getCurrentGlobalTransactionMgr().getTransactionState(db.getId(), transactionId) != null) {
                    cancelJob(deleteJob, CancelType.UNKNOWN, t.getMessage());
                }
                throw new DdlException(t.getMessage(), t);
            } finally {
                table.readUnlock();
            }

            long timeoutMs = deleteJob.getTimeoutMs();
            LOG.info("waiting delete Job finish, signature: {}, timeout: {}", transactionId, timeoutMs);
            boolean ok = false;
            try {
                ok = countDownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOG.warn("InterruptedException: ", e);
                ok = false;
            }

            if (!ok) {
                String errMsg = "";
                List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                // only show at most 5 results
                List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 5));
                if (!subList.isEmpty()) {
                    errMsg = "unfinished replicas: " + Joiner.on(", ").join(subList);
                }
                LOG.warn(errMsg);

                try {
                    deleteJob.checkAndUpdateQuorum();
                } catch (MetaNotFoundException e) {
                    cancelJob(deleteJob, CancelType.METADATA_MISSING, e.getMessage());
                    throw new DdlException(e.getMessage(), e);
                }
                DeleteState state = deleteJob.getState();
                switch (state) {
                    case UN_QUORUM:
                        LOG.warn("delete job timeout: transactionId {}, timeout {}, {}", transactionId, timeoutMs, errMsg);
                        cancelJob(deleteJob, CancelType.TIMEOUT, "delete job timeout");
                        throw new DdlException("failed to execute delete. transaction id " + transactionId +
                                ", timeout(ms) " + timeoutMs + ", " + errMsg);
                    case QUORUM_FINISHED:
                    case FINISHED:
                        try {
                            long nowQuorumTimeMs = System.currentTimeMillis();
                            long endQuorumTimeoutMs = nowQuorumTimeMs + timeoutMs / 2;
                            // if job's state is quorum_finished then wait for a period of time and commit it.
                            while (deleteJob.getState() == DeleteState.QUORUM_FINISHED && endQuorumTimeoutMs > nowQuorumTimeMs) {
                                deleteJob.checkAndUpdateQuorum();
                                Thread.sleep(1000);
                                nowQuorumTimeMs = System.currentTimeMillis();
                                LOG.debug("wait for quorum finished delete job: {}, txn id: {}" + deleteJob.getId(), transactionId);
                            }
                        } catch (MetaNotFoundException e) {
                            cancelJob(deleteJob, CancelType.METADATA_MISSING, e.getMessage());
                            throw new DdlException(e.getMessage(), e);
                        } catch (InterruptedException e) {
                            cancelJob(deleteJob, CancelType.UNKNOWN, e.getMessage());
                            throw new DdlException(e.getMessage(), e);
                        }
                        commitJob(deleteJob, db, table, timeoutMs);
                        break;
                    default:
                        Preconditions.checkState(false, "wrong delete job state: " + state.name());
                        break;
                }
            } else {
                commitJob(deleteJob, db, table, timeoutMs);
            }
        } finally {
            if (!FeConstants.runningUnitTest) {
                clearJob(deleteJob);
            }
        }
    }

    private void commitJob(DeleteJob job, Database db, Table table, long timeoutMs) throws DdlException, QueryStateException {
        TransactionStatus status = null;
        try {
            unprotectedCommitJob(job, db, table, timeoutMs);
            status = Catalog.getCurrentGlobalTransactionMgr().
                    getTransactionState(db.getId(), job.getTransactionId()).getTransactionStatus();
        } catch (UserException e) {
            if (cancelJob(job, CancelType.COMMIT_FAIL, e.getMessage())) {
                throw new DdlException(e.getMessage(), e);
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(job.getLabel()).append("', 'status':'").append(status.name());
        sb.append("', 'txnId':'").append(job.getTransactionId()).append("'");

        switch (status) {
            case COMMITTED: {
                // Although publish is unfinished we should tell user that commit already success.
                String errMsg = "delete job is committed but may be taking effect later";
                sb.append(", 'err':'").append(errMsg).append("'");
                sb.append("}");
                throw new QueryStateException(MysqlStateType.OK, sb.toString());
            }
            case VISIBLE: {
                sb.append("}");
                throw new QueryStateException(MysqlStateType.OK, sb.toString());
            }
            default:
                Preconditions.checkState(false, "wrong transaction status: " + status.name());
                break;
        }
    }

    /**
     * unprotected commit delete job
     * return true when successfully commit and publish
     * return false when successfully commit but publish unfinished.
     * A UserException thrown if both commit and publish failed.
     * @param job
     * @param db
     * @param timeoutMs
     * @return
     * @throws UserException
     */
    private boolean unprotectedCommitJob(DeleteJob job, Database db, Table table, long timeoutMs) throws UserException {
        long transactionId = job.getTransactionId();
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<TabletCommitInfo>();
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        for (TabletDeleteInfo tDeleteInfo : job.getTabletDeleteInfo()) {
            for (Replica replica : tDeleteInfo.getFinishedReplicas()) {
                // the inverted index contains rolling up replica
                Long tabletId = invertedIndex.getTabletIdByReplica(replica.getId());
                if (tabletId == null) {
                    LOG.warn("could not find tablet id for replica {}, the tablet maybe dropped", replica);
                    continue;
                }
                tabletCommitInfos.add(new TabletCommitInfo(tabletId, replica.getBackendId()));
            }
        }
        return globalTransactionMgr.commitAndPublishTransaction(db, Lists.newArrayList(table), transactionId, tabletCommitInfos, timeoutMs);
    }

    /**
     * This method should always be called in the end of the delete process to clean the job.
     * Better put it in finally block.
     * @param job
     */
    private void clearJob(DeleteJob job) {
        if (job != null) {
            long signature = job.getTransactionId();
            if (idToDeleteJob.containsKey(signature)) {
                idToDeleteJob.remove(signature);
            }
            for (PushTask pushTask : job.getPushTasks()) {
                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                        pushTask.getVersion(), pushTask.getVersionHash(),
                        pushTask.getPushType(), pushTask.getTaskType());
            }

            // NOT remove callback from GlobalTransactionMgr's callback factory here.
            // the callback will be removed after transaction is aborted of visible.
        }
    }

    public void recordFinishedJob(DeleteJob job) {
        if (job != null) {
            long dbId = job.getDeleteInfo().getDbId();
            LOG.info("record finished deleteJob, transactionId {}, dbId {}",
                    job.getTransactionId(), dbId);
            dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
            List<DeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
            synchronized (deleteInfoList) {
                deleteInfoList.add(job.getDeleteInfo());
            }
        }
    }

    /**
     * abort delete job
     * return true when successfully abort.
     * return true when some unknown error happened, just ignore it.
     * return false when the job is already committed
     * @param job
     * @param cancelType
     * @param reason
     * @return
     */
    public boolean cancelJob(DeleteJob job, CancelType cancelType, String reason) {
        LOG.info("start to cancel delete job, transactionId: {}, cancelType: {}", job.getTransactionId(), cancelType.name());
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        try {
            if (job != null) {
                globalTransactionMgr.abortTransaction(job.getDeleteInfo().getDbId(), job.getTransactionId(), reason);
            }
        } catch (Exception e) {
            TransactionState state = globalTransactionMgr.getTransactionState(job.getDeleteInfo().getDbId(), job.getTransactionId());
            if (state == null) {
                LOG.warn("cancel delete job failed because txn not found, transactionId: {}", job.getTransactionId());
            } else if (state.getTransactionStatus() == TransactionStatus.COMMITTED || state.getTransactionStatus() == TransactionStatus.VISIBLE) {
                LOG.warn("cancel delete job {} failed because it has been committed, transactionId: {}", job.getTransactionId());
                return false;
            } else {
                LOG.warn("errors while abort transaction", e);
            }
        }
        return true;
    }

    public DeleteJob getDeleteJob(long transactionId) {
        return idToDeleteJob.get(transactionId);
    }

    private SlotRef getSlotRef(Predicate condition) {
        SlotRef slotRef = null;
        if (condition instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
            slotRef = (SlotRef) binaryPredicate.getChild(0);
        } else if (condition instanceof IsNullPredicate) {
            IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
            slotRef = (SlotRef) isNullPredicate.getChild(0);
        } else if (condition instanceof InPredicate) {
            InPredicate inPredicate = (InPredicate) condition;
            slotRef = (SlotRef) inPredicate.getChild(0);
        }
        return slotRef;
    }

    private void checkDeleteV2(OlapTable table, Partition partition, List<Predicate> conditions, List<String> deleteConditions)
            throws DdlException {

        // check partition state
        Partition.PartitionState state = partition.getState();
        if (state != Partition.PartitionState.NORMAL) {
            // ErrorReport.reportDdlException(ErrorCode.ERR_BAD_PARTITION_STATE, partition.getName(), state.name());
            throw new DdlException("Partition[" + partition.getName() + "]' state is not NORMAL: " + state.name());
        }

        // check condition column is key column and condition value
        Map<String, Column> nameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (Column column : table.getBaseSchema()) {
            nameToColumn.put(column.getName(), column);
        }
        for (Predicate condition : conditions) {
            SlotRef slotRef = getSlotRef(condition);
            String columnName = slotRef.getColumnName();
            if (!nameToColumn.containsKey(columnName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
            }

            Column column = nameToColumn.get(columnName);
            // Due to rounding errors, most floating-point numbers end up being slightly imprecise,
            // it also means that numbers expected to be equal often differ slightly, so we do not allow compare with
            // floating-point numbers, floating-point number not allowed in where clause
            if (!column.isKey() && table.getKeysType() != KeysType.DUP_KEYS
                    || column.getDataType().isFloatingPointType()) {
                // ErrorReport.reportDdlException(ErrorCode.ERR_NOT_KEY_COLUMN, columnName);
                throw new DdlException("Column[" + columnName + "] is not key column or storage model " +
                        "is not duplicate or column type is float or double.");
            }

            if (condition instanceof BinaryPredicate) {
                String value = null;
                try {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    // if a bool cond passed to be, be's zone_map cannot handle bool correctly,
                    // change it to a tinyint type here;
                    value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                    if (column.getDataType() == PrimitiveType.BOOLEAN ) {
                        if (value.toLowerCase().equals("true")) {
                            binaryPredicate.setChild(1, LiteralExpr.create("1", Type.TINYINT));
                        } else if (value.toLowerCase().equals("false")) {
                            binaryPredicate.setChild(1, LiteralExpr.create("0", Type.TINYINT));
                        }
                    }
                    LiteralExpr.create(value, Type.fromPrimitiveType(column.getDataType()));
                } catch (AnalysisException e) {
                    // ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_VALUE, value);
                    throw new DdlException("Invalid column value[" + value + "] for column " + columnName);
                }
            } else if (condition instanceof InPredicate) {
                String value = null;
                try {
                    InPredicate inPredicate = (InPredicate) condition;
                    for (int i = 1; i <= inPredicate.getInElementNum(); i++) {
                        value = ((LiteralExpr) inPredicate.getChild(i)).getStringValue();
                        LiteralExpr.create(value, Type.fromPrimitiveType(column.getDataType()));
                    }
                } catch (AnalysisException e) {
                    throw new DdlException("Invalid column value[" + value + "] for column " + columnName);
                }
            }

            // set schema column name
            slotRef.setCol(column.getName());
        }
        Map<Long, List<Column>> indexIdToSchema = table.getIndexIdToSchema();
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            if (table.getBaseIndexId() == index.getId()) {
                continue;
            }

            // check table has condition column
            Map<String, Column> indexColNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Column column : indexIdToSchema.get(index.getId())) {
                indexColNameToColumn.put(column.getName(), column);
            }
            String indexName = table.getIndexNameById(index.getId());
            for (Predicate condition : conditions) {
                SlotRef slotRef = getSlotRef(condition);
                String columnName = slotRef.getColumnName();
                Column column = indexColNameToColumn.get(columnName);
                if (column == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, "index[" + indexName +"]");
                }
                MaterializedIndexMeta indexMeta = table.getIndexIdToMeta().get(index.getId());
                if (indexMeta.getKeysType() != KeysType.DUP_KEYS && !column.isKey()) {
                    throw new DdlException("Column[" + columnName + "] is not key column in index[" + indexName + "]");
                }
            }
        }

        if (deleteConditions == null) {
            return;
        }

        // save delete conditions
        for (Predicate condition : conditions) {
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName).append(" ").append(binaryPredicate.getOp().name()).append(" \"")
                        .append(((LiteralExpr) binaryPredicate.getChild(1)).getStringValue()).append("\"");
                deleteConditions.add(sb.toString());
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                SlotRef slotRef = (SlotRef) isNullPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder sb = new StringBuilder();
                sb.append(columnName);
                if (isNullPredicate.isNotNull()) {
                    sb.append(" IS NOT NULL");
                } else {
                    sb.append(" IS NULL");
                }
                deleteConditions.add(sb.toString());
            } else if (condition instanceof InPredicate) {
                InPredicate inPredicate = (InPredicate) condition;
                SlotRef slotRef = (SlotRef) inPredicate.getChild(0);
                String columnName = slotRef.getColumnName();
                StringBuilder strBuilder = new StringBuilder();
                String notStr = inPredicate.isNotIn() ? "NOT " : "";
                strBuilder.append(columnName).append(" ").append(notStr).append("IN (");
                for (int i = 1; i <= inPredicate.getInElementNum(); ++i) {
                    strBuilder.append(inPredicate.getChild(i).toSql());
                    strBuilder.append((i != inPredicate.getInElementNum()) ? ", " : "");
                }
                strBuilder.append(")");
                deleteConditions.add(strBuilder.toString());
            }
        }
    }

    // show delete stmt
    public List<List<Comparable>> getDeleteInfosByDb(long dbId, boolean forUser) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            return infos;
        }

        String dbName = db.getFullName();
        List<DeleteInfo> deleteInfos = dbToDeleteInfos.get(dbId);
        if (deleteInfos == null) {
            return infos;
        }

        for (DeleteInfo deleteInfo : deleteInfos) {

            if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), dbName,
                    deleteInfo.getTableName(),
                    PrivPredicate.LOAD)) {
                continue;
            }


            List<Comparable> info = Lists.newArrayList();
            if (!forUser) {
                info.add(-1L);
                info.add(deleteInfo.getTableId());
            }
            info.add(deleteInfo.getTableName());
            if (!forUser) {
                info.add(deleteInfo.getPartitionId());
            }
            info.add(deleteInfo.getPartitionName());

            info.add(TimeUtils.longToTimeString(deleteInfo.getCreateTimeMs()));
            String conds = Joiner.on(", ").join(deleteInfo.getDeleteConditions());
            info.add(conds);

            if (!forUser) {
                info.add(deleteInfo.getPartitionVersion());
                info.add(deleteInfo.getPartitionVersionHash());
            }
            // for loading state, should not display loading, show deleting instead
//                if (loadJob.getState() == LoadJob.JobState.LOADING) {
//                    info.add("DELETING");
//                } else {
//                    info.add(loadJob.getState().name());
//                }
            info.add("FINISHED");
            infos.add(info);
        }
        // sort by createTimeMs
        int sortIndex;
        if (!forUser) {
            sortIndex = 5;
        } else {
            sortIndex = 2;
        }
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(sortIndex);
        Collections.sort(infos, comparator);
        return infos;
    }

    public void replayDelete(DeleteInfo deleteInfo, Catalog catalog) {
        // add to deleteInfos
        long dbId = deleteInfo.getDbId();
        LOG.info("replay delete, dbId {}", dbId);
        dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
        List<DeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
        synchronized (deleteInfoList) {
            deleteInfoList.add(deleteInfo);
        }
    }

    // for delete handler, we only persist those delete already finished.
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DeleteHandler read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DeleteHandler.class);
    }
}
