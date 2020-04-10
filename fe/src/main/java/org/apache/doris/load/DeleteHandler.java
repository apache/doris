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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DeleteStmt;
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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionType;
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
import org.apache.doris.common.MarkedCountDownLatch;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.DeleteJob;
import org.apache.doris.task.DeleteJob.DeleteState;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
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
    private Map<Long, List<DeleteInfo>> dbToDeleteInfos;

    public DeleteHandler() {
        idToDeleteJob = Maps.newConcurrentMap();
        dbToDeleteInfos = Maps.newConcurrentMap();
    }

    public void process(DeleteStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        String partitionName = stmt.getPartitionName();
        List<Predicate> conditions = stmt.getDeleteConditions();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            throw new DdlException("Db does not exist. name: " + dbName);
        }

        DeleteJob deleteJob = null;
        DeleteInfo deleteInfo = null;
        long transactionId;
        MarkedCountDownLatch<Long, Long> countDownLatch;
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new DdlException("Table does not exist. name: " + tableName);
            }

            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("Not olap type table. type: " + table.getType().name());
            }
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
            checkDeleteV2(olapTable, partition, conditions, deleteConditions, true);

            // generate label
            String label = "delete_" + UUID.randomUUID();

            // begin txn here and generate txn id
            transactionId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                    Lists.newArrayList(table.getId()), label,"FE: " + FrontendOptions.getLocalHostAddress(),
                    TransactionState.LoadJobSourceType.FRONTEND, Config.stream_load_default_timeout_second);

            deleteInfo = new DeleteInfo(db.getId(), olapTable.getId(), tableName,
                    partition.getId(), partitionName,
                    -1, 0, deleteConditions);
            deleteJob = new DeleteJob(transactionId, deleteInfo);
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
                        pushTask.setIsSchemaChanging(true);
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
            throw new DdlException(t.getMessage(), t);
        } finally {
            db.readUnlock();
        }

        long timeoutMs = deleteJob.getTimeout();
        LOG.info("waiting delete Job finish, signature: {}, timeout: {}", transactionId, timeoutMs);
        boolean ok = false;
        try {
            ok = countDownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
            ok = false;
        }

        if (ok) {
            commitJob(deleteJob, db, timeoutMs);
        } else {
            deleteJob.checkQuorum();
            if (deleteJob.getState() != DeleteState.UN_QUORUM) {
                long nowQuorumTimeMs = System.currentTimeMillis();
                long endQuorumTimeoutMs = nowQuorumTimeMs + timeoutMs / 2;
                // if job's state is finished or stay in quorum_finished for long time, try to commit it.
                try {
                    while (deleteJob.getState() == DeleteState.QUORUM_FINISHED && endQuorumTimeoutMs > nowQuorumTimeMs) {
                        deleteJob.checkQuorum();
                        Thread.sleep(1000);
                        nowQuorumTimeMs = System.currentTimeMillis();
                    }
                    commitJob(deleteJob, db, timeoutMs);
                } catch (InterruptedException e) {
                }
            } else {
                List<Entry<Long, Long>> unfinishedMarks = countDownLatch.getLeftMarks();
                // only show at most 5 results
                List<Entry<Long, Long>> subList = unfinishedMarks.subList(0, Math.min(unfinishedMarks.size(), 5));
                String errMsg = "Unfinished replicas:" + Joiner.on(", ").join(subList);
                LOG.warn("delete job timeout: {}, {}", transactionId, errMsg);
                cancelJob(deleteJob, "delete job timeout");
                throw new DdlException("failed to delete replicas from job: {}, {}, transactionId, errMsg");
            }
        }
    }

    private void commitJob(DeleteJob job, Database db, long timeout) {
        long transactionId = job.getTransactionId();
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        TransactionState transactionState = globalTransactionMgr.getTransactionState(transactionId);
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<TabletCommitInfo>();
        try {
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
            boolean isSuccess = globalTransactionMgr.commitAndPublishTransaction(db, transactionId, tabletCommitInfos, timeout);
            if (!isSuccess) {
                cancelJob(job, "delete timeout when waiting transaction commit");
            }
        } catch (UserException e) {
            LOG.warn("errors while commit delete job, transaction [{}], reason is {}",
                    transactionState.getTransactionId(), e);
            cancelJob(job, transactionState.getReason());
        }
    }

    private void removeJob(DeleteJob job) {
        long signature = job.getTransactionId();
        if (idToDeleteJob.containsKey(signature)) {
            idToDeleteJob.remove(signature);
        }
        for (PushTask pushTask : job.getPushTasks()) {
            AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                    pushTask.getVersion(), pushTask.getVersionHash(),
                    pushTask.getPushType(), pushTask.getTaskType());
        }
    }

    public void recordFinishedJob(DeleteJob job) {
        if (job!= null) {
            removeJob(job);
            long dbId = job.getDeleteInfo().getDbId();
            List<DeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
            if (deleteInfoList == null) {
                deleteInfoList = Lists.newArrayList();
                dbToDeleteInfos.put(dbId, deleteInfoList);
            }
            deleteInfoList.add(job.getDeleteInfo());
        }
    }

    private boolean cancelJob(DeleteJob job, String reason) {
        try {
            if (job != null) {
                removeJob(job);
                Catalog.getCurrentGlobalTransactionMgr().abortTransaction(
                        job.getTransactionId(), reason);
                return true;
            }
        } catch (Exception e) {
            LOG.info("errors while abort transaction", e);
        }
        return false;
    }

    public DeleteJob getDeleteJob(long transactionId) {
        return idToDeleteJob.get(transactionId);
    }

    private void checkDeleteV2(OlapTable table, Partition partition, List<Predicate> conditions, List<String> deleteConditions, boolean preCheck)
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
            SlotRef slotRef = null;
            if (condition instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                slotRef = (SlotRef) binaryPredicate.getChild(0);
            } else if (condition instanceof IsNullPredicate) {
                IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                slotRef = (SlotRef) isNullPredicate.getChild(0);
            }
            String columnName = slotRef.getColumnName();
            if (!nameToColumn.containsKey(columnName)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, table.getName());
            }

            Column column = nameToColumn.get(columnName);
            if (!column.isKey()) {
                // ErrorReport.reportDdlException(ErrorCode.ERR_NOT_KEY_COLUMN, columnName);
                throw new DdlException("Column[" + columnName + "] is not key column");
            }

            if (condition instanceof BinaryPredicate) {
                String value = null;
                try {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    value = ((LiteralExpr) binaryPredicate.getChild(1)).getStringValue();
                    LiteralExpr.create(value, Type.fromPrimitiveType(column.getDataType()));
                } catch (AnalysisException e) {
                    // ErrorReport.reportDdlException(ErrorCode.ERR_INVALID_VALUE, value);
                    throw new DdlException("Invalid column value[" + value + "]");
                }
            }

            // set schema column name
            slotRef.setCol(column.getName());
        }
        Map<Long, List<Column>> indexIdToSchema = table.getIndexIdToSchema();
        for (MaterializedIndex index : partition.getMaterializedIndices(MaterializedIndex.IndexExtState.VISIBLE)) {
            // check table has condition column
            Map<String, Column> indexColNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
            for (Column column : indexIdToSchema.get(index.getId())) {
                indexColNameToColumn.put(column.getName(), column);
            }
            String indexName = table.getIndexNameById(index.getId());
            for (Predicate condition : conditions) {
                String columnName = null;
                if (condition instanceof BinaryPredicate) {
                    BinaryPredicate binaryPredicate = (BinaryPredicate) condition;
                    columnName = ((SlotRef) binaryPredicate.getChild(0)).getColumnName();
                } else if (condition instanceof IsNullPredicate) {
                    IsNullPredicate isNullPredicate = (IsNullPredicate) condition;
                    columnName = ((SlotRef) isNullPredicate.getChild(0)).getColumnName();
                }
                Column column = indexColNameToColumn.get(columnName);
                if (column == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName, indexName);
                }

                if (table.getKeysType() == KeysType.DUP_KEYS && !column.isKey()) {
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
            }
        }
    }

    // show delete stmt
    public List<List<Comparable>> getDeleteInfosByDb(long dbId, boolean forUser) {
        LinkedList<List<Comparable>> infos = new LinkedList<List<Comparable>>();
        Database db = Catalog.getInstance().getDb(dbId);
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
                // There is no job for delete, set job id to -1
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
        Database db = catalog.getDb(deleteInfo.getDbId());
        db.writeLock();
        try {
            // add to deleteInfos
            long dbId = deleteInfo.getDbId();
            List<DeleteInfo> deleteInfos = dbToDeleteInfos.get(dbId);
            if (deleteInfos == null) {
                deleteInfos = Lists.newArrayList();
                dbToDeleteInfos.put(dbId, deleteInfos);
            }
            deleteInfos.add(deleteInfo);
        } finally {
            db.writeUnlock();
        }
    }

    // for delete handler, we only persist those delete already finished.
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(dbToDeleteInfos.size());
        for (Entry<Long, List<DeleteInfo>> dbToDeleteInfoList : dbToDeleteInfos.entrySet()) {
            out.writeLong(dbToDeleteInfoList.getKey());
            out.writeInt(dbToDeleteInfoList.getValue().size());
            for (DeleteInfo deleteInfo : dbToDeleteInfoList.getValue()) {
                deleteInfo.write(out);
            }
        }
    }

    public void readField(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long dbId = in.readLong();
            int length = in.readInt();
            List<DeleteInfo> deleteInfoList = Lists.newArrayList();
            for (int j = 0; j < length; j++) {
                DeleteInfo deleteInfo = new DeleteInfo();
                deleteInfo.readFields(in);
                deleteInfoList.add(deleteInfo);
            }
            dbToDeleteInfos.put(dbId, deleteInfoList);
        }
    }
}
