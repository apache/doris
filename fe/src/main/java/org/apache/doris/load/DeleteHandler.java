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
import org.apache.doris.task.DeleteTask;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DeleteHandler implements Writable {
    private static final Logger LOG = LogManager.getLogger(DeleteHandler.class);

    private Catalog catalog;

    // TransactionId -> DeleteTask
    private Map<Long, DeleteTask> idToDeleteTask;

    // Db -> DeleteInfo list
    private Map<Long, List<DeleteInfo>> dbToDeleteInfos;

    private MasterTaskExecutor executor;

    private BlockingQueue<DeleteTask> queue;

    private ReentrantReadWriteLock lock;

    private DeleteTaskChecker checker;

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    public DeleteHandler() {
        // for persist
    }

    public DeleteHandler(Catalog catalog) {
        this.catalog = catalog;
        idToDeleteTask = Maps.newConcurrentMap();
        dbToDeleteInfos = Maps.newHashMap();
        executor = new MasterTaskExecutor(Config.delete_thread_num);
        queue = new LinkedBlockingQueue(Config.delete_thread_num);
        lock = new ReentrantReadWriteLock(true);
        // start checker
        checker = new DeleteTaskChecker(queue);
        checker.start();
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

        DeleteTask deleteTask = null;
        DeleteInfo deleteInfo = null;
        long transactionId;
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

            TransactionState state = Catalog.getCurrentGlobalTransactionMgr()
                    .getTransactionState(transactionId);
            if (state == null) {
                throw new DdlException("begin transaction failed, cancel delete");
            }

            deleteInfo = new DeleteInfo(db.getId(), olapTable.getId(), tableName,
                    partition.getId(), partitionName,
                    -1, 0, deleteConditions);

            // task in fe
            deleteTask = new DeleteTask(transactionId, deleteInfo);
            // task sent to be
            AgentBatchTask batchTask = new AgentBatchTask();

            for (MaterializedIndex index : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                long indexId = index.getId();
                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);

                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();

                    // set push type
                    TPushType type = TPushType.DELETE;

                    Set<Long> allReplicas = new HashSet<Long>();

                    for (Replica replica : tablet.getReplicas()) {
                        long replicaId = replica.getId();
                        allReplicas.add(replicaId);

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

                        if (AgentTaskQueue.addTask(pushTask)) {
                            batchTask.addTask(pushTask);
                            deleteTask.addTablet(tabletId);
                            deleteTask.addPushTask(pushTask);
                        }

                    }
                }
            }

            // submit push tasks
            if (batchTask.getTaskNum() > 0) {
                AgentTaskExecutor.submit(batchTask);
                queue.put(deleteTask);
            }

        } catch (Throwable t) {
            LOG.warn("error occurred during delete process", t);
            throw new DdlException(t.getMessage(), t);
        } finally {
            db.readUnlock();
        }

        // for show delete
        writeLock();
        try {
            if (dbToDeleteInfos.containsKey(db.getId())) {
                dbToDeleteInfos.get(db.getId()).add(deleteInfo);
            } else {
                List<DeleteInfo> deleteInfoList = Lists.newArrayList();
                deleteInfoList.add(deleteInfo);
                dbToDeleteInfos.put(db.getId(), deleteInfoList);
            }
        } finally {
            writeUnlock();
        }

        long startDeleteTime = System.currentTimeMillis();
        long timeout = deleteTask.getTimeout();
        long stragglerTimeout = timeout / 2;
        // wait until delete task finish or timeout
        LOG.info("waiting delete task finish, signature: {}, timeout: {}", transactionId, timeout);
        deleteTask.join(stragglerTimeout);
        if (deleteTask.isQuorum()) {
            commitTask(deleteTask, db);
        } else {
            boolean isSuccess = cancelTask(deleteTask, "delete task timeout");
            if (isSuccess) {
                throw new DdlException("timeout when waiting delete");
            }
        }

        long leftTime = timeout - (System.currentTimeMillis() - startDeleteTime);
        afterCommit(deleteTask, db, System.currentTimeMillis(), leftTime);
    }

    private void afterCommit(DeleteTask deleteTask, Database db, long startDeleteTime, long leftTime) throws DdlException {
        try {
            long transactionId = deleteTask.getSignature();
            while (leftTime > 0) {
                db.writeLock();
                try {
                    // check if the job is aborted in transaction manager
                    TransactionState state = Catalog.getCurrentGlobalTransactionMgr()
                            .getTransactionState(transactionId);
                    if (state == null) {
                        LOG.warn("cancel delete, transactionId {},  because could not find transaction state", transactionId);
                        cancelTask(deleteTask,"transaction state lost");
                        return;
                    }
                    TransactionStatus status = state.getTransactionStatus();
                    switch (status) {
                        case ABORTED:
                            cancelTask(deleteTask,"delete transaction is aborted in transaction manager [" + state + "]");
                            return;
                        case COMMITTED:
                            LOG.debug("delete task is already committed, just wait it to be visible, transactionId {}, transaction state {}", transactionId, state);
                            return;
                        case VISIBLE:
                            LOG.debug("delete committed, transactionId: {}, transaction state {}", transactionId, state);
                            removeTask(deleteTask);
                            return;
                    }
                    leftTime -= (System.currentTimeMillis() - startDeleteTime);
                } finally {
                    db.writeUnlock();
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            String failMsg = "delete unknown, " + e.getMessage();
            LOG.warn(failMsg, e);
            throw new DdlException(failMsg);
        }
    }

    public class DeleteTaskChecker extends Thread {
        private BlockingQueue<DeleteTask> queue;

        public DeleteTaskChecker(BlockingQueue<DeleteTask> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            LOG.info("delete task checker start");
            try {
                loop();
            } finally {
                synchronized(queue) {
                    queue.clear();
                }
            }
        }

        public void loop() {
            while (true) {
                try {
                    DeleteTask task = queue.take();
                    while (!task.isQuorum()) {
                        long signature = task.getSignature();
                        if (task.isCancel()) {
                            break;
                        }
                        if (!executor.submit(task)) {
                            Thread.sleep(1000);
                            continue;
                        }
                        // re add to the tail
                        queue.put(task);
                    }
                    // task isQuorum or isCanceled need remove
                    removeTask(task);
                } catch (InterruptedException e) {
                    // do nothing
                }
            }
        }
    }

    private void commitTask(DeleteTask task, Database db) {
        long transactionId = task.getSignature();
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        TransactionState transactionState = globalTransactionMgr.getTransactionState(transactionId);
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<TabletCommitInfo>();
        // when be finish load task, fe will update job's finish task info, use lock here to prevent
        // concurrent problems
        db.writeLock();
        try {
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            for (TabletDeleteInfo tDeleteInfo : task.getTabletDeleteInfo()) {
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
            globalTransactionMgr.commitTransaction(db.getId(), transactionId, tabletCommitInfos);
        } catch (UserException e) {
            LOG.warn("errors while commit delete, transaction [{}], reason is {}",
                    transactionState.getTransactionId(),  e);
            cancelTask(task, transactionState.getReason());
        } finally {
            db.writeUnlock();
        }
    }

    public void removeTask(DeleteTask task) {
        task.unJoin();
        writeLock();
        try {
            long signature = task.getSignature();
            if (idToDeleteTask.containsKey(signature)) {
                idToDeleteTask.remove(signature);
            }
            for (PushTask pushTask : task.getPushTasks()) {
                AgentTaskQueue.removePushTask(pushTask.getBackendId(), pushTask.getSignature(),
                        pushTask.getVersion(), pushTask.getVersionHash(),
                        pushTask.getPushType(), pushTask.getTaskType());
            }
        } finally {
            writeUnlock();
        }
    }

    public boolean cancelTask(DeleteTask task, String reason) {
        try {
            if (task != null) {
                task.setCancel();
                Catalog.getCurrentGlobalTransactionMgr().abortTransaction(
                        task.getSignature(), reason);
                return true;
            }
        } catch (Exception e) {
            LOG.info("errors while abort transaction", e);
        }
        return false;
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
        readLock();
        try {
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
                info.add("DELETING");
                infos.add(info);
            }

        } finally {
            readUnlock();
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

    public boolean addFinishedReplica(Long transactionId, long tabletId, Replica replica) {
        readLock();
        try {
            DeleteTask task = idToDeleteTask.get(transactionId);
            if (task != null) {
                return task.addFinishedReplica(tabletId, replica);
            } else {
                return false;
            }
        } finally {
            readUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
//        repoMgr.write(out);
//
//        out.writeInt(dbIdToBackupOrRestoreJob.size());
//        for (AbstractJob job : dbIdToBackupOrRestoreJob.values()) {
//            job.write(out);
//        }
    }
}
