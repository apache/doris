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

import org.apache.doris.analysis.DeleteStmt;
import org.apache.doris.analysis.Predicate;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DeleteHandler implements Writable {
    private static final Logger LOG = LogManager.getLogger(DeleteHandler.class);

    // TransactionId -> DeleteJob
    private final Map<Long, DeleteJob> idToDeleteJob;

    // Db -> DeleteInfo list
    @SerializedName(value = "dbToDeleteInfos")
    private final Map<Long, List<DeleteInfo>> dbToDeleteInfos;

    private final ReentrantReadWriteLock lock;

    public DeleteHandler() {
        idToDeleteJob = Maps.newConcurrentMap();
        dbToDeleteInfos = Maps.newConcurrentMap();
        lock = new ReentrantReadWriteLock();
    }

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

    /**
     * use for Nereids process empty relation
     */
    public void processEmptyRelation(QueryState execState) {
        String sb = "{'label':'" + DeleteJob.DELETE_PREFIX + UUID.randomUUID()
                + "', 'txnId':'" + -1
                + "', 'status':'" + TransactionStatus.VISIBLE.name() + "'}";
        execState.setOk(0, 0, sb);
    }

    /**
     * used for Nereids planner
     */
    public void process(Database targetDb, OlapTable targetTbl, List<String> partitionNames,
            List<Predicate> deleteConditions, QueryState execState) {
        DeleteJob deleteJob = null;
        try {
            targetTbl.readLock();
            try {
                if (targetTbl.getState() != OlapTable.OlapTableState.NORMAL) {
                    // table under alter operation can also do delete.
                    // just add a comment here to notice.
                }
                deleteJob = DeleteJob.newBuilder()
                        .buildWith(new DeleteJob.BuildParams(
                                targetDb,
                                targetTbl,
                                partitionNames,
                                deleteConditions));

                long txnId = deleteJob.beginTxn();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(targetDb.getId(), txnId);
                // must call this to make sure we only handle the tablet in the mIndex we saw here.
                // table may be under schema change or rollup, and the newly created tablets will not be checked later,
                // to make sure that the delete transaction can be done successfully.
                deleteJob.addTableIndexes(txnState);
                idToDeleteJob.put(txnId, deleteJob);
                deleteJob.dispatch();
            } finally {
                targetTbl.readUnlock();
            }
            deleteJob.await();
            String commitMsg = deleteJob.commit();
            execState.setOk(0, 0, commitMsg);
        } catch (Exception ex) {
            if (deleteJob != null) {
                deleteJob.cancel(ex.getMessage());
            }
            execState.setError(ex.getMessage());
        } finally {
            if (!FeConstants.runningUnitTest) {
                clearJob(deleteJob);
            }
        }
    }

    /**
     * used for legacy planner
     */
    public void process(DeleteStmt stmt, QueryState execState) throws DdlException {
        Database targetDb = Env.getCurrentInternalCatalog().getDbOrDdlException(stmt.getDbName());
        OlapTable targetTbl = targetDb.getOlapTableOrDdlException(stmt.getTableName());
        DeleteJob deleteJob = null;
        try {
            targetTbl.readLock();
            try {
                if (targetTbl.getState() != OlapTable.OlapTableState.NORMAL) {
                    // table under alter operation can also do delete.
                    // just add a comment here to notice.
                }
                deleteJob = DeleteJob.newBuilder()
                        .buildWith(new DeleteJob.BuildParams(
                                targetDb,
                                targetTbl,
                                stmt.getPartitionNames(),
                                stmt.getDeleteConditions()));

                long txnId = deleteJob.beginTxn();
                TransactionState txnState = Env.getCurrentGlobalTransactionMgr()
                        .getTransactionState(targetDb.getId(), txnId);
                // must call this to make sure we only handle the tablet in the mIndex we saw here.
                // table may be under schema change or rollup, and the newly created tablets will not be checked later,
                // to make sure that the delete transaction can be done successfully.
                txnState.addTableIndexes(targetTbl);
                idToDeleteJob.put(txnId, deleteJob);
                deleteJob.dispatch();
            } finally {
                targetTbl.readUnlock();
            }
            deleteJob.await();
            String commitMsg = deleteJob.commit();
            execState.setOk(0, 0, commitMsg);
        } catch (Exception ex) {
            if (deleteJob != null) {
                deleteJob.cancel(ex.getMessage());
            }
            execState.setError(ex.getMessage());
        } finally {
            if (!FeConstants.runningUnitTest) {
                clearJob(deleteJob);
            }
        }
    }

    /**
     * This method should always be called in the end of the delete process to clean the job.
     * Better put it in finally block.
     *
     * @param job
     */
    private void clearJob(DeleteJob job) {
        if (job == null) {
            return;
        }
        long signature = job.getTransactionId();
        idToDeleteJob.remove(signature);
        job.cleanUp();
        // do not remove callback from GlobalTransactionMgr's callback factory here.
        // the callback will be removed after transaction is aborted or visible.
    }

    public void recordFinishedJob(DeleteJob job) {
        if (job != null) {
            long dbId = job.getDeleteInfo().getDbId();
            LOG.info("record finished deleteJob, transactionId {}, dbId {}",
                    job.getTransactionId(), dbId);
            dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
            List<DeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
            writeLock();
            try {
                deleteInfoList.add(job.getDeleteInfo());
            } finally {
                writeUnlock();
            }
        }
    }

    public DeleteJob getDeleteJob(long transactionId) {
        return idToDeleteJob.get(transactionId);
    }

    // show delete stmt
    public List<List<Comparable>> getDeleteInfosByDb(long dbId) {
        LinkedList<List<Comparable>> infos = new LinkedList<>();
        Database db = Env.getCurrentInternalCatalog().getDbNullable(dbId);
        if (db == null) {
            return infos;
        }

        String dbName = db.getFullName();
        List<DeleteInfo> deleteInfoList = new ArrayList<>();
        if (dbId == -1) {
            for (Long tempDbId : dbToDeleteInfos.keySet()) {
                if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(),
                        InternalCatalog.INTERNAL_CATALOG_NAME,
                        Env.getCurrentEnv().getCatalogMgr().getDbNullable(tempDbId).getFullName(),
                        PrivPredicate.LOAD)) {
                    continue;
                }

                deleteInfoList.addAll(dbToDeleteInfos.get(tempDbId));
            }
        } else {
            deleteInfoList = dbToDeleteInfos.get(dbId);
        }

        readLock();
        try {
            if (deleteInfoList == null) {
                return infos;
            }

            for (DeleteInfo deleteInfo : deleteInfoList) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                                deleteInfo.getTableName(),
                                PrivPredicate.LOAD)) {
                    continue;
                }

                List<Comparable> info = Lists.newArrayList();
                info.add(deleteInfo.getTableName());
                if (deleteInfo.isNoPartitionSpecified()) {
                    info.add("*");
                } else {
                    info.add(Joiner.on(", ").join(deleteInfo.getPartitionNames()));
                }

                info.add(TimeUtils.longToTimeString(deleteInfo.getCreateTimeMs()));
                String conds = Joiner.on(", ").join(deleteInfo.getDeleteConditions());
                info.add(conds);

                info.add("FINISHED");
                infos.add(info);
            }
        } finally {
            readUnlock();
        }

        // sort by createTimeMs
        ListComparator<List<Comparable>> comparator = new ListComparator<>(2);
        infos.sort(comparator);
        return infos;
    }

    public void replayDelete(DeleteInfo deleteInfo, Env env) {
        // add to deleteInfos
        long dbId = deleteInfo.getDbId();
        LOG.info("replay delete, dbId {}", dbId);
        dbToDeleteInfos.putIfAbsent(dbId, Lists.newArrayList());
        List<DeleteInfo> deleteInfoList = dbToDeleteInfos.get(dbId);
        writeLock();
        try {
            deleteInfoList.add(deleteInfo);
        } finally {
            writeUnlock();
        }
    }

    // for delete handler, we only persist those delete already finished.
    @Override
    public void write(DataOutput out) throws IOException {
        removeOldDeleteInfos();
    }

    public static DeleteHandler read(DataInput in) throws IOException {
        String json = Text.readString(in);
        DeleteHandler deleteHandler = GsonUtils.GSON.fromJson(json, DeleteHandler.class);
        deleteHandler.removeOldDeleteInfos();
        return deleteHandler;
    }

    public void removeOldDeleteInfos() {
        long curTime = System.currentTimeMillis();
        int counter = 0;
        Iterator<Entry<Long, List<DeleteInfo>>> iter1 = dbToDeleteInfos.entrySet().iterator();
        while (iter1.hasNext()) {
            List<DeleteInfo> deleteInfoList = iter1.next().getValue();

            writeLock();
            try {
                Iterator<DeleteInfo> iter2 = deleteInfoList.iterator();
                while (iter2.hasNext()) {
                    DeleteInfo deleteInfo = iter2.next();
                    if ((curTime - deleteInfo.getCreateTimeMs()) / 1000
                            > Config.streaming_label_keep_max_second) {
                        iter2.remove();
                        ++counter;
                    }
                }
            } finally {
                writeUnlock();
            }

            if (deleteInfoList.isEmpty()) {
                iter1.remove();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("remove expired delete job info num: {}", counter);
        }
    }
}
