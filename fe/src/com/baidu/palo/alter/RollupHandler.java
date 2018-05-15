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

package com.baidu.palo.alter;

import com.baidu.palo.alter.AlterJob.JobState;
import com.baidu.palo.analysis.AddRollupClause;
import com.baidu.palo.analysis.AlterClause;
import com.baidu.palo.analysis.CancelAlterTableStmt;
import com.baidu.palo.analysis.CancelStmt;
import com.baidu.palo.analysis.DropRollupClause;
import com.baidu.palo.catalog.AggregateType;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.KeysType;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.Partition.PartitionState;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.TabletInvertedIndex;
import com.baidu.palo.catalog.TabletMeta;
import com.baidu.palo.clone.Clone;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.util.ListComparator;
import com.baidu.palo.common.util.PropertyAnalyzer;
import com.baidu.palo.common.util.TimeUtils;
import com.baidu.palo.common.util.Util;
import com.baidu.palo.persist.DropInfo;
import com.baidu.palo.persist.EditLog;
import com.baidu.palo.qe.ConnectContext;
import com.baidu.palo.task.AgentBatchTask;
import com.baidu.palo.task.AgentTaskExecutor;
import com.baidu.palo.task.DropReplicaTask;
import com.baidu.palo.thrift.TKeysType;
import com.baidu.palo.thrift.TResourceInfo;
import com.baidu.palo.thrift.TStorageType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RollupHandler extends AlterHandler {
    private static final Logger LOG = LogManager.getLogger(RollupHandler.class);

    public RollupHandler() {
        super("rollup");
    }

    private void processAddRollup(AddRollupClause alterClause, Database db, OlapTable olapTable, boolean isRestore)
            throws DdlException {

        if (!isRestore) {
            if (olapTable.getState() == OlapTableState.ROLLUP) {
                throw new DdlException("Table[" + olapTable.getName() + "]'s is under ROLLUP");
            }

            // up to here, table's state can only be NORMAL
            Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL, olapTable.getState().name());
        }

        String rollupIndexName = alterClause.getRollupName();
        String baseIndexName = alterClause.getBaseRollupName();
        List<String> rollupColumnNames = alterClause.getColumnNames();

        LOG.info("process add rollup[{}] based on [{}]", rollupIndexName, baseIndexName);

        // 1. check if rollup index already exists
        if (olapTable.hasMaterializedIndex(rollupIndexName)) {
            throw new DdlException("Rollup index[" + rollupIndexName + "] already exists");
        }

        // 2. get base index schema
        if (baseIndexName == null) {
            // use table name as base table name
            baseIndexName = olapTable.getName();
        }
        Long baseIndexId = olapTable.getIndexIdByName(baseIndexName);
        if (baseIndexId == null) {
            throw new DdlException("Base index[" + baseIndexName + "] does not exist");
        }

        // check state
        for (Partition partition : olapTable.getPartitions()) {
            MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
            // up to here. index's state should only be NORMAL
            Preconditions.checkState(baseIndex.getState() == IndexState.NORMAL, baseIndex.getState().name());
        }

        // 3 check if rollup columns are valid
        // a. all columns should exist in base rollup schema
        // b. value after key
        List<Column> rollupSchema = new ArrayList<Column>();
        // check (a)(b)
        boolean meetValue = false;
        boolean hasKey = false;
        KeysType keysType = olapTable.getKeysType();
        if (KeysType.UNIQUE_KEYS == keysType || KeysType.AGG_KEYS == keysType) {
            int keysNumOfRollup = 0;
            for (String columnName : rollupColumnNames) {
                Column oneColumn = olapTable.getColumn(columnName);
                if (oneColumn == null) {
                    throw new DdlException("Column[" + columnName + "] does not exist");
                }
                if (oneColumn.isKey() && meetValue) {
                    throw new DdlException("Invalid column order. value should be after key");
                }
                if (oneColumn.isKey()) {
                    keysNumOfRollup += 1;
                    hasKey = true;
                } else {
                    meetValue = true;
                }
                rollupSchema.add(oneColumn);
            }

            if (!hasKey) {
                throw new DdlException("No key column is found");
            }
            
            if (KeysType.UNIQUE_KEYS == keysType) {
                // rollup of unique key table should have all keys of basetable
                if (keysNumOfRollup !=  olapTable.getKeysNum()) {
                    throw new DdlException("rollup should contains all unique keys in basetable");
                }       
            }
        } else if (KeysType.DUP_KEYS == keysType) {
            /*
             * eg.
             * Base Table's schema is (k1,k2,k3,k4,k5) dup key (k1,k2,k3).
             * The following rollup is allowed:
             * 1. (k1) dup key (k1)
             * 2. (k2,k3) dup key (k2)
             * 3. (k1,k2,k3) dup key (k1,k2)
             * 
             * The following rollup is forbidden:
             * 1. (k1) dup key (k2)
             * 2. (k2,k3) dup key (k3,k2)
             * 3. (k1,k2,k3) dup key (k2,k3)
             */
            if (alterClause.getDupKeys() == null || alterClause.getDupKeys().isEmpty()) {
                // user does not specify duplicate key for rollup,
                // use base table's duplicate key.
                // so we should check if rollup column contains all base table's duplicate key.
                List<Column> baseIdxCols = olapTable.getSchemaByIndexId(baseIndexId);
                Set<String> baseIdxKeyColNames = Sets.newHashSet();
                for (Column baseCol : baseIdxCols) {
                    if (baseCol.isKey()) {
                        baseIdxKeyColNames.add(baseCol.getName());
                    } else {
                        break;
                    }
                }

                boolean found = false;
                for (String baseIdxKeyColName : baseIdxKeyColNames) {
                    found = false;
                    for (String rollupColName : rollupColumnNames) {
                        if (rollupColName.equalsIgnoreCase(baseIdxKeyColName)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new DdlException("Rollup should contains all base table's duplicate keys if "
                                + "no duplicate key is specified: " + baseIdxKeyColName);
                    }
                }

                for (String columnName : rollupColumnNames) {
                    Column oneColumn = olapTable.getColumn(columnName);
                    if (oneColumn == null) {
                        throw new DdlException("Column[" + columnName + "] does not exist");
                    }
                    if (oneColumn.isKey() && meetValue) {
                        throw new DdlException("Invalid column order. key should before all values: " + columnName);
                    }
                    if (oneColumn.isKey()) {
                        hasKey = true;
                    } else {
                        meetValue = true;
                    }
                    rollupSchema.add(oneColumn);
                }

                if (!hasKey) {
                    throw new DdlException("No key column is found");
                }
            } else {
                // rollup have different dup keys with base table
                List<String> dupKeys = alterClause.getDupKeys();
                if (dupKeys.size() > rollupColumnNames.size()) {
                    throw new DdlException("Duplicate key should be the prefix of rollup columns. Exceeded");
                }

                for (int i = 0; i < rollupColumnNames.size(); i++) {
                    String rollupColName = rollupColumnNames.get(i);
                    boolean isKey = false;
                    if (i < dupKeys.size()) {
                        String dupKeyName = dupKeys.get(i);
                        if (!rollupColName.equalsIgnoreCase(dupKeyName)) {
                            throw new DdlException("Duplicate key should be the prefix of rollup columns");
                        }
                        isKey = true;
                    }

                    if (olapTable.getColumn(rollupColName) == null) {
                        throw new DdlException("Column[" + rollupColName + "] does not exist");
                    }

                    if (isKey && meetValue) {
                        throw new DdlException("Invalid column order. key should before all values: " + rollupColName);
                    }

                    Column oneColumn = new Column(olapTable.getColumn(rollupColName));
                    if (isKey) {
                        hasKey = true;
                        oneColumn.setIsKey(true);
                        oneColumn.setAggregationType(null, true);
                    } else {
                        meetValue = true;
                        oneColumn.setIsKey(false);
                        oneColumn.setAggregationType(AggregateType.NONE, true);
                    }
                    rollupSchema.add(oneColumn);
                }
            }
        }

        // 4. do create things
        // 4.1 get storage type. default is COLUMN
        
        TKeysType rollupKeysType;
        if (keysType == KeysType.DUP_KEYS) {
            rollupKeysType = TKeysType.DUP_KEYS;
        } else if (keysType == KeysType.UNIQUE_KEYS) {
            rollupKeysType = TKeysType.UNIQUE_KEYS;
        } else {
            rollupKeysType = TKeysType.AGG_KEYS;
        }
        
        Map<String, String> properties = alterClause.getProperties();
        TStorageType rollupStorageType = null;
        try {
            rollupStorageType = PropertyAnalyzer.analyzeStorageType(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        // check storage type if has null column
        boolean hasNullColumn = false;
        for (Column column : rollupSchema) {
            if (column.isAllowNull()) {
                hasNullColumn = true;
                break;
            }
        }
        if (hasNullColumn && rollupStorageType != TStorageType.COLUMN) {
            throw new DdlException("Only column rollup support null columns");
        }

        // 4.2 get rollup schema hash
        int schemaVersion = 0;
        try {
            schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        int rollupSchemaHash = Util.schemaHash(schemaVersion, rollupSchema, olapTable.getCopiedBfColumns(),
                                               olapTable.getBfFpp());

        // 4.3 get short key column count
        short rollupShortKeyColumnCount = Catalog.calcShortKeyColumnCount(rollupSchema, properties);

        // 4.4 get user resource info
        TResourceInfo resourceInfo = null;
        if (ConnectContext.get() != null) {
            resourceInfo = ConnectContext.get().toResourceCtx();
        }

        // 4.5 create rollup job
        long dbId = db.getId();
        long tableId = olapTable.getId();
        int baseSchemaHash = olapTable.getSchemaHashByIndexId(baseIndexId);

        Catalog catalog = Catalog.getInstance();
        long rollupIndexId = catalog.getNextId();
        RollupJob rollupJob = new RollupJob(dbId, tableId, baseIndexId, rollupIndexId,
                                            baseIndexName, rollupIndexName, rollupSchema,
                                            baseSchemaHash, rollupSchemaHash, rollupStorageType,
                                            rollupShortKeyColumnCount, resourceInfo, rollupKeysType);

        for (Partition partition : olapTable.getPartitions()) {
            long partitionId = partition.getId();
            MaterializedIndex rollupIndex = new MaterializedIndex(rollupIndexId, IndexState.ROLLUP);
            if (isRestore) {
                rollupIndex.setState(IndexState.NORMAL);
            }
            MaterializedIndex baseIndex = partition.getIndex(baseIndexId);
            TabletMeta rollupTabletMeta = new TabletMeta(dbId, tableId, partitionId, rollupIndexId,
                                                         rollupSchemaHash);
            short replicationNum = olapTable.getPartitionInfo().getReplicationNum(partition.getId());
            for (Tablet baseTablet : baseIndex.getTablets()) {
                long baseTabletId = baseTablet.getId();
                long rollupTabletId = catalog.getNextId();

                Tablet newTablet = new Tablet(rollupTabletId);
                rollupIndex.addTablet(newTablet, rollupTabletMeta);

                rollupJob.setTabletIdMap(partitionId, rollupTabletId, baseTabletId);
                List<Replica> baseReplicas = baseTablet.getReplicas();

                int replicaNum = 0;
                for (Replica baseReplica : baseReplicas) {
                    long rollupReplicaId = catalog.getNextId();
                    long backendId = baseReplica.getBackendId();
                    if (baseReplica.getState() == ReplicaState.CLONE) {
                        // just skip it.
                        continue;
                    }
                    Preconditions.checkState(baseReplica.getState() == ReplicaState.NORMAL);
                    ++replicaNum;

                    Replica rollupReplica = new Replica(rollupReplicaId, backendId, ReplicaState.ROLLUP);
                    if (isRestore) {
                        rollupReplica.setState(ReplicaState.NORMAL);
                    }
                    newTablet.addReplica(rollupReplica);
                } // end for baseReplica

                if (replicaNum < replicationNum / 2 + 1) {
                    String errMsg = "Tablet[" + baseTabletId + "] does not have enough replica. ["
                            + replicaNum + "/" + replicationNum + "]";
                    LOG.warn(errMsg);
                    throw new DdlException(errMsg);
                }
            } // end for baseTablets

            if (isRestore) {
                partition.createRollupIndex(rollupIndex);
            } else {
                rollupJob.addRollupIndex(partitionId, rollupIndex);
            }

            LOG.debug("create rollup index[{}] based on index[{}] in partition[{}], restore: {}",
                      rollupIndexId, baseIndexId, partitionId, isRestore);
        } // end for partitions

        if (isRestore) {
            olapTable.setIndexSchemaInfo(rollupIndexId, rollupIndexName, rollupSchema, 0,
                                         rollupSchemaHash, rollupShortKeyColumnCount);
            olapTable.setStorageTypeToIndex(rollupIndexId, rollupStorageType);
        } else {
            // update partition and table state
            for (Partition partition : olapTable.getPartitions()) {
                partition.setState(PartitionState.ROLLUP);
            }
            olapTable.setState(OlapTableState.ROLLUP);

            addAlterJob(rollupJob);

            // log rollup operation
            EditLog editLog = catalog.getEditLog();
            editLog.logStartRollup(rollupJob);
            LOG.debug("sync start create rollup index[{}] in table[{}]", rollupIndexId, tableId);
        }
    }

    public void processDropRollup(DropRollupClause alterClause, Database db, OlapTable olapTable)
            throws DdlException {
        // make sure we got db write lock here
        // up to here, table's state can be NORMAL or ROLLUP
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL
                || olapTable.getState() == OlapTableState.ROLLUP, olapTable.getState().name());

        String rollupIndexName = alterClause.getRollupName();
        if (rollupIndexName.equals(olapTable.getName())) {
            throw new DdlException("Cannot drop base index by using DROP ROLLUP.");
        }

        long dbId = db.getId();
        long tableId = olapTable.getId();
        if (!olapTable.hasMaterializedIndex(rollupIndexName)) {
            // when rollup job is unfinished, rollup index is not added to the table
            AlterJob alterJob = getAlterJob(tableId);
            if (alterJob == null || !((RollupJob) alterJob).getRollupIndexName().equals(rollupIndexName)) {
                throw new DdlException("Rollup index[" + rollupIndexName + "] does not exist in table["
                        + olapTable.getName() + "]");
            }

            // cancel rollup job
            cancelInternal(alterJob, olapTable, "rollup index is dropped");
            return;
        }

        // 1. check if any rollup job is based on this index
        AlterJob alterJob = null;
        if ((alterJob = checkIfAnyRollupBasedOn(tableId, rollupIndexName)) != null) {
            throw new DdlException("Rollup index[" + ((RollupJob) alterJob).getRollupIndexName()
                    + "] is doing rollup based on this index[" + rollupIndexName + "] and not finished yet.");
        }

        // drop rollup for each partition
        long rollupIndexId = olapTable.getIndexIdByName(rollupIndexName);
        int rollupSchemaHash = olapTable.getSchemaHashByIndexId(rollupIndexId);
        Preconditions.checkState(rollupSchemaHash != -1);
        Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL);

        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
        AgentBatchTask batchTask = new AgentBatchTask();
        final String cloneFailMsg = "rollup index[" + rollupIndexName + "] has been dropped";
        for (Partition partition : olapTable.getPartitions()) {
            MaterializedIndex rollupIndex = partition.getIndex(rollupIndexId);
            Preconditions.checkNotNull(rollupIndex);

            // 1. remove clone job
            Clone clone = Catalog.getInstance().getCloneInstance();
            for (Tablet tablet : rollupIndex.getTablets()) {
                clone.cancelCloneJob(tablet.getId(), cloneFailMsg);
            }

            // 2. delete rollup index
            partition.deleteRollupIndex(rollupIndexId);

            // 3. send DropReplicaTask
            for (Tablet tablet : rollupIndex.getTablets()) {
                long tabletId = tablet.getId();
                List<Replica> replicas = tablet.getReplicas();
                for (Replica replica : replicas) {
                    long backendId = replica.getBackendId();
                    DropReplicaTask dropTask = new DropReplicaTask(backendId, tabletId, rollupSchemaHash);
                    batchTask.addTask(dropTask);
                } // end for replicas

                // remove from inverted index
                invertedIndex.deleteTablet(tabletId);
            } // end for tablets
        } // end for partitions

        olapTable.deleteIndexInfo(rollupIndexName);
        AgentTaskExecutor.submit(batchTask);

        // 5. log drop rollup operation
        EditLog editLog = Catalog.getInstance().getEditLog();
        DropInfo dropInfo = new DropInfo(dbId, tableId, rollupIndexId);
        editLog.logDropRollup(dropInfo);
        LOG.debug("log drop rollup index[{}] finished in table[{}]", dropInfo.getIndexId(),
                  dropInfo.getTableId());

        LOG.info("finished drop rollup index[{}] in table[{}]", rollupIndexName, olapTable.getName());
    }

    public void replayDropRollup(DropInfo dropInfo, Catalog catalog) {
        Database db = catalog.getDb(dropInfo.getDbId());
        db.writeLock();
        try {
            long tableId = dropInfo.getTableId();
            long rollupIndexId = dropInfo.getIndexId();

            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            for (Partition partition : olapTable.getPartitions()) {
                MaterializedIndex rollupIndex = partition.deleteRollupIndex(rollupIndexId);

                if (!Catalog.isCheckpointThread()) {
                    // remove from inverted index
                    for (Tablet tablet : rollupIndex.getTablets()) {
                        invertedIndex.deleteTablet(tablet.getId());
                    }
                }
            }

            String rollupIndexName = olapTable.getIndexNameById(rollupIndexId);
            olapTable.deleteIndexInfo(rollupIndexName);
        } finally {
            db.writeUnlock();
        }
    }

    public void removeReplicaRelatedTask(long tableId, long partitionId, long indexId, long tabletId, long backendId) {
        // make sure to get db writeLock
        AlterJob alterJob = checkIfAnyRollupBasedOn(tableId, indexId);
        if (alterJob != null) {
            alterJob.removeReplicaRelatedTask(partitionId, tabletId, -1L, backendId);
        }
    }

    // this is for handle delete replica op
    private AlterJob checkIfAnyRollupBasedOn(long tableId, long baseIndexId) {
        this.jobsLock.readLock().lock();
        try {
            AlterJob alterJob = this.alterJobs.get(tableId);
            if (alterJob != null && ((RollupJob) alterJob).getBaseIndexId() == baseIndexId) {
                return alterJob;
            }
            return null;
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    // this is for drop rollup op
    private AlterJob checkIfAnyRollupBasedOn(long tableId, String baseIndexName) {
        this.jobsLock.readLock().lock();
        try {
            AlterJob alterJob = this.alterJobs.get(tableId);
            if (alterJob != null && ((RollupJob) alterJob).getBaseIndexName().equals(baseIndexName)) {
                return alterJob;
            }
            return null;
        } finally {
            this.jobsLock.readLock().unlock();
        }
    }

    private List<Comparable> getJobInfo(RollupJob rollupJob, String tableName) {
        List<Comparable> jobInfo = new ArrayList<Comparable>();

        // job id
        jobInfo.add(rollupJob.getTableId());

        // table name
        jobInfo.add(tableName);

        // create time
        long createTime = rollupJob.getCreateTimeMs();
        jobInfo.add(TimeUtils.longToTimeString(createTime));

        long finishedTime = rollupJob.getFinishedTime();
        jobInfo.add(TimeUtils.longToTimeString(finishedTime));

        // base index and rollup index name
        jobInfo.add(rollupJob.getBaseIndexName());
        jobInfo.add(rollupJob.getRollupIndexName());

        // job state
        jobInfo.add(rollupJob.getState().name());

        // msg
        jobInfo.add(rollupJob.getMsg());

        // progress
        if (rollupJob.getState() == JobState.PENDING) {
            jobInfo.add("0%");
        } else if (rollupJob.getState() == JobState.RUNNING) {
            int unfinishedReplicaNum = rollupJob.getUnfinishedReplicaNum();
            int totalReplicaNum = rollupJob.getTotalReplicaNum();
            Preconditions.checkState(unfinishedReplicaNum <= totalReplicaNum);
            jobInfo.add(((totalReplicaNum - unfinishedReplicaNum) * 100 / totalReplicaNum) + "%");
        } else {
            jobInfo.add("N/A");
        }

        return jobInfo;
    }

    @Override
    protected void runOneCycle() {
        super.runOneCycle();

        List<AlterJob> cancelledJobs = new LinkedList<AlterJob>();
        this.jobsLock.writeLock().lock();
        try {
            Iterator<Entry<Long, AlterJob>> iterator = this.alterJobs.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<Long, AlterJob> entry = iterator.next();
                AlterJob rollupJob = entry.getValue();

                JobState state = rollupJob.getState();
                switch (state) {
                    case PENDING: {
                        // if rollup job's status is PENDING, we need to send tasks.
                        if (!rollupJob.sendTasks()) {
                            cancelledJobs.add(rollupJob);
                            LOG.warn("sending rollup job[" + rollupJob.getTableId() + "] tasks failed. cancel it.");
                        }
                        break;
                    }
                    case RUNNING: {
                        if (rollupJob.isTimeout()) {
                            cancelledJobs.add(rollupJob);
                        } else {
                            int res = rollupJob.tryFinishJob();
                            if (res == -1) {
                                // cancel rollup
                                cancelledJobs.add(rollupJob);
                                LOG.warn("cancel rollup[{}] cause bad rollup job[{}]",
                                         ((RollupJob) rollupJob).getRollupIndexName(), rollupJob.getTableId());
                            }
                        }
                        break;
                    }
                    case FINISHED: {
                        // remove from alterJobs
                        iterator.remove();
                        addFinishedOrCancelledAlterJob(rollupJob);
                        break;
                    }
                    case CANCELLED: {
                        // all CANCELLED state should be handled immediately
                        Preconditions.checkState(false);
                        break;
                    }
                    default:
                        Preconditions.checkState(false);
                        break;
                }
            } // end for jobs
        } finally {
            this.jobsLock.writeLock().unlock();
        }

        // handle cancelled rollup jobs
        for (AlterJob rollupJob : cancelledJobs) {
            Database db = Catalog.getInstance().getDb(rollupJob.getDbId());
            if (db == null) {
                cancelInternal(rollupJob, null, null);
            }
            db.writeLock();
            try {
                OlapTable olapTable = (OlapTable) db.getTable(rollupJob.getTableId());
                cancelInternal(rollupJob, olapTable, null);
            } finally {
                db.writeUnlock();
            }
        }
    }

    @Override
    public List<List<Comparable>> getAlterJobInfosByDb(Database db) {
        List<List<Comparable>> rollupJobInfos = new LinkedList<List<Comparable>>();
        db.readLock();
        try {
            long dbId = db.getId();
            this.jobsLock.readLock().lock();
            try {
                for (AlterJob alterJob : this.alterJobs.values()) {
                    if (alterJob.getDbId() != dbId) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) db.getTable(alterJob.getTableId());
                    if (olapTable == null) {
                        continue;
                    }

                    rollupJobInfos.add(getJobInfo((RollupJob) alterJob, olapTable.getName()));
                } // end for rollupJobs

                for (AlterJob alterJob : this.finishedOrCancelledAlterJobs) {
                    if (alterJob.getDbId() != dbId) {
                        continue;
                    }

                    String tableName = "";
                    OlapTable olapTable = (OlapTable) db.getTable(alterJob.getTableId());
                    if (olapTable != null) {
                        tableName = olapTable.getName();
                    }

                    rollupJobInfos.add(getJobInfo((RollupJob) alterJob, tableName));
                } // end for rollupJobs

                // sort by
                // "JobId", "TableName", "CreateTime", "FinishedTime", "BaseIndexName", "RollupIndexName"
                ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0, 1, 2, 3, 4, 5);
                Collections.sort(rollupJobInfos, comparator);

            } catch (Exception e) {
                LOG.warn("failed to get rollup job info.", e);
            } finally {
                this.jobsLock.readLock().unlock();
            }
        } finally {
            db.readUnlock();
        }
        return rollupJobInfos;
    }

    @Override
    public void process(List<AlterClause> alterClauses, String clusterName, Database db, OlapTable olapTable)
            throws DdlException {
        process(alterClauses, db, olapTable, false);
    }

    public void process(List<AlterClause> alterClauses, Database db, OlapTable olapTable, boolean isRestore)
            throws DdlException {
        for (AlterClause alterClause : alterClauses) {
            if (alterClause instanceof AddRollupClause) {
                processAddRollup((AddRollupClause) alterClause, db, olapTable, isRestore);
            } else if (alterClause instanceof DropRollupClause) {
                processDropRollup((DropRollupClause) alterClause, db, olapTable);
            } else {
                Preconditions.checkState(false);
            }
        }
    }

    @Override
    public void cancel(CancelStmt stmt) throws DdlException {
        CancelAlterTableStmt cancelAlterTableStmt = (CancelAlterTableStmt) stmt;

        String dbName = cancelAlterTableStmt.getDbName();
        String tableName = cancelAlterTableStmt.getTableName();
        Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
        Preconditions.checkState(!Strings.isNullOrEmpty(tableName));

        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        db.writeLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
            }
            if (!(table instanceof OlapTable)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NOT_OLAP_TABLE, tableName);
            }
            OlapTable olapTable = (OlapTable) table;
            if (olapTable.getState() != OlapTableState.ROLLUP) {
                throw new DdlException("Table[" + tableName + "] is not under ROLLUP. "
                        + "Use 'ALTER TABLE DROP ROLLUP' if you want to.");
            }

            AlterJob rollupJob = getAlterJob(olapTable.getId());
            Preconditions.checkNotNull(rollupJob);

            cancelInternal(rollupJob, olapTable, "user cancelled");
        } finally {
            db.writeUnlock();
        }
    }
}
