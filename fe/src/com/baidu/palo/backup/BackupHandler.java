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

package com.baidu.palo.backup;

import com.baidu.palo.analysis.AbstractBackupStmt;
import com.baidu.palo.analysis.BackupStmt;
import com.baidu.palo.analysis.CancelBackupStmt;
import com.baidu.palo.analysis.LabelName;
import com.baidu.palo.analysis.PartitionName;
import com.baidu.palo.analysis.RestoreStmt;
import com.baidu.palo.backup.BackupJob.BackupJobState;
import com.baidu.palo.backup.RestoreJob.RestoreJobState;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.catalog.PartitionType;
import com.baidu.palo.catalog.Table;
import com.baidu.palo.catalog.OlapTable.OlapTableState;
import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.FeNameFormat;
import com.baidu.palo.common.PatternMatcher;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.common.util.TimeUtils;
import com.baidu.palo.task.RestoreTask;
import com.baidu.palo.task.SnapshotTask;
import com.baidu.palo.task.UploadTask;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BackupHandler extends Daemon {
    private static final Logger LOG = LogManager.getLogger(BackupHandler.class);

    private Map<Long, AbstractBackupJob> dbIdToBackupJob;
    private Map<Long, AbstractBackupJob> dbIdToRestoreJob;
    private List<AbstractBackupJob> finishedOrCancelledBackupJobs;
    private List<AbstractBackupJob> finishedOrCancelledRestoreJobs;

    private Multimap<Long, String> dbIdToLabels;

    // lock before db.lock
    private ReentrantReadWriteLock lock;

    private final AsynchronousCmdExecutor<String> cmdExecutor;

    public BackupHandler() {
        super("backupHandler", 5000L);
        dbIdToBackupJob = Maps.newHashMap();
        dbIdToRestoreJob = Maps.newHashMap();
        finishedOrCancelledBackupJobs = Lists.newArrayList();
        finishedOrCancelledRestoreJobs = Lists.newArrayList();

        dbIdToLabels = HashMultimap.create();

        lock = new ReentrantReadWriteLock();

        cmdExecutor = new AsynchronousCmdExecutor<String>();
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

    public AsynchronousCmdExecutor<String> getAsynchronousCmdExecutor() {
        return cmdExecutor;
    }

    public void process(AbstractBackupStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = Catalog.getInstance().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }


        long dbId = db.getId();
        String label = stmt.getLabel();
        writeLock();
        try {
            // 1. check if db has job underway
            checkJobExist(dbId);

            // 2. check if label is used
            checkAndAddLabel(dbId, label);
            
            // create job
            if (stmt instanceof BackupStmt) {
                createAndAddBackupJob(db, stmt.getLabelName(), stmt.getObjNames(), stmt.getRemotePath(),
                                      stmt.getProperties());
            } else if (stmt instanceof RestoreStmt) {
                createAndAddRestoreJob(db, stmt.getLabelName(), stmt.getObjNames(), stmt.getRemotePath(),
                                       stmt.getProperties());
            }
        } catch (DdlException e) {
            // remove label
            removeLabel(dbId, label);
            throw e;
        } finally {
            writeUnlock();
        }
    }

    private void checkJobExist(long dbId) throws DdlException {
        if (dbIdToBackupJob.containsKey(dbId)) {
            throw new DdlException("Database[" + dbId + "] has backup job underway");
        }

        if (dbIdToRestoreJob.containsKey(dbId)) {
            throw new DdlException("Database[" + dbId + "] has restore job underway");
        }
    }

    private void checkAndAddLabel(long dbId, String label) throws DdlException {
        try {
            FeNameFormat.checkLabel(label);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        if (dbIdToLabels.containsKey(dbId)) {
            if (dbIdToLabels.get(dbId).contains(label)) {
                throw new DdlException("label " + label + " is already used");
            }
        }

        dbIdToLabels.put(dbId, label);
    }

    private void removeLabel(long dbId, String label) {
        writeLock();
        try {
            if (dbIdToLabels.containsKey(dbId)) {
                dbIdToLabels.get(dbId).remove(label);
            }
        } finally {
            writeUnlock();
        }
    }

    private BackupJob createAndAddBackupJob(Database db, LabelName labelName, List<PartitionName> backupObjNames,
                                            String backupPath, Map<String, String> properties) throws DdlException {
        long jobId = Catalog.getInstance().getNextId();
        BackupJob job = new BackupJob(jobId, db.getId(), labelName, backupPath, properties);
        db.writeLock();
        try {
            if (backupObjNames.isEmpty()) {
                // backup all tables
                for (String tableName : db.getTableNamesWithLock()) {
                    backupObjNames.add(new PartitionName(tableName, null, null, null));
                }
            }

            if (backupObjNames.isEmpty()) {
                throw new DdlException("Database[" + db.getFullName() + "] is empty. no need to backup");
            }

            List<OlapTable> backupTables = Lists.newArrayList();
            for (PartitionName backupObj : backupObjNames) {
                String tableName = backupObj.getTableName();
                Table table = db.getTable(tableName);
                if (table == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
                }

                long tableId = table.getId();
                if (table.getType() == TableType.OLAP) {
                    OlapTable olapTable = (OlapTable) table;

                    // check state
                    if (olapTable.getState() != OlapTableState.NORMAL) {
                        throw new DdlException("Table[" + table.getName() + "]' state is not NORMAL");
                    }

                    // add partition
                    String partitionName = backupObj.getPartitionName();
                    if (partitionName.isEmpty()) {
                        // add all partitions
                        for (Partition partition : olapTable.getPartitions()) {
                            job.addPartitionId(tableId, partition.getId());
                        }
                    } else {
                        if (olapTable.getPartitionInfo().getType() != PartitionType.RANGE) {
                            throw new DdlException("Table[" + table.getName() + "] is not range partitioned");
                        }
                        // find and add specified partition
                        Partition partition = olapTable.getPartition(partitionName);
                        if (partition == null) {
                            throw new DdlException("Partition[" + partitionName + "] does not exist in table["
                                    + tableName + "]");
                        }

                        job.addPartitionId(tableId, partition.getId());
                    }

                    // add all indices
                    for (long indexId : olapTable.getIndexIdToSchema().keySet()) {
                        job.addIndexId(tableId, indexId);
                    }

                    backupTables.add(olapTable);
                } else {
                    // non-olap table;
                    job.addPartitionId(tableId, -1L);
                }
            } // end for backup objs

            // set table state
            for (OlapTable olapTable : backupTables) {
                Preconditions.checkState(olapTable.getState() == OlapTableState.NORMAL);
                olapTable.setState(OlapTableState.BACKUP);
            }
        } finally {
            db.writeUnlock();
        }

        // add
        Preconditions.checkState(!dbIdToBackupJob.containsKey(db.getId()));
        dbIdToBackupJob.put(db.getId(), job);

        // log
        Catalog.getInstance().getEditLog().logBackupStart(job);

        LOG.info("finished create backup job[{}]", job.getJobId());
        return job;
    }

    private RestoreJob createAndAddRestoreJob(Database db, LabelName labelName, List<PartitionName> restoreObjNames,
                                              String restorePath, Map<String, String> properties) throws DdlException {
        Map<String, Set<String>> tableToPartitionNames = Maps.newHashMap();
        Map<String, String> tableRenameMap = Maps.newHashMap();
        List<Table> existTables = Lists.newArrayList();
        db.writeLock();
        try {
            for (PartitionName partitionName : restoreObjNames) {
                String newTableName = partitionName.getNewTableName();
                String newPartitionName = partitionName.getNewPartitionName();
                
                Table table = db.getTable(newTableName);
                if (table != null) {
                    if (newPartitionName.isEmpty()) {
                        // do not allow overwrite entire table
                        throw new DdlException("Table[" + table.getName() + "]' already exist. "
                                + "Drop table first or restore to another table");
                    }

                    existTables.add(table);
                }

                Set<String> partitionNames = tableToPartitionNames.get(partitionName.getTableName());
                if (partitionNames == null) {
                    partitionNames = Sets.newHashSet();
                    tableToPartitionNames.put(newTableName, partitionNames);
                }

                if (!newPartitionName.isEmpty()) {
                    partitionNames.add(newPartitionName);
                }

                tableRenameMap.put(newTableName, partitionName.getTableName());
            }

            // set exist table's state
            for (Table table : existTables) {
                if (table.getType() == TableType.OLAP) {
                    ((OlapTable) table).setState(OlapTableState.RESTORE);
                }
            }
        } finally {
            db.writeUnlock();
        }

        long jobId = Catalog.getInstance().getNextId();
        RestoreJob job = new RestoreJob(jobId, db.getId(), labelName, restorePath, properties,
                                        tableToPartitionNames, tableRenameMap);

        // add
        Preconditions.checkState(!dbIdToRestoreJob.containsKey(db.getId()));
        dbIdToRestoreJob.put(db.getId(), job);

        // log
        Catalog.getInstance().getEditLog().logRestoreJobStart(job);

        LOG.info("finished create restore job[{}]", job.getJobId());
        return job;
    }

    public void handleFinishedSnapshot(SnapshotTask snapshotTask, String snapshotPath) {
        long dbId = snapshotTask.getDbId();
        long tabletId = snapshotTask.getTabletId();
        readLock();
        try {
            BackupJob job = (BackupJob) dbIdToBackupJob.get(dbId);
            if (job == null) {
                LOG.warn("db[{}] does not have backup job. tablet: {}", dbId, tabletId);
                return;
            }

            if (job.getJobId() != snapshotTask.getJobId()) {
                LOG.warn("tablet[{}] does not belong to backup job[{}]. tablet job[{}], tablet db[{}]",
                         tabletId, job.getJobId(), snapshotTask.getJobId(), dbId);
                return;
            }

            job.handleFinishedSnapshot(tabletId, snapshotTask.getBackendId(), snapshotPath);

        } finally {
            readUnlock();
        }
    }

    public void handleFinishedUpload(UploadTask uploadTask) {
        long dbId = uploadTask.getDbId();
        long tabletId = uploadTask.getTabletId();
        readLock();
        try {
            BackupJob job = (BackupJob) dbIdToBackupJob.get(dbId);
            if (job == null) {
                LOG.warn("db[{}] does not have backup job. tablet: {}", dbId, tabletId);
                return;
            }

            if (job.getJobId() != uploadTask.getJobId()) {
                LOG.warn("tablet[{}] does not belong to backup job[{}]. tablet job[{}], tablet db[{}]",
                         tabletId, job.getJobId(), uploadTask.getJobId(), dbId);
                return;
            }

            job.handleFinishedUpload(tabletId, uploadTask.getBackendId());
        } finally {
            readUnlock();
        }
    }

    public void handleFinishedRestore(RestoreTask restoreTask) {
        long dbId = restoreTask.getDbId();
        long tabletId = restoreTask.getTabletId();
        readLock();
        try {
            RestoreJob job = (RestoreJob) dbIdToRestoreJob.get(dbId);
            if (job == null) {
                LOG.warn("db[{}] does not have restore job. tablet: {}", dbId, tabletId);
                return;
            }

            if (job.getJobId() != restoreTask.getJobId()) {
                LOG.warn("tablet[{}] does not belong to restore job[{}]. tablet job[{}], tablet db[{}]",
                         tabletId, job.getJobId(), restoreTask.getJobId(), dbId);
                return;
            }

            job.handleFinishedRestore(tabletId, restoreTask.getBackendId());
        } finally {
            readUnlock();
        }
    }

    public void cancel(CancelBackupStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Database db = Catalog.getInstance().getDb(stmt.getDbName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }

        Map<Long, AbstractBackupJob> dbIdToJob = null;
        List<AbstractBackupJob> finishedOrCancelledJobs = null;
        if (stmt.isRestore()) {
            dbIdToJob = dbIdToRestoreJob;
            finishedOrCancelledJobs = finishedOrCancelledRestoreJobs;
        } else {
            dbIdToJob = dbIdToBackupJob;
            finishedOrCancelledJobs = finishedOrCancelledBackupJobs;
        }

        cancelInternal(db, dbIdToJob, finishedOrCancelledJobs);
    }

    private void cancelInternal(Database db, Map<Long, AbstractBackupJob> dbIdToJob,
                                List<AbstractBackupJob> finishedOrCancelledJobs) throws DdlException {
        writeLock();
        try {
            long dbId = db.getId();
            AbstractBackupJob job = dbIdToJob.get(dbId);
            if (job == null) {
                throw new DdlException("There is no job in database[" + db.getFullName() + "]");
            }

            job.setErrMsg("user cancelled");
            if (job instanceof BackupJob) {
                ((BackupJob) job).setState(BackupJobState.CANCELLED);
            } else {
                ((RestoreJob) job).setState(RestoreJobState.CANCELLED);
            }

            job.end(Catalog.getInstance(), false);

            dbIdToJob.remove(dbId);
            finishedOrCancelledJobs.add(job);
            removeLabel(dbId, job.getLabel());

            LOG.info("cancel job[{}] from db[{}]", job.getJobId(), dbId);
        } finally {
            writeUnlock();
        }
    }

    @Override
    protected void runOneCycle() {
        // backup
        LOG.debug("run backup jobs once");
        runOnce(dbIdToBackupJob, finishedOrCancelledBackupJobs);

        // restore
        LOG.debug("run restore jobs once");
        runOnce(dbIdToRestoreJob, finishedOrCancelledRestoreJobs);
    }

    private void runOnce(Map<Long, AbstractBackupJob> dbIdToJobs, List<AbstractBackupJob> finishedOrCancelledJobs) {
        // backup jobs
        writeLock();
        try {
            Iterator<Map.Entry<Long, AbstractBackupJob>> iterator = dbIdToJobs.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, AbstractBackupJob> entry = iterator.next();
                AbstractBackupJob job = entry.getValue();
                job.runOnce();

                // handle finished or cancelled jobs
                if (job instanceof BackupJob) {
                    BackupJob backupJob = (BackupJob) job;
                    if (backupJob.getState() == BackupJobState.FINISHED
                            || backupJob.getState() == BackupJobState.CANCELLED) {
                        finishedOrCancelledJobs.add(backupJob);

                        if (backupJob.getState() == BackupJobState.CANCELLED) {
                            // remove label
                            removeLabel(backupJob.getDbId(), backupJob.getLabel());
                        }
                        iterator.remove();
                    }
                } else if (job instanceof RestoreJob) {
                    RestoreJob restoreJob = (RestoreJob) job;
                    if (restoreJob.getState() == RestoreJobState.FINISHED
                            || restoreJob.getState() == RestoreJobState.CANCELLED) {
                        finishedOrCancelledJobs.add(restoreJob);

                        if (restoreJob.getState() == RestoreJobState.CANCELLED) {
                            // remove label
                            removeLabel(restoreJob.getDbId(), restoreJob.getLabel());
                        }
                        iterator.remove();
                    }
                }
            }

            // clear historical jobs
            Iterator<AbstractBackupJob> iter = finishedOrCancelledJobs.iterator();
            while (iter.hasNext()) {
                AbstractBackupJob job = iter.next();
                if ((System.currentTimeMillis() - job.getCreateTime()) / 1000 > Config.label_keep_max_second) {
                    iter.remove();
                    LOG.info("remove history job[{}]. created at {}", job.getJobId(),
                             TimeUtils.longToTimeString(job.getCreateTime()));
                }
            }

        } finally {
            writeUnlock();
        }
    }

    public int getBackupJobNum(BackupJobState state, long dbId) {
        int jobNum = 0;
        readLock();
        try {
            if (dbIdToBackupJob.containsKey(dbId)) {
                BackupJob job = (BackupJob) dbIdToBackupJob.get(dbId);
                if (job.getState() == state) {
                    ++jobNum;
                }
            }

            if (state == BackupJobState.FINISHED || state == BackupJobState.CANCELLED) {
                for (AbstractBackupJob job : finishedOrCancelledBackupJobs) {
                    if (job.getDbId() != dbId) {
                        continue;
                    }

                    if (((BackupJob) job).getState() == state) {
                        ++jobNum;
                    }
                }
            }
        } finally {
            readUnlock();
        }

        return jobNum;
    }

    public int getRestoreJobNum(RestoreJobState state, long dbId) {
        int jobNum = 0;
        readLock();
        try {
            if (dbIdToRestoreJob.containsKey(dbId)) {
                RestoreJob job = (RestoreJob) dbIdToRestoreJob.get(dbId);
                if (job.getState() == state) {
                    ++jobNum;
                }
            }

            if (state == RestoreJobState.FINISHED || state == RestoreJobState.CANCELLED) {
                for (AbstractBackupJob job : finishedOrCancelledRestoreJobs) {
                    if (job.getDbId() != dbId) {
                        continue;
                    }

                    if (((RestoreJob) job).getState() == state) {
                        ++jobNum;
                    }
                }
            }
        } finally {
            readUnlock();
        }

        return jobNum;
    }

    public List<List<Comparable>> getJobInfosByDb(long dbId, Class<? extends AbstractBackupJob> jobClass,
                                                  PatternMatcher matcher) {
        Map<Long, AbstractBackupJob> dbIdToJob = null;
        List<AbstractBackupJob> finishedOrCancelledJobs = null;
        if (jobClass.equals(BackupJob.class)) {
            dbIdToJob = dbIdToBackupJob;
            finishedOrCancelledJobs = finishedOrCancelledBackupJobs;
        } else {
            dbIdToJob = dbIdToRestoreJob;
            finishedOrCancelledJobs = finishedOrCancelledRestoreJobs;
        }

        List<List<Comparable>> jobInfos = new LinkedList<List<Comparable>>();
        readLock();
        try {
            AbstractBackupJob abstractJob = dbIdToJob.get(dbId);
            if (abstractJob != null) {
                List<Comparable> jobInfo = abstractJob.getJobInfo();
                if (matcher != null) {
                    String label = jobInfo.get(1).toString();
                    if (matcher.match(label)) {
                        jobInfos.add(jobInfo);
                    }
                } else {
                    jobInfos.add(jobInfo);
                }
            }

            for (AbstractBackupJob job : finishedOrCancelledJobs) {
                if (job.getDbId() != dbId) {
                    continue;
                }
                List<Comparable> jobInfo = job.getJobInfo();
                if (matcher != null) {
                    String label = jobInfo.get(1).toString();
                    if (matcher.match(label)) {
                        jobInfos.add(jobInfo);
                    }
                } else {
                    jobInfos.add(jobInfo);
                }
            }
        } finally {
            readUnlock();
        }
        return jobInfos;
    }

    public List<List<Comparable>> getJobUnfinishedTablet(long dbId, Class<? extends AbstractBackupJob> jobClass) {
        Map<Long, AbstractBackupJob> dbIdToJob = null;
        if (jobClass.equals(BackupJob.class)) {
            dbIdToJob = dbIdToBackupJob;
        } else {
            dbIdToJob = dbIdToRestoreJob;
        }

        List<List<Comparable>> jobInfos = Lists.newArrayList();
        readLock();
        try {
            AbstractBackupJob abstractJob = dbIdToJob.get(dbId);
            if (abstractJob != null) {
                jobInfos = abstractJob.getUnfinishedInfos();
            }
        } finally {
            readUnlock();
        }
        return jobInfos;
    }

    private void setTableState(Catalog catalog, BackupJob job) {
        Database db = catalog.getDb(job.getDbId());
        db.writeLock();
        try {
            for (long tableId : job.getTableIdToPartitionIds().keySet()) {
                Table table = db.getTable(tableId);
                if (table.getType() == TableType.OLAP) {
                    ((OlapTable) table).setState(OlapTableState.BACKUP);
                }
            }
        } finally {
            db.writeUnlock();
        }
        LOG.info("finished set backup tables' state to BACKUP. job: {}", job.getJobId());
    }

    private void setTableState(Catalog catalog, RestoreJob job) {
        Database db = catalog.getDb(job.getDbId());
        db.writeLock();
        try {
            for (String tableName : job.getTableToPartitionNames().keySet()) {
                Table table = db.getTable(tableName);
                if (table != null && table.getType() == TableType.OLAP) {
                    ((OlapTable) table).setState(OlapTableState.RESTORE);
                }
            }
        } finally {
            db.writeUnlock();
        }
        LOG.info("finished set restore tables' state to RESTORE. job: {}", job.getJobId());
    }

    public void replayBackupStart(Catalog catalog, BackupJob job) {
        setTableState(catalog, job);

        writeLock();
        try {
            dbIdToLabels.put(job.getDbId(), job.getLabel());
            Preconditions.checkState(!dbIdToBackupJob.containsKey(job.getDbId()));
            dbIdToBackupJob.put(job.getDbId(), job);
            LOG.debug("replay start backup job, put {} to map", job.getDbId());
        } finally {
            writeUnlock();
        }

        LOG.info("finished replay start backup job[{}]", job.getJobId());
    }

    public void replayBackupFinishSnapshot(BackupJob job) {
        Preconditions.checkState(job.getState() == BackupJobState.UPLOAD);
        writeLock();
        try {
            long dbId = job.getDbId();
            BackupJob currentJob = (BackupJob) dbIdToBackupJob.get(dbId);
            Preconditions.checkState(currentJob.getJobId() == job.getJobId());
            dbIdToBackupJob.remove(dbId);
            dbIdToBackupJob.put(dbId, job);
        } finally {
            writeUnlock();
        }

        LOG.info("finished replay backup finish snapshot. job[{}]", job.getJobId());
    }

    public void replayBackupFinish(Catalog catalog, BackupJob job) {
        job.end(catalog, true);

        writeLock();
        try {
            BackupJob currentJob = (BackupJob) dbIdToBackupJob.remove(job.getDbId());
            Preconditions.checkNotNull(currentJob, job.getDbId());

            finishedOrCancelledBackupJobs.add(job);
            if (job.getState() == BackupJobState.CANCELLED) {
                removeLabel(job.getDbId(), job.getLabel());
            }
        } finally {
            writeUnlock();
        }

        LOG.info("finished replay backup job finish. job: {}", job.getJobId());
    }

    public void replayRestoreStart(Catalog catalog, RestoreJob job) {
        setTableState(catalog, job);
        writeLock();
        try {
            dbIdToLabels.put(job.getDbId(), job.getLabel());
            Preconditions.checkState(!dbIdToRestoreJob.containsKey(job.getDbId()));
            dbIdToRestoreJob.put(job.getDbId(), job);
            LOG.debug("replay start restore job, put {} to map", job.getDbId());
        } finally {
            writeUnlock();
        }

        LOG.info("finished replay start restore job[{}]", job.getJobId());
    }

    public void replayRestoreFinish(Catalog catalog, RestoreJob job) {
        try {
            job.finishing(catalog, true);
            job.end(catalog, true);
        } catch (DdlException e) {
            LOG.error("should not happend", e);
        }

        writeLock();
        try {
            RestoreJob currentJob = (RestoreJob) dbIdToRestoreJob.remove(job.getDbId());
            Preconditions.checkNotNull(currentJob, job.getDbId());

            finishedOrCancelledRestoreJobs.add(job);
            if (job.getState() == RestoreJobState.CANCELLED) {
                removeLabel(job.getDbId(), job.getLabel());
            }
        } finally {
            writeUnlock();
        }

        LOG.info("finished replay restore job finish. job: {}", job.getJobId());
    }

    public Map<Long, AbstractBackupJob> unprotectedGetBackupJobs() {
        return dbIdToBackupJob;
    }

    public List<AbstractBackupJob> unprotectedGetFinishedOrCancelledBackupJobs() {
        return finishedOrCancelledBackupJobs;
    }

    public Map<Long, AbstractBackupJob> unprotectedGetRestoreJobs() {
        return dbIdToRestoreJob;
    }

    public List<AbstractBackupJob> unprotectedGetFinishedOrCancelledRestoreJobs() {
        return finishedOrCancelledRestoreJobs;
    }

    public Multimap<Long, String> unprotectedGetDbIdToLabels() {
        return dbIdToLabels;
    }
}
