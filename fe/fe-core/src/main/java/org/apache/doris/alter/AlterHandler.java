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

package org.apache.doris.alter;

import org.apache.doris.alter.AlterJob.JobState;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.RemoveAlterJobV2OperationLog;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.task.AgentTask;
import org.apache.doris.task.AlterReplicaTask;
import org.apache.doris.thrift.TTabletInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AlterHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(AlterHandler.class);

    // tableId -> AlterJob
    @Deprecated
    protected ConcurrentHashMap<Long, AlterJob> alterJobs = new ConcurrentHashMap<>();
    @Deprecated
    protected ConcurrentLinkedQueue<AlterJob> finishedOrCancelledAlterJobs = new ConcurrentLinkedQueue<>();
    
    // queue of alter job v2
    protected ConcurrentMap<Long, AlterJobV2> alterJobsV2 = Maps.newConcurrentMap();

    /**
     * lock to perform atomic operations.
     * eg.
     *  When job is finished, it will be moved from alterJobs to finishedOrCancelledAlterJobs,
     *  and this requires atomic operations. So the lock must be held to do this operations.
     *  Operations like Get or Put do not need lock.
     */
    protected ReentrantLock lock = new ReentrantLock();
    
    protected void lock() {
        lock.lock();
    }
    
    protected void unlock() {
        lock.unlock();
    }
    
    public AlterHandler(String name) {
        super(name, FeConstants.default_scheduler_interval_millisecond);
    }

    protected void addAlterJobV2(AlterJobV2 alterJob) {
        this.alterJobsV2.put(alterJob.getJobId(), alterJob);
        LOG.info("add {} job {}", alterJob.getType(), alterJob.getJobId());
    }

    public List<AlterJobV2> getUnfinishedAlterJobV2ByTableId(long tblId) {
        List<AlterJobV2> unfinishedAlterJobList = new ArrayList<>();
        for (AlterJobV2 alterJob : alterJobsV2.values()) {
            if (alterJob.getTableId() == tblId
                    && alterJob.getJobState() != AlterJobV2.JobState.FINISHED
                    && alterJob.getJobState() != AlterJobV2.JobState.CANCELLED) {
                unfinishedAlterJobList.add(alterJob);
            }
        }
        return unfinishedAlterJobList;
    }

    public AlterJobV2 getUnfinishedAlterJobV2ByJobId(long jobId) {
        for (AlterJobV2 alterJob : alterJobsV2.values()) {
            if (alterJob.getJobId() == jobId && !alterJob.isDone()) {
                return alterJob;
            }
        }
        return null;
    }

    public Map<Long, AlterJobV2> getAlterJobsV2() {
        return this.alterJobsV2;
    }

    // should be removed in version 0.13
    @Deprecated
    private void clearExpireFinishedOrCancelledAlterJobs() {
        long curTime = System.currentTimeMillis();
        // clean history job
        Iterator<AlterJob> iter = finishedOrCancelledAlterJobs.iterator();
        while (iter.hasNext()) {
            AlterJob historyJob = iter.next();
            if ((curTime - historyJob.getCreateTimeMs()) / 1000 > Config.history_job_keep_max_second) {
                iter.remove();
                LOG.info("remove history {} job[{}]. finish at {}", historyJob.getType(),
                        historyJob.getTableId(), TimeUtils.longToTimeString(historyJob.getFinishedTime()));
            }
        }
    }

    private void clearExpireFinishedOrCancelledAlterJobsV2() {
        Iterator<Map.Entry<Long, AlterJobV2>> iterator = alterJobsV2.entrySet().iterator();
        while (iterator.hasNext()) {
            AlterJobV2 alterJobV2 = iterator.next().getValue();
            if (alterJobV2.isExpire()) {
                iterator.remove();
                RemoveAlterJobV2OperationLog log = new RemoveAlterJobV2OperationLog(alterJobV2.getJobId(), alterJobV2.getType());
                Catalog.getCurrentCatalog().getEditLog().logRemoveExpiredAlterJobV2(log);
                LOG.info("remove expired {} job {}. finish at {}", alterJobV2.getType(),
                        alterJobV2.getJobId(), TimeUtils.longToTimeString(alterJobV2.getFinishedTimeMs()));
            }
        }
    }

    public void replayRemoveAlterJobV2(RemoveAlterJobV2OperationLog log) {
        if (alterJobsV2.remove(log.getJobId()) != null) {
            LOG.info("replay removing expired {} job {}.", log.getType(), log.getJobId());
        } else {
            // should not happen, but it does no matter, just add a warn log here to observe
            LOG.warn("failed to find {} job {} when replay removing expired job.", log.getType(), log.getJobId());
        }
    }

    @Deprecated
    protected void addAlterJob(AlterJob alterJob) {
        this.alterJobs.put(alterJob.getTableId(), alterJob);
        LOG.info("add {} job[{}]", alterJob.getType(), alterJob.getTableId());
    }

    @Deprecated
    public AlterJob getAlterJob(long tableId) {
        return this.alterJobs.get(tableId);
    }
    
    @Deprecated
    public boolean hasUnfinishedAlterJob(long tableId) {
        return this.alterJobs.containsKey(tableId);
    }

    @Deprecated
    public int getAlterJobNum(JobState state, long dbId) {
        int jobNum = 0;
        if (state == JobState.PENDING || state == JobState.RUNNING || state == JobState.FINISHING) {
            for (AlterJob alterJob : alterJobs.values()) {
                if (alterJob.getState() == state && alterJob.getDbId() == dbId) {
                    ++jobNum;
                }
            }
        } else if (state == JobState.FINISHED) {
            // lock to perform atomically
            lock();
            try {
                for (AlterJob alterJob : alterJobs.values()) {
                    if (alterJob.getState() == JobState.FINISHED && alterJob.getDbId() == dbId) {
                        ++jobNum;
                    }
                }

                for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                    if (alterJob.getState() == JobState.FINISHED && alterJob.getDbId() == dbId) {
                        ++jobNum;
                    }
                }
            } finally {
                unlock();
            }

        } else if (state == JobState.CANCELLED) {
            for (AlterJob alterJob : finishedOrCancelledAlterJobs) {
                if (alterJob.getState() == JobState.CANCELLED && alterJob.getDbId() == dbId) {
                    ++jobNum;
                }
            }
        }

        return jobNum;
    }

    public Long getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState state, long dbId) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state && e.getDbId() == dbId).count();
    }

    public Long getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState state) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state).count();
    }

    @Deprecated
    public Map<Long, AlterJob> unprotectedGetAlterJobs() {
        return this.alterJobs;
    }

    @Deprecated
    public ConcurrentLinkedQueue<AlterJob> unprotectedGetFinishedOrCancelledAlterJobs() {
        return this.finishedOrCancelledAlterJobs;
    }
    
    @Deprecated
    public void addFinishedOrCancelledAlterJob(AlterJob alterJob) {
        alterJob.clear();
        LOG.info("add {} job[{}] to finished or cancel list", alterJob.getType(), alterJob.getTableId());
        this.finishedOrCancelledAlterJobs.add(alterJob);
    }

    @Deprecated
    protected AlterJob removeAlterJob(long tableId) {
        return this.alterJobs.remove(tableId);
    }

    @Deprecated
    public void removeDbAlterJob(long dbId) {
        Iterator<Map.Entry<Long, AlterJob>> iterator = alterJobs.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, AlterJob> entry = iterator.next();
            AlterJob alterJob = entry.getValue();
            if (alterJob.getDbId() == dbId) {
                iterator.remove();
            }
        }
    }

    /*
     * handle task report
     * reportVersion is used in schema change job.
     */
    @Deprecated
    public void handleFinishedReplica(AgentTask task, TTabletInfo finishTabletInfo, long reportVersion)
            throws MetaNotFoundException {
        long tableId = task.getTableId();

        AlterJob alterJob = getAlterJob(tableId);
        if (alterJob == null) {
            throw new MetaNotFoundException("Cannot find " + task.getTaskType().name() + " job[" + tableId + "]");
        }
        alterJob.handleFinishedReplica(task, finishTabletInfo, reportVersion);
    }

    protected void cancelInternal(AlterJob alterJob, OlapTable olapTable, String msg) {
        // cancel
        if (olapTable != null) {
            olapTable.writeLock();
        }
        try {
            alterJob.cancel(olapTable, msg);
        } finally {
            if (olapTable != null) {
                olapTable.writeUnlock();
            }
        }
        jobDone(alterJob);
    }

    protected void jobDone(AlterJob alterJob) {
        lock();
        try {
            // remove job
            AlterJob alterJobRemoved  = removeAlterJob(alterJob.getTableId());
            // add to finishedOrCancelledAlterJobs
            if (alterJobRemoved != null) {
                // add alterJob not alterJobRemoved, because the alterJob maybe a new object
                // deserialized from journal, and the finished state is set to the new object
                addFinishedOrCancelledAlterJob(alterJob);
            }
        } finally {
            unlock();
        }
    }

    public void replayInitJob(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        alterJob.replayInitJob(db);
        // add rollup job
        addAlterJob(alterJob);
    }
    
    public void replayFinishing(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        alterJob.replayFinishing(db);
        alterJob.setState(JobState.FINISHING);
        // !!! the alter job should add to the cache again, because the alter job is deserialized from journal
        // it is a different object compared to the cache
        addAlterJob(alterJob);
    }

    public void replayFinish(AlterJob alterJob, Catalog catalog) {
        Database db = catalog.getDb(alterJob.getDbId());
        alterJob.replayFinish(db);
        alterJob.setState(JobState.FINISHED);

        jobDone(alterJob);
    }

    public void replayCancel(AlterJob alterJob, Catalog catalog) {
        removeAlterJob(alterJob.getTableId());
        alterJob.setState(JobState.CANCELLED);
        Database db = catalog.getDb(alterJob.getDbId());
        if (db != null) {
            // we log rollup job cancelled even if db is dropped.
            // so check db != null here
            alterJob.replayCancel(db);
        }

        addFinishedOrCancelledAlterJob(alterJob);
    }

    @Override
    protected void runAfterCatalogReady() {
        clearExpireFinishedOrCancelledAlterJobs();
        clearExpireFinishedOrCancelledAlterJobsV2();
    }

    @Override
    public void start() {
        super.start();
    }

    /*
     * abstract
     */
    /*
     * get alter job's info for show
     */
    public abstract List<List<Comparable>> getAlterJobInfosByDb(Database db);

    /*
     * entry function. handle alter ops 
     */
    public abstract void process(List<AlterClause> alterClauses, String clusterName, Database db, OlapTable olapTable)
            throws UserException;

    /*
     * entry function. handle alter ops for external table
     */
    public void processExternalTable(List<AlterClause> alterClauses, Database db, Table externalTable)
            throws UserException {};

    /*
     * cancel alter ops
     */
    public abstract void cancel(CancelStmt stmt) throws DdlException;

    @Deprecated
    public Integer getAlterJobNumByState(JobState state) {
        int jobNum = 0;
        for (AlterJob alterJob : alterJobs.values()) {
            if (alterJob.getState() == state) {
                ++jobNum;
            }
        }
        return jobNum;
    }

    /*
     * Handle the finish report of alter task.
     * If task is success, which means the history data before specified version has been transformed successfully.
     * So here we should modify the replica's version.
     * We assume that the specified version is X.
     * Case 1:
     *      After alter table process starts, there is no new load job being submitted. So the new replica
     *      should be with version (1-0). So we just modify the replica's version to partition's visible version, which is X.
     * Case 2:
     *      After alter table process starts, there are some load job being processed.
     * Case 2.1:
     *      Only one new load job, and it failed on this replica. so the replica's last failed version should be X + 1
     *      and version is still 1. We should modify the replica's version to (last failed version - 1)
     * Case 2.2 
     *      There are new load jobs after alter task, and at least one of them is succeed on this replica.
     *      So the replica's version should be larger than X. So we don't need to modify the replica version
     *      because its already looks like normal.
     */
    public void handleFinishAlterTask(AlterReplicaTask task) throws MetaNotFoundException {
        Database db = Catalog.getCurrentCatalog().getDb(task.getDbId());
        if (db == null) {
            throw new MetaNotFoundException("database " + task.getDbId() + " does not exist");
        }

        OlapTable tbl = (OlapTable) db.getTableOrThrowException(task.getTableId(), Table.TableType.OLAP);
        tbl.writeLock();
        try {
            Partition partition = tbl.getPartition(task.getPartitionId());
            if (partition == null) {
                throw new MetaNotFoundException("partition " + task.getPartitionId() + " does not exist");
            }
            MaterializedIndex index = partition.getIndex(task.getIndexId());
            if (index == null) {
                throw new MetaNotFoundException("index " + task.getIndexId() + " does not exist");
            }
            Tablet tablet = index.getTablet(task.getTabletId());
            Preconditions.checkNotNull(tablet, task.getTabletId());
            Replica replica = tablet.getReplicaById(task.getNewReplicaId());
            if (replica == null) {
                throw new MetaNotFoundException("replica " + task.getNewReplicaId() + " does not exist");
            }
            
            LOG.info("before handle alter task tablet {}, replica: {}, task version: {}-{}",
                    task.getSignature(), replica, task.getVersion(), task.getVersionHash());
            boolean versionChanged = false;
            if (replica.getVersion() > task.getVersion()) {
                // Case 2.2, do nothing
            } else {
                if (replica.getLastFailedVersion() > task.getVersion()) {
                    // Case 2.1
                    replica.updateVersionInfo(task.getVersion(), task.getVersionHash(), replica.getDataSize(), replica.getRowCount());
                    versionChanged = true;
                } else {
                    // Case 1
                    Preconditions.checkState(replica.getLastFailedVersion() == -1, replica.getLastFailedVersion());
                    replica.updateVersionInfo(task.getVersion(), task.getVersionHash(), replica.getDataSize(), replica.getRowCount());
                    versionChanged = true;
                }
            }

            if (versionChanged) {
                ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(task.getDbId(), task.getTableId(),
                        task.getPartitionId(), task.getIndexId(), task.getTabletId(), task.getBackendId(),
                        replica.getId(), replica.getVersion(), replica.getVersionHash(), -1,
                        replica.getDataSize(), replica.getRowCount(),
                        replica.getLastFailedVersion(), replica.getLastFailedVersionHash(),
                        replica.getLastSuccessVersion(), replica.getLastSuccessVersionHash());
                Catalog.getCurrentCatalog().getEditLog().logUpdateReplica(info);
            }
            
            LOG.info("after handle alter task tablet: {}, replica: {}", task.getSignature(), replica);
        } finally {
            tbl.writeUnlock();
        }
    }

    // replay the alter job v2
    public void replayAlterJobV2(AlterJobV2 alterJob) {
        AlterJobV2 existingJob = alterJobsV2.get(alterJob.getJobId());
        if (existingJob == null) {
            // This is the first time to replay the alter job, so just using the replayed alterJob to call replay();
            alterJob.replay(alterJob);
            alterJobsV2.put(alterJob.getJobId(), alterJob);
        } else {
            existingJob.replay(alterJob);
        }
    }
}
