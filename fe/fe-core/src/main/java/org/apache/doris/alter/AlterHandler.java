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

import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.CancelStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.RemoveAlterJobV2OperationLog;
import org.apache.doris.persist.ReplicaPersistInfo;
import org.apache.doris.task.AlterReplicaTask;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AlterHandler extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(AlterHandler.class);

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

    public AlterHandler(String name) {
        this(name, FeConstants.default_scheduler_interval_millisecond);
    }

    public AlterHandler(String name, int schedulerIntervalMillisecond) {
        super(name, schedulerIntervalMillisecond);
    }

    protected void lock() {
        lock.lock();
    }

    protected void unlock() {
        lock.unlock();
    }

    protected void addAlterJobV2(AlterJobV2 alterJob) throws AnalysisException {
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

    private void clearExpireFinishedOrCancelledAlterJobsV2() {
        Iterator<Map.Entry<Long, AlterJobV2>> iterator = alterJobsV2.entrySet().iterator();
        while (iterator.hasNext()) {
            AlterJobV2 alterJobV2 = iterator.next().getValue();
            if (alterJobV2.isExpire()) {
                iterator.remove();
                RemoveAlterJobV2OperationLog log = new RemoveAlterJobV2OperationLog(
                        alterJobV2.getJobId(), alterJobV2.getType());
                Env.getCurrentEnv().getEditLog().logRemoveExpiredAlterJobV2(log);
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

    public Long getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState state, long dbId) {
        return alterJobsV2.values().stream().filter(e -> e.getJobState() == state && e.getDbId() == dbId).count();
    }

    public Long getAlterJobV2Num(org.apache.doris.alter.AlterJobV2.JobState state) {
        Long counter = 0L;

        for (AlterJobV2 job : alterJobsV2.values()) {
            // no need to check priv here. This method is only called in show proc stmt,
            // which already check the ADMIN priv.
            if (job.getJobState() == state) {
                counter++;
            }
        }
        return counter;
    }

    @Override
    protected void runAfterCatalogReady() {
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
    public abstract void process(String rawSql, List<AlterClause> alterClauses, Database db,
                                 OlapTable olapTable)
            throws UserException;

    /*
     * entry function. handle alter ops
     */
    public void process(List<AlterClause> alterClauses, Database db, OlapTable olapTable)
            throws UserException {
        process("", alterClauses, db, olapTable);
    }

    /*
     * entry function. handle alter ops for external table
     */
    public void processExternalTable(List<AlterClause> alterClauses, Database db, Table externalTable)
            throws UserException {}

    /*
     * cancel alter ops
     */
    public abstract void cancel(CancelStmt stmt) throws DdlException;

    /*
     * Handle the finish report of alter task.
     * If task is success, which means the history data before specified version has been transformed successfully.
     * So here we should modify the replica's version.
     * We assume that the specified version is X.
     * Case 1:
     *      After alter table process starts, there is no new load job being submitted. So the new replica
     *      should be with version (0-1). So we just modify the replica's version to
     *      partition's visible version, which is X.
     * Case 2:
     *      After alter table process starts, there are some load job being processed.
     * Case 2.1:
     *      None of them succeed on this replica. so the version is still 1.
     *      We should modify the replica's version to X.
     * Case 2.2
     *      There are new load jobs after alter task, and at least one of them is succeed on this replica.
     *      So the replica's version should be larger than X. So we don't need to modify the replica version
     *      because its already looks like normal.
     * In summary, we only need to update replica's version when replica's version is smaller than X
     */
    public void handleFinishAlterTask(AlterReplicaTask task) throws MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(task.getDbId());

        OlapTable tbl = (OlapTable) db.getTableOrMetaException(task.getTableId(), Table.TableType.OLAP);
        tbl.writeLockOrMetaException();
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

            LOG.info("before handle alter task tablet {}, replica: {}, task version: {}",
                    task.getSignature(), replica, task.getVersion());
            boolean versionChanged = false;
            if (replica.getVersion() < task.getVersion()) {
                replica.updateVersion(task.getVersion());
                versionChanged = true;
            }

            if (versionChanged) {
                ReplicaPersistInfo info = ReplicaPersistInfo.createForClone(task.getDbId(), task.getTableId(),
                        task.getPartitionId(), task.getIndexId(), task.getTabletId(), task.getBackendId(),
                        replica.getId(), replica.getVersion(), -1,
                        replica.getDataSize(), replica.getRemoteDataSize(), replica.getRowCount(),
                        replica.getLastFailedVersion(), replica.getLastSuccessVersion());
                Env.getCurrentEnv().getEditLog().logUpdateReplica(info);
            }

            LOG.info("after handle alter task tablet: {}, replica: {}", task.getSignature(), replica);
        } finally {
            tbl.writeUnlock();
        }
    }

    // replay the alter job v2
    public void replayAlterJobV2(AlterJobV2 alterJob) throws AnalysisException {
        AlterJobV2 existingJob = alterJobsV2.get(alterJob.getJobId());
        if (existingJob == null) {
            // This is the first time to replay the alter job, so just using the replayed alterJob to call replay();
            alterJob.replay(alterJob);
            alterJobsV2.put(alterJob.getJobId(), alterJob);
        } else {
            existingJob.failedTabletBackends = alterJob.failedTabletBackends;
            existingJob.replay(alterJob);
        }
    }

    /**
     * there will be OOM if there are too many replicas of the table when schema change.
     */
    protected void checkReplicaCount(OlapTable olapTable) throws DdlException {
        olapTable.readLock();
        try {
            long replicaCount = olapTable.getReplicaCount();
            long maxReplicaCount = Config.max_replica_count_when_schema_change;
            if (replicaCount > maxReplicaCount) {
                String msg = String.format("%s have %d replicas reach %d limit when schema change.",
                        olapTable.getName(), replicaCount, maxReplicaCount);
                LOG.warn(msg);
                throw new DdlException(msg);
            }
        } finally {
            olapTable.readUnlock();
        }
    }
}
