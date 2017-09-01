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

package com.baidu.palo.load;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.Database;
import com.baidu.palo.catalog.Replica;
import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.catalog.MaterializedIndex.IndexState;
import com.baidu.palo.catalog.OlapTable;
import com.baidu.palo.catalog.Tablet;
import com.baidu.palo.catalog.MaterializedIndex;
import com.baidu.palo.catalog.Partition;
import com.baidu.palo.clone.Clone;
import com.baidu.palo.clone.CloneJob.JobPriority;
import com.baidu.palo.clone.CloneJob.JobType;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.load.AsyncDeleteJob.DeleteState;
import com.baidu.palo.load.FailMsg.CancelType;
import com.baidu.palo.load.LoadJob.EtlJobType;
import com.baidu.palo.load.LoadJob.JobState;
import com.baidu.palo.persist.ReplicaPersistInfo;
import com.baidu.palo.task.AgentBatchTask;
import com.baidu.palo.task.AgentTask;
import com.baidu.palo.task.AgentTaskExecutor;
import com.baidu.palo.task.AgentTaskQueue;
import com.baidu.palo.task.HadoopLoadEtlTask;
import com.baidu.palo.task.MiniLoadEtlTask;
import com.baidu.palo.task.MiniLoadPendingTask;
import com.baidu.palo.task.HadoopLoadPendingTask;
import com.baidu.palo.task.InsertLoadEtlTask;
import com.baidu.palo.task.MasterTask;
import com.baidu.palo.task.MasterTaskExecutor;
import com.baidu.palo.task.PullLoadEtlTask;
import com.baidu.palo.task.PullLoadPendingTask;
import com.baidu.palo.task.PushTask;
import com.baidu.palo.thrift.TPriority;
import com.baidu.palo.thrift.TPushType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class LoadChecker extends Daemon {
    private static final Logger LOG = LogManager.getLogger(LoadChecker.class);

    // checkers for running job state
    private static Map<JobState, LoadChecker> checkers = Maps.newHashMap();
    // executors for pending and etl tasks
    private static Map<JobState, Map<TPriority, MasterTaskExecutor>> executors = Maps.newHashMap();
    private JobState jobState;

    private LoadChecker(JobState jobState, long intervalMs) {
        super("load checker " + jobState.name().toLowerCase(), intervalMs);
        this.jobState = jobState;
    }
    
    /**
     * init dpp config and load checker threads executors
     * @param intervalMs
     */
    public static void init(long intervalMs) {
        checkers.put(JobState.PENDING, new LoadChecker(JobState.PENDING, intervalMs));
        checkers.put(JobState.ETL, new LoadChecker(JobState.ETL, intervalMs));
        checkers.put(JobState.LOADING, new LoadChecker(JobState.LOADING, intervalMs));
        checkers.put(JobState.QUORUM_FINISHED, new LoadChecker(JobState.QUORUM_FINISHED, intervalMs));
        
        Map<TPriority, MasterTaskExecutor> pendingPriorityMap = Maps.newHashMap();
        pendingPriorityMap.put(TPriority.NORMAL,
                               new MasterTaskExecutor(Config.load_pending_thread_num_normal_priority));
        pendingPriorityMap.put(TPriority.HIGH,
                               new MasterTaskExecutor(Config.load_pending_thread_num_high_priority));
        executors.put(JobState.PENDING, pendingPriorityMap);

        Map<TPriority, MasterTaskExecutor> etlPriorityMap = Maps.newHashMap();
        etlPriorityMap.put(TPriority.NORMAL, new MasterTaskExecutor(Config.load_etl_thread_num_normal_priority));
        etlPriorityMap.put(TPriority.HIGH, new MasterTaskExecutor(Config.load_etl_thread_num_high_priority));
        executors.put(JobState.ETL, etlPriorityMap);
    }
    
    /**
     * start all load checker threads
     */
    public static void startAll() {
        for (LoadChecker loadChecker : checkers.values()) {
            loadChecker.start();
        }
    }
    
    @Override
    protected void runOneCycle() {
        LOG.debug("start check load jobs. job state: {}", jobState.name());
        switch (jobState) {
            case PENDING:
                runPendingJobs();
                break;
            case ETL:
                runEtlJobs();
                break;
            case LOADING:
                runLoadingJobs();
                break;
            case QUORUM_FINISHED:
                runQuorumFinishedJobs();
                break;
            default:
                LOG.warn("wrong job state: {}", jobState.name());
                break;
        }
    }

    private void runPendingJobs() {
        Load load = Catalog.getInstance().getLoadInstance();
        List<LoadJob> pendingJobs = load.getLoadJobs(JobState.PENDING);

        // check to limit running etl job num
        int runningJobNumLimit = Config.load_running_job_num_limit;
        if (runningJobNumLimit > 0 && !pendingJobs.isEmpty()) {
            // pending executor running + etl state
            int runningJobNum = executors.get(JobState.PENDING).get(TPriority.NORMAL).getTaskNum()
                    + executors.get(JobState.PENDING).get(TPriority.HIGH).getTaskNum()
                    + load.getLoadJobs(JobState.ETL).size();
            if (runningJobNum >= runningJobNumLimit) {
                LOG.debug("running load job num {} exceeds system limit {}", runningJobNum, runningJobNumLimit);
                return;
            }

            int remain = runningJobNumLimit - runningJobNum;
            if (pendingJobs.size() > remain) {
                pendingJobs = pendingJobs.subList(0, remain);
            }
        }

        for (LoadJob job : pendingJobs) {
            try {
                MasterTask task = null;
                EtlJobType etlJobType = job.getEtlJobType();
                switch (etlJobType) {
                    case HADOOP:
                        task = new HadoopLoadPendingTask(job);
                        break;
                    case MINI:
                        task = new MiniLoadPendingTask(job);
                        break;
                    case BROKER:
                        task = new PullLoadPendingTask(job);
                        break;
                    default:
                        LOG.warn("unknown etl job type. type: {}", etlJobType.name());
                        break;
                }
                if (task != null) {
                    if (executors.get(JobState.PENDING).get(job.getPriority()).submit(task)) {
                        LOG.info("run pending job. job: {}", job);
                    }
                }
            } catch (Exception e) {
                LOG.warn("run pending job error", e);
            }
        }
    }

    private void runEtlJobs() {
        List<LoadJob> etlJobs = Catalog.getInstance().getLoadInstance().getLoadJobs(JobState.ETL);
        for (LoadJob job : etlJobs) {
            try {
                MasterTask task = null;
                EtlJobType etlJobType = job.getEtlJobType();
                switch (etlJobType) {
                    case HADOOP:
                        task = new HadoopLoadEtlTask(job);
                        break;
                    case MINI:
                        task = new MiniLoadEtlTask(job);
                        break;
                    case INSERT:
                        task = new InsertLoadEtlTask(job);
                        break;
                    case BROKER:
                        task = new PullLoadEtlTask(job);
                        break;
                    default:
                        LOG.warn("unknown etl job type. type: {}", etlJobType.name());
                        break;
                }
                if (task != null) {
                    if (executors.get(JobState.ETL).get(job.getPriority()).submit(task)) {
                        LOG.info("run etl job. job: {}", job);
                    }
                }
            } catch (Exception e) {
                LOG.warn("run etl job error", e);
            }
        }
    }
    
    private void runLoadingJobs() {
        List<LoadJob> loadingJobs = Catalog.getInstance().getLoadInstance().getLoadJobs(JobState.LOADING);
        for (LoadJob job : loadingJobs) {
            try {
                LOG.info("run loading job. job: {}", job);
                runOneLoadingJob(job);
            } catch (Exception e) {
                LOG.warn("run loading job error", e);
            }
        }
    }
    
    private void runOneLoadingJob(LoadJob job) {
        // check timeout
        Load load = Catalog.getInstance().getLoadInstance();
        if (checkTimeout(job)) {
            load.cancelLoadJob(job, CancelType.TIMEOUT, "loading timeout to cancel");
            return;
        }
 
        // get db
        long dbId = job.getDbId();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            load.cancelLoadJob(job, CancelType.LOAD_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        // submit push tasks to backends
        Set<Long> jobTotalTablets = submitPushTasks(job, db);
        if (jobTotalTablets == null) {
            load.cancelLoadJob(job, CancelType.LOAD_RUN_FAIL, "submit push tasks fail");
            return;
        }
        
        // update load progress
        Set<Long> quorumTablets = job.getQuorumTablets();
        job.setProgress(quorumTablets.size() * 100 / jobTotalTablets.size());

        // check job quorum finished
        if (quorumTablets.containsAll(jobTotalTablets)) {
            if (load.updateLoadJobState(job, JobState.QUORUM_FINISHED)) {
                LOG.info("load job quorum finished. job: {}", job);
            }
        }
    }

    private Set<Long> submitPushTasks(LoadJob job, Database db) {
        Map<Long, TabletLoadInfo> tabletLoadInfos = job.getIdToTabletLoadInfo();
        boolean needDecompress = (job.getEtlJobType() == EtlJobType.HADOOP) ? true : false;
        AgentBatchTask batchTask = new AgentBatchTask();
        Set<Long> jobTotalTablets = new HashSet<Long>();

        long currentTimeMs = System.currentTimeMillis();
        Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
        for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
            long tableId = tableEntry.getKey();
            OlapTable table = null;
            db.readLock();
            try {
                table = (OlapTable) db.getTable(tableId);
            } finally {
                db.readUnlock();
            }
            if (table == null) {
                LOG.warn("table does not exist. id: {}", tableId);
                if (job.getState() == JobState.QUORUM_FINISHED) {
                    continue;
                } else {
                    return null;
                }
            }

            TableLoadInfo tableLoadInfo = tableEntry.getValue();
            for (Entry<Long, PartitionLoadInfo> partitionEntry : tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                long partitionId = partitionEntry.getKey();
                PartitionLoadInfo partitionLoadInfo = partitionEntry.getValue();
                if (!partitionLoadInfo.isNeedLoad()) {
                    continue;
                }

                db.readLock();
                try {
                    Partition partition = table.getPartition(partitionId);
                    if (partition == null) {
                        LOG.warn("partition does not exist. id: {}", partitionId);
                        if (job.getState() == JobState.QUORUM_FINISHED) {
                            continue;
                        } else {
                            return null;
                        }
                    }
                    
                    short replicationNum = table.getPartitionInfo().getReplicationNum(partition.getId());
                    long version = partitionLoadInfo.getVersion();
                    long versionHash = partitionLoadInfo.getVersionHash();
                    // check all indices (base + roll up (not include ROLLUP state index))
                    List<MaterializedIndex> indices = partition.getMaterializedIndices();
                    for (MaterializedIndex index : indices) {
                        long indexId = index.getId();
                        if (index.getState() == IndexState.ROLLUP) {
                            // XXX(cmy): this should not happend anymore. 
                            // index with ROLLUP state is all in RollupJob instance
                            // observe and then remove
                            LOG.error("skip table under rollup[{}]", indexId);
                            continue;
                        }
                        
                        // add to jobTotalTablets first.
                        for (Tablet tablet : index.getTablets()) {
                            jobTotalTablets.add(tablet.getId());
                        }

                        // after rollup finished, we should push the next version to rollup index first to clear the 
                        // relationship between base and rollup index.
                        // so here we check if rollup index is push finished before sending push task
                        // to base index.
                        long rollupIndexId = index.getRollupIndexId();
                        if (rollupIndexId != -1L) {
                            MaterializedIndex rollupIndex = partition.getIndex(rollupIndexId);
                            if (rollupIndex != null) {
                                long rollupFinishedVersion = index.getRollupFinishedVersion();
                                Preconditions.checkState(rollupFinishedVersion != -1L);
                                if (version == rollupFinishedVersion + 1) {
                                    // this materializedIndex is a base index.
                                    // and this push version is the next version after rollup finished.
                                    // check if rollup index is push finished with this version
                                    boolean pushFinished = true;
                                    for (Tablet tablet : rollupIndex.getTablets()) {
                                        for (Replica replica : tablet.getReplicas()) {
                                            if (!replica.checkVersionCatchUp(version, versionHash)) {
                                                LOG.debug("waiting for rollup replica[{}] push "
                                                        + "next version[{}] version hash[{}]. "
                                                        + "current version[{}] version hash[{}]. "
                                                        + "tablet[{}]",
                                                          replica.getId(), version, versionHash,
                                                          replica.getVersion(), replica.getVersionHash(),
                                                          tablet.getId());
                                                pushFinished = false;
                                                break;
                                            }
                                        } // end for replicas
                                        if (!pushFinished) {
                                            break;
                                        }
                                    } // end for tablets
                                    
                                    if (!pushFinished) {
                                        // skip this base index
                                        continue;
                                    } else {
                                        // rollup index is push finished
                                        // clear rollup info in base index
                                        LOG.info("clear rollup index[{}] info in base index[{}]"
                                                + " after finished pushing version[{}] in partition[{}]",
                                                 rollupIndex.getId(), indexId, version, partitionId);
                                        index.clearRollupIndexInfo();
                                        // log
                                        ReplicaPersistInfo info =
                                                ReplicaPersistInfo.createForClearRollupInfo(db.getId(),
                                                                                            tableId,
                                                                                            partitionId,
                                                                                            indexId);
                                        Catalog.getInstance().getEditLog().logClearRollupIndexInfo(info);
                                    }
                                } else {
                                    // XXX(cmy): 
                                    // this should not happend. add log to observe
                                    LOG.error("base index[{}], rollup index[{}], push version[{}], rollup version[{}]",
                                              indexId, rollupIndexId, version, rollupFinishedVersion);
                                    index.clearRollupIndexInfo();
                                    // log
                                    ReplicaPersistInfo info =
                                            ReplicaPersistInfo.createForClearRollupInfo(db.getId(),
                                                                                        tableId,
                                                                                        partitionId,
                                                                                        indexId);
                                    Catalog.getInstance().getEditLog().logClearRollupIndexInfo(info);
                                }
                            } else {
                                // this can only happend when rollup index has been dropped.
                                // do nothing
                                Preconditions.checkState(true);
                            }
                        } // end for handling rollup
                        
                        int schemaHash = tableLoadInfo.getIndexSchemaHash(indexId);
                        short quorumNum = (short) (replicationNum / 2 + 1);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            
                            // get tablet file path
                            TabletLoadInfo tabletLoadInfo = tabletLoadInfos.get(tabletId);
                            String filePath = tabletLoadInfo.getFilePath();
                            long fileSize = tabletLoadInfo.getFileSize();

                            // get push type
                            TPushType type = TPushType.LOAD;
                            if (job.getDeleteFlag()) {
                                type = TPushType.LOAD_DELETE;
                            }
                            
                            // add task to batchTask
                            Set<Long> allReplicas = new HashSet<Long>();
                            Set<Long> finishedReplicas = new HashSet<Long>();
                            for (Replica replica : tablet.getReplicas()) {
                                long replicaId = replica.getId();
                                allReplicas.add(replicaId);
                                
                                // check replica state and replica version
                                ReplicaState state = replica.getState();
                                boolean checkByState = (state == ReplicaState.NORMAL 
                                        || state == ReplicaState.SCHEMA_CHANGE);
                                long replicaVersion = replica.getVersion();
                                long replicaVersionHash = replica.getVersionHash();
                                boolean checkByVersion = (replicaVersion == version - 1
                                        || (replicaVersion == version && replicaVersionHash != versionHash));

                                if (checkByState && checkByVersion) {
                                    if (!tabletLoadInfo.isReplicaSent(replicaId)) {
                                        AgentTask pushTask = new PushTask(job.getResourceInfo(),
                                                                          replica.getBackendId(), db.getId(), tableId,
                                                                          partitionId, indexId,
                                                                          tabletId, replicaId, schemaHash,
                                                                          version, versionHash, filePath, fileSize, 0,
                                                                          job.getId(), type, null,
                                                                          needDecompress, job.getPriority());
                                        if (AgentTaskQueue.addTask(pushTask)) {
                                            batchTask.addTask(pushTask);
                                            job.addPushTask((PushTask) pushTask);
                                            tabletLoadInfo.addSentReplica(replicaId);
                                        }
                                    }
                                } else if (replicaVersion > version 
                                        || (replicaVersion == version && replicaVersionHash == versionHash)) {
                                    finishedReplicas.add(replicaId);
                                    // add replica persist info
                                    long dataSize = replica.getDataSize();
                                    long rowCount = replica.getRowCount();
                                    ReplicaPersistInfo info = ReplicaPersistInfo.createForLoad(tableId, partitionId,
                                                                                               indexId, tabletId,
                                                                                               replicaId,
                                                                                               replicaVersion,
                                                                                               replicaVersionHash,
                                                                                               dataSize, rowCount);
                                    job.addReplicaPersistInfos(info);
                                } else {
                                    if (replicaVersion != version || replicaVersionHash != versionHash) {
                                        LOG.warn("replica version is lower than job. replica: {}-{}-{}-{}-{}, "
                                                + "replica state: {}, replica version: {}, replica version hash: {},"
                                                + " job version: {}, job version hash: {}, backend id: {}",
                                                 db.getId(), tableId, partitionId, tabletId, replicaId,
                                                 state, replicaVersion, replicaVersionHash,
                                                 version, versionHash, replica.getBackendId());
                                    }
                                }
                            } // end for replicas

                            if (allReplicas.size() == 0) {
                                LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                            }

                            // check tablet push statis
                            if (finishedReplicas.size() >= quorumNum) {
                                job.addQuorumTablet(tabletId);
                                if (finishedReplicas.size() == allReplicas.size()) {
                                    job.addFullTablet(tabletId);
                                }
                            }
                        } // end for tablets
                    } // end for indices
                } finally {
                    db.readUnlock();
                }
            } // end for partitions
        } // end for tables

        if (batchTask.getTaskNum() > 0) {
            AgentTaskExecutor.submit(batchTask);
        }
        return jobTotalTablets;
    }
    
    private void runQuorumFinishedJobs() {
        List<LoadJob> quorumFinishedJobs = Catalog.getInstance().getLoadInstance().getLoadJobs(
                JobState.QUORUM_FINISHED);
        for (LoadJob job : quorumFinishedJobs) {
            try {
                LOG.info("run quorum finished job. job: {}", job);
                runOneQuorumFinishedJob(job);
            } catch (Exception e) {
                LOG.warn("run quorum job error", e);
            }
        }

        // handle async delete job
        List<AsyncDeleteJob> quorumFinishedDeleteJobs =
                Catalog.getInstance().getLoadInstance().getQuorumFinishedDeleteJobs();
        for (AsyncDeleteJob job : quorumFinishedDeleteJobs) {
            try {
                LOG.info("run quorum finished delete job. job: {}", job.getJobId());
                runOneQuorumFinishedDeleteJob(job);
            } catch (Exception e) {
                LOG.warn("run quorum delete job error", e);
            }
        }
    }
    
    private void runOneQuorumFinishedJob(LoadJob job) {
        // if db is null, cancel load job
        Load load = Catalog.getInstance().getLoadInstance();
        long dbId = job.getDbId();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            load.cancelLoadJob(job, CancelType.LOAD_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        // submit push tasks to backends
        Set<Long> jobTotalTablets = submitPushTasks(job, db);
        if (jobTotalTablets == null) {
            LOG.warn("submit push tasks fail");
            return;
        }
        
        // check job finished
        boolean isJobFinished = false;
        if (job.getFullTablets().containsAll(jobTotalTablets)) {
            // db lock and check
            LOG.info("all tablets have been finished, check all replicas version. job id: {}", job.getId());
            db.writeLock();
            try {
                boolean allReplicaFinished = true;
                Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
            OUTER_LOOP:
                for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
                    long tableId = tableEntry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        LOG.warn("table does not exist. id: {}", tableId);
                        continue;
                    }
                    
                    TableLoadInfo tableLoadInfo = tableEntry.getValue();
                    for (Entry<Long, PartitionLoadInfo> partitionEntry : 
                            tableLoadInfo.getIdToPartitionLoadInfo().entrySet()) {
                        long partitionId = partitionEntry.getKey();
                        PartitionLoadInfo partitionLoadInfo = partitionEntry.getValue();
                        if (!partitionLoadInfo.isNeedLoad()) {
                            LOG.debug("partition does not have data to load. table id: {}, partition id: {}", 
                                    tableId, partitionId);
                            continue;
                        }

                        long version = partitionLoadInfo.getVersion();
                        long versionHash = partitionLoadInfo.getVersionHash();
                        
                        // all tables
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            LOG.warn("partition does not exist. id: " + partitionId);
                            continue;
                        }
                        List<MaterializedIndex> indices = partition.getMaterializedIndices();
                        for (MaterializedIndex materializedIndex : indices) {
                            if (materializedIndex.getState() == IndexState.ROLLUP) {
                                LOG.debug("index state is rollup. id: {}", materializedIndex.getId());
                                continue;
                            }
                            
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                for (Replica replica : tablet.getReplicas()) {
                                    // check version
                                    long replicaVersion = replica.getVersion();
                                    long replicaVersionHash = replica.getVersionHash();
                                    if ((replicaVersion == version && replicaVersionHash != versionHash) 
                                            || replicaVersion < version) {
                                        LOG.warn("replica does not catch up job version. replica: {}-{}-{}-{}-{}"
                                                + ". replica version: {}, replica version hash: {}" 
                                                + ", partition version: {}, partition version hash: {},"
                                                + " backend id: {}", dbId, tableId, partitionId, tablet.getId(),
                                                 replica.getId(), replicaVersion, replicaVersionHash,
                                                 version, versionHash, replica.getBackendId());
                                        allReplicaFinished = false;
                                        break OUTER_LOOP;
                                    }
                                } // end for replicas
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } // end for tables

                if (allReplicaFinished) {
                    if (load.updateLoadJobState(job, JobState.FINISHED)) {
                        isJobFinished = true;
                        LOG.info("load job finished. job: {}", job);
                    }
                }
            } finally {
                db.writeUnlock();
            }
            
            // clear
            if (isJobFinished) {
                load.clearJob(job, JobState.QUORUM_FINISHED);
                return;
            }
        }

        // handle if job is stay in QUORUM_FINISHED for a long time
        // maybe this job cannot be done. try to use Clone to finish this job
        long currentTimeMs = System.currentTimeMillis();
        if ((currentTimeMs - job.getCreateTimeMs()) / 1000 > Config.quorum_load_job_max_second) {
            LOG.warn("load job {} stay in QUORUM_FINISHED for a long time.", job.getId());

            db.readLock();
            try {
                Map<Long, TableLoadInfo> idToTableLoadInfo = job.getIdToTableLoadInfo();
                for (Entry<Long, TableLoadInfo> tableEntry : idToTableLoadInfo.entrySet()) {
                    long tableId = tableEntry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        continue;
                    }

                    TableLoadInfo tableLoadInfo = tableEntry.getValue();
                    for (Entry<Long, PartitionLoadInfo> partitionEntry : tableLoadInfo.getIdToPartitionLoadInfo()
                            .entrySet()) {
                        long partitionId = partitionEntry.getKey();
                        PartitionLoadInfo partitionLoadInfo = partitionEntry.getValue();
                        if (!partitionLoadInfo.isNeedLoad()) {
                            continue;
                        }

                        long version = partitionLoadInfo.getVersion();
                        long versionHash = partitionLoadInfo.getVersionHash();

                        // all tables
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            continue;
                        }
                        List<MaterializedIndex> indices = partition.getMaterializedIndices();
                        for (MaterializedIndex materializedIndex : indices) {
                            if (materializedIndex.getState() == IndexState.ROLLUP) {
                                continue;
                            }

                            Clone clone = Catalog.getInstance().getCloneInstance();
                            for (Tablet tablet : materializedIndex.getTablets()) {
                                if (clone.containsTablet(tablet.getId())) {
                                    // this tablet already has a clone job
                                    continue;
                                }
                                for (Replica replica : tablet.getReplicas()) {
                                    // check version
                                    long replicaVersion = replica.getVersion();
                                    long replicaVersionHash = replica.getVersionHash();
                                    boolean checkByState = (replica.getState() == ReplicaState.NORMAL
                                            || replica.getState() == ReplicaState.SCHEMA_CHANGE);
                                    boolean checkByVersion =
                                            (replicaVersion == version - 1
                                            || (replicaVersion == version && replicaVersionHash != versionHash));

                                    if (checkByState && checkByVersion) {
                                        // find a dest backend
                                        Set<Long> tabletBeIds = tablet.getBackendIds();
                                        List<Long> destBeId = null;
                                        int tryTime = tabletBeIds.size() + 1;
                                        LOG.debug("tryTime: {}, tablet be: {}", tryTime, tabletBeIds);
                                        do {
                                            destBeId = Catalog.getCurrentSystemInfo().seqChooseBackendIds(
                                                                                 1, true, false, db.getClusterName());
                                            LOG.debug("descBeId: {}", destBeId);
                                            --tryTime;
                                            if (destBeId == null) {
                                                break;
                                            }
                                        } while (tabletBeIds.contains(destBeId.get(0)) && tryTime > 0);

                                        if (destBeId != null && !tabletBeIds.contains(destBeId.get(0))) {
                                            if (clone.addCloneJob(dbId, tableId, partitionId,
                                                                  materializedIndex.getId(),
                                                                  tablet.getId(), destBeId.get(0),
                                                                  JobType.MIGRATION, JobPriority.HIGH,
                                                                  Config.clone_job_timeout_second * 1000L)) {
                                                LOG.info("try use Clone to finish load job: {}. Tablet: {}, desc be: {}"
                                                                 + "lower version replica: {} "
                                                                 + "with version {} in be: {}",
                                                         job.getId(), tablet.getId(), destBeId.get(0),
                                                         replica.getId(), replicaVersion, replica.getBackendId());
                                            }
                                        } else {
                                            LOG.warn("failed to choose be to do Clone. Load job: {}. tablet: {}",
                                                     job.getId(), tablet.getId());
                                        }
                                    }
                                } // end for replicas
                            } // end for tablets
                        } // end for indices
                    } // end for partitions
                } // end for tables
            } finally {
                db.readUnlock();
            }
        }
    }
    
    private void runOneQuorumFinishedDeleteJob(AsyncDeleteJob job) {
        Load load = Catalog.getInstance().getLoadInstance();
        long dbId = job.getDbId();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            load.removeDeleteJobAndSetState(job);
            return;
        }
        db.readLock();
        try {
            long tableId = job.getTableId();
            OlapTable olapTable = (OlapTable) db.getTable(tableId);
            if (olapTable == null) {
                load.removeDeleteJobAndSetState(job);
                return;
            }

            long partitionId = job.getPartitionId();
            Partition partition = olapTable.getPartition(partitionId);
            if (partition == null) {
                load.removeDeleteJobAndSetState(job);
                return;
            }

            boolean allReplicaFinished = true;
            long jobId = job.getJobId();
            long version = job.getPartitionVersion();
            long versionHash = job.getPartitionVersionHash();
            Set<Long> tabletIds = job.getTabletIds();
            AgentBatchTask batchTask = new AgentBatchTask();
            for (MaterializedIndex index : partition.getMaterializedIndices()) {
                long indexId = index.getId();
                int schemaHash = olapTable.getSchemaHashByIndexId(indexId);
                for (Tablet tablet : index.getTablets()) {
                    long tabletId = tablet.getId();
                    if (!tabletIds.contains(tabletId)) {
                        continue;
                    }
                    for (Replica replica : tablet.getReplicas()) {
                        // check version
                        long replicaVersion = replica.getVersion();
                        long replicaVersionHash = replica.getVersionHash();
                        if ((replicaVersion == version && replicaVersionHash != versionHash)
                                || replicaVersion < version) {
                            LOG.warn("delete job:{}. replica does not catch up job version. replica: {}-{}-{}-{}-{}"
                                    + ". replica version: {}, replica version hash: {}"
                                    + ", partition version: {}, partition version hash: {},"
                                    + " backend id: {}, replica state: {}",
                                     jobId, dbId, tableId, partitionId, tabletId, replica.getId(),
                                     replicaVersion, replicaVersionHash, version, versionHash,
                                     replica.getBackendId(), replica.getState());
                            allReplicaFinished = false;

                            if (replica.getState() != ReplicaState.NORMAL) {
                                allReplicaFinished = false;
                                continue;
                            }

                            if (replicaVersion != version && replicaVersion != version - 1) {
                                continue;
                            }

                            if (!job.hasSend(replica.getId())) {
                                PushTask task = new PushTask(null, replica.getBackendId(),
                                                             dbId, tableId, partitionId,
                                                             indexId, tabletId, replica.getId(),
                                                             schemaHash, version,
                                                             versionHash, null, -1,
                                                             -1, job.getJobId(), TPushType.DELETE,
                                                             job.getConditions(), false, TPriority.HIGH);
                                task.setIsSyncDelete(false);
                                task.setAsyncDeleteJobId(job.getJobId());
                                if (AgentTaskQueue.addTask(task)) {
                                    batchTask.addTask(task);
                                    job.setIsSend(replica.getId(), task);
                                }
                            }
                        }
                    }
                }
            } // end for indices

            AgentTaskExecutor.submit(batchTask);

            if (allReplicaFinished) {
                // clear and finish delete job
                job.clearTasks();
                job.setState(DeleteState.FINISHED);

                // log
                Catalog.getInstance().getEditLog().logFinishAsyncDelete(job);
                load.removeDeleteJobAndSetState(job);
                LOG.info("delete job {} finished", job.getJobId());
            }

        } finally {
            db.readUnlock();
        }
    }

    public static boolean checkTimeout(LoadJob job) {
        int timeoutSecond = job.getTimeoutSecond();
        if (timeoutSecond == 0) {
            return false;
        }
        
        long deltaSecond = (System.currentTimeMillis() - job.getCreateTimeMs()) / 1000;
        if (deltaSecond > timeoutSecond) {
            return true;
        }

        return false;
    }
   
}
