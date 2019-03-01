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

import org.apache.doris.alter.RollupJob;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.common.Config;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.load.AsyncDeleteJob.DeleteState;
import org.apache.doris.load.FailMsg.CancelType;
import org.apache.doris.load.LoadJob.EtlJobType;
import org.apache.doris.load.LoadJob.JobState;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.HadoopLoadEtlTask;
import org.apache.doris.task.HadoopLoadPendingTask;
import org.apache.doris.task.InsertLoadEtlTask;
import org.apache.doris.task.MasterTask;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.task.MiniLoadEtlTask;
import org.apache.doris.task.MiniLoadPendingTask;
import org.apache.doris.task.PullLoadEtlTask;
import org.apache.doris.task.PullLoadPendingTask;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
        // get db
        long dbId = job.getDbId();
        Database db = Catalog.getInstance().getDb(dbId);
        if (db == null) {
            load.cancelLoadJob(job, CancelType.LOAD_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        if (job.getTransactionId() < 0) {
            LOG.warn("cancel load job {}  because it is an old type job, user should resubmit it", job);
            load.cancelLoadJob(job, CancelType.UNKNOWN, "cancelled because system is during upgrade, user should resubmit it");
            return;
        }
        // check if the job is aborted in transaction manager
        TransactionState state = Catalog.getCurrentGlobalTransactionMgr()
                .getTransactionState(job.getTransactionId());
        if (state == null) {
            LOG.warn("cancel load job {}  because could not find transaction state", job);
            load.cancelLoadJob(job, CancelType.UNKNOWN, "transaction state lost");
            return;
        }
        if (state.getTransactionStatus() == TransactionStatus.ABORTED) {
            load.cancelLoadJob(job, CancelType.LOAD_RUN_FAIL, 
                    "job is aborted in transaction manager [" + state + "]");
            return;
        } else if (state.getTransactionStatus() == TransactionStatus.COMMITTED) {
            // if job is committed and then fe restart, the progress is not persisted, so that set it here
            job.setProgress(100);
            LOG.debug("job {} is already committed, just wait it to be visiable, transaction state {}", job, state);
            return;
        } else if (state.getTransactionStatus() == TransactionStatus.VISIBLE) {
            if (load.updateLoadJobState(job, JobState.FINISHED)) {
                load.clearJob(job, JobState.QUORUM_FINISHED);
            }
            return;
        }
        
        if (checkTimeout(job)) {
            load.cancelLoadJob(job, CancelType.TIMEOUT, "loading timeout to cancel");
            return;
        }
        // submit push tasks to backends
        Set<Long> jobTotalTablets = submitPushTasks(job, db);
        if (jobTotalTablets == null) {
            load.cancelLoadJob(job, CancelType.LOAD_RUN_FAIL, "submit push tasks fail");
            return;
        }
        
        // yiguolei: for real time load we use full finished replicas
        Set<Long> fullTablets = job.getFullTablets();
        if (state.isRunning()) {
            job.setProgress(fullTablets.size() * 100 / jobTotalTablets.size());
        } else {
            job.setProgress(100);
        }
        
        long stragglerTimeout = job.isSyncDeleteJob() ? job.getDeleteJobTimeout() / 2 
                                                    : Config.load_straggler_wait_second * 1000;
        if (job.getQuorumTablets().containsAll(jobTotalTablets)) {
            // commit the job to transaction manager and not care about the result
            // if could not commit successfully and commit again until job is timeout
            if (job.getQuorumFinishTimeMs() < 0) {
                job.setQuorumFinishTimeMs(System.currentTimeMillis());
            }

            // if all tablets are finished or stay in quorum finished for long time, try to commit it.
            if (System.currentTimeMillis() - job.getQuorumFinishTimeMs() > stragglerTimeout
                    || job.getFullTablets().containsAll(jobTotalTablets)) {
                tryCommitJob(job, db);
            }
        }
    }
    
    private void tryCommitJob(LoadJob job, Database db) {
        // check transaction state
        Load load = Catalog.getInstance().getLoadInstance();
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        TransactionState transactionState = globalTransactionMgr.getTransactionState(job.getTransactionId());
        List<TabletCommitInfo> tabletCommitInfos = new ArrayList<TabletCommitInfo>();
        // when be finish load task, fe will update job's finish task info, use lock here to prevent
        // concurrent problems
        db.writeLock();
        try {
            TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();
            for (Replica replica : job.getFinishedReplicas()) {
                // the inverted index contains rolling up replica
                Long tabletId = invertedIndex.getTabletIdByReplica(replica.getId());
                if (tabletId == null) {
                    LOG.warn("could not find tablet id for replica {}, the tablet maybe dropped", replica);
                    continue;
                }
                tabletCommitInfos.add(new TabletCommitInfo(tabletId, replica.getBackendId()));
            }
            globalTransactionMgr.commitTransaction(job.getDbId(), job.getTransactionId(), tabletCommitInfos);
        } catch (MetaNotFoundException | TransactionException e) {
            LOG.warn("errors while commit transaction [{}], cancel the job {}, reason is {}", 
                    transactionState.getTransactionId(), job, e);
            load.cancelLoadJob(job, CancelType.UNKNOWN, transactionState.getReason());
        } finally {
            db.writeUnlock();
        }
    }

    private Set<Long> submitPushTasks(LoadJob job, Database db) {
        Map<Long, TabletLoadInfo> tabletLoadInfos = job.getIdToTabletLoadInfo();
        boolean needDecompress = (job.getEtlJobType() == EtlJobType.HADOOP) ? true : false;
        AgentBatchTask batchTask = new AgentBatchTask();
        Set<Long> jobTotalTablets = new HashSet<Long>();

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
                // if table is dropped during load, the the job is failed
                return null;
            }
            TableLoadInfo tableLoadInfo = tableEntry.getValue();
            // check if the job is submit during rollup
            RollupJob rollupJob = (RollupJob) Catalog.getInstance().getRollupHandler().getAlterJob(tableId);
            boolean autoLoadToTwoTablet = true;
            if (rollupJob != null && job.getTransactionId() > 0) {
                long rollupIndexId = rollupJob.getRollupIndexId();
                if (tableLoadInfo.containsIndex(rollupIndexId)) {
                    autoLoadToTwoTablet = false;
                }
            }
            
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
                        // if partition is 
                        return null;
                    }
                    
                    short replicationNum = table.getPartitionInfo().getReplicationNum(partition.getId());
                    // check all indices (base + roll up (not include ROLLUP state index))
                    List<MaterializedIndex> indices = partition.getMaterializedIndices();
                    for (MaterializedIndex index : indices) {
                        long indexId = index.getId();
                        // if index is in rollup, then not load into it, be will automatically convert the data
                        if (index.getState() == IndexState.ROLLUP) {
                            LOG.error("skip table under rollup[{}]", indexId);
                            continue;
                        }
                        // 1. the load job's etl is started before rollup finished
                        // 2. rollup job comes into finishing state, add rollup index to catalog
                        // 3. load job's etl finished, begin to load
                        // 4. load will send data to new rollup index, but could not get schema hash, load will failed
                        if (!tableLoadInfo.containsIndex(indexId)) {
                            if (rollupJob.getRollupIndexId() == indexId) {
                                continue;
                            } else {
                                // if the index is not during rollup and not contained in table load info, it a fatal error
                                // return null, will cancel the load job
                                LOG.warn("could not find index {} in table load info, and could not find " 
                                        + "it in rollup job, it is a fatal error", indexId);
                                return null;
                            }
                        }
                        
                        // add to jobTotalTablets first.
                        for (Tablet tablet : index.getTablets()) {
                            // the job is submmitted before rollup finished and try to finish after rollup finished
                            // then the job's tablet load info does not contain the new rollup index's tablet
                            // not deal with this case because the finished replica will include new rollup index's replica
                            // and check it at commit time 
                            if (tabletLoadInfos.containsKey(tablet.getId())) {
                                jobTotalTablets.add(tablet.getId());
                            }
                        }
                        
                        int schemaHash = tableLoadInfo.getIndexSchemaHash(indexId);
                        short quorumNum = (short) (replicationNum / 2 + 1);
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            // get tablet file path
                            TabletLoadInfo tabletLoadInfo = tabletLoadInfos.get(tabletId);
                            // the tabletinfo maybe null, in this case:
                            // the job is submmitted before rollup finished and try to finish after rollup finished
                            // then the job's tablet load info does not contain the new rollup index's tablet
                            // not deal with this case because the finished replica will include new rollup index's replica
                            // and check it at commit time
                            if (tabletLoadInfo == null) {
                                continue;
                            }
                            String filePath = tabletLoadInfo.getFilePath();
                            long fileSize = tabletLoadInfo.getFileSize();

                            // get push type
                            TPushType type = TPushType.LOAD;
                            if (job.isSyncDeleteJob()) {
                                type = TPushType.DELETE;
                            } else if (job.getDeleteFlag()) {
                                type = TPushType.LOAD_DELETE;
                            }
                            
                            // add task to batchTask
                            Set<Long> allReplicas = new HashSet<Long>();
                            Set<Long> finishedReplicas = new HashSet<Long>();
                            for (Replica replica : tablet.getReplicas()) {
                                long replicaId = replica.getId();
                                allReplicas.add(replicaId);
                                // yiguolei: real time load do not need check replica state and version, version hashs
                                // check replica state and replica version
                                if (!tabletLoadInfo.isReplicaSent(replicaId)) {
                                    PushTask pushTask = new PushTask(job.getResourceInfo(),
                                                                      replica.getBackendId(), db.getId(), tableId,
                                                                      partitionId, indexId,
                                                                      tabletId, replicaId, schemaHash,
                                                                      -1, 0, filePath, fileSize, 0,
                                                                      job.getId(), type, job.getConditions(),
                                                                      needDecompress, job.getPriority(), 
                                                                      TTaskType.REALTIME_PUSH, 
                                                                      job.getTransactionId(), 
                                                                      Catalog.getCurrentGlobalTransactionMgr().getTransactionIDGenerator().getNextTransactionId());
                                    pushTask.setIsSchemaChanging(autoLoadToTwoTablet);
                                    if (AgentTaskQueue.addTask(pushTask)) {
                                        batchTask.addTask(pushTask);
                                        job.addPushTask((PushTask) pushTask);
                                        tabletLoadInfo.addSentReplica(replicaId);
                                    }
                                }
                                // yiguolei: wait here to check if quorum finished, should exclude the replica that is in clone state
                                // for example, there are 3 replicas, A normal  B normal C clone, if A and C finish loading, we should not commit
                                // because commit will failed, then the job is failed
                                if (job.isReplicaFinished(replicaId) && replica.getLastFailedVersion() < 0) {
                                    finishedReplicas.add(replicaId);
                                }
                            } // end for replicas

                            if (allReplicas.size() == 0) {
                                LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                            }

                            // check tablet push states
                            // quorum tablets and full tablets should be in tabletload infos or the process will > 100%
                            if (finishedReplicas.size() >= quorumNum && tabletLoadInfos.containsKey(tabletId)) {
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
        // if the job is quorum finished, just set it to finished and clear related etl job
        if (load.updateLoadJobState(job, JobState.FINISHED)) {
            load.clearJob(job, JobState.QUORUM_FINISHED);
            return;
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
            // if the delete job is quorum finished, just set it to finished
            job.clearTasks();
            job.setState(DeleteState.FINISHED);
            // log
            Catalog.getInstance().getEditLog().logFinishAsyncDelete(job);
            load.removeDeleteJobAndSetState(job);
            LOG.info("delete job {} finished", job.getJobId());
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
