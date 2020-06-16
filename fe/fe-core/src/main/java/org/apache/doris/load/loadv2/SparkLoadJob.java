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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ResourceDesc;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DataQualityException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.dpp.DppResult;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TabletQuorumFailedException;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * There are 4 steps in SparkLoadJob:
 * Step1: SparkLoadPendingTask will be created by unprotectedExecuteJob method and submit spark etl job.
 * Step2: LoadEtlChecker will check spark etl job status periodically and send push tasks to be when spark etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodically and commit transaction when push tasks are finished.
 * Step4: PublishVersionDaemon will send publish version tasks to be and finish transaction.
 */
public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    // --- members below need persist ---
    // create from resourceDesc when job created
    private SparkResource sparkResource;
    // members below updated when job state changed to etl
    private long etlStartTimestamp = -1;
    // for spark yarn
    private String appId = "";
    // spark job outputPath
    private String etlOutputPath = "";
    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    private Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

    // --- members below not persist ---
    private ResourceDesc resourceDesc;
    // for spark standalone
    private SparkLoadAppHandle sparkLoadAppHandle = new SparkLoadAppHandle();
    // for straggler wait long time to commit transaction
    private long quorumFinishTimestamp = -1;
    // below for push task
    private Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();
    private Map<Long, PushBrokerReaderParams> indexToPushBrokerReaderParams = Maps.newHashMap();
    private Map<Long, Integer> indexToSchemaHash = Maps.newHashMap();
    private Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask = Maps.newHashMap();
    private Set<Long> finishedReplicas = Sets.newHashSet();
    private Set<Long> quorumTablets = Sets.newHashSet();
    private Set<Long> fullTablets = Sets.newHashSet();

    // only for log replay
    public SparkLoadJob() {
        super();
        jobType = EtlJobType.SPARK;
    }

    public SparkLoadJob(long dbId, String label, ResourceDesc resourceDesc,
                        OriginStatement originStmt, UserIdentity userInfo)
            throws MetaNotFoundException {
        super(dbId, label, originStmt, userInfo);
        this.resourceDesc = resourceDesc;
        timeoutSecond = Config.spark_load_default_timeout_second;
        jobType = EtlJobType.SPARK;
    }

    @Override
    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        super.setJobProperties(properties);

        // set spark resource and broker desc
        setResourceInfo();
    }

    /**
     * merge system conf with load stmt
     * @throws DdlException
     */
    private void setResourceInfo() throws DdlException {
        // spark resource
        String resourceName = resourceDesc.getName();
        Resource oriResource = Catalog.getCurrentCatalog().getResourceMgr().getResource(resourceName);
        if (oriResource == null) {
            throw new DdlException("Resource does not exist. name: " + resourceName);
        }
        sparkResource = ((SparkResource) oriResource).getCopiedResource();
        sparkResource.update(resourceDesc);

        // broker desc
        Map<String, String> brokerProperties = sparkResource.getBrokerPropertiesWithoutPrefix();
        brokerDesc = new BrokerDesc(sparkResource.getBroker(), brokerProperties);
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
       transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                                  new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                                  LoadJobSourceType.FRONTEND, id, timeoutSecond);
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        // create pending task
        LoadTask task = new SparkLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(),
                                                 sparkResource, brokerDesc);
        task.init();
        idToTasks.put(task.getSignature(), task);
        Catalog.getCurrentCatalog().getPendingLoadTaskScheduler().submit(task);
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof SparkPendingTaskAttachment) {
            onPendingTaskFinished((SparkPendingTaskAttachment) attachment);
        }
    }

    private void onPendingTaskFinished(SparkPendingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                 .add("state", state)
                                 .add("error_msg", "this task will be ignored when job is: " + state)
                                 .build());
                return;
            }

            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                 .add("task_id", attachment.getTaskId())
                                 .add("error_msg", "this is a duplicated callback of pending task "
                                         + "when broker already has loading task")
                                 .build());
                return;
            }

            // add task id into finishedTaskIds
            finishedTaskIds.add(attachment.getTaskId());

            sparkLoadAppHandle = attachment.getHandle();
            appId = attachment.getAppId();
            etlOutputPath = attachment.getOutputPath();

            executeEtl();
            // log etl state
            unprotectedLogUpdateStateInfo();
        } finally {
            writeUnlock();
        }
    }

    /**
     * update etl start time and state in spark load job
     */
    private void executeEtl() {
        etlStartTimestamp = System.currentTimeMillis();
        state = JobState.ETL;
        LOG.info("update to {} state success. job id: {}", state, id);
    }

    private boolean checkState(JobState expectState) {
        readLock();
        try {
            if (state == expectState) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    /**
     * Check the status of etl job regularly
     * 1. RUNNING, update etl job progress
     * 2. CANCELLED, cancel load job
     * 3. FINISHED, get the etl output file paths, update job state to LOADING and log job update info
     *
     * Send push tasks if job state changed to LOADING
     */
    public void updateEtlStatus() throws Exception {
        if (!checkState(JobState.ETL)) {
            return;
        }

        // get etl status
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        EtlStatus status = handler.getEtlJobStatus(sparkLoadAppHandle, appId, id, etlOutputPath, sparkResource, brokerDesc);
        writeLock();
        try {
            switch (status.getState()) {
                case RUNNING:
                    unprotectedUpdateEtlStatusInternal(status);
                    break;
                case FINISHED:
                    unprotectedProcessEtlFinish(status, handler);
                    break;
                case CANCELLED:
                    throw new LoadException("spark etl job failed. msg: " + status.getFailMsg());
                default:
                    LOG.warn("unknown etl state: {}", status.getState().name());
                    break;
            }
        } finally {
            writeUnlock();
        }

        if (checkState(JobState.LOADING)) {
            // create and send push tasks
            submitPushTasks();
        }
    }

    private void unprotectedUpdateEtlStatusInternal(EtlStatus etlStatus) {
        loadingStatus = etlStatus;
        progress = etlStatus.getProgress();
        if (!sparkResource.isYarnMaster()) {
            loadingStatus.setTrackingUrl(appId);
        }

        DppResult dppResult = etlStatus.getDppResult();
        if (dppResult != null) {
            // update load statistic and counters when spark etl job finished
            // fe gets these infos from spark dpp, so we use dummy load id and dummy backend id here
            loadStatistic.fileNum = (int) dppResult.fileNumber;
            loadStatistic.totalFileSizeB = dppResult.fileSize;
            TUniqueId dummyId = new TUniqueId(0, 0);
            long dummyBackendId = -1L;
            loadStatistic.initLoad(dummyId, Sets.newHashSet(dummyId), Lists.newArrayList(dummyBackendId));
            loadStatistic.updateLoadProgress(dummyBackendId, dummyId, dummyId, dppResult.scannedRows, true);

            Map<String, String> counters = loadingStatus.getCounters();
            counters.put(DPP_NORMAL_ALL, String.valueOf(dppResult.normalRows));
            counters.put(DPP_ABNORMAL_ALL, String.valueOf(dppResult.abnormalRows));
            counters.put(UNSELECTED_ROWS, String.valueOf(dppResult.unselectRows));
        }
    }

    private void unprotectedProcessEtlFinish(EtlStatus etlStatus, SparkEtlJobHandler handler) throws Exception {
        unprotectedUpdateEtlStatusInternal(etlStatus);
        // checkDataQuality
        if (!checkDataQuality()) {
            throw new DataQualityException(DataQualityException.QUALITY_FAIL_MSG);
        }

        // get etl output files and update loading state
        unprotectedUpdateToLoadingState(etlStatus, handler.getEtlFilePaths(etlOutputPath, brokerDesc));
        // log loading state
        unprotectedLogUpdateStateInfo();
        // prepare loading infos
        unprotectedPrepareLoadingInfos();
    }

    private void unprotectedUpdateToLoadingState(EtlStatus etlStatus, Map<String, Long> filePathToSize) throws LoadException {
        try {
            for (Map.Entry<String, Long> entry : filePathToSize.entrySet()) {
                String filePath = entry.getKey();
                if (!filePath.endsWith(EtlJobConfig.ETL_OUTPUT_FILE_FORMAT)) {
                    continue;
                }
                String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
                tabletMetaToFileInfo.put(tabletMetaStr, Pair.create(filePath, entry.getValue()));
            }

            loadingStatus = etlStatus;
            progress = 0;
            unprotectedUpdateState(JobState.LOADING);
            LOG.info("update to {} state success. job id: {}", state, id);
        } catch (Exception e) {
            LOG.warn("update to {} state failed. job id: {}", state, id, e);
            throw new LoadException(e.getMessage(), e);
        }
    }

    private void unprotectedPrepareLoadingInfos() {
        for (String tabletMetaStr : tabletMetaToFileInfo.keySet()) {
            String[] fileNameArr = tabletMetaStr.split("\\.");
            // tableId.partitionId.indexId.bucket.schemaHash
            Preconditions.checkState(fileNameArr.length == 5);
            long tableId = Long.parseLong(fileNameArr[0]);
            long partitionId = Long.parseLong(fileNameArr[1]);
            long indexId = Long.parseLong(fileNameArr[2]);
            int schemaHash = Integer.parseInt(fileNameArr[4]);

            if (!tableToLoadPartitions.containsKey(tableId)) {
                tableToLoadPartitions.put(tableId, Sets.newHashSet());
            }
            tableToLoadPartitions.get(tableId).add(partitionId);

            indexToSchemaHash.put(indexId, schemaHash);
        }
    }

    private PushBrokerReaderParams getPushBrokerReaderParams(OlapTable table, long indexId) throws UserException {
        if (!indexToPushBrokerReaderParams.containsKey(indexId)) {
            PushBrokerReaderParams pushBrokerReaderParams = new PushBrokerReaderParams();
            pushBrokerReaderParams.init(table.getSchemaByIndexId(indexId), brokerDesc);
            indexToPushBrokerReaderParams.put(indexId, pushBrokerReaderParams);
        }
        return indexToPushBrokerReaderParams.get(indexId);
    }

    private Set<Long> submitPushTasks() throws UserException {
        // check db exist
        Database db = null;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("label", label)
                    .add("error_msg", "db has been deleted when job is loading")
                    .build();
            throw new MetaNotFoundException(errMsg);
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        boolean hasLoadPartitions = false;
        Set<Long> totalTablets = Sets.newHashSet();
        db.readLock();
        try {
            writeLock();
            try {
                // check state is still loading. If state is cancelled or finished, return.
                // if state is cancelled or finished and not return, this would throw all partitions have no load data exception,
                // because tableToLoadPartitions was already cleaned up,
                if (state != JobState.LOADING) {
                    LOG.warn("job state is not loading. job id: {}, state: {}", id, state);
                    return totalTablets;
                }

                for (Map.Entry<Long, Set<Long>> entry : tableToLoadPartitions.entrySet()) {
                    long tableId = entry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        LOG.warn("table does not exist. id: {}", tableId);
                        continue;
                    }

                    Set<Long> partitionIds = entry.getValue();
                    for (long partitionId : partitionIds) {
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            LOG.warn("partition does not exist. id: {}", partitionId);
                            continue;
                        }

                        hasLoadPartitions = true;
                        int quorumReplicaNum = table.getPartitionInfo().getReplicationNum(partitionId) / 2 + 1;

                        List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.ALL);
                        for (MaterializedIndex index : indexes) {
                            long indexId = index.getId();
                            int schemaHash = indexToSchemaHash.get(indexId);

                            int bucket = 0;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletId = tablet.getId();
                                totalTablets.add(tabletId);
                                String tabletMetaStr = String.format("%d.%d.%d.%d.%d", tableId, partitionId,
                                                                     indexId, bucket++, schemaHash);
                                Set<Long> tabletAllReplicas = Sets.newHashSet();
                                Set<Long> tabletFinishedReplicas = Sets.newHashSet();
                                for (Replica replica : tablet.getReplicas()) {
                                    long replicaId = replica.getId();
                                    tabletAllReplicas.add(replicaId);
                                    if (!tabletToSentReplicaPushTask.containsKey(tabletId)
                                            || !tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId)) {
                                        long backendId = replica.getBackendId();
                                        long taskSignature = Catalog.getCurrentGlobalTransactionMgr()
                                                .getTransactionIDGenerator().getNextTransactionId();

                                        PushBrokerReaderParams params = getPushBrokerReaderParams(table, indexId);
                                        // deep copy TBrokerScanRange because filePath and fileSize will be updated
                                        // in different tablet push task
                                        TBrokerScanRange tBrokerScanRange = new TBrokerScanRange(params.tBrokerScanRange);
                                        // update filePath fileSize
                                        TBrokerRangeDesc tBrokerRangeDesc = tBrokerScanRange.getRanges().get(0);
                                        tBrokerRangeDesc.setPath("");
                                        tBrokerRangeDesc.setFileSize(-1);
                                        if (tabletMetaToFileInfo.containsKey(tabletMetaStr)) {
                                            Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
                                            tBrokerRangeDesc.setPath(fileInfo.first);
                                            tBrokerRangeDesc.setFileSize(fileInfo.second);
                                        }

                                        // update broker address
                                        Backend backend = Catalog.getCurrentCatalog().getCurrentSystemInfo()
                                                .getBackend(backendId);
                                        FsBroker fsBroker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(
                                                brokerDesc.getName(), backend.getHost());
                                        tBrokerScanRange.getBrokerAddresses().add(
                                                new TNetworkAddress(fsBroker.ip, fsBroker.port));

                                        LOG.debug("push task for replica {}, broker {}:{}, backendId {}, filePath {}, fileSize {}" ,
                                                  replicaId, fsBroker.ip, fsBroker.port, backendId, tBrokerRangeDesc.path,
                                                  tBrokerRangeDesc.file_size);

                                        PushTask pushTask = new PushTask(backendId, dbId, tableId, partitionId,
                                                                         indexId, tabletId, replicaId, schemaHash,
                                                                         0, id, TPushType.LOAD_V2,
                                                                         TPriority.NORMAL, transactionId, taskSignature,
                                                                         tBrokerScanRange, params.tDescriptorTable);
                                        if (AgentTaskQueue.addTask(pushTask)) {
                                            batchTask.addTask(pushTask);
                                            if (!tabletToSentReplicaPushTask.containsKey(tabletId)) {
                                                tabletToSentReplicaPushTask.put(tabletId, Maps.newHashMap());
                                            }
                                            tabletToSentReplicaPushTask.get(tabletId).put(replicaId, pushTask);
                                        }
                                    }

                                    if (finishedReplicas.contains(replicaId) && replica.getLastFailedVersion() < 0) {
                                        tabletFinishedReplicas.add(replicaId);
                                    }
                                }

                                if (tabletAllReplicas.size() == 0) {
                                    LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                                }

                                // check tablet push states
                                if (tabletFinishedReplicas.size() >= quorumReplicaNum) {
                                    quorumTablets.add(tabletId);
                                    if (tabletFinishedReplicas.size() == tabletAllReplicas.size()) {
                                        fullTablets.add(tabletId);
                                    }
                                }
                            }
                        }
                    }
                }

                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }

                if (!hasLoadPartitions) {
                    String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                            .add("database_id", dbId)
                            .add("label", label)
                            .add("error_msg", "all partitions have no load data")
                            .build();
                    throw new LoadException(errMsg);
                }

                return totalTablets;
            } finally {
                writeUnlock();
            }
        } finally {
            db.readUnlock();
        }
    }

    public void addFinishedReplica(long replicaId, long tabletId, long backendId) {
        writeLock();
        try {
            if (finishedReplicas.add(replicaId)) {
                commitInfos.add(new TabletCommitInfo(tabletId, backendId));
                // set replica push task null
                Map<Long, PushTask> sentReplicaPushTask = tabletToSentReplicaPushTask.get(tabletId);
                if (sentReplicaPushTask != null) {
                    if (sentReplicaPushTask.containsKey(replicaId)) {
                        sentReplicaPushTask.put(replicaId, null);
                    }
                }
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * 1. Sends push tasks to Be
     * 2. Commit transaction after all push tasks execute successfully
     */
    public void updateLoadingStatus() throws UserException {
        if (!checkState(JobState.LOADING)) {
            return;
        }

        // submit push tasks
        Set<Long> totalTablets = submitPushTasks();
        if (totalTablets.isEmpty()) {
            LOG.warn("total tablets set is empty. job id: {}, state: {}", id, state);
            return;
        }

        // update status
        boolean canCommitJob = false;
        writeLock();
        try {
            // loading progress
            // 100: txn status is visible and load has been finished
            progress = fullTablets.size() * 100 / totalTablets.size();
            if (progress == 100) {
                progress = 99;
            }

            // quorum finish ts
            if (quorumFinishTimestamp < 0 && quorumTablets.containsAll(totalTablets)) {
                quorumFinishTimestamp = System.currentTimeMillis();
            }

            // if all replicas are finished or stay in quorum finished for long time, try to commit it.
            long stragglerTimeout = Config.load_straggler_wait_second * 1000;
            if ((quorumFinishTimestamp > 0 && System.currentTimeMillis() - quorumFinishTimestamp > stragglerTimeout)
                    || fullTablets.containsAll(totalTablets)) {
                canCommitJob = true;
            }
        } finally {
            writeUnlock();
        }

        // try commit transaction
        if (canCommitJob) {
            tryCommitJob();
        }
    }

    private void tryCommitJob() throws UserException {
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                         .add("txn_id", transactionId)
                         .add("msg", "Load job try to commit txn")
                         .build());
        Database db = getDb();
        db.writeLock();
        try {
//            Catalog.getCurrentGlobalTransactionMgr().commitTransaction(
//                    dbId, transactionId, commitInfos,
//                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
//                                              finishTimestamp, state, failMsg));
            throw new TabletQuorumFailedException(1, 1, 1, 1, null);
        } catch (TabletQuorumFailedException e) {
            // retry in next loop
        } finally {
            db.writeUnlock();
        }
    }

    /**
     * load job already cancelled or finished, clear job below:
     * 1. kill etl job and delete etl files
     * 2. clear push tasks and infos that not persist
     */
    private void clearJob() {
        Preconditions.checkState(state == JobState.FINISHED || state == JobState.CANCELLED);

        LOG.debug("kill etl job and delete etl files. id: {}, state: {}", id, state);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        if (state == JobState.CANCELLED) {
            if ((!Strings.isNullOrEmpty(appId) && sparkResource.isYarnMaster()) || sparkLoadAppHandle != null) {
                try {
                    handler.killEtlJob(sparkLoadAppHandle, appId, id, sparkResource);
                } catch (Exception e) {
                    LOG.warn("kill etl job failed. id: {}, state: {}", id, state, e);
                }
            }
        }
        if (!Strings.isNullOrEmpty(etlOutputPath)) {
            try {
                // delete label dir, remove the last taskId dir
                String outputPath = etlOutputPath.substring(0, etlOutputPath.lastIndexOf("/"));
                handler.deleteEtlOutputPath(outputPath, brokerDesc);
            } catch (Exception e) {
                LOG.warn("delete etl files failed. id: {}, state: {}", id, state, e);
            }
        }

        LOG.debug("clear push tasks and infos that not persist. id: {}, state: {}", id, state);
        writeLock();
        try {
            // clear push task first
            for (Map<Long, PushTask> sentReplicaPushTask : tabletToSentReplicaPushTask.values()) {
                for (PushTask pushTask : sentReplicaPushTask.values()) {
                    if (pushTask == null) {
                        continue;
                    }
                    AgentTaskQueue.removeTask(pushTask.getBackendId(), pushTask.getTaskType(), pushTask.getSignature());
                }
            }
            // clear job infos that not persist
            sparkLoadAppHandle = null;
            resourceDesc = null;
            tableToLoadPartitions.clear();
            indexToPushBrokerReaderParams.clear();
            indexToSchemaHash.clear();
            tabletToSentReplicaPushTask.clear();
            finishedReplicas.clear();
            quorumTablets.clear();
            fullTablets.clear();
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void afterVisible(TransactionState txnState, boolean txnOperated) {
        super.afterVisible(txnState, txnOperated);
        clearJob();
    }

    @Override
    public void cancelJobWithoutCheck(FailMsg failMsg, boolean abortTxn, boolean needLog) {
        super.cancelJobWithoutCheck(failMsg, abortTxn, needLog);
        clearJob();
    }

    @Override
    public void cancelJob(FailMsg failMsg) throws DdlException {
        super.cancelJob(failMsg);
        clearJob();
    }

    @Override
    protected String getResourceName() {
        return sparkResource.getName();
    }

    @Override
    protected long getEtlStartTimestamp() {
        return etlStartTimestamp;
    }

    public SparkLoadAppHandle getHandle() {
        return sparkLoadAppHandle;
    }

    public void clearSparkLauncherLog() {
        String logPath = sparkLoadAppHandle.getLogPath();
        if (!Strings.isNullOrEmpty(logPath)) {
            File file = new File(logPath);
            if (file.exists()) {
                file.delete();
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        sparkResource.write(out);
        sparkLoadAppHandle.write(out);
        out.writeLong(etlStartTimestamp);
        Text.writeString(out, appId);
        Text.writeString(out, etlOutputPath);
        out.writeInt(tabletMetaToFileInfo.size());
        for (Map.Entry<String, Pair<String, Long>> entry : tabletMetaToFileInfo.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().first);
            out.writeLong(entry.getValue().second);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        sparkResource = (SparkResource) Resource.read(in);
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_91) {
            sparkLoadAppHandle = SparkLoadAppHandle.read(in);
        }
        etlStartTimestamp = in.readLong();
        appId = Text.readString(in);
        etlOutputPath = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tabletMetaStr = Text.readString(in);
            Pair<String, Long> fileInfo = Pair.create(Text.readString(in), in.readLong());
            tabletMetaToFileInfo.put(tabletMetaStr, fileInfo);
        }
    }

    /**
     * log load job update info when job state changed to etl or loading
     */
    private void unprotectedLogUpdateStateInfo() {
        SparkLoadJobStateUpdateInfo info = new SparkLoadJobStateUpdateInfo(
                id, state, transactionId, sparkLoadAppHandle, etlStartTimestamp, appId, etlOutputPath,
                loadStartTimestamp, tabletMetaToFileInfo);
        Catalog.getCurrentCatalog().getEditLog().logUpdateLoadJob(info);
    }

    @Override
    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        super.replayUpdateStateInfo(info);
        SparkLoadJobStateUpdateInfo sparkJobStateInfo = (SparkLoadJobStateUpdateInfo) info;
        sparkLoadAppHandle = sparkJobStateInfo.getSparkLoadAppHandle();
        etlStartTimestamp = sparkJobStateInfo.getEtlStartTimestamp();
        appId = sparkJobStateInfo.getAppId();
        etlOutputPath = sparkJobStateInfo.getEtlOutputPath();
        tabletMetaToFileInfo = sparkJobStateInfo.getTabletMetaToFileInfo();

        switch (state) {
            case ETL:
                // nothing to do
                break;
            case LOADING:
                unprotectedPrepareLoadingInfos();
                break;
            default:
                LOG.warn("replay update load job state info failed. error: wrong state. job id: {}, state: {}",
                         id, state);
                break;
        }
    }

    /**
     * Used for spark load job journal log when job state changed to ETL or LOADING
     */
    public static class SparkLoadJobStateUpdateInfo extends LoadJobStateUpdateInfo {
        @SerializedName(value = "sparkLoadAppHandle")
        private SparkLoadAppHandle sparkLoadAppHandle;
        @SerializedName(value = "etlStartTimestamp")
        private long etlStartTimestamp;
        @SerializedName(value = "appId")
        private String appId;
        @SerializedName(value = "etlOutputPath")
        private String etlOutputPath;
        @SerializedName(value = "tabletMetaToFileInfo")
        private Map<String, Pair<String, Long>> tabletMetaToFileInfo;

        public SparkLoadJobStateUpdateInfo(long jobId, JobState state, long transactionId, SparkLoadAppHandle sparkLoadAppHandle,
                                           long etlStartTimestamp, String appId, String etlOutputPath, long loadStartTimestamp,
                                           Map<String, Pair<String, Long>> tabletMetaToFileInfo) {
            super(jobId, state, transactionId, loadStartTimestamp);
            this.sparkLoadAppHandle = sparkLoadAppHandle;
            this.etlStartTimestamp = etlStartTimestamp;
            this.appId = appId;
            this.etlOutputPath = etlOutputPath;
            this.tabletMetaToFileInfo = tabletMetaToFileInfo;
        }

        public SparkLoadAppHandle getSparkLoadAppHandle() {
            return sparkLoadAppHandle;
        }

        public long getEtlStartTimestamp() {
            return etlStartTimestamp;
        }

        public String getAppId() {
            return appId;
        }

        public String getEtlOutputPath() {
            return etlOutputPath;
        }

        public Map<String, Pair<String, Long>> getTabletMetaToFileInfo() {
            return tabletMetaToFileInfo;
        }
    }

    /**
     * Params for be push broker reader
     * 1. TBrokerScanRange: file path and size, broker address, tranform expr
     * 2. TDescriptorTable: src and dest SlotDescriptors, src and dest tupleDescriptors
     *
     * These params are sent to Be through push task
     */
    private static class PushBrokerReaderParams {
        TBrokerScanRange tBrokerScanRange;
        TDescriptorTable tDescriptorTable;

        public PushBrokerReaderParams() {
            this.tBrokerScanRange = new TBrokerScanRange();
            this.tDescriptorTable = null;
        }

        public void init(List<Column> columns, BrokerDesc brokerDesc) throws UserException {
            // Generate tuple descriptor
            DescriptorTable descTable = new DescriptorTable();
            TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
            // use index schema to fill the descriptor table
            for (Column column : columns) {
                SlotDescriptor destSlotDesc = descTable.addSlotDescriptor(destTupleDesc);
                destSlotDesc.setIsMaterialized(true);
                destSlotDesc.setColumn(column);
                if (column.isAllowNull()) {
                    destSlotDesc.setIsNullable(true);
                } else {
                    destSlotDesc.setIsNullable(false);
                }
            }
            initTBrokerScanRange(descTable, destTupleDesc, columns, brokerDesc);
            initTDescriptorTable(descTable);

        }

        private void initTBrokerScanRange(DescriptorTable descTable, TupleDescriptor destTupleDesc,
                                          List<Column> columns, BrokerDesc brokerDesc) throws AnalysisException {
            // scan range params
            TBrokerScanRangeParams params = new TBrokerScanRangeParams();
            params.setStrictMode(false);
            params.setProperties(brokerDesc.getProperties());
            TupleDescriptor srcTupleDesc = descTable.createTupleDescriptor();
            Map<String, SlotDescriptor> srcSlotDescByName = Maps.newHashMap();
            for (Column column : columns) {
                SlotDescriptor srcSlotDesc = descTable.addSlotDescriptor(srcTupleDesc);
                srcSlotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                srcSlotDesc.setIsMaterialized(true);
                srcSlotDesc.setIsNullable(true);
                srcSlotDesc.setColumn(new Column(column.getName(), PrimitiveType.VARCHAR));
                params.addToSrcSlotIds(srcSlotDesc.getId().asInt());
                srcSlotDescByName.put(column.getName(), srcSlotDesc);
            }

            Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
            for (SlotDescriptor destSlotDesc : destTupleDesc.getSlots()) {
                if (!destSlotDesc.isMaterialized()) {
                    continue;
                }

                SlotDescriptor srcSlotDesc = srcSlotDescByName.get(destSlotDesc.getColumn().getName());
                destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                Expr expr = new SlotRef(srcSlotDesc);
                expr = castToSlot(destSlotDesc, expr);
                params.putToExprOfDestSlot(destSlotDesc.getId().asInt(), expr.treeToThrift());
            }
            params.setDestSidToSrcSidWithoutTrans(destSidToSrcSidWithoutTrans);
            params.setSrcTupleId(srcTupleDesc.getId().asInt());
            params.setDestTupleId(destTupleDesc.getId().asInt());
            tBrokerScanRange.setParams(params);

            // broker address updated for each replica
            tBrokerScanRange.setBrokerAddresses(Lists.newArrayList());

            // broker range desc
            TBrokerRangeDesc tBrokerRangeDesc = new TBrokerRangeDesc();
            tBrokerRangeDesc.setFileType(TFileType.FILE_BROKER);
            tBrokerRangeDesc.setFormatType(TFileFormatType.FORMAT_PARQUET);
            tBrokerRangeDesc.setSplittable(false);
            tBrokerRangeDesc.setStartOffset(0);
            tBrokerRangeDesc.setSize(-1);
            // path and file size updated for each replica
            tBrokerScanRange.setRanges(Lists.newArrayList(tBrokerRangeDesc));
        }

        private Expr castToSlot(SlotDescriptor slotDesc, Expr expr) throws AnalysisException {
            PrimitiveType dstType = slotDesc.getType().getPrimitiveType();
            PrimitiveType srcType = expr.getType().getPrimitiveType();
            if (dstType == PrimitiveType.BOOLEAN && srcType == PrimitiveType.VARCHAR) {
                // there is no cast VARCHAR to BOOLEAN function
                // so we cast VARCHAR to TINYINT first, then cast TINYINT to BOOLEAN
                return new CastExpr(Type.BOOLEAN, new CastExpr(Type.TINYINT, expr));
            }
            if (dstType != srcType) {
                return expr.castTo(slotDesc.getType());
            }
            return expr;
        }

        private void initTDescriptorTable(DescriptorTable descTable) {
            descTable.computeMemLayout();
            tDescriptorTable = descTable.toThrift();
        }
    }
}