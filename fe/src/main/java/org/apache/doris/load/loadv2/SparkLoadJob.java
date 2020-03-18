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
import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * There are 4 steps in SparkLoadJob:
 * Step1: SparkLoadPendingTask will be created by unprotectedExecuteJob method and submit spark etl job.
 * Step2: LoadEtlChecker will check spark etl job status periodly and submit push tasks when spark etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodly and commit transaction when push tasks are finished.
 * Step4: CommitAndPublicTxn will be called by updateLoadingStatus method when push tasks are finished.
 */
public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    // for global dict
    public static final String BITMAP_DATA_PROPERTY = "bitmap_data";

    private EtlClusterDesc etlClusterDesc;

    private long etlStartTimestamp = -1;
    private long etlFinishTimestamp = -1;
    private long quorumFinishTimestamp = -1;

    // spark job handle
    private SparkAppHandle sparkAppHandle;
    // spark job outputPath
    private String etlOutputPath = "";

    // hivedb.table for global dict
    // temporary use: one SparkLoadJob has only one table to load
    private String hiveTableName = "";

    // etl file paths
    private Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

    // no persist
    private Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();
    private Map<Long, Integer> indexToSchemaHash = Maps.newHashMap();
    private Map<Long, Set<Long>> tabletToSentReplicas = Maps.newHashMap();
    private Set<Long> finishedReplicas = Sets.newHashSet();
    private Set<Long> quorumTablets = Sets.newHashSet();
    private Set<Long> fullTablets = Sets.newHashSet();

    // only for log replay
    public SparkLoadJob() {
        super();
        jobType = EtlJobType.SPARK;
    }

    SparkLoadJob(long dbId, String label, EtlClusterDesc etlClusterDesc, String originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.etlClusterDesc = etlClusterDesc;
        timeoutSecond = Config.spark_load_default_timeout_second;
        jobType = EtlJobType.SPARK;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    @Override
    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        super.setJobProperties(properties);

        // global dict
        if (properties != null) {
            if (properties.containsKey(BITMAP_DATA_PROPERTY)) {
                hiveTableName = properties.get(BITMAP_DATA_PROPERTY);
            }
        }
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        LoadTask task = new SparkLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(),
                                                 etlClusterDesc);
        task.init();
        idToTasks.put(task.getSignature(), task);
        Catalog.getCurrentCatalog().getLoadTaskScheduler().submit(task);
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

            sparkAppHandle = attachment.getHandle();
            etlOutputPath = attachment.getOutputPath();

            unprotectedUpdateState(JobState.ETL);
        } finally {
            writeUnlock();
        }
    }

    @Override
    protected void unprotectedUpdateState(JobState jobState) {
        super.unprotectedUpdateState(jobState);

        if (jobState == JobState.ETL) {
            executeEtl();
        }
    }

    // update etl time and state in spark load job
    private void executeEtl() {
        etlStartTimestamp = System.currentTimeMillis();
        state = JobState.ETL;
    }

    public void updateEtlStatus() throws Exception {
        if (state != JobState.ETL) {
            return;
        }

        // get etl status
        Preconditions.checkNotNull(sparkAppHandle);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        EtlStatus status = handler.getEtlJobStatus(sparkAppHandle, id,
                                                   etlClusterDesc.getProperties().get("spark.status_server"));
        switch (status.getState()) {
            case RUNNING:
                updateEtlStatusInternal(status);
                break;
            case FINISHED:
                processEtlFinish(status, handler);
                break;
            case CANCELLED:
                throw new LoadException("spark etl job failed");
            default:
                LOG.warn("unknown etl state: {}", status.getState().name());
                break;
        }
    }

    private void updateEtlStatusInternal(EtlStatus etlStatus) {
        writeLock();
        try {
            loadingStatus = etlStatus;

            int numTasks = Integer.parseInt(etlStatus.getStats().get(SparkEtlJobHandler.NUM_TASKS));
            int numCompletedTasks = Integer.parseInt(etlStatus.getStats().get(SparkEtlJobHandler.NUM_COMPLETED_TASKS));
            if (numTasks > 0) {
                progress = numCompletedTasks * 100 / numTasks;
            }
        } finally {
            writeUnlock();
        }
    }

    private void processEtlFinish(EtlStatus etlStatus, SparkEtlJobHandler handler) throws Exception {
        // checkDataQuality

        // etl output files
        Map<String, Long> filePathToSize = handler.getEtlFilePaths(etlOutputPath,
                                                                   new BrokerDesc("", null));
        updateToLoadingState(etlStatus, filePathToSize);

        // log job etl finish state

        // create push tasks
        prepareLoadingInfos();
        submitPushTasks();
    }

    private void updateToLoadingState(EtlStatus etlStatus, Map<String, Long> filePathToSize) throws LoadException {
        writeLock();
        try {
            for (Map.Entry<String, Long> entry : filePathToSize.entrySet()) {
                String filePath = entry.getKey();
                String tablePartitionIndexBucketSchemaStr = EtlJobConfig.getTablePartitionIndexBucketSchemaStr(filePath);
                tabletMetaToFileInfo.put(tablePartitionIndexBucketSchemaStr,
                                         Pair.create(etlClusterDesc.getProperties().get("fs.default.name") + filePath,
                                                     entry.getValue()));
            }

            loadingStatus = etlStatus;
            progress = 0;
            etlFinishTimestamp = System.currentTimeMillis();
            unprotectedUpdateState(JobState.LOADING);
        } finally {
            writeUnlock();
        }
    }

    private void prepareLoadingInfos() {
        writeLock();
        try {
            for (String tablePartitionIndexBucketStr : tabletMetaToFileInfo.keySet()) {
                String[] fileNameArr = tablePartitionIndexBucketStr.split("\\.");
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
        } finally {
            writeUnlock();
        }
    }

    private Set<Long> submitPushTasks() throws LoadException, MetaNotFoundException {
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
        db.writeLock();
        try {
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
                        for (Tablet tablet : index.getTablets()) {
                            long tabletId = tablet.getId();
                            totalTablets.add(tabletId);

                            String filePath = null;
                            long fileSize = 0;
                            String tablePartitionIndexBucketSchemaStr = String.format("%d.%d.%d.%d.%d",
                                                                                      tableId, partitionId, indexId,
                                                                                      tabletId, schemaHash);
                            if (tabletMetaToFileInfo.containsKey(tablePartitionIndexBucketSchemaStr)) {
                                Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tablePartitionIndexBucketSchemaStr);
                                filePath = fileInfo.first;
                                fileSize = fileInfo.second;
                            }

                            Set<Long> tabletAllReplicas = Sets.newHashSet();
                            Set<Long> tabletFinishedReplicas = Sets.newHashSet();
                            for (Replica replica : tablet.getReplicas()) {
                                long replicaId = replica.getId();
                                tabletAllReplicas.add(replicaId);

                                if (!tabletToSentReplicas.containsKey(tabletId)
                                        || !tabletToSentReplicas.get(tabletId).contains(replica.getId())) {
                                    long taskSignature = Catalog.getCurrentGlobalTransactionMgr()
                                            .getTransactionIDGenerator().getNextTransactionId();
                                    // todo: check args necessary
                                    PushTask pushTask = new PushTask(null,
                                                                     replica.getBackendId(), dbId, tableId,
                                                                     partitionId, indexId,
                                                                     tabletId, replica.getId(), schemaHash,
                                                                     -1, 0, filePath, fileSize, 0,
                                                                     getId(), TPushType.LOAD, null,
                                                                     false, TPriority.NORMAL,
                                                                     TTaskType.REALTIME_PUSH,
                                                                     transactionId, taskSignature);
                                    if (AgentTaskQueue.addTask(pushTask)) {
                                        batchTask.addTask(pushTask);

                                        if (!tabletToSentReplicas.containsKey(tabletId)) {
                                            tabletToSentReplicas.put(tabletId, Sets.newHashSet());
                                        }
                                        tabletToSentReplicas.get(tabletId).add(replicaId);
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
            db.writeUnlock();
        }
    }

    public void updateLoadingStatus() throws UserException {
        // submit push tasks
        Set<Long> totalTablets = submitPushTasks();

        // update status
        boolean canCommitJob = false;
        writeLock();
        try {
            // loading progress
            progress = fullTablets.size() * 100 / totalTablets.size();
            // quorum finish ts
            if (quorumTablets.containsAll(totalTablets)) {
                if (quorumFinishTimestamp < 0) {
                    quorumFinishTimestamp = System.currentTimeMillis();
                }
            }

            long stragglerTimeout = Config.load_straggler_wait_second * 1000;
            // if all tablets are finished or stay in quorum finished for long time, try to commit it.
            if (System.currentTimeMillis() - quorumFinishTimestamp > stragglerTimeout
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
        Database db = getDb();

        db.writeLock();
        try {
            Catalog.getCurrentGlobalTransactionMgr().commitTransaction(dbId, transactionId, commitInfos);
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        etlClusterDesc.write(out);
        Text.writeString(out, hiveTableName);
        // spark handle and outputPath?
        out.writeInt(tabletMetaToFileInfo.size());
        for (Map.Entry<String, Pair<String, Long>> entry : tabletMetaToFileInfo.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().first);
            out.writeLong(entry.getValue().second);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        etlClusterDesc = EtlClusterDesc.read(in);
        hiveTableName = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tabletMetaStr = Text.readString(in);
            Pair<String, Long> fileInfo = Pair.create(Text.readString(in), in.readLong());
            tabletMetaToFileInfo.put(tabletMetaStr, fileInfo);
        }
    }
}
