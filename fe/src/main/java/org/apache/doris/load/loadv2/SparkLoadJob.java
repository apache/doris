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
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.common.base.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// TODO: add description
public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    // for global dict
    public static final String BITMAP_DATA_PROPERTY = "bitmap_data";

    private EtlClusterDesc etlClusterDesc;

    private long etlStartTimestamp = -1;

    // spark job handle
    private SparkAppHandle sparkAppHandle;
    // spark job outputPath
    private String etlOutputPath;

    // hivedb.table for global dict
    // temporary use: one SparkLoadJob has only one table to load
    private String hiveTableName;

    // only for log replay
    public SparkLoadJob() {
        super();
        this.jobType = EtlJobType.SPARK;
    }

    SparkLoadJob(long dbId, String label, EtlClusterDesc etlClusterDesc, String originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.timeoutSecond = Config.spark_load_default_timeout_second;
        this.etlClusterDesc = etlClusterDesc;
        this.jobType = EtlJobType.SPARK;
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

    public void updateEtlStatus() throws UserException {
        if (state != JobState.ETL) {
            return;
        }

        // get etl status
        Preconditions.checkState(sparkAppHandle != null);
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        EtlStatus status = handler.getEtlJobStatus(sparkAppHandle, id,
                                                   etlClusterDesc.getProperties().get("spark.status_server"));
        switch (status.getState()) {
            case RUNNING:
                updateEtlStatusInternal(status);
                break;
            case FINISHED:
                updateEtlFinished(status, handler);
                break;
            case CANCELLED:
                cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL), true, true);
                break;
            default:
                LOG.warn("unknown etl state: {}", status.getState().name());
                break;
        }
    }

    private void updateEtlStatusInternal(EtlStatus status) {
        writeLock();
        try {
            loadingStatus = status;

            int numTasks = Integer.parseInt(status.getStats().get(SparkEtlJobHandler.NUM_TASKS));
            int numCompletedTasks = Integer.parseInt(status.getStats().get(SparkEtlJobHandler.NUM_COMPLETED_TASKS));
            if (numTasks > 0) {
                progress = numCompletedTasks * 100 / numTasks;
            }
        } finally {
            writeUnlock();
        }
    }

    private void updateEtlFinished(EtlStatus status, SparkEtlJobHandler handler) throws UserException {
        // checkDataQuality

        // etl output files
        Map<String, Long> fileNameToSize = handler.getEtlFilePaths(etlOutputPath,
                                                                   new BrokerDesc("", null));

        writeLock();
        try {
            loadingStatus = status;
            progress = 0;
            unprotectedUpdateState(JobState.LOADING);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        etlClusterDesc.write(out);
        Text.writeString(out, hiveTableName);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in, null);
        etlClusterDesc = EtlClusterDesc.read(in);
        hiveTableName = Text.readString(in);
    }
}
