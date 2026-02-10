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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.job.offset.kafka.KafkaPartitionOffset;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Log4j2
@Getter
public class StreamingInsertTask extends AbstractStreamingTask {
    private String sql;
    private StmtExecutor stmtExecutor;
    private InsertIntoTableCommand taskCommand;
    private String currentDb;
    private ConnectContext ctx;
    private StreamingJobProperties jobProperties;
    private Map<String, String> originTvfProps;
    SourceOffsetProvider offsetProvider;
    
    /**
     * Kafka partition offset for this task (only for Kafka streaming jobs).
     * When set, this task handles a specific partition with the given offset range.
     */
    private KafkaPartitionOffset kafkaPartitionOffset;

    public StreamingInsertTask(long jobId,
                               long taskId,
                               String sql,
                               SourceOffsetProvider offsetProvider,
                               String currentDb,
                               StreamingJobProperties jobProperties,
                               Map<String, String> originTvfProps,
                               UserIdentity userIdentity) {
        super(jobId, taskId, userIdentity);
        this.sql = sql;
        this.currentDb = currentDb;
        this.offsetProvider = offsetProvider;
        this.jobProperties = jobProperties;
        this.originTvfProps = originTvfProps;
        this.kafkaPartitionOffset = null;
    }
    
    /**
     * Constructor for Kafka partition-specific tasks.
     * Each task handles exactly one Kafka partition for exactly-once semantics.
     */
    public StreamingInsertTask(long jobId,
                               long taskId,
                               String sql,
                               SourceOffsetProvider offsetProvider,
                               String currentDb,
                               StreamingJobProperties jobProperties,
                               Map<String, String> originTvfProps,
                               UserIdentity userIdentity,
                               KafkaPartitionOffset kafkaPartitionOffset) {
        super(jobId, taskId, userIdentity);
        this.sql = sql;
        this.currentDb = currentDb;
        this.offsetProvider = offsetProvider;
        this.jobProperties = jobProperties;
        this.originTvfProps = originTvfProps;
        this.kafkaPartitionOffset = kafkaPartitionOffset;
    }

    @Override
    public void before() throws Exception {
        if (getIsCanceled().get()) {
            log.info("streaming insert task has been canceled, task id is {}", getTaskId());
            return;
        }
        this.status = TaskStatus.RUNNING;
        this.startTimeMs = System.currentTimeMillis();
        ctx = InsertTask.makeConnectContext(userIdentity, currentDb);
        ctx.setSessionVariable(jobProperties.getSessionVariable(ctx.getSessionVariable()));
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);

        // For Kafka partition tasks, use the pre-assigned partition offset
        // For other sources (S3, etc.), get the next offset from the provider
        if (kafkaPartitionOffset != null) {
            this.runningOffset = kafkaPartitionOffset;
            log.info("streaming insert task {} using kafka partition offset: {}", taskId, runningOffset.toString());
        } else {
            this.runningOffset = offsetProvider.getNextOffset(jobProperties, originTvfProps);
            log.info("streaming insert task {} get running offset: {}", taskId, runningOffset.toString());
        }
        
        InsertIntoTableCommand baseCommand = (InsertIntoTableCommand) new NereidsParser().parseSingle(sql);
        baseCommand.setJobId(getTaskId());
        StmtExecutor baseStmtExecutor =
                new StmtExecutor(ctx, new LogicalPlanAdapter(baseCommand, ctx.getStatementContext()));
        baseCommand.initPlan(ctx, baseStmtExecutor, false);
        if (!baseCommand.getParsedPlan().isPresent()) {
            throw new JobException("Can not get Parsed plan");
        }
        this.taskCommand = offsetProvider.rewriteTvfParams(baseCommand, runningOffset);
        this.taskCommand.setLabelName(Optional.of(labelName));
        this.stmtExecutor = new StmtExecutor(ctx, new LogicalPlanAdapter(taskCommand, ctx.getStatementContext()));
    }

    @Override
    public void run() throws JobException {
        if (getIsCanceled().get()) {
            log.info("task has been canceled, task id is {}", getTaskId());
            return;
        }
        log.info("start to run streaming insert task, label {}, offset is {}", labelName, runningOffset.toString());
        String errMsg = null;
        try {
            taskCommand.run(ctx, stmtExecutor);
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.OK) {
                return;
            } else {
                errMsg = ctx.getState().getErrorMessage();
            }
            throw new JobException(errMsg);
        } catch (Exception e) {
            log.warn("execute insert task error, label is {},offset is {}", taskCommand.getLabelName(),
                    runningOffset.toString(), e);
            throw new JobException(Util.getRootCauseMessage(e));
        }
    }

    @Override
    public boolean onSuccess() throws JobException {
        if (getIsCanceled().get()) {
            return false;
        }
        this.status = TaskStatus.SUCCESS;
        this.finishTimeMs = System.currentTimeMillis();
        if (!isCallable()) {
            return false;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        if (null == job) {
            log.info("job is null, job id is {}", jobId);
            return false;
        }

        StreamingInsertJob streamingInsertJob = (StreamingInsertJob) job;
        
        // For Kafka partition tasks, use the partition-specific callback
        if (kafkaPartitionOffset != null) {
            long consumedRows = getConsumedRowCount();
            streamingInsertJob.onKafkaPartitionTaskSuccess(this, 
                    kafkaPartitionOffset.getPartitionId(), consumedRows);
        } else {
            // Standard callback for non-Kafka sources
            streamingInsertJob.onStreamTaskSuccess(this);
        }
        return true;
    }
    
    /**
     * Get the number of rows consumed by this task.
     * Used for updating Kafka partition offsets after task completion.
     */
    public long getConsumedRowCount() {
        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager()
                .queryLoadJobsByJobIds(Arrays.asList(this.getTaskId()));
        if (!loadJobs.isEmpty()) {
            LoadJob loadJob = loadJobs.get(0);
            if (loadJob.getLoadStatistic() != null) {
                return loadJob.getLoadStatistic().getScannedRows();
            }
        }
        return 0;
    }

    @Override
    protected void onFail(String errMsg) throws JobException {
        super.onFail(errMsg);
    }

    @Override
    public void cancel(boolean needWaitCancelComplete) {
        super.cancel(needWaitCancelComplete);
        if (null != stmtExecutor) {
            log.info("cancelling streaming insert task, job id is {}, task id is {}",
                    getJobId(), getTaskId());
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "streaming insert task cancelled"),
                    needWaitCancelComplete);
        }
    }

    @Override
    public void closeOrReleaseResources() {
        if (null != stmtExecutor) {
            stmtExecutor = null;
        }
        if (null != taskCommand) {
            taskCommand = null;
        }
        if (null != ctx) {
            ctx = null;
        }
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = super.getTvfInfo(jobName);
        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager()
                .queryLoadJobsByJobIds(Arrays.asList(this.getTaskId()));
        if (!loadJobs.isEmpty()) {
            LoadJob loadJob = loadJobs.get(0);
            if (loadJob.getLoadingStatus() != null && loadJob.getLoadingStatus().getTrackingUrl() != null) {
                trow.addToColumnValue(new TCell().setStringVal(loadJob.getLoadingStatus().getTrackingUrl()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
            }

            if (loadJob.getLoadStatistic() != null) {
                trow.addToColumnValue(new TCell().setStringVal(loadJob.getLoadStatistic().toJson()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
            }
        } else {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        }

        if (this.getUserIdentity() == null) {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(this.getUserIdentity().getQualifiedUser()));
        }
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(runningOffset == null
                ? FeConstants.null_string : runningOffset.showRange()));
        return trow;
    }
}
