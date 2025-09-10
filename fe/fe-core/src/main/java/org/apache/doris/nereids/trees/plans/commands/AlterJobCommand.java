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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * alter job command.
 */
public class AlterJobCommand extends AlterCommand implements ForwardWithSync {
    // exclude job name prefix, which is used by inner job
    private static final String excludeJobNamePrefix = "inner_";
    private String jobName;
    private Map<String, String> properties;
    private String sql;

    public AlterJobCommand(String jobName, Map<String, String> properties, String sql) {
        super(PlanType.ALTER_JOB_COMMAND);
        this.jobName = jobName;
        this.properties = properties;
        this.sql = sql;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        AbstractJob job = analyzeAndBuildJobInfo(ctx);
        ctx.getEnv().getJobManager().alterJob(job);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterJobCommand(this, context);
    }

    private AbstractJob analyzeAndBuildJobInfo(ConnectContext ctx) throws JobException {
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJobByName(jobName);
        if (job instanceof StreamingInsertJob) {
            StreamingInsertJob originJob = (StreamingInsertJob) job;
            String updateSQL = StringUtils.isEmpty(sql) ? originJob.getExecuteSql() : sql;
            Map<String, String> updateProps = properties == null || properties.isEmpty() ? originJob.getJobProperties()
                    .getProperties() : properties;
            StreamingJobProperties streamJobProps = new StreamingJobProperties(updateProps);
            // rebuild time definition
            JobExecutionConfiguration execConfig = originJob.getJobConfig();
            TimerDefinition timerDefinition = new TimerDefinition();
            timerDefinition.setInterval(streamJobProps.getMaxIntervalSecond());
            timerDefinition.setIntervalUnit(IntervalUnit.SECOND);
            timerDefinition.setStartTimeMs(execConfig.getTimerDefinition().getStartTimeMs());
            execConfig.setTimerDefinition(timerDefinition);
            return new StreamingInsertJob(jobName,
                    job.getJobStatus(),
                    job.getCurrentDbName(),
                    job.getComment(),
                    ConnectContext.get().getCurrentUserIdentity(),
                    execConfig,
                    System.currentTimeMillis(),
                    updateSQL,
                    streamJobProps);
        } else {
            throw new JobException("Unsupported job type for ALTER:" + job.getJobType());
        }
    }

    private void validate() throws Exception {
        if (jobName.startsWith(excludeJobNamePrefix)) {
            throw new AnalysisException("Can't alter inner job");
        }
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJobByName(jobName);
        if (!JobStatus.PAUSED.equals(job.getJobStatus())) {
            throw new AnalysisException("Only PAUSED job can be altered");
        }

        if (job.getJobConfig().getExecuteType().equals(JobExecuteType.STREAMING)
                && job instanceof StreamingInsertJob) {
            StreamingInsertJob streamingJob = (StreamingInsertJob) job;
            boolean proCheck = checkProperties(streamingJob.getJobProperties().getProperties());
            boolean sqlCheck = checkSql(streamingJob.getExecuteSql());
            if (!proCheck && !sqlCheck) {
                throw new AnalysisException("No properties or sql changed in ALTER JOB");
            }
        } else {
            throw new AnalysisException("Unsupported job type for ALTER:" + job.getJobType());
        }
    }

    private boolean checkProperties(Map<String, String> originProps) {
        if (originProps.isEmpty()) {
            return false;
        }
        if (!originProps.equals(properties)) {
            return true;
        }
        return false;
    }

    private boolean checkSql(String sql) {
        if (sql == null || sql.isEmpty()) {
            return false;
        }
        if (!sql.equals(sql)) {
            return true;
        }
        return false;
    }

}
