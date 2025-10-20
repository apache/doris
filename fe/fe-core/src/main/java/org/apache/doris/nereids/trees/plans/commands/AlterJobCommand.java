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
import org.apache.doris.common.Pair;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.extensions.insert.streaming.StreamingJobProperties;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertUtils;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * alter job command.
 */
public class AlterJobCommand extends AlterCommand implements ForwardWithSync, NeedAuditEncryption {
    // exclude job name prefix, which is used by inner job
    private static final String excludeJobNamePrefix = "inner_";
    private final String jobName;
    private final Map<String, String> properties;
    private final String sql;

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
            StreamingInsertJob updateJob = (StreamingInsertJob) job;
            // update sql
            if (StringUtils.isNotEmpty(sql)) {
                updateJob.updateExecuteSql(sql);
            }
            // update properties
            if (!properties.isEmpty()) {
                updateJob.updateProperties(properties);
            }
            return updateJob;
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
            streamingJob.checkPrivilege(ConnectContext.get());

            boolean propModified = isPropertiesModified(streamingJob.getProperties());
            if (propModified) {
                validateProps(streamingJob);
            }
            boolean sqlModified = isSqlModified(streamingJob.getExecuteSql());
            if (sqlModified) {
                checkUnmodifiableProperties(streamingJob.getExecuteSql());
            }
            if (!propModified && !sqlModified) {
                throw new AnalysisException("No properties or sql changed in ALTER JOB");
            }
        } else {
            throw new AnalysisException("Unsupported job type for ALTER:" + job.getJobType());
        }
    }

    private void validateProps(StreamingInsertJob streamingJob) throws AnalysisException {
        StreamingJobProperties jobProperties = new StreamingJobProperties(properties);
        jobProperties.validate();
        if (jobProperties.getStartOffset() != null) {
            streamingJob.validateOffset(jobProperties.getStartOffset());
        }
    }

    /**
     * Check if there are any unmodifiable properties in TVF
     */
    private void checkUnmodifiableProperties(String originExecuteSql) throws AnalysisException {
        Pair<List<String>, UnboundTVFRelation> origin = getTargetTableAndTvf(originExecuteSql);
        Pair<List<String>, UnboundTVFRelation> input = getTargetTableAndTvf(sql);
        UnboundTVFRelation originTvf = origin.second;
        UnboundTVFRelation inputTvf = input.second;

        Preconditions.checkArgument(Objects.equals(origin.first, input.first),
                "The target table cannot be modified in ALTER JOB");

        Preconditions.checkArgument(originTvf.getFunctionName().equalsIgnoreCase(inputTvf.getFunctionName()),
                "The tvf type cannot be modified in ALTER JOB: original=%s, new=%s",
                originTvf.getFunctionName(), inputTvf.getFunctionName());

        switch (originTvf.getFunctionName().toLowerCase()) {
            case "s3":
                Preconditions.checkArgument(Objects.equals(originTvf.getProperties().getMap().get("uri"),
                        inputTvf.getProperties().getMap().get("uri")),
                        "The uri property cannot be modified in ALTER JOB");
                break;
            default:
                throw new IllegalArgumentException("Unsupported tvf type:" + inputTvf.getFunctionName());
        }
    }

    private Pair<List<String>, UnboundTVFRelation> getTargetTableAndTvf(String sql) throws AnalysisException {
        LogicalPlan logicalPlan = new NereidsParser().parseSingle(sql);
        if (!(logicalPlan instanceof InsertIntoTableCommand)) {
            throw new AnalysisException("Only support insert command");
        }
        LogicalPlan logicalQuery = ((InsertIntoTableCommand) logicalPlan).getLogicalQuery();
        List<String> targetTable = InsertUtils.getTargetTableQualified(logicalQuery, ConnectContext.get());
        InsertIntoTableCommand baseCommand = (InsertIntoTableCommand) logicalPlan;
        List<UnboundTVFRelation> allTVFRelation = baseCommand.getAllTVFRelation();
        Preconditions.checkArgument(allTVFRelation.size() == 1, "Only support one source in insert streaming job");
        UnboundTVFRelation unboundTVFRelation = allTVFRelation.get(0);
        return Pair.of(targetTable, unboundTVFRelation);
    }

    private boolean isPropertiesModified(Map<String, String> originProps) {
        if (this.properties == null || this.properties.isEmpty()) {
            return false;
        }
        if (!Objects.equals(this.properties, originProps)) {
            return true;
        }
        return false;
    }

    private boolean isSqlModified(String originSql) {
        if (originSql == null || originSql.isEmpty() || sql == null || sql.isEmpty()) {
            return false;
        }
        if (!originSql.equals(this.sql)) {
            return true;
        }
        return false;
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }
}
