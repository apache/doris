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
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.streaming.DataSourceConfigValidator;
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
    private final String sourceType;
    private final String targetDb;
    private final Map<String, String> sourceProperties;
    private final Map<String, String> targetProperties;

    /**
     * AlterJobCommand constructor.
     */
    public AlterJobCommand(String jobName,
            Map<String, String> properties,
            String sql,
            String sourceType,
            String targetDb,
            Map<String, String> sourceProperties,
            Map<String, String> targetProperties) {
        super(PlanType.ALTER_JOB_COMMAND);
        this.jobName = jobName;
        this.properties = properties;
        this.sql = sql;
        this.sourceType = sourceType;
        this.targetDb = targetDb;
        this.sourceProperties = sourceProperties;
        this.targetProperties = targetProperties;
    }

    public String getJobName() {
        return jobName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getSql() {
        return sql;
    }

    public String getSourceType() {
        return sourceType;
    }

    public String getTargetDb() {
        return targetDb;
    }

    public Map<String, String> getSourceProperties() {
        return sourceProperties;
    }

    public Map<String, String> getTargetProperties() {
        return targetProperties;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate();
        ctx.getEnv().getJobManager().alterJob(this);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAlterJobCommand(this, context);
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

            if (sourceType == null) {
                boolean propModified =
                        isPropertiesModified(streamingJob.getProperties(), this.getProperties());
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
                if (!sourceType.toUpperCase().equals(streamingJob.getDataSourceType().name())) {
                    throw new AnalysisException("source type can't be modified in ALTER JOB");
                }

                if (StringUtils.isNotEmpty(targetDb) && !targetDb.equals(streamingJob.getTargetDb())) {
                    throw new AnalysisException("target database can't be modified in ALTER JOB");
                }

                boolean propModified = isPropertiesModified(streamingJob.getProperties(), this.getProperties());
                if (propModified) {
                    validateProps(streamingJob);
                }

                boolean sourcePropModified =
                        isPropertiesModified(streamingJob.getSourceProperties(), this.getSourceProperties());
                if (sourcePropModified) {
                    DataSourceConfigValidator.validateSource(this.getSourceProperties());
                    checkUnmodifiableSourceProperties(streamingJob.getSourceProperties());
                }

                boolean targetPropModified =
                        isPropertiesModified(streamingJob.getTargetProperties(), this.getTargetProperties());
                if (targetPropModified) {
                    DataSourceConfigValidator.validateTarget(this.getTargetProperties());
                }
                if (!propModified && !targetPropModified && !sourcePropModified) {
                    throw new AnalysisException("No properties or source or target properties changed in ALTER JOB");
                }
            }
        } else {
            throw new AnalysisException("Unsupported job type for ALTER:" + job.getJobType());
        }
    }

    private void checkUnmodifiableSourceProperties(Map<String, String> originSourceProperties) {
        if (sourceProperties.containsKey(DataSourceConfigKeys.JDBC_URL)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.JDBC_URL),
                    sourceProperties.get(DataSourceConfigKeys.JDBC_URL)),
                    "The jdbc_url property cannot be modified in ALTER JOB");
        }

        if (sourceProperties.containsKey(DataSourceConfigKeys.DATABASE)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.DATABASE),
                    sourceProperties.get(DataSourceConfigKeys.DATABASE)),
                    "The database property cannot be modified in ALTER JOB");
        }

        if (sourceProperties.containsKey(DataSourceConfigKeys.INCLUDE_TABLES)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.INCLUDE_TABLES),
                    sourceProperties.get(DataSourceConfigKeys.INCLUDE_TABLES)),
                    "The include_tables property cannot be modified in ALTER JOB");
        }

        if (sourceProperties.containsKey(DataSourceConfigKeys.EXCLUDE_TABLES)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.EXCLUDE_TABLES),
                    sourceProperties.get(DataSourceConfigKeys.EXCLUDE_TABLES)),
                    "The exclude_tables property cannot be modified in ALTER JOB");
        }
    }

    private void validateProps(StreamingInsertJob streamingJob) throws AnalysisException {
        StreamingJobProperties jobProperties = new StreamingJobProperties(properties);
        jobProperties.validate();
        // from to job no need valiate offset in job properties
        if (streamingJob.getDataSourceType() == null
                && jobProperties.getOffsetProperty() != null) {
            streamingJob.validateOffset(jobProperties.getOffsetProperty());
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

    private boolean isPropertiesModified(Map<String, String> originProps, Map<String, String> modifiedProps) {
        if (modifiedProps == null || modifiedProps.isEmpty()) {
            return false;
        }
        if (!Objects.equals(modifiedProps, originProps)) {
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
