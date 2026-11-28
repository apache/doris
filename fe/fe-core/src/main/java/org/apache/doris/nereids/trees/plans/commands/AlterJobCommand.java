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
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.nereids.DorisLexer;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.tablefunction.CdcStreamTableValuedFunction;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
                    checkUnmodifiableProperties(streamingJob.getExecuteSql(), streamingJob.getSqlMode());
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
                    StreamingJobUtils.checkCdcDriverPrivilege(ConnectContext.get(),
                            sourceProperties.get(DataSourceConfigKeys.DRIVER_URL));
                    DataSourceConfigValidator.validateSource(this.getSourceProperties(),
                            streamingJob.getDataSourceType().name());
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

        if (sourceProperties.containsKey(DataSourceConfigKeys.SCHEMA)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.SCHEMA),
                    sourceProperties.get(DataSourceConfigKeys.SCHEMA)),
                    "The schema property cannot be modified in ALTER JOB");
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

        if (sourceProperties.containsKey(DataSourceConfigKeys.OFFSET)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.OFFSET),
                    sourceProperties.get(DataSourceConfigKeys.OFFSET)),
                    "The offset in source properties cannot be modified in ALTER JOB. "
                    + "Use PROPERTIES('offset'='{...}') to alter offset");
        }

        String tablePrefix = DataSourceConfigKeys.TABLE + ".";
        for (String key : sourceProperties.keySet()) {
            if (!key.startsWith(tablePrefix)) {
                continue;
            }
            if (key.endsWith("." + DataSourceConfigKeys.TABLE_EXCLUDE_COLUMNS_SUFFIX)
                    || key.endsWith("." + DataSourceConfigKeys.TABLE_TARGET_TABLE_SUFFIX)) {
                Preconditions.checkArgument(Objects.equals(
                        originSourceProperties.get(key),
                        sourceProperties.get(key)),
                        "The " + key + " property cannot be modified in ALTER JOB");
            }
        }

        // slot_name / publication_name decide Doris-vs-user ownership at create time; flipping
        // them afterwards would orphan Doris-created resources or let Doris drop user-owned ones.
        if (sourceProperties.containsKey(DataSourceConfigKeys.SLOT_NAME)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.SLOT_NAME),
                    sourceProperties.get(DataSourceConfigKeys.SLOT_NAME)),
                    "The slot_name property cannot be modified in ALTER JOB");
        }

        if (sourceProperties.containsKey(DataSourceConfigKeys.PUBLICATION_NAME)) {
            Preconditions.checkArgument(Objects.equals(
                    originSourceProperties.get(DataSourceConfigKeys.PUBLICATION_NAME),
                    sourceProperties.get(DataSourceConfigKeys.PUBLICATION_NAME)),
                    "The publication_name property cannot be modified in ALTER JOB");
        }
    }

    private void validateProps(StreamingInsertJob streamingJob) throws Exception {
        StreamingJobProperties originJobProperties =
                new StreamingJobProperties(streamingJob.getProperties());
        Map<String, String> mergedProperties = new HashMap<>(streamingJob.getProperties());
        mergedProperties.putAll(properties);
        StreamingJobProperties updatedJobProperties = new StreamingJobProperties(mergedProperties);
        updatedJobProperties.validate();
        if (sourceType == null && originJobProperties.getSessionVariable(VariableMgr.newSessionVariable()).getSqlMode()
                != updatedJobProperties.getSessionVariable(VariableMgr.newSessionVariable()).getSqlMode()) {
            throw new AnalysisException("session.sql_mode cannot be modified in ALTER JOB");
        }
        if (!originJobProperties.getS3IngestionMode().equals(updatedJobProperties.getS3IngestionMode())
                || !StringUtils.equalsIgnoreCase(originJobProperties.getS3EventSource(),
                        updatedJobProperties.getS3EventSource())
                || !Objects.equals(originJobProperties.getS3SqsQueueUrl(),
                        updatedJobProperties.getS3SqsQueueUrl())) {
            throw new AnalysisException("s3.ingestion_mode, s3.event.source and s3.sqs.queue_url cannot be altered");
        }
        String offset = properties.get(StreamingJobProperties.OFFSET_PROPERTY);
        if (offset != null) {
            streamingJob.validateAlterOffset(offset);
            streamingJob.validateOffset(offset);
        }
    }

    /**
     * Check if there are any unmodifiable properties in TVF
     * and keep the query outside TVF properties unchanged.
     */
    private void checkUnmodifiableProperties(String originExecuteSql, long sqlMode) throws AnalysisException {
        Pair<String, UnboundTVFRelation> origin = SqlModeHelper.withSqlMode(sqlMode,
                () -> getQueryAndTvf(originExecuteSql));
        Pair<String, UnboundTVFRelation> input = SqlModeHelper.withSqlMode(sqlMode,
                () -> getQueryAndTvf(sql));
        Preconditions.checkArgument(origin.first.equals(input.first),
                "The query outside TVF properties cannot be modified in ALTER JOB");

        Map<String, String> originProperties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        originProperties.putAll(origin.second.getProperties().getMap());
        Map<String, String> inputProperties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        inputProperties.putAll(input.second.getProperties().getMap());
        Preconditions.checkArgument(originProperties.size() == origin.second.getProperties().getMap().size()
                        && inputProperties.size() == input.second.getProperties().getMap().size(),
                "Duplicate TVF property names differing only in case are not supported in ALTER JOB");
        switch (origin.second.getFunctionName().toLowerCase(Locale.ROOT)) {
            case "s3":
                Preconditions.checkArgument(Objects.equals(originProperties.get("uri"), inputProperties.get("uri")),
                        "The uri property cannot be modified in ALTER JOB");
                break;
            case "cdc_stream":
                // type, jdbc_url, database, schema, and table identify the source and cannot be changed.
                // snapshot_split_key determines persisted split boundaries and cannot be changed.
                // slot_name / publication_name are fixed at create time to keep ownership stable.
                // user, password, driver_url, driver_class, etc. are modifiable (credential rotation).
                for (String unmodifiable : new String[] {
                        DataSourceConfigKeys.TYPE,
                        DataSourceConfigKeys.JDBC_URL,
                        DataSourceConfigKeys.DATABASE,
                        DataSourceConfigKeys.SCHEMA,
                        DataSourceConfigKeys.TABLE,
                        DataSourceConfigKeys.SNAPSHOT_SPLIT_KEY,
                        DataSourceConfigKeys.SLOT_NAME,
                        DataSourceConfigKeys.PUBLICATION_NAME}) {
                    Preconditions.checkArgument(Objects.equals(originProperties.get(unmodifiable),
                                    inputProperties.get(unmodifiable)),
                            "The '%s' property cannot be modified in ALTER JOB for cdc_stream", unmodifiable);
                }
                // Validate merged resource properties as the ALTER caller, before schema discovery.
                new CdcStreamTableValuedFunction(input.second.getProperties().getMap()).checkAuth(ConnectContext.get());
                break;
            default:
                throw new IllegalArgumentException("Unsupported tvf type:" + origin.second.getFunctionName());
        }
    }

    private Pair<String, UnboundTVFRelation> getQueryAndTvf(String sql) {
        Map<Pair<Integer, Integer>, String> propertyRanges = new HashMap<>();
        LogicalPlan plan = new NereidsParser().parseForEncryption(sql, propertyRanges);
        Preconditions.checkArgument(plan instanceof InsertIntoTableCommand,
                "Streaming jobs only support INSERT statements");
        List<UnboundTVFRelation> relations = ((InsertIntoTableCommand) plan).getAllTVFRelation();
        Preconditions.checkArgument(relations.size() == 1 && propertyRanges.size() == 1,
                "Only support one source in insert streaming job");
        Pair<Integer, Integer> range = propertyRanges.keySet().iterator().next();
        // parseForEncryption returns inclusive UTF-16 offsets for property replacement.
        DorisLexer lexer = (DorisLexer) NereidsParser.scan(sql.substring(range.first, range.second + 1));
        lexer.isNoBackslashEscapes = SqlModeHelper.hasNoBackSlashEscapes();
        // Hints affect the query even when written inside the TVF property list.
        String hints = lexer.getAllTokens().stream()
                .filter(token -> token.getType() == DorisLexer.BRACKETED_COMMENT && token.getText().startsWith("/*+"))
                .map(token -> token.getText()).collect(Collectors.joining(" "));
        String query = new StringBuilder(sql).replace(range.first, range.second + 1, hints).toString();
        return Pair.of(NereidsParser.removeCommentAndTrimBlank(query), relations.get(0));
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
