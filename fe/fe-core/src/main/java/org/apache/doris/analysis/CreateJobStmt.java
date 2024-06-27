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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;

/**
 * syntax:
 * CREATE
 * [DEFINER = user]
 * JOB
 * event_name
 * ON SCHEDULE schedule
 * [COMMENT 'string']
 * DO event_body;
 * schedule: {
 * [STREAMING] AT timestamp
 * | EVERY interval
 * [STARTS timestamp ]
 * [ENDS timestamp ]
 * }
 * interval:
 * quantity { DAY | HOUR | MINUTE |
 * WEEK | SECOND }
 */
@Slf4j
public class CreateJobStmt extends DdlStmt {

    @Getter
    private StatementBase doStmt;

    @Getter
    private AbstractJob jobInstance;

    private final LabelName labelName;

    private final String onceJobStartTimestamp;

    private final Long interval;

    private final String intervalTimeUnit;

    private final String startsTimeStamp;

    private final String endsTimeStamp;

    private final String comment;

    private String jobName;

    public static final String CURRENT_TIMESTAMP_STRING = "current_timestamp";
    private JobExecuteType executeType;

    // exclude job name prefix, which is used by inner job
    private static final String excludeJobNamePrefix = "inner_";

    private static final ImmutableSet<Class<? extends DdlStmt>> supportStmtSuperClass
            = new ImmutableSet.Builder<Class<? extends DdlStmt>>().add(InsertStmt.class)
            .build();

    private static final HashSet<String> supportStmtClassNamesCache = new HashSet<>(16);

    public CreateJobStmt(LabelName labelName, JobExecuteType executeType, String onceJobStartTimestamp,
                         Long interval, String intervalTimeUnit,
                         String startsTimeStamp, String endsTimeStamp, String comment, StatementBase doStmt) {
        this.labelName = labelName;
        this.onceJobStartTimestamp = onceJobStartTimestamp;
        this.interval = interval;
        this.intervalTimeUnit = intervalTimeUnit;
        this.startsTimeStamp = startsTimeStamp;
        this.endsTimeStamp = endsTimeStamp;
        this.comment = comment;
        this.doStmt = doStmt;
        this.executeType = executeType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkAuth();
        labelName.analyze(analyzer);
        String dbName = labelName.getDbName();
        Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        analyzerSqlStmt();
        // check its insert stmt,currently only support insert stmt
        //todo when support other stmt,need to check stmt type and generate jobInstance
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        jobExecutionConfiguration.setExecuteType(executeType);
        TimerDefinition timerDefinition = new TimerDefinition();

        if (null != onceJobStartTimestamp) {
            if (onceJobStartTimestamp.equalsIgnoreCase(CURRENT_TIMESTAMP_STRING)) {
                jobExecutionConfiguration.setImmediate(true);
                timerDefinition.setStartTimeMs(System.currentTimeMillis());
            } else {
                timerDefinition.setStartTimeMs(TimeUtils.timeStringToLong(onceJobStartTimestamp));
            }
        }
        if (null != interval) {
            timerDefinition.setInterval(interval);
        }
        if (null != intervalTimeUnit) {
            IntervalUnit intervalUnit = IntervalUnit.fromString(intervalTimeUnit.toUpperCase());
            if (null == intervalUnit) {
                throw new AnalysisException("interval time unit can not be " + intervalTimeUnit);
            }
            if (intervalUnit.equals(IntervalUnit.SECOND)
                    && !Config.enable_job_schedule_second_for_test) {
                throw new AnalysisException("interval time unit can not be second");
            }
            timerDefinition.setIntervalUnit(intervalUnit);
        }
        if (null != startsTimeStamp) {
            if (startsTimeStamp.equalsIgnoreCase(CURRENT_TIMESTAMP_STRING)) {
                jobExecutionConfiguration.setImmediate(true);
                //To avoid immediate re-scheduling, set the start time of the timer 100ms before the current time.
                timerDefinition.setStartTimeMs(System.currentTimeMillis());
            } else {
                timerDefinition.setStartTimeMs(TimeUtils.timeStringToLong(startsTimeStamp));
            }
        }
        if (null != endsTimeStamp) {
            timerDefinition.setEndTimeMs(TimeUtils.timeStringToLong(endsTimeStamp));
        }
        checkJobName(labelName.getLabelName());
        this.jobName = labelName.getLabelName();
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
        String originStmt = getOrigStmt().originStmt;
        String executeSql = parseExecuteSql(originStmt, jobName, comment);
        // create job use label name as its job name
        InsertJob job = new InsertJob(jobName,
                JobStatus.RUNNING,
                labelName.getDbName(),
                comment,
                ConnectContext.get().getCurrentUserIdentity(),
                jobExecutionConfiguration,
                System.currentTimeMillis(),
                executeSql);
        jobInstance = job;
    }

    private void checkJobName(String jobName) throws AnalysisException {
        if (StringUtils.isBlank(jobName)) {
            throw new AnalysisException("job name can not be null");
        }
        if (jobName.startsWith(excludeJobNamePrefix)) {
            throw new AnalysisException("job name can not start with " + excludeJobNamePrefix);
        }
    }

    protected static void checkAuth() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    private void checkStmtSupport() throws AnalysisException {
        if (supportStmtClassNamesCache.contains(doStmt.getClass().getSimpleName())) {
            return;
        }
        for (Class<? extends DdlStmt> clazz : supportStmtSuperClass) {
            if (clazz.isAssignableFrom(doStmt.getClass())) {
                supportStmtClassNamesCache.add(doStmt.getClass().getSimpleName());
                return;
            }
        }
        throw new AnalysisException("Not support " + doStmt.getClass().getSimpleName() + " type in job");
    }

    private void analyzerSqlStmt() throws UserException {
        checkStmtSupport();
        doStmt.analyze(analyzer);
    }

    /**
     * parse execute sql from create job stmt
     * Some stmt not implement toSql method,so we need to parse sql from originStmt
     */
    private static String parseExecuteSql(String sql, String jobName, String comment) throws AnalysisException {
        String lowerCaseSql = sql.toLowerCase();
        String lowerCaseJobName = jobName.toLowerCase();
        // Find the end position of the job name in the SQL statement.
        int jobNameEndIndex = lowerCaseSql.indexOf(lowerCaseJobName) + lowerCaseJobName.length();
        String subSqlStmt = lowerCaseSql.substring(jobNameEndIndex);
        String originSubSqlStmt = sql.substring(jobNameEndIndex);
        // If the comment is not empty, extract the SQL statement from the end position of the comment.
        if (StringUtils.isNotBlank(comment)) {

            String lowerCaseComment = comment.toLowerCase();
            int splitDoIndex = subSqlStmt.indexOf(lowerCaseComment) + lowerCaseComment.length();
            subSqlStmt = subSqlStmt.substring(splitDoIndex);
            originSubSqlStmt = originSubSqlStmt.substring(splitDoIndex);
        }
        // Find the position of the "do" keyword and extract the execution SQL statement from this position.
        int executeSqlIndex = subSqlStmt.indexOf("do");
        String executeSql = originSubSqlStmt.substring(executeSqlIndex + 2).trim();
        if (StringUtils.isBlank(executeSql)) {
            throw new AnalysisException("execute sql has invalid format");
        }
        return executeSql;
    }

    protected static boolean isInnerJob(String jobName) {
        return jobName.startsWith(excludeJobNamePrefix);
    }
}
