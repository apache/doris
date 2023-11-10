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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.common.IntervalUnit;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.constants.JobStatus;
import org.apache.doris.scheduler.constants.JobType;
import org.apache.doris.scheduler.executor.SqlJobExecutor;
import org.apache.doris.scheduler.job.Job;

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
    private StatementBase stmt;

    @Getter
    private Job job;

    private final LabelName labelName;

    private final String onceJobStartTimestamp;

    private final Long interval;

    private final String intervalTimeUnit;

    private final String startsTimeStamp;

    private final String endsTimeStamp;

    private final String comment;

    private String timezone = TimeUtils.DEFAULT_TIME_ZONE;

    private static final ImmutableSet<Class<? extends DdlStmt>> supportStmtSuperClass
            = new ImmutableSet.Builder<Class<? extends DdlStmt>>().add(InsertStmt.class)
            .add(UpdateStmt.class).build();

    private static HashSet<String> supportStmtClassNamesCache = new HashSet<>(16);

    public CreateJobStmt(LabelName labelName, String jobTypeName, String onceJobStartTimestamp,
                         Long interval, String intervalTimeUnit,
                         String startsTimeStamp, String endsTimeStamp, String comment, StatementBase doStmt) {
        this.labelName = labelName;
        this.onceJobStartTimestamp = onceJobStartTimestamp;
        this.interval = interval;
        this.intervalTimeUnit = intervalTimeUnit;
        this.startsTimeStamp = startsTimeStamp;
        this.endsTimeStamp = endsTimeStamp;
        this.comment = comment;
        this.stmt = doStmt;
        this.job = new Job();
        JobType jobType = JobType.valueOf(jobTypeName.toUpperCase());
        job.setJobType(jobType);
    }

    private String parseExecuteSql(String sql) throws AnalysisException {
        sql = sql.toLowerCase();
        int executeSqlIndex = sql.indexOf(" do ");
        String executeSql = sql.substring(executeSqlIndex + 4).trim();
        if (StringUtils.isBlank(executeSql)) {
            throw new AnalysisException("execute sql has invalid format");
        }
        return executeSql;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkAuth();
        labelName.analyze(analyzer);
        String dbName = labelName.getDbName();
        Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        job.setDbName(labelName.getDbName());
        job.setJobName(labelName.getLabelName());
        if (StringUtils.isNotBlank(onceJobStartTimestamp)) {
            analyzerOnceTimeJob();
        } else {
            analyzerCycleJob();
        }
        if (ConnectContext.get() != null) {
            timezone = ConnectContext.get().getSessionVariable().getTimeZone();
        }
        timezone = TimeUtils.checkTimeZoneValidAndStandardize(timezone);
        job.setTimezone(timezone);
        job.setComment(comment);
        //todo support user define
        job.setUser(ConnectContext.get().getQualifiedUser());
        job.setJobStatus(JobStatus.RUNNING);
        job.setJobCategory(JobCategory.SQL);
        analyzerSqlStmt();
    }

    protected static void checkAuth() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    private void checkStmtSupport() throws AnalysisException {
        if (supportStmtClassNamesCache.contains(stmt.getClass().getSimpleName())) {
            return;
        }
        for (Class<? extends DdlStmt> clazz : supportStmtSuperClass) {
            if (clazz.isAssignableFrom(stmt.getClass())) {
                supportStmtClassNamesCache.add(stmt.getClass().getSimpleName());
                return;
            }
        }
        throw new AnalysisException("Not support this stmt type");
    }

    private void analyzerSqlStmt() throws UserException {
        checkStmtSupport();
        stmt.analyze(analyzer);
        String originStmt = getOrigStmt().originStmt;
        String executeSql = parseExecuteSql(originStmt);
        SqlJobExecutor sqlJobExecutor = new SqlJobExecutor(executeSql);
        job.setExecutor(sqlJobExecutor);
    }


    private void analyzerCycleJob() throws UserException {
        if (null == interval) {
            throw new AnalysisException("interval is null");
        }
        if (interval <= 0) {
            throw new AnalysisException("interval must be greater than 0");
        }

        if (StringUtils.isBlank(intervalTimeUnit)) {
            throw new AnalysisException("intervalTimeUnit is null");
        }
        try {
            IntervalUnit intervalUnit = IntervalUnit.valueOf(intervalTimeUnit.toUpperCase());
            job.setIntervalUnit(intervalUnit);
            long intervalTimeMs = intervalUnit.getParameterValue(interval);
            job.setIntervalMs(intervalTimeMs);
            job.setOriginInterval(interval);
        } catch (IllegalArgumentException e) {
            throw new AnalysisException("interval time unit is not valid, we only support second,minute,hour,day,week");
        }
        if (StringUtils.isNotBlank(startsTimeStamp)) {
            long startsTimeMillis = TimeUtils.timeStringToLong(startsTimeStamp);
            if (startsTimeMillis < System.currentTimeMillis()) {
                throw new AnalysisException("starts time must be greater than current time");
            }
            job.setStartTimeMs(startsTimeMillis);
        }
        if (StringUtils.isNotBlank(endsTimeStamp)) {
            long endTimeMillis = TimeUtils.timeStringToLong(endsTimeStamp);
            if (endTimeMillis < System.currentTimeMillis()) {
                throw new AnalysisException("ends time must be greater than current time");
            }
            job.setEndTimeMs(endTimeMillis);
        }
        if (job.getStartTimeMs() > 0 && job.getEndTimeMs() > 0
                && (job.getEndTimeMs() - job.getStartTimeMs() < job.getIntervalMs())) {
            throw new AnalysisException("ends time must be greater than start time and interval time");
        }
    }


    private void analyzerOnceTimeJob() throws UserException {
        job.setIntervalMs(0L);

        long executeAtTimeMillis = TimeUtils.timeStringToLong(onceJobStartTimestamp);
        if (executeAtTimeMillis < System.currentTimeMillis()) {
            throw new AnalysisException("job time stamp must be greater than current time");
        }
        job.setStartTimeMs(executeAtTimeMillis);
    }
}
