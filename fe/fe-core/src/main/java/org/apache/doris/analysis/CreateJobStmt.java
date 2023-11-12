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
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.JobExecuteType;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.common.IntervalUnit;
import org.apache.doris.job.common.JobStatus;
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
    private StatementBase stmt;

    @Getter
    private AbstractJob job;

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
        this.stmt = doStmt;
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        jobExecutionConfiguration.setExecuteType(executeType);

        TimerDefinition timerDefinition = new TimerDefinition();

        if (null != onceJobStartTimestamp) {
            timerDefinition.setStartTimeMs(TimeUtils.timeStringToLong(onceJobStartTimestamp));
        }
        if (null != interval) {
            timerDefinition.setInterval(interval);
        }
        if (null != intervalTimeUnit) {
            timerDefinition.setIntervalUnit(IntervalUnit.valueOf(intervalTimeUnit.toUpperCase()));
        }
        if (null != startsTimeStamp) {
            timerDefinition.setStartTimeMs(TimeUtils.timeStringToLong(startsTimeStamp));
        }
        if (null != endsTimeStamp) {
            timerDefinition.setEndTimeMs(TimeUtils.timeStringToLong(endsTimeStamp));
        }
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
        job.setComment(comment);
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
        job.setCurrentDbName(labelName.getDbName());
        job.setJobName(labelName.getLabelName());
        job.checkJobParams();
        job.setComment(comment);
        //todo support user define
        job.setCurrentUser(ConnectContext.get().getQualifiedUser());
        job.setJobStatus(JobStatus.RUNNING);
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
        //String originStmt = getOrigStmt().originStmt;
        //String executeSql = parseExecuteSql(originStmt);
    }

}
