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

package org.apache.doris.nereids.trees.plans.commands.info;

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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.Optional;

/**
 * Build job info and analyze the SQL statement to create a job.
 */
public class CreateJobInfo {

    // exclude job name prefix, which is used by inner job
    private static final String excludeJobNamePrefix = "inner_";

    private final Optional<String> labelNameOptional;

    private final Optional<String> onceJobStartTimestampOptional;

    private final Optional<Long> intervalOptional;

    private final Optional<String> intervalTimeUnitOptional;

    private final Optional<String> startsTimeStampOptional;

    private final Optional<String> endsTimeStampOptional;

    private final Optional<Boolean> immediateStartOptional;

    private final String comment;

    private final String executeSql;

    /**
     * Constructor for CreateJobInfo.
     *
     * @param labelNameOptional             Job name.
     * @param onceJobStartTimestampOptional Start time for a one-time job.
     * @param intervalOptional              Interval for a recurring job.
     * @param intervalTimeUnitOptional      Interval time unit for a recurring job.
     * @param startsTimeStampOptional       Start time for a recurring job.
     * @param endsTimeStampOptional         End time for a recurring job.
     * @param immediateStartOptional        Immediate start for a job.
     * @param comment                       Comment for the job.
     * @param executeSql                    Original SQL statement.
     */
    public CreateJobInfo(Optional<String> labelNameOptional, Optional<String> onceJobStartTimestampOptional,
                         Optional<Long> intervalOptional, Optional<String> intervalTimeUnitOptional,
                         Optional<String> startsTimeStampOptional, Optional<String> endsTimeStampOptional,
                         Optional<Boolean> immediateStartOptional, String comment, String executeSql) {
        this.labelNameOptional = labelNameOptional;
        this.onceJobStartTimestampOptional = onceJobStartTimestampOptional;
        this.intervalOptional = intervalOptional;
        this.intervalTimeUnitOptional = intervalTimeUnitOptional;
        this.startsTimeStampOptional = startsTimeStampOptional;
        this.endsTimeStampOptional = endsTimeStampOptional;
        this.immediateStartOptional = immediateStartOptional;
        this.comment = comment;
        this.executeSql = executeSql;

    }

    /**
     * Analyzes the provided SQL statement and builds the job information.
     *
     * @param ctx Connect context.
     * @return AbstractJob instance.
     * @throws UserException If there is an error during SQL analysis or job creation.
     */
    public AbstractJob analyzeAndBuildJobInfo(ConnectContext ctx) throws UserException {
        checkAuth();
        if (labelNameOptional.orElseThrow(() -> new AnalysisException("labelName is null")).isEmpty()) {
            throw new AnalysisException("Job name can not be empty");
        }

        String jobName = labelNameOptional.get();
        checkJobName(jobName);
        String dbName = ctx.getDatabase();

        Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        // check its insert stmt,currently only support insert stmt
        JobExecutionConfiguration jobExecutionConfiguration = new JobExecutionConfiguration();
        JobExecuteType executeType = intervalOptional.isPresent() ? JobExecuteType.RECURRING : JobExecuteType.ONE_TIME;
        jobExecutionConfiguration.setExecuteType(executeType);
        TimerDefinition timerDefinition = new TimerDefinition();

        if (executeType.equals(JobExecuteType.ONE_TIME)) {
            buildOnceJob(timerDefinition, jobExecutionConfiguration);
        } else {
            buildRecurringJob(timerDefinition, jobExecutionConfiguration);
        }
        jobExecutionConfiguration.setTimerDefinition(timerDefinition);
        return analyzeAndCreateJob(executeSql, dbName, jobExecutionConfiguration);
    }

    /**
     * Builds a TimerDefinition for a once-job.
     *
     * @param timerDefinition           Timer definition to be built.
     * @param jobExecutionConfiguration Job execution configuration.
     * @throws AnalysisException If the job is not configured correctly.
     */
    private void buildOnceJob(TimerDefinition timerDefinition,
                              JobExecutionConfiguration jobExecutionConfiguration) throws AnalysisException {
        if (immediateStartOptional.isPresent() && Boolean.TRUE.equals(immediateStartOptional.get())) {
            jobExecutionConfiguration.setImmediate(true);
            timerDefinition.setStartTimeMs(System.currentTimeMillis());
            return;
        }

        // Ensure start time is provided for once jobs.
        String startTime = onceJobStartTimestampOptional.orElseThrow(()
                -> new AnalysisException("Once time job must set start time"));
        timerDefinition.setStartTimeMs(stripQuotesAndParseTimestamp(startTime));
    }

    /**
     * Builds a TimerDefinition for a recurring job.
     *
     * @param timerDefinition           Timer definition to be built.
     * @param jobExecutionConfiguration Job execution configuration.
     * @throws AnalysisException If the job is not configured correctly.
     */
    private void buildRecurringJob(TimerDefinition timerDefinition,
                                   JobExecutionConfiguration jobExecutionConfiguration) throws AnalysisException {
        // Ensure interval is provided for recurring jobs.
        long interval = intervalOptional.orElseThrow(()
                -> new AnalysisException("Interval must be set for recurring job"));
        timerDefinition.setInterval(interval);

        // Ensure interval time unit is provided for recurring jobs.
        String intervalTimeUnit = intervalTimeUnitOptional.orElseThrow(()
                -> new AnalysisException("Interval time unit must be set for recurring job"));
        IntervalUnit intervalUnit = IntervalUnit.fromString(intervalTimeUnit.toUpperCase());
        if (intervalUnit == null) {
            throw new AnalysisException("Invalid interval time unit: " + intervalTimeUnit);
        }

        // Check if interval unit is second and disable if not in test mode.
        if (intervalUnit.equals(IntervalUnit.SECOND) && !Config.enable_job_schedule_second_for_test) {
            throw new AnalysisException("Interval time unit can not be second in production mode");
        }

        timerDefinition.setIntervalUnit(intervalUnit);

        // Set end time if provided.
        endsTimeStampOptional.ifPresent(s -> timerDefinition.setEndTimeMs(stripQuotesAndParseTimestamp(s)));

        // Set immediate start if configured.
        if (immediateStartOptional.isPresent() && Boolean.TRUE.equals(immediateStartOptional.get())) {
            jobExecutionConfiguration.setImmediate(true);
            // Avoid immediate re-scheduling by setting start time slightly in the past.
            timerDefinition.setStartTimeMs(System.currentTimeMillis() - 100);
            return;
        }
        // Set start time if provided.
        startsTimeStampOptional.ifPresent(s -> timerDefinition.setStartTimeMs(stripQuotesAndParseTimestamp(s)));
    }

    protected static void checkAuth() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    /**
     * Analyzes the provided SQL statement and creates an appropriate job based on the parsed logical plan.
     * Currently, only "InsertIntoTableCommand" is supported for job creation.
     *
     * @param sql                       the SQL statement to be analyzed
     * @param currentDbName             the current database name where the SQL statement will be executed
     * @param jobExecutionConfiguration the configuration for job execution
     * @return an instance of AbstractJob corresponding to the SQL statement
     * @throws UserException if there is an error during SQL analysis or job creation
     */
    private AbstractJob analyzeAndCreateJob(String sql, String currentDbName,
                                            JobExecutionConfiguration jobExecutionConfiguration) throws UserException {
        NereidsParser parser = new NereidsParser();
        LogicalPlan logicalPlan = parser.parseSingle(sql);
        if (logicalPlan instanceof InsertIntoTableCommand) {
            InsertIntoTableCommand insertIntoTableCommand = (InsertIntoTableCommand) logicalPlan;
            try {
                insertIntoTableCommand.initPlan(ConnectContext.get(), ConnectContext.get().getExecutor(), false);
                return new InsertJob(labelNameOptional.get(),
                        JobStatus.RUNNING,
                        currentDbName,
                        comment,
                        ConnectContext.get().getCurrentUserIdentity(),
                        jobExecutionConfiguration,
                        System.currentTimeMillis(),
                        sql);
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            }
        } else {
            throw new AnalysisException("Not support this sql : " + sql + " Command class is "
                    + logicalPlan.getClass().getName() + ".");
        }
    }

    private void checkJobName(String jobName) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobName)) {
            throw new AnalysisException("job name can not be null");
        }
        if (jobName.startsWith(excludeJobNamePrefix)) {
            throw new AnalysisException("job name can not start with " + excludeJobNamePrefix);
        }
    }

    /**
     * Strips quotes from the input string and parses it to a timestamp.
     *
     * @param str The input string potentially enclosed in single or double quotes.
     * @return The parsed timestamp as a long value, or -1L if the input is null or empty.
     */
    public static Long stripQuotesAndParseTimestamp(String str) {
        if (str == null || str.isEmpty()) {
            return -1L;
        }
        if (str.startsWith("'") && str.endsWith("'")) {
            str = str.substring(1, str.length() - 1);
        } else if (str.startsWith("\"") && str.endsWith("\"")) {
            str = str.substring(1, str.length() - 1);
        }
        return TimeUtils.timeStringToLong(str.trim());
    }
}
