package org.apache.doris.analysis;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.scheduler.common.IntervalUnit;
import org.apache.doris.scheduler.job.Job;

@Slf4j
public class CreateJobStmt extends DdlStmt {

    private String jobName;

    private String executeAtTimeStamp;
    private IntervalUnit executeAtIntervalUnit;
    private long executeAtInterval;
    private boolean isCycleJob;
    private long cycleSchedulerInterval;

    private IntervalUnit cycleSchedulerIntervalUnit;

    private String startsTimeStamp;

    private long startsInterval;

    private IntervalUnit startsIntervalUnit;

    private String endsTimeStamp;

    private long endsInterval;


    private IntervalUnit endsIntervalUnit;

    private String executeSql;

    @Getter
    private Job job;

    public CreateJobStmt() {
    }

    public CreateJobStmt(String jobName, boolean isRecurring, String executeAtTimeStamp, IntervalUnit executeAtIntervalUnit, long executeAtInterval, long cycleSchedulerInterval, IntervalUnit cycleSchedulerIntervalUnit, String startsTimeStamp, long startsInterval, IntervalUnit startsIntervalUnit, String endsTimeStamp, long endsInterval, IntervalUnit endsIntervalUnit, String executeSql) {
        this.jobName = jobName;
        this.executeAtTimeStamp = executeAtTimeStamp;
        this.executeAtIntervalUnit = executeAtIntervalUnit;
        this.executeAtInterval = executeAtInterval;
        this.isCycleJob = isRecurring;
        this.cycleSchedulerInterval = cycleSchedulerInterval;
        this.cycleSchedulerIntervalUnit = cycleSchedulerIntervalUnit;
        this.startsTimeStamp = startsTimeStamp;
        this.startsInterval = startsInterval;
        this.startsIntervalUnit = startsIntervalUnit;
        this.endsTimeStamp = endsTimeStamp;
        this.endsInterval = endsInterval;
        this.endsIntervalUnit = endsIntervalUnit;
        this.executeSql = executeSql;
        this.job = new Job();
        job.setJobName(jobName);
        job.setCycleJob(isRecurring);

    }

    private void checkOneTimeJobExecuteAtTimeStamp() throws AnalysisException {
        if (StringUtils.isBlank(executeAtTimeStamp)) {
            throw new AnalysisException("job time stamp is null");
        }
        long executeAtTimeMillis = TimeUtils.timeStringToLong(executeAtTimeStamp);
        if (executeAtTimeMillis <= 0) {
            throw new AnalysisException("job time stamp must be greater than 0");
        }
        if (executeAtInterval <= 0) {
            if (executeAtTimeMillis < System.currentTimeMillis()) {
                throw new AnalysisException("job time stamp must be greater than current time");
            }
            job.setStartTimeMs(executeAtTimeMillis);
            return;
        }

        long intervalSecondTime = executeAtIntervalUnit.getParameterValue(executeAtInterval);
        if (executeAtTimeMillis + intervalSecondTime < System.currentTimeMillis()) {
            throw new AnalysisException("job time stamp must be greater than current time");
        }
        job.setStartTimeMs(executeAtTimeMillis + intervalSecondTime);


    }


    private void checkSchedulerCycleTime() throws AnalysisException {
        if (cycleSchedulerInterval <= 0) {
            throw new AnalysisException("scheduler interval must be greater than 0");
        }
        job.setIntervalUnit(cycleSchedulerIntervalUnit);
        long intervalTimeMs = cycleSchedulerIntervalUnit.getParameterValue(cycleSchedulerInterval);
        job.setIntervalMs(intervalTimeMs);
    }


    private void checkStartsAndEndsTimeStamp() throws AnalysisException {
        long startsTimeMillis = 0L;

        if (StringUtils.isNotBlank(startsTimeStamp)) {
            startsTimeMillis = TimeUtils.timeStringToLong(startsTimeStamp);
            if (startsInterval <= 0 && (startsTimeMillis < System.currentTimeMillis())) {
                throw new AnalysisException("starts time stamp must be greater than current time");
            }
            if (startsInterval > 0 && (startsTimeMillis + startsIntervalUnit.getParameterValue(startsInterval) < System.currentTimeMillis())) {
                throw new AnalysisException("starts time stamp must be greater than current time");
            }
        }
        long endTimeMillis = 0L;
        if (StringUtils.isNotBlank(endsTimeStamp)) {
            endTimeMillis = TimeUtils.timeStringToLong(endsTimeStamp);
            if (endsInterval <= 0 && (endTimeMillis < System.currentTimeMillis())) {
                throw new AnalysisException("ends time stamp must be greater than current time");
            }
            if (endsInterval > 0 && (endTimeMillis + endsIntervalUnit.getParameterValue(endsInterval) < System.currentTimeMillis())) {
                throw new AnalysisException("ends time stamp must be greater than current time");

            }
        }
        if (endTimeMillis != 0 && endTimeMillis < startsTimeMillis) {
            throw new AnalysisException("ends time stamp must be greater than starts time stamp");
        }
        // check job time interval
        if (endTimeMillis != 0) {
            long intervalTimeMillis = cycleSchedulerIntervalUnit.getParameterValue(cycleSchedulerInterval);
            if (endTimeMillis - intervalTimeMillis - startsTimeMillis < 0) {
                throw new AnalysisException("ends time stamp must be greater than start time stamp and interval time");
            }
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (!isCycleJob) {
            checkOneTimeJobExecuteAtTimeStamp();
            return;
        }
        checkSchedulerCycleTime();
        checkStartsAndEndsTimeStamp();
        // check SQL(todo)
    }
}
