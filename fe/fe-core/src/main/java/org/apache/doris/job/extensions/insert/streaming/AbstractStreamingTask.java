package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
@Getter
public abstract class AbstractStreamingTask {
    private static final int MAX_RETRY = 3;
    private static final String LABEL_SPLITTER = "_";
    private int retryCount = 0;
    protected String labelName;
    protected Offset runningOffset;
    protected UserIdentity userIdentity;
    @Setter
    protected volatile TaskStatus status;
    @Setter
    protected String errMsg;
    protected long jobId;
    protected long taskId;
    protected Long createTimeMs;
    protected Long startTimeMs;
    protected Long finishTimeMs;
    @Getter
    private AtomicBoolean isCanceled = new AtomicBoolean(false);

    public AbstractStreamingTask(long jobId, long taskId) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.labelName = getJobId() + LABEL_SPLITTER + getTaskId();
        this.createTimeMs = System.currentTimeMillis();
    }

    public abstract void before() throws Exception;
    public abstract void run() throws JobException ;
    public abstract boolean onSuccess() throws JobException;
    public abstract void closeOrReleaseResources();

    public void execute() throws JobException {
        while (retryCount <= MAX_RETRY) {
            try {
                before();
                run();
                onSuccess();
                return;
            } catch (Exception e) {
                if (TaskStatus.CANCELED.equals(status)) {
                    return;
                }
                this.errMsg = e.getMessage();
                retryCount++;
                if (retryCount > MAX_RETRY) {
                    log.error("Task execution failed after {} retries.", MAX_RETRY, e);
                    onFail(e.getMessage());
                    return;
                }
                log.warn("execute streaming task error, job id is {}, task id is {}, retrying {}/{}: {}",
                        jobId, taskId, retryCount, MAX_RETRY, e.getMessage());
            } finally {
                // The cancel logic will call the closeOrReleased Resources method by itself.
                // If it is also called here,
                // it may result in the inability to obtain relevant information when canceling the task
                if (!TaskStatus.CANCELED.equals(status)) {
                    closeOrReleaseResources();
                }
            }
        }
    }

    protected void onFail(String errMsg) throws JobException {
        if (getIsCanceled().get()) {
            return;
        }
        this.errMsg = errMsg;
        this.status = TaskStatus.FAILED;
        this.finishTimeMs = System.currentTimeMillis();
        if (!isCallable()) {
            return;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        StreamingInsertJob streamingInsertJob = (StreamingInsertJob) job;
        streamingInsertJob.onStreamTaskFail(this);
    }

    protected boolean isCallable() {
        if (status.equals(TaskStatus.CANCELED)) {
            return false;
        }
        if (null != Env.getCurrentEnv().getJobManager().getJob(jobId)) {
            return true;
        }
        return false;
    }

    public void cancel(boolean needWaitCancelComplete) {
        if (TaskStatus.SUCCESS.equals(status) || TaskStatus.FAILED.equals(status)
                || TaskStatus.CANCELED.equals(status)) {
            return;
        }
        status = TaskStatus.CANCELED;
        if (getIsCanceled().get()) {
            return;
        }
        getIsCanceled().getAndSet(true);
        this.errMsg = "task cancelled";
    }

    /**
     * show streaming insert task info detail
     */
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(this.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(this.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(this.getLabelName()));
        trow.addToColumnValue(new TCell().setStringVal(this.getStatus().name()));
        // err msg
        trow.addToColumnValue(new TCell().setStringVal(StringUtils.isNotBlank(errMsg)
                ? errMsg : FeConstants.null_string));

        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(this.getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getStartTimeMs() ? FeConstants.null_string
                : TimeUtils.longToTimeString(this.getStartTimeMs())));
        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(this.getFinishTimeMs())));

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
