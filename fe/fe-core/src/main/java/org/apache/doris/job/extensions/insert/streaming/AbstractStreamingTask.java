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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
@Getter
public abstract class AbstractStreamingTask {
    private static final int MAX_RETRY = 3;
    private static final String LABEL_SPLITTER = "_";
    private int retryCount = 0;
    // in-place retry would reuse this taskId, breaking ownership-based zombie isolation
    protected volatile boolean noRetry;
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
    private final Object executionCompletion = new Object();
    private boolean executionStarted;
    private boolean executionFinished;
    private Thread executionOwner;

    public AbstractStreamingTask(long jobId, long taskId, UserIdentity userIdentity) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.userIdentity = userIdentity;
        this.labelName = getJobId() + LABEL_SPLITTER + getTaskId();
        this.createTimeMs = System.currentTimeMillis();
    }

    public abstract void before() throws Exception;

    public abstract void run() throws JobException;

    /**
     * Returns the IDs of backends that ran the scan node for this task.
     * Subclasses backed by a TVF query (e.g. StreamingInsertTask) override this
     * to return the actual scan backend IDs from the coordinator.
     */
    public List<Long> getScanBackendIds() {
        return Collections.emptyList();
    }

    public abstract boolean onSuccess() throws JobException;

    public abstract void closeOrReleaseResources();

    // Release the remote cdc reader (keep slot). No-op for tasks without a cdc reader (e.g. TVF).
    public void releaseRemoteReader() {
    }

    public long getRunningBackendId() {
        return -1;
    }

    public void execute() throws JobException {
        synchronized (executionCompletion) {
            executionStarted = true;
            executionOwner = Thread.currentThread();
        }
        try {
            while (retryCount <= MAX_RETRY) {
                Exception attemptFailure = null;
                boolean executionSucceeded = false;
                try {
                    before();
                    run();
                    executionSucceeded = true;
                } catch (Exception e) {
                    attemptFailure = e;
                } finally {
                    // Only the scheduler worker that created this attempt's ConnectContext may tear it down.
                    // A cancelling thread waits for this handoff instead of racing before() and clearing fields
                    // while planning is still publishing them.
                    try {
                        closeOrReleaseResources();
                    } catch (RuntimeException cleanupFailure) {
                        if (attemptFailure == null) {
                            attemptFailure = cleanupFailure;
                        } else {
                            attemptFailure.addSuppressed(cleanupFailure);
                        }
                    }
                }
                // A completed insert must never be replayed merely because teardown failed. Likewise,
                // successor-publication failures belong to the job state machine, not to the insert retry loop.
                if (executionSucceeded) {
                    if (attemptFailure != null) {
                        failCompletedAttempt(attemptFailure);
                        return;
                    }
                    try {
                        onSuccess();
                    } catch (Exception completionFailure) {
                        failCompletedAttempt(completionFailure);
                    }
                    return;
                }
                if (attemptFailure == null) {
                    return;
                }
                if (TaskStatus.CANCELED.equals(status)) {
                    return;
                }
                this.errMsg = attemptFailure.getMessage();
                retryCount++;
                if (noRetry || retryCount > MAX_RETRY) {
                    log.error("Task execution failed, job id {}, task id {}, noRetry {}, retry {}.",
                            jobId, taskId, noRetry, retryCount, attemptFailure);
                    onFail(attemptFailure.getMessage());
                    return;
                }
                log.warn("execute streaming task error, job id is {}, task id is {}, retrying {}/{}: {}",
                        jobId, taskId, retryCount, MAX_RETRY, attemptFailure.getMessage());
            }
        } finally {
            synchronized (executionCompletion) {
                executionFinished = true;
                executionOwner = null;
                executionCompletion.notifyAll();
            }
            onExecutionFinished();
        }
    }

    protected void onExecutionFinished() {
    }

    private void failCompletedAttempt(Exception failure) throws JobException {
        this.errMsg = failure.getMessage();
        log.error("Completed streaming task could not publish its terminal state, job id {}, task id {}.",
                jobId, taskId, failure);
        onFail(failure.getMessage());
    }

    protected void awaitExecutionCompletion(long timeoutMs) {
        boolean interrupted = false;
        synchronized (executionCompletion) {
            if (Thread.currentThread() == executionOwner) {
                return;
            }
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (executionStarted && !executionFinished) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }
                try {
                    executionCompletion.wait(remaining);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** True when cancellation can hand off the job slot without overlapping the execution owner. */
    boolean canHandoffAfterCancellation() {
        synchronized (executionCompletion) {
            return !executionStarted || executionFinished;
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

    /** Publishes cancellation without performing task-specific RPCs or waits. */
    public void publishCancellation() {
        // Flip isCanceled even on terminal states so late BE callbacks short-circuit.
        if (getIsCanceled().getAndSet(true)) {
            return;
        }
        if (TaskStatus.SUCCESS.equals(status) || TaskStatus.FAILED.equals(status)
                || TaskStatus.CANCELED.equals(status)) {
            return;
        }
        status = TaskStatus.CANCELED;
        this.errMsg = "task cancelled";
    }

    public void cancel(boolean needWaitCancelComplete) {
        publishCancellation();
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
        return trow;
    }
}
