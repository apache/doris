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

import org.apache.doris.common.Config;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.job.common.FailureReason;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.thrift.TRow;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class StreamingJobSchedulerTask extends AbstractTask {
    private static final long BACK_OFF_BASIC_TIME_SEC = 10L;
    private static final long MAX_BACK_OFF_TIME_SEC = 60 * 5;
    private StreamingInsertJob streamingInsertJob;

    public StreamingJobSchedulerTask(StreamingInsertJob streamingInsertJob) {
        this.streamingInsertJob = streamingInsertJob;
    }

    @Override
    public void run() throws JobException {
        switch (streamingInsertJob.getJobStatus()) {
            case PENDING:
                handlePendingState();
                break;
            case RUNNING:
                handleRunningState();
                break;
            case PAUSED:
                autoResumeHandler();
                break;
            default:
                break;
        }
    }

    private void handlePendingState() throws JobException {
        if (Config.isCloudMode()) {
            try {
                streamingInsertJob.replayOnCloudMode();
            } catch (JobException e) {
                streamingInsertJob.setFailureReason(
                    new FailureReason(InternalErrorCode.INTERNAL_ERR, e.getMessage()));
                streamingInsertJob.updateJobStatus(JobStatus.PAUSED);
                return;
            }
        }
        streamingInsertJob.replayOffsetProviderIfNeed();
        streamingInsertJob.createStreamingTask();
        streamingInsertJob.setSampleStartTime(System.currentTimeMillis());
        streamingInsertJob.updateJobStatus(JobStatus.RUNNING);
        streamingInsertJob.setAutoResumeCount(0);
    }

    private void handleRunningState() throws JobException {
        streamingInsertJob.processTimeoutTasks();
        streamingInsertJob.fetchMeta();
    }

    private void autoResumeHandler() throws JobException {
        final FailureReason failureReason = streamingInsertJob.getFailureReason();
        final long latestAutoResumeTimestamp = streamingInsertJob.getLatestAutoResumeTimestamp();
        final long autoResumeCount = streamingInsertJob.getAutoResumeCount();
        final long current = System.currentTimeMillis();

        if (failureReason != null
                && failureReason.getCode() != InternalErrorCode.MANUAL_PAUSE_ERR
                && failureReason.getCode() != InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR
                && failureReason.getCode() != InternalErrorCode.CANNOT_RESUME_ERR) {
            long autoResumeIntervalTimeSec = autoResumeCount < 5
                        ? Math.min((long) Math.pow(2, autoResumeCount) * BACK_OFF_BASIC_TIME_SEC,
                                MAX_BACK_OFF_TIME_SEC) : MAX_BACK_OFF_TIME_SEC;
            if (current - latestAutoResumeTimestamp > autoResumeIntervalTimeSec * 1000L) {
                streamingInsertJob.setLatestAutoResumeTimestamp(current);
                if (autoResumeCount < Long.MAX_VALUE) {
                    streamingInsertJob.setAutoResumeCount(autoResumeCount + 1);
                }
                streamingInsertJob.updateJobStatus(JobStatus.PENDING);
                return;
            }
        }
    }

    @Override
    protected void closeOrReleaseResources() {
        // do nothing
    }

    @Override
    protected void executeCancelLogic(boolean needWaitCancelComplete) throws Exception {
        // cancel logic in streaming insert task
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        // only show streaming insert task info in job tvf
        return null;
    }
}
