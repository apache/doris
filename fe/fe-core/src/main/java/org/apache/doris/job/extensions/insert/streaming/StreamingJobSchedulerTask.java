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

import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.PauseReason;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.thrift.TRow;

public class StreamingJobSchedulerTask extends AbstractTask {

    private static final long BACK_OFF_BASIC_TIME_SEC = 10L;
    private static final long MAX_BACK_OFF_TIME_SEC = 60 * 5;

    private StreamingInsertJob streamingInsertJob;

    public StreamingJobSchedulerTask(StreamingInsertJob streamingInsertJob) {
        this.streamingInsertJob = streamingInsertJob;
    }

    @Override
    public void run() throws JobException {
        switch (streamingInsertJob.getStatus()) {
            case PENDING:
                streamingInsertJob.createStreamingInsertTask();
                streamingInsertJob.updateJobStatus(JobStatus.RUNNING);
                streamingInsertJob.setAutoResumeCount(0);
                break;
            case RUNNING:
                streamingInsertJob.fetchMeta();
                break;
            case PAUSED:
                autoResumeHandler();
                break;
            default:
                break;
        }
    }

    private void autoResumeHandler() throws JobException {
        final PauseReason pauseReason = streamingInsertJob.getPauseReason();
        final long latestAutoResumeTimestamp = streamingInsertJob.getLatestAutoResumeTimestamp();
        final long autoResumeCount = streamingInsertJob.getAutoResumeCount();
        final long current = System.currentTimeMillis();

        if (pauseReason != null
                && pauseReason.getCode() != InternalErrorCode.MANUAL_PAUSE_ERR
                && pauseReason.getCode() != InternalErrorCode.TOO_MANY_FAILURE_ROWS_ERR
                && pauseReason.getCode() != InternalErrorCode.CANNOT_RESUME_ERR) {
            long autoResumeIntervalTimeSec = autoResumeCount < 5
                        ? Math.min((long) Math.pow(2, autoResumeCount) * BACK_OFF_BASIC_TIME_SEC,
                                MAX_BACK_OFF_TIME_SEC) : MAX_BACK_OFF_TIME_SEC;
            if (current - latestAutoResumeTimestamp > autoResumeIntervalTimeSec * 1000L) {
                streamingInsertJob.setLatestAutoResumeTimestamp(current);
                if (autoResumeCount < Long.MAX_VALUE) {
                    streamingInsertJob.setAutoResumeCount(autoResumeCount + 1);
                }
                streamingInsertJob.updateJobStatus(JobStatus.RUNNING);
                return;
            }
        }
    }

    @Override
    protected void closeOrReleaseResources() {
    }

    @Override
    protected void executeCancelLogic(boolean needWaitCancelComplete) throws Exception {
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        return null;
    }
}
