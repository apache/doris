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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.InternalErrorCode;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.common.FailureReason;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

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
        if (streamingInsertJob.getRunningStreamTask() != null) {
            streamingInsertJob.getRunningStreamTask().cancel(needWaitCancelComplete);
        }
    }

    @Override
    public TRow getTvfInfo(String jobName) {
        StreamingInsertTask runningTask = streamingInsertJob.getRunningStreamTask();
        if (runningTask == null) {
            return null;
        }
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(runningTask.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(runningTask.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(runningTask.getLabelName()));
        trow.addToColumnValue(new TCell().setStringVal(runningTask.getStatus().name()));
        // err msg
        String errMsg = "";
        if (StringUtils.isNotBlank(runningTask.getErrMsg())
                && !FeConstants.null_string.equals(runningTask.getErrMsg())) {
            errMsg = runningTask.getErrMsg();
        } else {
            errMsg = runningTask.getOtherMsg();
        }
        trow.addToColumnValue(new TCell().setStringVal(StringUtils.isNotBlank(errMsg)
                ? errMsg : FeConstants.null_string));

        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(runningTask.getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getStartTimeMs() ? FeConstants.null_string
                : TimeUtils.longToTimeString(runningTask.getStartTimeMs())));
        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(runningTask.getFinishTimeMs())));

        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager()
                .queryLoadJobsByJobIds(Arrays.asList(runningTask.getTaskId()));
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

        if (runningTask.getUserIdentity() == null) {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(runningTask.getUserIdentity().getQualifiedUser()));
        }
        trow.addToColumnValue(new TCell().setStringVal(""));
        return trow;
    }
}
