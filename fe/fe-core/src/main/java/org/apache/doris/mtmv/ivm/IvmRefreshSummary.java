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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.job.extensions.mtmv.MTMVTask;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

/**
 * What the refresh tasks of an IVM MV have come to: consecutive failures, the latest success and failure, how the
 * latest task refreshed the MV and how long it took, and how often a refresh fell back from the incremental path.
 *
 * <p>Kept on the MV rather than read from its task history, which holds only the latest
 * max_persistence_task_count tasks. It is updated from each task result as the MV records it, on the Master and in
 * the replay of the same journal record alike, and only from what that record carries, so a follower that takes
 * over keeps the same summary.
 *
 * <p>A canceled task leaves it as it is: it neither succeeded nor failed, and it has no finish time to measure it
 * by.
 */
public class IvmRefreshSummary {
    // What the latest failure says, cut to this many characters.
    public static final int MAX_ERROR_MESSAGE_LENGTH = 512;

    // Failed tasks since the latest successful one.
    @SerializedName("cf")
    private int consecutiveFailures;
    // Finish times in milliseconds, 0 when there has been none.
    @SerializedName("lst")
    private long lastSuccessTimeMs;
    @SerializedName("lft")
    private long lastFailureTimeMs;
    // An IvmFailureReason or an IvmRefreshFailureStage; null for a failure recorded before the task carried it.
    @SerializedName("lfr")
    private String lastFailureReason;
    @SerializedName("lem")
    private String lastErrorMessage;
    // Of the latest task that succeeded or failed.
    @SerializedName("lrm")
    private IvmRefreshMethod lastRefreshMethod;
    @SerializedName("lrd")
    private long lastRefreshDurationMs;
    @SerializedName("fc")
    private long fallbackCount;
    @SerializedName("lfbr")
    private String lastFallbackReason;
    @SerializedName("lfbt")
    private long lastFallbackTimeMs;
    // Whether a refresh has ever succeeded: until one has, the MV has no baseline for the incremental refresh to
    // keep up, and building it is not the incremental refresh falling back.
    @SerializedName("s")
    private boolean succeeded;

    /** Takes in the result of a task, which has ended as SUCCESS, FAILED or CANCELED. */
    public void update(MTMVTask task) {
        switch (task.getStatus()) {
            case SUCCESS:
                consecutiveFailures = 0;
                lastSuccessTimeMs = task.getFinishTimeMs();
                succeeded = true;
                break;
            case FAILED:
                consecutiveFailures++;
                lastFailureTimeMs = task.getFinishTimeMs();
                lastFailureReason = task.getIvmFailureReason();
                lastErrorMessage = StringUtils.abbreviate(task.getErrMsg(), MAX_ERROR_MESSAGE_LENGTH);
                break;
            case CANCELED:
                return;
            default:
                throw new IllegalStateException("A task result cannot be " + task.getStatus()
                        + ", taskId=" + task.getTaskId());
        }
        lastRefreshMethod = task.getIvmRefreshMethod();
        lastRefreshDurationMs = task.getFinishTimeMs() - task.getStartTimeMs();
        if (task.isIvmFallback()) {
            fallbackCount++;
            lastFallbackReason = task.getIvmFallbackReason();
            lastFallbackTimeMs = task.getFinishTimeMs();
        }
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures;
    }

    public long getLastSuccessTimeMs() {
        return lastSuccessTimeMs;
    }

    public long getLastFailureTimeMs() {
        return lastFailureTimeMs;
    }

    public String getLastFailureReason() {
        return lastFailureReason;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    public IvmRefreshMethod getLastRefreshMethod() {
        return lastRefreshMethod;
    }

    public long getLastRefreshDurationMs() {
        return lastRefreshDurationMs;
    }

    public long getFallbackCount() {
        return fallbackCount;
    }

    public String getLastFallbackReason() {
        return lastFallbackReason;
    }

    public long getLastFallbackTimeMs() {
        return lastFallbackTimeMs;
    }

    public boolean hasSucceeded() {
        return succeeded;
    }
}
