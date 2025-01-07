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

package org.apache.doris.job.base;

import org.apache.doris.common.util.TimeUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;


public class JobExecutionConfiguration {

    @Getter
    @Setter
    @SerializedName(value = "td")
    private TimerDefinition timerDefinition;
    @Getter
    @Setter
    @SerializedName(value = "ec")
    private JobExecuteType executeType;

    @Getter
    @Setter
    private boolean immediate = false;

    /**
     * Maximum number of concurrent tasks, <= 0 means no limit
     * if the number of tasks exceeds the limit, the task will be delayed execution
     * todo: implement this later, we need to consider concurrency strategies
     */
    private Integer maxConcurrentTaskNum;

    public void checkParams() {
        if (executeType == null) {
            throw new IllegalArgumentException("executeType cannot be null");
        }

        if (executeType == JobExecuteType.INSTANT || executeType == JobExecuteType.MANUAL) {
            return;
        }
        checkTimerDefinition();
        if (executeType == JobExecuteType.ONE_TIME) {
            validateStartTimeMs();
            return;
        }

        if (executeType == JobExecuteType.STREAMING) {
            validateStartTimeMs();
            return;
        }

        if (executeType == JobExecuteType.RECURRING) {
            if (timerDefinition.getInterval() == null) {
                throw new IllegalArgumentException("interval cannot be null when executeType is RECURRING");
            }
            if (timerDefinition.getIntervalUnit() == null) {
                throw new IllegalArgumentException("intervalUnit cannot be null when executeType is RECURRING");
            }
        }
    }

    private void checkTimerDefinition() {
        if (timerDefinition == null) {
            throw new IllegalArgumentException(
                    "timerDefinition cannot be null when executeType is not instant or manual");
        }
        timerDefinition.checkParams();
    }

    private void validateStartTimeMs() {
        if (timerDefinition.getStartTimeMs() == null) {
            throw new IllegalArgumentException("startTimeMs cannot be null");
        }
        if (isImmediate()) {
            return;
        }
        if (timerDefinition.getStartTimeMs() < System.currentTimeMillis()) {
            throw new IllegalArgumentException("startTimeMs must be greater than current time");
        }
    }


    // Returns a list of delay times in seconds for triggering the job
    public List<Long> getTriggerDelayTimes(Long currentTimeMs, Long startTimeMs, Long endTimeMs) {
        List<Long> delayTimeSeconds = new ArrayList<>();

        if (JobExecuteType.ONE_TIME.equals(executeType)) {
            // If the job is already executed or in the schedule queue, or not within this schedule window
            if (null != timerDefinition.getLatestSchedulerTimeMs() || endTimeMs < timerDefinition.getStartTimeMs()) {
                return delayTimeSeconds;
            }

            delayTimeSeconds.add(queryDelayTimeSecond(currentTimeMs, timerDefinition.getStartTimeMs()));
            this.timerDefinition.setLatestSchedulerTimeMs(timerDefinition.getStartTimeMs());
            return delayTimeSeconds;
        }

        if (JobExecuteType.STREAMING.equals(executeType) && null != timerDefinition) {
            if (null == timerDefinition.getStartTimeMs() || null != timerDefinition.getLatestSchedulerTimeMs()) {
                return delayTimeSeconds;
            }

            // If the job is already executed or in the schedule queue, or not within this schedule window
            if (endTimeMs < timerDefinition.getStartTimeMs()) {
                return delayTimeSeconds;
            }

            delayTimeSeconds.add(queryDelayTimeSecond(currentTimeMs, timerDefinition.getStartTimeMs()));
            this.timerDefinition.setLatestSchedulerTimeMs(timerDefinition.getStartTimeMs());
            return delayTimeSeconds;
        }

        if (JobExecuteType.RECURRING.equals(executeType)) {
            if (timerDefinition.getStartTimeMs() > endTimeMs || null != timerDefinition.getEndTimeMs()
                    && timerDefinition.getEndTimeMs() < startTimeMs) {
                return delayTimeSeconds;
            }
            long intervalValue = timerDefinition.getIntervalUnit().getIntervalMs(timerDefinition.getInterval());
            long jobStartTimeMs = timerDefinition.getStartTimeMs();
            if (isImmediate()) {
                jobStartTimeMs += intervalValue;
                if (jobStartTimeMs > endTimeMs) {
                    return delayTimeSeconds;
                }
            }
            return getExecutionDelaySeconds(startTimeMs, endTimeMs, jobStartTimeMs,
                    intervalValue, currentTimeMs);
        }

        return delayTimeSeconds;
    }

    // Returns the delay time in seconds between the current time and the specified start time
    private Long queryDelayTimeSecond(Long currentTimeMs, Long startTimeMs) {
        if (startTimeMs <= currentTimeMs) {
            return 0L;
        }

        return (startTimeMs * 1000 / 1000 - currentTimeMs) / 1000;
    }

    // Returns a list of delay times in seconds for executing the job within the specified window
    private List<Long> getExecutionDelaySeconds(long windowStartTimeMs, long windowEndTimeMs, long startTimeMs,
                                                long intervalMs, long currentTimeMs) {
        List<Long> timestamps = new ArrayList<>();

        long windowDuration = windowEndTimeMs - windowStartTimeMs;

        if (windowDuration <= 0 || intervalMs <= 0) {
            return timestamps; // Return an empty list if there won't be any trigger time
        }

        long firstTriggerTime = windowStartTimeMs + (intervalMs - ((windowStartTimeMs - startTimeMs)
                % intervalMs)) % intervalMs;
        if (firstTriggerTime < currentTimeMs) {
            // Calculate how many intervals to add to get the largest trigger time < currentTimeMs
            long intervalsToAdd = (currentTimeMs - firstTriggerTime) / intervalMs;
            firstTriggerTime += intervalsToAdd * intervalMs;
        }
        if (firstTriggerTime > windowEndTimeMs) {
            return timestamps; // Return an empty list if there won't be any trigger time
        }

        // Calculate the trigger time list
        for (long triggerTime = firstTriggerTime; triggerTime <= windowEndTimeMs; triggerTime += intervalMs) {
            if (null == timerDefinition.getEndTimeMs()
                    || triggerTime < timerDefinition.getEndTimeMs()) {
                timerDefinition.setLatestSchedulerTimeMs(triggerTime);
                timestamps.add(queryDelayTimeSecond(currentTimeMs, triggerTime));
            }
        }

        return timestamps;
    }

    public String convertRecurringStrategyToString() {
        switch (executeType) {
            case ONE_TIME:
                return "AT " + TimeUtils.longToTimeString(timerDefinition.getStartTimeMs());
            case RECURRING:
                String result = "EVERY " + timerDefinition.getInterval() + " "
                        + timerDefinition.getIntervalUnit().name() + " STARTS "
                        + TimeUtils.longToTimeString(timerDefinition.getStartTimeMs());

                if (null != timerDefinition.getEndTimeMs()) {
                    result += " ENDS " + TimeUtils.longToTimeString(timerDefinition.getEndTimeMs());
                }
                return result;
                /*            case STREAMING:
                return "STREAMING" + (startTimeMs > 0 ? " AT " + TimeUtils.longToTimeString(startTimeMs) : "");*/
            case MANUAL:
                return "MANUAL TRIGGER";
            case INSTANT:
                return "INSTANT";
            default:
                return "UNKNOWN";
        }
    }

    public boolean checkIsTimerJob() {
        return null != timerDefinition;
    }

}
