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

package org.apache.doris.mtmv;

import org.apache.doris.mtmv.metadata.MTMVJob;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class MTMVUtils {

    public enum TriggerMode {
        MANUAL, ONCE, PERIODICAL, ON_COMMIT
    }

    /**
     * All the job init status: UNKNOWN
     * create job: UNKNOWN -> ACTIVE
     * after job scheduler:
     * - ONCE Job: ACTIVE -> COMPLETE (no matter task success or fail)
     * - PERIODICAL Job: ACTIVE -> PAUSE (if the task failed too many times)
     * - PERIODICAL Job: ACTIVE -> COMPLETE (when the job is expired.)
     * recover job: PAUSE -> ACTIVE (specific command)
     */
    public enum JobState {
        UNKNOWN, ACTIVE, PAUSE, COMPLETE
    }

    public enum TaskRetryPolicy {
        NEVER(0), // no any retry
        TIMES(3), // fix 3 times retry
        ALWAYS(100); // treat 100 as big number since most case will cause a lot of resource
        private final int times;

        TaskRetryPolicy(int times) {
            this.times = times;
        }

        public int getTimes() {
            return times;
        }
    }

    public enum TaskState {
        PENDING, RUNNING, FAILED, SUCCESS,
    }

    enum TaskSubmitStatus {
        SUBMITTED, REJECTED, FAILED
    }

    public enum TaskPriority {
        LOW(0), NORMAL(50), HIGH(100);

        private final int value;

        TaskPriority(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    public static long getDelaySeconds(MTMVJob job) {
        return getDelaySeconds(job, LocalDateTime.now());
    }

    // this method only for test
    public static long getDelaySeconds(MTMVJob job, LocalDateTime now) {
        long lastModifyTime = job.getLastModifyTime();
        long nextTime = 0;
        // if set lastModifyTime, use lastModifyTime otherwise use the start time.
        if (lastModifyTime > 0) {
            // check null of schedule outside.
            nextTime = lastModifyTime + job.getSchedule().getSecondPeriod();
        } else {
            nextTime = job.getSchedule().getStartTime();
        }
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(nextTime), ZoneId.systemDefault());
        Duration duration = Duration.between(now, time);
        long delaySeconds = duration.getSeconds();
        // start immediately if startTime < now
        return delaySeconds < 0 ? 0 : delaySeconds;
    }

    public static MTMVTaskExecutor buildTask(MTMVJob job) {
        MTMVTaskExecutor taskExecutor = new MTMVTaskExecutor();
        taskExecutor.setJobId(job.getId());
        taskExecutor.setProperties(job.getProperties());
        taskExecutor.setJob(job);
        taskExecutor.setProcessor(new MTMVTaskProcessor());

        return taskExecutor;
    }

    public static TimeUnit getTimeUint(String strTimeUnit) {
        switch (strTimeUnit.toUpperCase()) {
            case "SECOND": return TimeUnit.SECONDS;
            case "HOUR": return TimeUnit.HOURS;
            case "DAY": return TimeUnit.DAYS;
            default:
                return TimeUnit.DAYS;
        }
    }

    // In MTMV package, all the timestamp unit is second.
    public static long getNowTimeStamp() {
        return System.currentTimeMillis() / 1000;
    }

    public static String getTimeString(long timestamp) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.systemDefault());
        return time.toString();
    }
}
