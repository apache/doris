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

package org.apache.doris.scheduler;

import org.apache.doris.scheduler.metadata.Job;
import org.apache.doris.thrift.TUniqueId;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Utils {
    public enum TriggerMode {
        MANUAL,
        ONCE,
        PERIODICAL,
        ON_COMMIT
    }

    public enum JobState {
        UNKNOWN,
        ACTIVE,
        PAUSE,
        COMPLETE
    }

    public enum RetryPolicy {
        NEVER, // no retry
        TIMES, // fix times
        ALWAYS // retry until success
    }

    public enum TaskState {
        PENDING, RUNNING, FAILED, SUCCESS,
    }

    public enum TaskPriority {
        LOWEST(0), LOW(20), NORMAL(50), HIGH(80), HIGHEST(100);

        private final int value;

        TaskPriority(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }
    }

    public static TUniqueId genTUniqueId(UUID id) {
        return new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    public static LocalDateTime getDatetimeFromLong(long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(dateTime), ZoneId.systemDefault());
    }

    public static long convertTimeUnitValueToSecond(long value, TimeUnit unit) {
        switch (unit) {
            case DAYS:
                return value * 60 * 60 * 24;
            case HOURS:
                return value * 60 * 60;
            case MINUTES:
                return value * 60;
            case SECONDS:
                return value;
            case MILLISECONDS:
                return value / 1000;
            case MICROSECONDS:
                return value / 1000 / 1000;
            case NANOSECONDS:
                return value / 1000 / 1000 / 1000;
            default:
                return 0;
        }
    }

    public static TaskExecutor buildTask(Job job) {
        TaskExecutor taskExecutor = new TaskExecutor();
        taskExecutor.setJobId(job.getId());
        taskExecutor.setProperties(job.getProperties());
        taskExecutor.setJob(job);


        return taskExecutor;
    }
}
