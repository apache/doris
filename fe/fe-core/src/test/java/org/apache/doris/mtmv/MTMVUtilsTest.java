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

import org.apache.doris.mtmv.MTMVUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.mtmv.metadata.MTMVJob.JobSchedule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class MTMVUtilsTest {
    public static final String dbName = "test";

    public static final String MV_NAME = "mvName";
    public static final String S_JOB = "SchedulerJob";
    public static final String O_JOB = "OnceJob";



    public static MTMVJob createDummyJob() {
        MTMVJob job = new MTMVJob("dummy");
        job.setDbName(dbName);
        job.setMvName(MV_NAME);
        return job;
    }

    public static MTMVJob createOnceJob() {
        MTMVJob job = new MTMVJob("");
        job.setTriggerMode(TriggerMode.ONCE);
        job.setDbName(dbName);
        job.setName(O_JOB);
        job.setMvName(MV_NAME);
        return job;
    }

    public static MTMVJob createSchedulerJob() {
        MTMVJob job = new MTMVJob("");
        JobSchedule jobSchedule = new JobSchedule(System.currentTimeMillis() / 1000, 1, TimeUnit.SECONDS);
        job.setSchedule(jobSchedule);
        job.setTriggerMode(TriggerMode.PERIODICAL);
        job.setDbName(dbName);
        job.setName(S_JOB);
        job.setMvName(MV_NAME);
        return job;
    }

    @Test
    public void testGetDelaySeconds() {
        MTMVJob job = MTMVUtilsTest.createDummyJob();

        // 2022-10-03 15:00:00
        JobSchedule jobSchedule = new JobSchedule(1664780400L, 1, TimeUnit.HOURS);
        job.setSchedule(jobSchedule);

        Assertions.assertEquals(0, MTMVUtils.getDelaySeconds(job));

        // 2222-10-03 15:00:00
        jobSchedule = new JobSchedule(7976127600L, 1, TimeUnit.HOURS);
        job.setSchedule(jobSchedule);

        // 2022-10-03 16:00:00
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(1664784000L), ZoneId.systemDefault());
        Assertions.assertEquals(7976127600L - 1664784000L, MTMVUtils.getDelaySeconds(job, time));

        // 2022-10-03 15:00:00
        jobSchedule = new JobSchedule(1664780400L, 1, TimeUnit.HOURS);
        job.setSchedule(jobSchedule);
        // 2222-10-03 15:00:00
        job.setLastModifyTime(1664780400L + 1L);
        Assertions.assertEquals(1L, MTMVUtils.getDelaySeconds(job, time));

        job.setLastModifyTime(1664780400L - 1L);
        Assertions.assertEquals(0L, MTMVUtils.getDelaySeconds(job, time));
    }
}
