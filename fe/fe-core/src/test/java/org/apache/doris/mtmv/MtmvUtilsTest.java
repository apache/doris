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

import org.apache.doris.mtmv.MtmvUtils.TriggerMode;
import org.apache.doris.mtmv.metadata.MtmvJob;
import org.apache.doris.mtmv.metadata.MtmvJob.JobSchedule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;

public class MtmvUtilsTest {
    public static final String dbName = "test";
    public static final String S_JOB = "SchedulerJob";
    public static final String O_JOB = "OnceJob";

    public static MtmvJob createDummyJob() {
        MtmvJob job = new MtmvJob("dummy");
        job.setDbName(dbName);
        return job;
    }

    public static MtmvJob createOnceJob() {
        MtmvJob job = new MtmvJob("");
        job.setTriggerMode(TriggerMode.ONCE);
        job.setDbName(dbName);
        job.setName(O_JOB);
        return job;
    }

    public static MtmvJob createSchedulerJob() {
        MtmvJob job = new MtmvJob("");
        JobSchedule jobSchedule = new JobSchedule(System.currentTimeMillis() / 1000, 1, TimeUnit.SECONDS);
        job.setSchedule(jobSchedule);
        job.setTriggerMode(TriggerMode.PERIODICAL);
        job.setDbName(dbName);
        job.setName(S_JOB);
        return job;
    }

    @Test
    public void testGetDelaySeconds() {
        MtmvJob job = MtmvUtilsTest.createDummyJob();

        // 2022-10-03 15:00:00
        JobSchedule jobSchedule = new JobSchedule(1664780400L, 1, TimeUnit.HOURS);
        job.setSchedule(jobSchedule);

        Assertions.assertEquals(0, MtmvUtils.getDelaySeconds(job));

        // 2222-10-03 15:00:00
        jobSchedule = new JobSchedule(7976127600L, 1, TimeUnit.HOURS);
        job.setSchedule(jobSchedule);

        // 2022-10-03 16:00:00
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochSecond(1664784000L), ZoneId.systemDefault());
        Assertions.assertEquals(7976127600L - 1664784000L, MtmvUtils.getDelaySeconds(job, time));

        // 2022-10-03 15:00:00
        jobSchedule = new JobSchedule(1664780400L, 1, TimeUnit.HOURS);
        job.setSchedule(jobSchedule);
        // 2222-10-03 15:00:00
        job.setLastModifyTime(1664780400L + 1L);
        Assertions.assertEquals(1L, MtmvUtils.getDelaySeconds(job, time));

        job.setLastModifyTime(1664780400L - 1L);
        Assertions.assertEquals(0L, MtmvUtils.getDelaySeconds(job, time));
    }
}
