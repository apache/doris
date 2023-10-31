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

package org.apache.doris.scheduler.disruptor;

import org.apache.doris.scheduler.common.IntervalUnit;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.constants.JobType;
import org.apache.doris.scheduler.executor.SqlJobExecutor;
import org.apache.doris.scheduler.job.Job;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JobTest {

    private static Job job;

    @BeforeAll
    public static void init() {
        SqlJobExecutor sqlJobExecutor = new SqlJobExecutor("insert into test values(1);");
        job = new Job("insertTest", 1000L, System.currentTimeMillis(), System.currentTimeMillis() + 100000, sqlJobExecutor);
        job.setJobType(JobType.RECURRING);
        job.setComment("test");
        job.setOriginInterval(10L);
        job.setIntervalUnit(IntervalUnit.SECOND);
        job.setUser("root");
        job.setDbName("test");
        job.setTimezone("Asia/Shanghai");
        job.setJobCategory(JobCategory.SQL);
    }

    @Test
    public void testSerialization() throws IOException {
        Path path = Paths.get("./scheduler-jobs");
        Files.deleteIfExists(path);
        Files.createFile(path);
        DataOutputStream dos = new DataOutputStream(Files.newOutputStream(path));
        job.write(dos);
        dos.flush();
        dos.close();
        DataInputStream dis = new DataInputStream(Files.newInputStream(path));
        Job readJob = Job.readFields(dis);
        Assertions.assertEquals(job.getJobName(), readJob.getJobName());
        Assertions.assertEquals(job.getTimezone(), readJob.getTimezone());

    }

    @AfterAll
    public static void clean() throws IOException {
        Path path = Paths.get("./scheduler-jobs");
        Files.deleteIfExists(path);
    }
}
