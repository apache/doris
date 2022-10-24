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

import org.apache.doris.common.DdlException;
import org.apache.doris.mtmv.MtmvUtils.JobState;
import org.apache.doris.mtmv.metadata.ChangeMvmtJob;
import org.apache.doris.mtmv.metadata.MtmvJob;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class MtmvJobManagerTest extends TestWithFeService {

    @Test
    public void testSampleCase() throws DdlException {
        MtmvJobManager jobManager = new MtmvJobManager();
        jobManager.start();
        MtmvJob job = MtmvUtilsTest.createDummyJob();
        jobManager.createJob(job, false);
        Assertions.assertEquals(1, jobManager.showAllJobs().size());
        MtmvJob resultJob = jobManager.getJob("dummy");
        Assertions.assertEquals("dummy", resultJob.getName());
        Assertions.assertEquals(JobState.ACTIVE, resultJob.getState());
        long jobId = resultJob.getId();
        ChangeMvmtJob changeMvmtJob = new ChangeMvmtJob(jobId, JobState.PAUSE);
        jobManager.updateJob(changeMvmtJob, false);
        resultJob = jobManager.getJob("dummy");
        Assertions.assertEquals("dummy", resultJob.getName());
        Assertions.assertEquals(JobState.PAUSE, resultJob.getState());
        jobManager.dropJobs(Collections.singletonList(jobId), false);
        Assertions.assertEquals(0, jobManager.showAllJobs().size());
    }

    @Test
    public void testSchedulerJob() throws DdlException, InterruptedException {
        MtmvJobManager jobManager = new MtmvJobManager();
        jobManager.start();
        MtmvJob job = MtmvUtilsTest.createSchedulerJob();
        jobManager.createJob(job, false);
        Assertions.assertEquals(1, jobManager.showJobs(MtmvUtilsTest.dbName).size());
        Thread.sleep(5000L);
        Assertions.assertTrue(jobManager.getTaskManager().getAllHistory().size() > 1);
    }

    @Test
    public void testOnceJob() throws DdlException, InterruptedException {
        MtmvJobManager jobManager = new MtmvJobManager();
        jobManager.start();
        MtmvJob job = MtmvUtilsTest.createOnceJob();
        jobManager.createJob(job, false);
        Assertions.assertEquals(1, jobManager.showJobs(MtmvUtilsTest.dbName).size());
        while (!jobManager.getJob(MtmvUtilsTest.O_JOB).getState().equals(JobState.COMPLETE)) {
            Thread.sleep(10000);
            System.out.println("Loop    once");
        }

        Assertions.assertEquals(1, jobManager.getTaskManager().getAllHistory().size());
    }

}
