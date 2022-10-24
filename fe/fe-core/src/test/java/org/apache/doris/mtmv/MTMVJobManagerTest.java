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
import org.apache.doris.mtmv.MTMVUtils.JobState;
import org.apache.doris.mtmv.metadata.ChangeMTMVJob;
import org.apache.doris.mtmv.metadata.MTMVJob;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class MTMVJobManagerTest extends TestWithFeService {

    @Test
    public void testSampleCase() throws DdlException {
        MTMVJobManager jobManager = new MTMVJobManager();
        jobManager.start();
        MTMVJob job = MTMVUtilsTest.createDummyJob();
        jobManager.createJob(job, false);
        Assertions.assertEquals(1, jobManager.showAllJobs().size());
        MTMVJob resultJob = jobManager.getJob("dummy");
        Assertions.assertEquals("dummy", resultJob.getName());
        Assertions.assertEquals(JobState.ACTIVE, resultJob.getState());
        long jobId = resultJob.getId();
        ChangeMTMVJob changeMTMVJob = new ChangeMTMVJob(jobId, JobState.PAUSE);
        jobManager.updateJob(changeMTMVJob, false);
        resultJob = jobManager.getJob("dummy");
        Assertions.assertEquals("dummy", resultJob.getName());
        Assertions.assertEquals(JobState.PAUSE, resultJob.getState());
        jobManager.dropJobs(Collections.singletonList(jobId), false);
        Assertions.assertEquals(0, jobManager.showAllJobs().size());
    }

    @Test
    public void testSchedulerJob() throws DdlException, InterruptedException {
        MTMVJobManager jobManager = new MTMVJobManager();
        jobManager.start();
        MTMVJob job = MTMVUtilsTest.createSchedulerJob();
        jobManager.createJob(job, false);
        Assertions.assertEquals(1, jobManager.showJobs(MTMVUtilsTest.dbName).size());
        Thread.sleep(5000L);
        Assertions.assertTrue(jobManager.getTaskManager().getAllHistory().size() > 1);
    }

    @Test
    public void testOnceJob() throws DdlException, InterruptedException {
        MTMVJobManager jobManager = new MTMVJobManager();
        jobManager.start();
        MTMVJob job = MTMVUtilsTest.createOnceJob();
        jobManager.createJob(job, false);
        Assertions.assertEquals(1, jobManager.showJobs(MTMVUtilsTest.dbName).size());
        while (!jobManager.getJob(MTMVUtilsTest.O_JOB).getState().equals(JobState.COMPLETE)) {
            Thread.sleep(10000);
            System.out.println("Loop    once");
        }

        Assertions.assertEquals(1, jobManager.getTaskManager().getAllHistory().size());
    }

}
