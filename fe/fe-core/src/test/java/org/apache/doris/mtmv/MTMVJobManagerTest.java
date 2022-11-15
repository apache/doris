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
import org.apache.doris.mtmv.metadata.MTMVTask;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

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
        Assertions.assertEquals(1, jobManager.showAllJobs().size());
        Assertions.assertEquals(1, jobManager.showJobs(MTMVUtilsTest.dbName).size());
        Assertions.assertEquals(1, jobManager.showJobs(MTMVUtilsTest.dbName, MTMVUtilsTest.MV_NAME).size());
        while (!jobManager.getJob(MTMVUtilsTest.O_JOB).getState().equals(JobState.COMPLETE)) {
            Thread.sleep(10000);
            System.out.println("Loop    once");
        }

        Assertions.assertEquals(1, jobManager.getTaskManager().getAllHistory().size());
        Assertions.assertEquals(1, jobManager.getTaskManager().showAllTasks().size());
        Assertions.assertEquals(1, jobManager.getTaskManager().showTasks(MTMVUtilsTest.dbName).size());
        Assertions.assertEquals(1,
                jobManager.getTaskManager().showTasks(MTMVUtilsTest.dbName, MTMVUtilsTest.MV_NAME).size());

        // verify job meta
        MTMVJob metaJob = jobManager.showAllJobs().get(0);
        List<String> jobRow = metaJob.toStringRow();
        Assertions.assertEquals(13, jobRow.size());
        // index 1: Name
        Assertions.assertEquals(MTMVUtilsTest.O_JOB, jobRow.get(1));
        // index 2: TriggerMode
        Assertions.assertEquals("ONCE", jobRow.get(2));
        // index 3: Schedule
        Assertions.assertEquals("NULL", jobRow.get(3));
        // index 4: DBName
        Assertions.assertEquals(MTMVUtilsTest.dbName, jobRow.get(4));
        // index 5: MVName
        Assertions.assertEquals(MTMVUtilsTest.MV_NAME, jobRow.get(5));
        // index 6: Query
        Assertions.assertEquals("", jobRow.get(6));
        // index 7: User
        Assertions.assertEquals("root", jobRow.get(7));
        // index 8: RetryPolicy
        Assertions.assertEquals("NEVER", jobRow.get(8));
        // index 9: State
        Assertions.assertEquals("COMPLETE", jobRow.get(9));

        // verify task meta
        MTMVTask metaTask = jobManager.getTaskManager().showAllTasks().get(0);
        List<String> taskRow = metaTask.toStringRow();
        Assertions.assertEquals(14, taskRow.size());
        // index 1: JobName
        Assertions.assertEquals(MTMVUtilsTest.O_JOB, taskRow.get(1));
        // index 2: DBName
        Assertions.assertEquals(MTMVUtilsTest.dbName, taskRow.get(2));
        // index 3: MVName
        Assertions.assertEquals(MTMVUtilsTest.MV_NAME, taskRow.get(3));
        // index 4: Query
        Assertions.assertEquals("", taskRow.get(4));
        // index 5: User
        Assertions.assertEquals("root", taskRow.get(5));
        // index 6: Priority
        Assertions.assertEquals("0", taskRow.get(6));
        // index 7: RetryTimes
        Assertions.assertEquals("0", taskRow.get(7));
        // index 8: State
        Assertions.assertEquals("SUCCESS", taskRow.get(8));
        // index 9: Message
        Assertions.assertEquals("", taskRow.get(9));
        // index 10: ErrorCode
        Assertions.assertEquals("0", taskRow.get(10));
    }
}
