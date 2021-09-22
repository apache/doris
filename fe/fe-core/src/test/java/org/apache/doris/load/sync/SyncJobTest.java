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

package org.apache.doris.load.sync;

import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.sync.SyncFailMsg.MsgType;
import org.apache.doris.load.sync.SyncJob.JobState;
import org.apache.doris.load.sync.SyncJob.SyncJobUpdateStateInfo;
import org.apache.doris.load.sync.canal.CanalSyncJob;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import mockit.Mocked;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class SyncJobTest {
    private long jobId;
    private long dbId;
    private String jobName;

    @Before
    public void setUp() {
        jobId = 1L;
        dbId = 1L;
        jobName = "test_job";
    }

    @Test
    public void testUpdateStateToRunning() {
        SyncJob syncJob = new CanalSyncJob(jobId, jobName, dbId);
        try {
            syncJob.updateState(JobState.RUNNING, true);
            Assert.assertEquals(JobState.RUNNING, syncJob.getJobState());
            Assert.assertNotEquals(-1L, (long) Deencapsulation.getField(syncJob, "lastStartTimeMs"));
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testUpdateStateInfoPersist() throws IOException {
        String fileName = "./testSyncJobUpdateStateInfoPersistFile";
        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();

        JobState jobState = JobState.CANCELLED;
        SyncFailMsg failMsg = new SyncFailMsg(MsgType.USER_CANCEL, "user cancel");
        long lastStartTimeMs = 1621914540L;
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        SyncJobUpdateStateInfo info = new SyncJobUpdateStateInfo(
                jobId, jobState, lastStartTimeMs, -1L, -1L, failMsg);
        info.write(out);
        out.flush();
        out.close();

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        SyncJobUpdateStateInfo replayedInfo = SyncJobUpdateStateInfo.read(in);
        Assert.assertEquals(jobId, replayedInfo.getId());
        Assert.assertEquals(jobState, replayedInfo.getJobState());
        Assert.assertEquals(lastStartTimeMs, replayedInfo.getLastStartTimeMs());
        Assert.assertEquals(-1L, replayedInfo.getLastStopTimeMs());
        Assert.assertEquals(-1L, replayedInfo.getFinishTimeMs());
        Assert.assertEquals(failMsg, replayedInfo.getFailMsg());
        in.close();

        // delete file
        if (file.exists()) {
            file.delete();
        }
    }
}