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

package org.apache.doris.persist;

import org.apache.doris.cloud.CloudWarmUpJob;
import org.apache.doris.cloud.CloudWarmUpJob.JobState;
import org.apache.doris.cloud.CloudWarmUpJob.JobType;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModifyCloudWarmUpJobTest {
    private static String fileName = "./ModifyCloudWarmUpJobTest";

    @Test
    public void testSerialization() throws IOException {
        Config.cloud_unique_id = "test_cloud";
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();

        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        long jobId = 12345;
        CloudWarmUpJob.JobState jobState = JobState.FINISHED;
        long createTimeMs = 11111;
        String errMsg = "testMsg";
        long finishedTimesMs = 22222;
        String clusterName = "cloudTest";
        long lastBatchId = 33333;
        Map<Long, List<List<Long>>> beToTabletIdBatches = new HashMap<>();
        List<List<Long>> batches = new ArrayList<>();
        List<Long> batch = new ArrayList<>();
        batch.add(123L);
        batches.add(batch);
        beToTabletIdBatches.put(999L, batches);
        Map<Long, String> beToThriftAddress = new HashMap<>();
        beToThriftAddress.put(998L, "address");
        CloudWarmUpJob.JobType jobType = JobType.TABLE;

        CloudWarmUpJob warmUpJob = new CloudWarmUpJob(jobId, clusterName, beToTabletIdBatches, jobType);
        warmUpJob.setJobState(jobState);
        warmUpJob.setCreateTimeMs(createTimeMs);
        warmUpJob.setErrMsg(errMsg);
        warmUpJob.setFinishedTimeMs(finishedTimesMs);
        warmUpJob.setLastBatchId(lastBatchId);
        warmUpJob.setBeToTabletIdBatches(beToTabletIdBatches);
        warmUpJob.setBeToThriftAddress(beToThriftAddress);
        String c1Json = GsonUtils.GSON.toJson(warmUpJob);
        Text.writeString(out, c1Json);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));

        String readJson = Text.readString(in);
        CloudWarmUpJob warmUpJob2 = GsonUtils.GSON.fromJson(readJson,
                CloudWarmUpJob.class);

        Assert.assertEquals(jobId, warmUpJob2.getJobId());
        Assert.assertEquals(jobState, warmUpJob2.getJobState());
        Assert.assertEquals(createTimeMs, warmUpJob2.getCreateTimeMs());
        Assert.assertEquals(errMsg, warmUpJob2.getErrMsg());
        Assert.assertEquals(finishedTimesMs, warmUpJob2.getFinishedTimeMs());
        Assert.assertEquals(clusterName, warmUpJob2.getCloudClusterName());
        Assert.assertEquals(lastBatchId, warmUpJob2.getLastBatchId());
        Map<Long, List<List<Long>>> beToTabletIdBatches2 = warmUpJob2.getBeToTabletIdBatches();
        Assert.assertEquals(1, beToTabletIdBatches2.size());
        Assert.assertNotNull(beToTabletIdBatches2.get(999L));
        Assert.assertEquals(1, beToTabletIdBatches2.get(999L).size());
        Assert.assertEquals(1, beToTabletIdBatches2.get(999L).get(0).size());
        Assert.assertEquals(123L, (long) beToTabletIdBatches2.get(999L).get(0).get(0));
        Map<Long, String> beToThriftAddress2 = warmUpJob2.getBeToThriftAddress();
        Assert.assertEquals(1, beToThriftAddress2.size());
        Assert.assertNotNull(beToThriftAddress2.get(998L));
        Assert.assertEquals("address", beToThriftAddress2.get(998L));
        Assert.assertEquals(jobType, warmUpJob2.getJobType());

    }

    @After
    public void tearDown() {
        File file = new File(fileName);
        file.delete();
    }
}
