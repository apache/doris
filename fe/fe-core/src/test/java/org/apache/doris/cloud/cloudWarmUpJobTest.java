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

package com.selectdb.cloud.warmup;

import com.selectdb.cloud.catalog.CacheHotspotManager;
import com.selectdb.cloud.catalog.CloudWarmUpJob;
import com.selectdb.cloud.catalog.CloudWarmUpJob.JobState;
import com.selectdb.cloud.catalog.CloudWarmUpJob.JobType;

import org.apache.doris.analysis.CancelCloudWarmUpStmt;
import org.apache.doris.analysis.WarmUpClusterStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.utframe.TestWithFeService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CloudWarmUpJobTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(CloudWarmUpJobTest.class);

    @Override
    protected void runBeforeAll() throws Exception {
        Config.cloud_warm_up_job_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testWarmUpCluster() throws Exception {
        Map<String, String> clusterNameToId = new ConcurrentHashMap<>();
        clusterNameToId.put("dstCluster", "1");
        clusterNameToId.put("srcCluster", "2");
        Deencapsulation.setField(Env.getCurrentSystemInfo(), "clusterNameToId", clusterNameToId);

        CacheHotspotManager newCacheHotspotManager = new CacheHotspotManager(Env.getCurrentSystemInfo());
        Deencapsulation.setField(Env.getCurrentEnv(), "cacheHotspotMgr", newCacheHotspotManager);
        newCacheHotspotManager.runAfterCatalogReady();

        // normal case:
        // step1: create
        String normalWarmUpStmtStr = "warm up cluster dstCluster with cluster srcCluster";
        WarmUpClusterStmt normalWarmUpStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(normalWarmUpStmtStr);
        long jobId = Env.getCurrentEnv().getCacheHotspotMgr().createJob(normalWarmUpStmt);
        Map<Long, CloudWarmUpJob> cloudWarmUpJobs = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs();
        Assert.assertEquals(1, cloudWarmUpJobs.size());
        CloudWarmUpJob job = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs().get(jobId);
        Assert.assertNotNull(job);
        Assert.assertEquals(jobId, job.getJobId());
        Assert.assertEquals(JobType.CLUSTER, job.getJobType());

        // step2: finish
        Thread.sleep(2000);
        Assert.assertEquals(JobState.FINISHED, job.getJobState());

        // step3: expired
        job.setFinishedTimeMs(0);
        Thread.sleep(2000);
        Assert.assertNull(Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs().get(jobId));

        // abnormal case1: warm up with the same name of dstCluster and srcCluster
        WarmUpClusterStmt failedStmt = null;
        try {
            failedStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(
                    "warm up cluster dstCluster with cluster dstCluster");
        } catch (Exception e) {
            System.out.println(e);
        }
        Assert.assertNull(failedStmt);

        // abnormal case2: warm up with a non-existent dstCluster
        try {
            failedStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(
                    "warm up cluster noDstCluster with cluster srcCluster");
        } catch (Exception e) {
            System.out.println(e);
        }
        Assert.assertNull(failedStmt);

        // abnormal case3: warm up with a non-existent srcCluster
        try {
            failedStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(
                    "warm up cluster dstCluster with cluster noSrcCluster");
        } catch (Exception e) {
            System.out.println(e);
        }
        Assert.assertNull(failedStmt);

        // abnormal case4: warm up the same cluster twice
        // step1: create job
        normalWarmUpStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(normalWarmUpStmtStr);
        long jobId2 = Env.getCurrentEnv().getCacheHotspotMgr().createJob(normalWarmUpStmt);
        cloudWarmUpJobs = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs();
        Assert.assertEquals(1, cloudWarmUpJobs.size());

        // step2: create the same job again
        long jobId3 = -1;
        try {
            jobId3 = Env.getCurrentEnv().getCacheHotspotMgr().createJob(normalWarmUpStmt);
        } catch (Exception e) {
            System.out.println(e);
        }
        Assert.assertEquals(-1, jobId3);
        cloudWarmUpJobs = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs();
        Assert.assertEquals(1, cloudWarmUpJobs.size());

        // step3: the first job will finish
        job = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs().get(jobId2);
        Assert.assertNotNull(job);
        Assert.assertEquals(jobId2, job.getJobId());
        Assert.assertEquals(JobType.CLUSTER, job.getJobType());
        Thread.sleep(2000);
        Assert.assertEquals(JobState.FINISHED, job.getJobState());

        // case: cancel job
        normalWarmUpStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(normalWarmUpStmtStr);
        long jobId4 = Env.getCurrentEnv().getCacheHotspotMgr().createJob(normalWarmUpStmt);
        cloudWarmUpJobs = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs();
        Assert.assertEquals(2, cloudWarmUpJobs.size());
        String cancelStmtStr = "cancel warm up job where id = " + jobId4;
        CancelCloudWarmUpStmt cancelCloudWarmUpStmt = (CancelCloudWarmUpStmt) parseAndAnalyzeStmt(cancelStmtStr);
        Env.getCurrentEnv().getCacheHotspotMgr().cancel(cancelCloudWarmUpStmt);
        job = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs().get(jobId4);
        Assert.assertEquals(JobState.CANCELLED, job.getJobState());

        job.setFinishedTimeMs(0);
        Thread.sleep(2000);
        cloudWarmUpJobs = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs();
        Assert.assertEquals(1, cloudWarmUpJobs.size());
        Assert.assertNull(cloudWarmUpJobs.get(jobId4));

    }

    @Test
    public void testWarmUpTable() throws Exception {
        Map<String, String> clusterNameToId = new ConcurrentHashMap<>();
        clusterNameToId.put("dstCluster", "1");
        Deencapsulation.setField(Env.getCurrentSystemInfo(), "clusterNameToId", clusterNameToId);

        CacheHotspotManager newCacheHotspotManager = new CacheHotspotManager(Env.getCurrentSystemInfo());
        Deencapsulation.setField(Env.getCurrentEnv(), "cacheHotspotMgr", newCacheHotspotManager);
        newCacheHotspotManager.runAfterCatalogReady();

        createDatabase("test");
        String createTestTableStmtStr = "CREATE TABLE test.warm_up (k1 int(11) NOT NULL, k2 int(11) NOT NULL) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1')";
        createTable(createTestTableStmtStr);

        // normal case:
        // step1: create
        String normalWarmUpStmtStr = "warm up cluster dstCluster with table test.warm_up";
        WarmUpClusterStmt normalWarmUpStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(normalWarmUpStmtStr);
        long jobId = Env.getCurrentEnv().getCacheHotspotMgr().createJob(normalWarmUpStmt);
        Map<Long, CloudWarmUpJob> cloudWarmUpJobs = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs();
        Assert.assertEquals(1, cloudWarmUpJobs.size());
        CloudWarmUpJob job = Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs().get(jobId);
        Assert.assertNotNull(job);
        Assert.assertEquals(jobId, job.getJobId());
        Assert.assertEquals(JobType.TABLE, job.getJobType());

        // step2: finish
        Thread.sleep(2000);
        Assert.assertEquals(JobState.FINISHED, job.getJobState());
        job.setFinishedTimeMs(0);
        Thread.sleep(2000);
        Assert.assertNull(Env.getCurrentEnv().getCacheHotspotMgr().getCloudWarmUpJobs().get(jobId));

        // abnormal case: warm up with a non-existent table
        WarmUpClusterStmt failedStmt = null;
        try {
            failedStmt = (WarmUpClusterStmt) parseAndAnalyzeStmt(
                    "warm up cluster dstCluster with table test.no_warm_up");
        } catch (Exception e) {
            System.out.println(e);
        }
        Assert.assertNull(failedStmt);
    }

    @Test
    public void testReplayWarmUpJob() throws Exception {
        CacheHotspotManager cacheHotspotManager = new CacheHotspotManager(Env.getCurrentSystemInfo());

        // case: replay job that was finished
        CloudWarmUpJob job1 = new CloudWarmUpJob(111, "test1", null, JobType.CLUSTER);
        cacheHotspotManager.replayCloudWarmUpJob(job1);
        job1.setJobState(JobState.RUNNING);
        cacheHotspotManager.replayCloudWarmUpJob(job1);
        job1.setJobState(JobState.FINISHED);
        cacheHotspotManager.replayCloudWarmUpJob(job1);
        Map<Long, CloudWarmUpJob> jobs = cacheHotspotManager.getCloudWarmUpJobs();
        Assert.assertEquals(1, jobs.size());
        Assert.assertNotNull(jobs.get(job1.getJobId()));
        Assert.assertEquals(JobState.FINISHED, jobs.get(job1.getJobId()).getJobState());
        Assert.assertEquals(0, cacheHotspotManager.getRunnableClusterSet().size());

        // case: replay job that was running
        CloudWarmUpJob job2 = new CloudWarmUpJob(222, "test2", null, JobType.CLUSTER);
        job2.setLastBatchId(0);
        cacheHotspotManager.replayCloudWarmUpJob(job2);
        job2.setJobState(JobState.RUNNING);
        job2.setLastBatchId(1);
        cacheHotspotManager.replayCloudWarmUpJob(job2);
        jobs = cacheHotspotManager.getCloudWarmUpJobs();
        Assert.assertEquals(2, jobs.size());
        Assert.assertNotNull(jobs.get(job2.getJobId()));
        Assert.assertEquals(JobState.RUNNING, jobs.get(job2.getJobId()).getJobState());
        Assert.assertEquals(1L, jobs.get(job2.getJobId()).getLastBatchId());
        Assert.assertEquals(1, cacheHotspotManager.getRunnableClusterSet().size());
        Assert.assertTrue(cacheHotspotManager.getRunnableClusterSet().contains("test2"));

        // case: replay job that was cancelled
        CloudWarmUpJob job3 = new CloudWarmUpJob(333, "test3", null, JobType.CLUSTER);
        cacheHotspotManager.replayCloudWarmUpJob(job3);
        job3.setJobState(JobState.RUNNING);
        cacheHotspotManager.replayCloudWarmUpJob(job3);
        job3.setJobState(JobState.CANCELLED);
        cacheHotspotManager.replayCloudWarmUpJob(job3);
        jobs = cacheHotspotManager.getCloudWarmUpJobs();
        Assert.assertEquals(3, jobs.size());
        Assert.assertNotNull(jobs.get(job3.getJobId()));
        Assert.assertEquals(JobState.CANCELLED, jobs.get(job3.getJobId()).getJobState());
        Assert.assertEquals(1, cacheHotspotManager.getRunnableClusterSet().size());

        // case: replay job that was deleted
        CloudWarmUpJob job4 = new CloudWarmUpJob(444, "test4", null, JobType.CLUSTER);
        cacheHotspotManager.replayCloudWarmUpJob(job4);
        job4.setJobState(JobState.RUNNING);
        cacheHotspotManager.replayCloudWarmUpJob(job4);
        job4.setJobState(JobState.FINISHED);
        cacheHotspotManager.replayCloudWarmUpJob(job4);
        job4.setJobState(JobState.DELETED);
        cacheHotspotManager.replayCloudWarmUpJob(job4);
        jobs = cacheHotspotManager.getCloudWarmUpJobs();
        Assert.assertEquals(3, jobs.size());
        Assert.assertNull(jobs.get(job4.getJobId()));
        Assert.assertEquals(1, cacheHotspotManager.getRunnableClusterSet().size());
    }
}