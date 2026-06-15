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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.cdc.request.CommitOffsetRequest;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamingInsertJobCheckDataQualityTest {

    private static final String MAX_FILTER_RATIO_KEY = "load.max_filter_ratio";

    private static StreamingInsertJob newJob(double maxFilterRatio, long sampleWindowMs) {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Deencapsulation.setField(job, "lock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(job, "sampleWindowMs", sampleWindowMs);
        Deencapsulation.setField(job, "sampleWindowScannedRows", 0L);
        Deencapsulation.setField(job, "sampleWindowFilteredRows", 0L);
        Deencapsulation.setField(job, "sampleStartTime", System.currentTimeMillis());

        Map<String, String> targetProps = new HashMap<>();
        targetProps.put(MAX_FILTER_RATIO_KEY, String.valueOf(maxFilterRatio));
        Deencapsulation.setField(job, "targetProperties", targetProps);

        Deencapsulation.setField(job, "jobId", 1L);
        Deencapsulation.setField(job, "jobName", "test_job");
        Deencapsulation.setField(job, "jobStatus", JobStatus.RUNNING);
        return job;
    }

    private static void invokeCheckDataQuality(StreamingInsertJob job, long scannedRows, long filteredRows)
            throws JobException {
        CommitOffsetRequest req = new CommitOffsetRequest();
        req.setScannedRows(scannedRows);
        req.setFilteredRows(filteredRows);
        try {
            Deencapsulation.invoke(job, "checkDataQuality", req);
        } catch (RuntimeException re) {
            // Deencapsulation.invoke wraps the target exception, unwrap to surface JobException to the test.
            Throwable cause = re.getCause();
            if (cause instanceof JobException) {
                throw (JobException) cause;
            }
            throw re;
        }
    }

    @Test
    public void testNormalBatchWithinWindow() throws Exception {
        StreamingInsertJob job = newJob(0.10, 60_000L);
        invokeCheckDataQuality(job, 1000, 50);
        Assert.assertEquals(1000L, (long) Deencapsulation.getField(job, "sampleWindowScannedRows"));
        Assert.assertEquals(50L, (long) Deencapsulation.getField(job, "sampleWindowFilteredRows"));
        Assert.assertEquals(JobStatus.RUNNING, job.getJobStatus());
    }

    @Test
    public void testCombinedRatioViolationInsideWindow() throws Exception {
        StreamingInsertJob job = newJob(0.10, 60_000L);
        Deencapsulation.setField(job, "sampleWindowScannedRows", 100L);
        Deencapsulation.setField(job, "sampleWindowFilteredRows", 5L);
        Deencapsulation.setField(job, "sampleStartTime", System.currentTimeMillis() - 1_000L);

        JobException thrown = null;
        try {
            invokeCheckDataQuality(job, 100, 30);
        } catch (JobException e) {
            thrown = e;
        }
        Assert.assertNotNull("expected pause when combined ratio exceeds threshold", thrown);
        Assert.assertEquals(JobStatus.PAUSED, job.getJobStatus());
    }

    // Bug reproducer: expired window with large clean data used to dilute a bad batch.
    // Fixed code rolls the window first so the bad batch is judged on its own.
    @Test
    public void testExpiredWindowDoesNotMaskBadBatch() throws Exception {
        StreamingInsertJob job = newJob(0.10, 60_000L);
        Deencapsulation.setField(job, "sampleWindowScannedRows", 10_000L);
        Deencapsulation.setField(job, "sampleWindowFilteredRows", 500L);
        Deencapsulation.setField(job, "sampleStartTime", System.currentTimeMillis() - 120_000L);

        JobException thrown = null;
        try {
            invokeCheckDataQuality(job, 100, 30);
        } catch (JobException e) {
            thrown = e;
        }
        Assert.assertNotNull("expected pause — bad batch (ratio=0.30) should not be diluted by expired window",
                thrown);
        Assert.assertEquals(JobStatus.PAUSED, job.getJobStatus());
    }

    @Test
    public void testExpiredWindowRollsBeforeAccumulation() throws Exception {
        StreamingInsertJob job = newJob(0.10, 60_000L);
        Deencapsulation.setField(job, "sampleWindowScannedRows", 1000L);
        Deencapsulation.setField(job, "sampleWindowFilteredRows", 50L);
        long oldStartTime = System.currentTimeMillis() - 120_000L;
        Deencapsulation.setField(job, "sampleStartTime", oldStartTime);

        invokeCheckDataQuality(job, 100, 5);

        Assert.assertEquals(100L, (long) Deencapsulation.getField(job, "sampleWindowScannedRows"));
        Assert.assertEquals(5L, (long) Deencapsulation.getField(job, "sampleWindowFilteredRows"));
        Assert.assertTrue((long) Deencapsulation.getField(job, "sampleStartTime") > oldStartTime);
    }

    @Test
    public void testZeroScanBatchStillRollsExpiredWindow() throws Exception {
        StreamingInsertJob job = newJob(0.10, 60_000L);
        Deencapsulation.setField(job, "sampleWindowScannedRows", 1000L);
        Deencapsulation.setField(job, "sampleWindowFilteredRows", 50L);
        long oldStartTime = System.currentTimeMillis() - 120_000L;
        Deencapsulation.setField(job, "sampleStartTime", oldStartTime);

        invokeCheckDataQuality(job, 0, 0);

        Assert.assertEquals(0L, (long) Deencapsulation.getField(job, "sampleWindowScannedRows"));
        Assert.assertEquals(0L, (long) Deencapsulation.getField(job, "sampleWindowFilteredRows"));
        Assert.assertTrue((long) Deencapsulation.getField(job, "sampleStartTime") > oldStartTime);
    }

    private static StreamingInsertJob jobWithProperties(String maxIntervalSec) {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Map<String, String> props = new HashMap<>();
        props.put(StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY, maxIntervalSec);
        Deencapsulation.setField(job, "properties", props);
        Deencapsulation.setField(job, "jobProperties", new StreamingJobProperties(props));

        JobExecutionConfiguration cfg = new JobExecutionConfiguration();
        cfg.setTimerDefinition(new TimerDefinition());
        Deencapsulation.setField(job, "jobConfig", cfg);
        return job;
    }

    @Test
    public void testRecomputeDerivedFieldsRebuildsSampleWindowMs() throws Exception {
        StreamingInsertJob job = jobWithProperties("10");
        Deencapsulation.setField(job, "sampleWindowMs", 0L);
        Deencapsulation.setField(job, "sampleStartTime", 0L);

        Deencapsulation.invoke(job, "recomputeDerivedFields");

        Assert.assertEquals(100_000L, (long) Deencapsulation.getField(job, "sampleWindowMs"));
        Assert.assertTrue((long) Deencapsulation.getField(job, "sampleStartTime") > 0L);
    }

    @Test
    public void testModifyPropertiesInternalRefreshesDerivedFields() throws Exception {
        StreamingInsertJob job = jobWithProperties("10");
        JobExecutionConfiguration cfg = (JobExecutionConfiguration) Deencapsulation.getField(job, "jobConfig");
        cfg.getTimerDefinition().setInterval(10L);
        Deencapsulation.setField(job, "sampleWindowMs", 100_000L);

        Map<String, String> alter = new HashMap<>();
        alter.put(StreamingJobProperties.MAX_INTERVAL_SECOND_PROPERTY, "30");
        Deencapsulation.invoke(job, "modifyPropertiesInternal", alter);

        Assert.assertEquals(300_000L, (long) Deencapsulation.getField(job, "sampleWindowMs"));
        Assert.assertEquals(30L, cfg.getTimerDefinition().getInterval().longValue());
    }

    @Test
    public void testRecomputeDerivedFieldsResetsSampleCounters() throws Exception {
        StreamingInsertJob job = jobWithProperties("10");
        Deencapsulation.setField(job, "sampleWindowScannedRows", 1000L);
        Deencapsulation.setField(job, "sampleWindowFilteredRows", 100L);

        Deencapsulation.invoke(job, "recomputeDerivedFields");

        Assert.assertEquals(0L, (long) Deencapsulation.getField(job, "sampleWindowScannedRows"));
        Assert.assertEquals(0L, (long) Deencapsulation.getField(job, "sampleWindowFilteredRows"));
    }

    @Test
    public void testMissingMaxFilterRatioIsNoop() throws Exception {
        StreamingInsertJob job = newJob(0.10, 60_000L);
        Deencapsulation.setField(job, "targetProperties", new HashMap<String, String>());

        invokeCheckDataQuality(job, 100, 50);

        Assert.assertEquals(0L, (long) Deencapsulation.getField(job, "sampleWindowScannedRows"));
        Assert.assertEquals(0L, (long) Deencapsulation.getField(job, "sampleWindowFilteredRows"));
        Assert.assertEquals(JobStatus.RUNNING, job.getJobStatus());
    }
}
