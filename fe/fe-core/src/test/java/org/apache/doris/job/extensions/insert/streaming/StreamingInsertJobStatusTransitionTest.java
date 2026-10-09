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
import org.apache.doris.job.cdc.request.TaskFailureRequest;
import org.apache.doris.job.common.FailureReason;
import org.apache.doris.job.common.JobStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamingInsertJobStatusTransitionTest {

    private static StreamingInsertJob newJob(JobStatus status) {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Deencapsulation.setField(job, "lock", new ReentrantReadWriteLock(true));
        Deencapsulation.setField(job, "jobId", 8001L);
        Deencapsulation.setField(job, "jobName", "test_job");
        Deencapsulation.setField(job, "jobStatus", status);
        return job;
    }

    @Test
    public void testPendingIsPromotedToRunning() throws Exception {
        StreamingInsertJob job = newJob(JobStatus.PENDING);

        Assertions.assertTrue(job.updateJobStatusIfCurrent(JobStatus.PENDING, JobStatus.RUNNING));
        Assertions.assertEquals(JobStatus.RUNNING, job.getJobStatus());
    }

    @Test
    public void testPausedSurvivesTheRunningWrite() throws Exception {
        // StreamingTaskScheduler failed the freshly registered task and paused the job before the
        // dispatching thread reached its RUNNING write. Overwriting PAUSED here would strand the
        // job: it holds a canceled task, and only the PAUSED branch can auto resume it.
        StreamingInsertJob job = newJob(JobStatus.PAUSED);

        Assertions.assertFalse(job.updateJobStatusIfCurrent(JobStatus.PENDING, JobStatus.RUNNING));
        Assertions.assertEquals(JobStatus.PAUSED, job.getJobStatus());
    }

    @Test
    public void testStoppedSurvivesTheRunningWrite() throws Exception {
        // A concurrent DROP/STOP JOB leaves a terminal status that must not be revived either.
        StreamingInsertJob job = newJob(JobStatus.STOPPED);

        Assertions.assertFalse(job.updateJobStatusIfCurrent(JobStatus.PENDING, JobStatus.RUNNING));
        Assertions.assertEquals(JobStatus.STOPPED, job.getJobStatus());
    }

    @Test
    public void testManualResumeToleratesSchemaChangesUntilSuccess() throws Exception {
        StreamingInsertJob job = newJob(JobStatus.PAUSED);
        job.setFailureReason(new FailureReason(TaskFailureRequest.SCHEMA_CHANGE_UNSUPPORTED + " INT -> BIGINT"));

        Assertions.assertFalse(job.isTolerateSchemaChange());
        job.updateJobStatus(JobStatus.PENDING);
        job.onManualStatusAltered(JobStatus.PENDING, null);
        Assertions.assertTrue(job.isTolerateSchemaChange());
        Assertions.assertNull(job.getFailureReason());
        Assertions.assertEquals(JobStatus.PENDING, job.getJobStatus());

        // A failed recovery attempt keeps the acceptance until a task succeeds.
        job.updateJobStatus(JobStatus.PAUSED);
        job.setFailureReason(new FailureReason("Connection reset"));
        job.updateJobStatus(JobStatus.PENDING);
        job.onManualStatusAltered(JobStatus.PENDING, null);
        Assertions.assertTrue(job.isTolerateSchemaChange());
    }

    @Test
    public void testOtherFailuresAndAutomaticResumeDoNotAcceptSchemaChanges() throws Exception {
        StreamingInsertJob job = newJob(JobStatus.PAUSED);
        job.setFailureReason(new FailureReason("Replication slot invalidated"));
        job.updateJobStatus(JobStatus.PENDING);
        job.onManualStatusAltered(JobStatus.PENDING, null);
        Assertions.assertFalse(job.isTolerateSchemaChange());

        job.updateJobStatus(JobStatus.PAUSED);
        job.setFailureReason(new FailureReason("Failed to execute schema change: injected DDL failure"));
        job.updateJobStatus(JobStatus.PENDING);
        job.onManualStatusAltered(JobStatus.PENDING, null);
        Assertions.assertFalse(job.isTolerateSchemaChange());

        job.updateJobStatus(JobStatus.PAUSED);
        job.setFailureReason(new FailureReason(TaskFailureRequest.SCHEMA_CHANGE_UNSUPPORTED + " RENAME"));
        new StreamingJobSchedulerTask(job).run();
        Assertions.assertEquals(JobStatus.PAUSED, job.getJobStatus());
        Assertions.assertFalse(job.isTolerateSchemaChange());
    }

    @Test
    public void testTvfResumeDoesNotAcceptSchemaChanges() throws Exception {
        StreamingInsertJob job = newJob(JobStatus.PAUSED);
        Deencapsulation.setField(job, "tvfType", "cdc_stream");
        job.setFailureReason(new FailureReason(TaskFailureRequest.SCHEMA_CHANGE_UNSUPPORTED + " RENAME"));

        job.updateJobStatus(JobStatus.PENDING);

        Assertions.assertEquals(JobStatus.PENDING, job.getJobStatus());
        Assertions.assertFalse(job.isTolerateSchemaChange());
    }
}
