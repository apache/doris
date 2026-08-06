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

package org.apache.doris.alter;

import org.apache.doris.common.Config;
import org.apache.doris.task.AgentTask;
import org.apache.doris.thrift.TStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AlterJobV2#getRetryTimes(AgentTask)}.
 * Verifies the retry whitelist covers the expected error codes.
 */
public class AlterJobV2RetryTest {

    private SchemaChangeJobV2 job;

    @Before
    public void setUp() {
        Config.enable_schema_change_retry = true;
        Config.schema_change_max_retry_time = 3;
        // Minimal SchemaChangeJobV2 instance – only getRetryTimes() is exercised.
        job = new SchemaChangeJobV2("", 1L, 1L, 1L, "test_table", 600000L);
    }

    private AgentTask makeTask(TStatusCode errorCode) {
        AgentTask task = Mockito.mock(AgentTask.class);
        Mockito.when(task.getErrorCode()).thenReturn(errorCode);
        return task;
    }

    @Test
    public void testScCompactionConflictIsRetryable() {
        AgentTask task = makeTask(TStatusCode.SC_COMPACTION_CONFLICT);
        Assert.assertEquals(Config.schema_change_max_retry_time, job.getRetryTimes(task));
    }

    @Test
    public void testDeleteBitmapLockErrorIsRetryable() {
        AgentTask task = makeTask(TStatusCode.DELETE_BITMAP_LOCK_ERROR);
        Assert.assertEquals(Config.schema_change_max_retry_time, job.getRetryTimes(task));
    }

    @Test
    public void testNetworkErrorIsRetryable() {
        AgentTask task = makeTask(TStatusCode.NETWORK_ERROR);
        Assert.assertEquals(Config.schema_change_max_retry_time, job.getRetryTimes(task));
    }

    @Test
    public void testInternalErrorIsNotRetryable() {
        AgentTask task = makeTask(TStatusCode.INTERNAL_ERROR);
        Assert.assertEquals(0, job.getRetryTimes(task));
    }

    @Test
    public void testAnalysisErrorIsNotRetryable() {
        AgentTask task = makeTask(TStatusCode.ANALYSIS_ERROR);
        Assert.assertEquals(0, job.getRetryTimes(task));
    }

    @Test
    public void testNullErrorCodeIsNotRetryable() {
        AgentTask task = Mockito.mock(AgentTask.class);
        Mockito.when(task.getErrorCode()).thenReturn(null);
        Assert.assertEquals(0, job.getRetryTimes(task));
    }

    @Test
    public void testRetryDisabledReturnsZero() {
        Config.enable_schema_change_retry = false;
        try {
            AgentTask task = makeTask(TStatusCode.SC_COMPACTION_CONFLICT);
            Assert.assertEquals(0, job.getRetryTimes(task));

            task = makeTask(TStatusCode.DELETE_BITMAP_LOCK_ERROR);
            Assert.assertEquals(0, job.getRetryTimes(task));
        } finally {
            Config.enable_schema_change_retry = true;
        }
    }
}
