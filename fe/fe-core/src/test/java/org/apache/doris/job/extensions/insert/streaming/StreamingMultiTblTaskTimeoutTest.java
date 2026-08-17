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

import org.apache.doris.common.Config;
import org.apache.doris.job.cdc.StreamingTaskStatus;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class StreamingMultiTblTaskTimeoutTest {

    @Before
    public void setup() {
        Config.streaming_task_timeout_multiplier = 10;
        Config.streaming_task_min_timeout_sec = 300;
    }

    private StreamingMultiTblTask newTask(long lastProgressMsAgo, long intervalSec) {
        StreamingJobProperties props = Mockito.mock(StreamingJobProperties.class);
        Mockito.when(props.getMaxIntervalSecond()).thenReturn(intervalSec);
        StreamingMultiTblTask t = new StreamingMultiTblTask(
                1L, 1L, null, null, null, "db", null, props, null, null);
        long now = System.currentTimeMillis();
        t.startTimeMs = now - lastProgressMsAgo;
        t.lastProgressMs = t.startTimeMs;
        t.lastScannedRows = 1000;
        return t;
    }

    private StreamingTaskStatus status(long scanned) {
        StreamingTaskStatus s = new StreamingTaskStatus();
        s.setScannedRows(scanned);
        return s;
    }

    @Test
    public void readAdvancingRenewsDeadline() {
        StreamingMultiTblTask t = newTask(10 * 3600_000L, 60L);
        Assert.assertFalse(t.isTimeout(status(2000)));
    }

    @Test
    public void noProgressWithinBudgetNotTimeout() {
        StreamingMultiTblTask t = newTask(5 * 60_000L, 60L);
        Assert.assertFalse(t.isTimeout(status(1000)));
    }

    @Test
    public void noProgressOverBudgetTimeout() {
        StreamingMultiTblTask t = newTask(11 * 60_000L, 60L);
        Assert.assertTrue(t.isTimeout(status(1000)));
    }

    @Test
    public void smallIntervalFlooredByMinTimeout() {
        StreamingMultiTblTask t = newTask(4 * 60_000L, 1L);
        Assert.assertFalse(t.isTimeout(status(1000)));
    }

    @Test
    public void nullProgressBehavesLikeOldTimeout() {
        StreamingMultiTblTask t = newTask(11 * 60_000L, 60L);
        Assert.assertTrue(t.isTimeout(null));
    }

    @Test
    public void localTimeoutGatesProgressPull() {
        Assert.assertFalse(newTask(5 * 60_000L, 60L).isLocalTimeout());
        Assert.assertTrue(newTask(11 * 60_000L, 60L).isLocalTimeout());
    }
}
