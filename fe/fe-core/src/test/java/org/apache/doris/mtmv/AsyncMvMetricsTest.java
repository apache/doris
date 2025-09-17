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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskRefreshMode;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class AsyncMvMetricsTest {
    @Test
    public void testRecordRefreshMetricsCommitNotRefresh() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.NOT_REFRESH, new MTMVTaskContext(
                MTMVTaskTriggerMode.COMMIT));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesSkipped().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getAutoRefreshesOnCommit().getValue());
    }

    @Test
    public void testRecordRefreshMetricsCommitPCT() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.PARTIAL, new MTMVTaskContext(
                MTMVTaskTriggerMode.COMMIT));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesPct().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getAutoRefreshesOnCommit().getValue());
    }

    @Test
    public void testRecordRefreshMetricsCommitComplete() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.COMPLETE, new MTMVTaskContext(
                MTMVTaskTriggerMode.COMMIT));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesComplete().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getAutoRefreshesOnCommit().getValue());
    }

    @Test
    public void testRecordRefreshMetricsScheduleComplete() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.COMPLETE, new MTMVTaskContext(
                MTMVTaskTriggerMode.SYSTEM));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesComplete().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getAutoRefreshesOnSchedule().getValue());
    }

    @Test
    public void testRecordRefreshMetricsManualComplete() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.COMPLETE, new MTMVTaskContext(
                MTMVTaskTriggerMode.MANUAL, null, true));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesComplete().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getManualRefreshesOnComplete().getValue());
    }

    @Test
    public void testRecordRefreshMetricsManualPartitions() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.COMPLETE, new MTMVTaskContext(
                MTMVTaskTriggerMode.MANUAL, Lists.newArrayList("p1"), false));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesComplete().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getManualRefreshesOnPartitions().getValue());
    }

    @Test
    public void testRecordRefreshMetricsManualAuto() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordRefreshMetrics(mtmv, MTMVTaskRefreshMode.COMPLETE, new MTMVTaskContext(
                MTMVTaskTriggerMode.MANUAL, null, false));
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getRefreshesComplete().getValue());
        Assert.assertEquals(1, (long) mtmv.getAsyncMvMetrics().getManualRefreshesOnAuto().getValue());
    }

    @Test
    public void testRecordLastQueryTime() {
        MTMV mtmv = new MTMV();
        AsyncMvMetrics.recordLastQueryTime(Lists.newArrayList(mtmv));
        Assert.assertTrue(mtmv.getAsyncMvMetrics().getLastQueryTime() > 0);
    }
}
