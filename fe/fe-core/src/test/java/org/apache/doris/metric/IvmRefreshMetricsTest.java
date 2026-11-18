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

package org.apache.doris.metric;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.extensions.mtmv.MTMVTask;
import org.apache.doris.job.extensions.mtmv.MTMVTask.MTMVTaskTriggerMode;
import org.apache.doris.job.extensions.mtmv.MTMVTaskContext;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmRefreshFailureStage;
import org.apache.doris.mtmv.ivm.IvmRefreshMethod;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** The counters of the refresh tasks of IVM materialized views, and how a task is counted in them as it ends. */
public class IvmRefreshMetricsTest {

    @BeforeAll
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    /**
     * Every series is registered up front, so each one is there from 0: the tasks by every result and method, the
     * run time by method, and one series per reason a task can fall back or fail with, labeled in lower case.
     */
    @Test
    public void testEverySeriesIsThereFromZero() {
        Assertions.assertEquals(4 * IvmRefreshMethod.values().length,
                MetricRepo.getMetricsByName("ivm_refresh_tasks_total").size());
        Assertions.assertEquals(IvmRefreshMethod.values().length,
                MetricRepo.getMetricsByName("ivm_refresh_duration_seconds_total").size());
        Assertions.assertEquals(IvmFailureReason.values().length + 1,
                MetricRepo.getMetricsByName("ivm_fallbacks_total").size());
        Assertions.assertEquals(IvmFailureReason.values().length + IvmRefreshFailureStage.values().length,
                MetricRepo.getMetricsByName("ivm_refresh_failures_total").size());

        // Series no other case here counts in, so they are still 0 whatever ran before.
        String metrics = getPrometheusMetrics();
        for (String type : new String[] {"ivm_refresh_tasks_total", "ivm_refresh_duration_seconds_total",
                "ivm_fallbacks_total", "ivm_refresh_failures_total", "ivm_schedule_rejected_total"}) {
            Assertions.assertTrue(metrics.contains("# TYPE doris_fe_" + type + " counter\n"), type);
        }
        Assertions.assertTrue(metrics.contains(
                "doris_fe_ivm_refresh_tasks_total{result=\"canceled\", method=\"mixed\"} 0\n"), metrics);
        Assertions.assertTrue(metrics.contains(
                "doris_fe_ivm_refresh_duration_seconds_total{method=\"partition_rebuild\"} 0.0\n"), metrics);
        Assertions.assertTrue(metrics.contains(
                "doris_fe_ivm_fallbacks_total{reason=\"incomplete_refresh_snapshot\"} 0\n"), metrics);
        Assertions.assertTrue(metrics.contains(
                "doris_fe_ivm_refresh_failures_total{reason=\"execution_failed\"} 0\n"), metrics);
    }

    /**
     * A task is counted by its result and method as it ends; one that ran to completion adds its run time, one that
     * fell back counts its reason, and one that failed counts why. A SUCCESS that committed nothing is no_change, and
     * a canceled task has no finish time to add and finished no fallback.
     */
    @Test
    public void testATaskIsCountedAsItEnds() {
        MTMVTask fellBack = endedTask(TaskStatus.SUCCESS, IvmRefreshMethod.COMPLETE, 1000L, 3500L);
        Deencapsulation.setField(fellBack, "ivmFallback", true);
        Deencapsulation.setField(fellBack, "ivmFallbackReason", IvmFailureReason.MIN_MAX_BOUNDARY_HIT.name());
        assertCounted(fellBack, MetricRepo.COUNTER_IVM_REFRESH_TASKS.get("success").get(IvmRefreshMethod.COMPLETE));
        Assertions.assertEquals(2.5,
                MetricRepo.COUNTER_IVM_REFRESH_DURATION_SECONDS.get(IvmRefreshMethod.COMPLETE).getValue(), 1e-9);
        Assertions.assertEquals(1L,
                MetricRepo.COUNTER_IVM_FALLBACKS.get(IvmFailureReason.MIN_MAX_BOUNDARY_HIT.name()).getValue());

        assertCounted(endedTask(TaskStatus.SUCCESS, IvmRefreshMethod.NONE, 1000L, 1200L),
                MetricRepo.COUNTER_IVM_REFRESH_TASKS.get("no_change").get(IvmRefreshMethod.NONE));
        Assertions.assertEquals(0L, MetricRepo.COUNTER_IVM_REFRESH_TASKS.get("success").get(IvmRefreshMethod.NONE)
                .getValue());

        MTMVTask failed = endedTask(TaskStatus.FAILED, IvmRefreshMethod.INCREMENTAL, 1000L, 2000L);
        Deencapsulation.setField(failed, "ivmFailureReason", IvmRefreshFailureStage.QUERY_UNUSABLE.name());
        assertCounted(failed, MetricRepo.COUNTER_IVM_REFRESH_TASKS.get("failed").get(IvmRefreshMethod.INCREMENTAL));
        Assertions.assertEquals(1L,
                MetricRepo.COUNTER_IVM_REFRESH_FAILURES.get(IvmRefreshFailureStage.QUERY_UNUSABLE.name()).getValue());

        MTMVTask canceled = endedTask(TaskStatus.CANCELED, IvmRefreshMethod.PARTITION_REBUILD, 1000L, 2000L);
        Deencapsulation.setField(canceled, "ivmFallback", true);
        Deencapsulation.setField(canceled, "ivmFallbackReason", IvmFailureReason.CHANGELOG_BROKEN.name());
        assertCounted(canceled,
                MetricRepo.COUNTER_IVM_REFRESH_TASKS.get("canceled").get(IvmRefreshMethod.PARTITION_REBUILD));
        Assertions.assertEquals(0.0,
                MetricRepo.COUNTER_IVM_REFRESH_DURATION_SECONDS.get(IvmRefreshMethod.PARTITION_REBUILD).getValue(),
                1e-9);
        Assertions.assertEquals(0L,
                MetricRepo.COUNTER_IVM_FALLBACKS.get(IvmFailureReason.CHANGELOG_BROKEN.name()).getValue());
    }

    /**
     * A task the full task queue turns away never runs, so it is counted where it is turned away -- if its MV is an
     * IVM MV, and still there.
     */
    @Test
    public void testATaskOfAnIvmMvTheQueueTurnsAwayIsCounted() {
        MTMV ivmMv = Mockito.mock(MTMV.class);
        Mockito.when(ivmMv.isIvm()).thenReturn(true);
        long rejected = MetricRepo.COUNTER_IVM_SCHEDULE_REJECTED.getValue();
        try (MockedStatic<MTMVUtil> mtmvUtil = Mockito.mockStatic(MTMVUtil.class)) {
            mtmvUtil.when(() -> MTMVUtil.getMTMV(1L, 10L)).thenReturn(ivmMv);
            mtmvUtil.when(() -> MTMVUtil.getMTMV(1L, 20L)).thenReturn(Mockito.mock(MTMV.class));
            mtmvUtil.when(() -> MTMVUtil.getMTMV(1L, 30L)).thenThrow(new MetaNotFoundException("dropped"));

            new MTMVTask(1L, 10L, new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM)).onRejected();
            Assertions.assertEquals(rejected + 1, MetricRepo.COUNTER_IVM_SCHEDULE_REJECTED.getValue());
            new MTMVTask(1L, 20L, new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM)).onRejected();
            new MTMVTask(1L, 30L, new MTMVTaskContext(MTMVTaskTriggerMode.SYSTEM)).onRejected();
            Assertions.assertEquals(rejected + 1, MetricRepo.COUNTER_IVM_SCHEDULE_REJECTED.getValue());
        }
    }

    private static void assertCounted(MTMVTask task, LongCounterMetric counter) {
        long before = counter.getValue();
        Deencapsulation.invoke(task, "recordIvmRefreshMetrics");
        Assertions.assertEquals(before + 1, counter.getValue());
    }

    private static MTMVTask endedTask(TaskStatus status, IvmRefreshMethod method, long startTimeMs,
            long finishTimeMs) {
        MTMVTask task = new MTMVTask();
        task.setStatus(status);
        task.setStartTimeMs(startTimeMs);
        task.setFinishTimeMs(finishTimeMs);
        Deencapsulation.setField(task, "ivmRefreshMethod", method);
        return task;
    }

    private static String getPrometheusMetrics() {
        MetricVisitor visitor = new PrometheusMetricVisitor();
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        return visitor.finish();
    }
}
